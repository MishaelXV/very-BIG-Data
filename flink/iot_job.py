import json
import logging
import os
from datetime import datetime, timezone
from typing import Iterable

import psycopg2
from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows

# ── Logging ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("iot_job")

# ── Configuration ────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_INPUT_TOPIC       = os.getenv("KAFKA_INPUT_TOPIC",       "iot_events")
KAFKA_OUTPUT_TOPIC      = os.getenv("KAFKA_OUTPUT_TOPIC",      "iot_aggregated")

PG_DSN: dict = {
    "host":     os.getenv("POSTGRES_HOST",     "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "iot_db"),
    "user":     os.getenv("POSTGRES_USER",     "iot_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "iot_pass"),
}

WINDOW_SIZE_SECONDS = 60


ENRICHED_TYPE = Types.TUPLE([
    Types.INT(),
    Types.DOUBLE(),
    Types.DOUBLE(),
    Types.STRING(),
])


# ── Helpers ──────────────────────────────────────────────────────────────────────

def epoch_ms_to_iso(ms: int) -> str:
    """Convert Flink window boundary (epoch ms) to ISO-8601 UTC string."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def compute_median(values: list) -> float:
    """Median without external libraries. Returns 0.0 for an empty list."""
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 == 1 else (s[mid - 1] + s[mid]) / 2


def load_device_types(dsn: dict) -> dict:
    """Connect to PostgreSQL, read device_types, return {id → type_name}."""
    logger.info(
        "Loading device_types from PostgreSQL "
        f"{dsn['host']}:{dsn['port']}/{dsn['dbname']} ..."
    )
    conn = psycopg2.connect(**dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, type_name FROM device_types")
            mapping = {row[0]: row[1] for row in cur.fetchall()}
    finally:
        conn.close()
    logger.info(f"Loaded {len(mapping)} device types: {mapping}")
    return mapping


# ── Step 1: TimestampAssigner ────────────────────────────────────────────────────
# Runs at the source level — receives raw JSON strings directly from Kafka.

class IotTimestampAssigner(TimestampAssigner):
    """Extracts event_time from raw Kafka JSON and returns epoch milliseconds."""

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        ts_str = json.loads(value)["event_time"].replace("Z", "+00:00")
        return int(datetime.fromisoformat(ts_str).timestamp() * 1000)


# ── Step 2: Parse + Enrich (single map, single json.loads per record) ────────────

def make_parse_and_enrich(device_types: dict):
    """
    Factory that returns a map function closing over device_types.

    Combining parse and enrich into one operator eliminates the intermediate
    JSON string between two Python operators — the root cause of the KeyError.
    dill serialises _dt (a plain {int: str} dict) reliably as part of the closure.
    """
    _dt = dict(device_types)  

    def _fn(raw: str):
        d = json.loads(raw)
        dev_id = int(d["device_type_id"])
        return (
            dev_id,
            float(d["temperature"]),
            float(d["humidity"]),
            _dt.get(dev_id, "unknown"),
        )

    return _fn


# ── Step 3: Window aggregation ───────────────────────────────────────────────────

class AggregateByDeviceType(ProcessWindowFunction):
    """
    Consumes all tuples for one (device_type_id, window) bucket.
    Emits exactly one JSON string per bucket.

    Element layout (matches ENRICHED_TYPE):
      [0] device_type_id   INT
      [1] temperature      DOUBLE
      [2] humidity         DOUBLE
      [3] device_type_name STRING
    """

    def process(
        self,
        key,                               # device_type_id extracted by key_by
        ctx: ProcessWindowFunction.Context,
        elements: Iterable,
    ) -> Iterable[str]:
        temps: list[float] = []
        hums:  list[float] = []
        device_type_name = "unknown"

        for elem in elements:
            temps.append(float(elem[1]))
            hums.append(float(elem[2]))
            device_type_name = str(elem[3])  # same value for all elements in key

        n = len(temps)
        if n == 0:
            return

        avg_temperature = round(sum(temps) / n, 2)
        median_humidity = round(compute_median(hums), 2)
        w_start = epoch_ms_to_iso(ctx.window().start)
        w_end   = epoch_ms_to_iso(ctx.window().end)

        logger.info(
            f"[window] {w_start} → {w_end} | "
            f"device_type_id={key} ({device_type_name}) | "
            f"events={n} avg_temp={avg_temperature} median_hum={median_humidity}"
        )

        yield json.dumps({
            "window_start":     w_start,
            "window_end":       w_end,
            "device_type_id":   key,
            "device_type_name": device_type_name,
            "avg_temperature":  avg_temperature,
            "median_humidity":  median_humidity,
            "events_count":     n,
        })


# ── Kafka builders ───────────────────────────────────────────────────────────────

def build_kafka_source() -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(KAFKA_INPUT_TOPIC)
        .set_group_id("flink-iot-consumer")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def build_kafka_sink() -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(KAFKA_OUTPUT_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


# ── Main ─────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("IoT Streaming Aggregation Job — PyFlink 1.18 DataStream API")
    logger.info(f"  Kafka bootstrap : {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Input  topic    : {KAFKA_INPUT_TOPIC}")
    logger.info(f"  Output topic    : {KAFKA_OUTPUT_TOPIC}")
    logger.info(f"  PostgreSQL      : {PG_DSN['host']}:{PG_DSN['port']}/{PG_DSN['dbname']}")
    logger.info(f"  Window size     : {WINDOW_SIZE_SECONDS}s tumbling event-time")
    logger.info("=" * 60)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_idleness(Duration.of_seconds(10))
        .with_timestamp_assigner(IotTimestampAssigner()))

    device_types = load_device_types(PG_DSN)

    (
        env
        .from_source(
            build_kafka_source(),
            watermark_strategy,
            "Kafka Source: iot_events",
        )

        .map(
            make_parse_and_enrich(device_types),
            output_type=ENRICHED_TYPE,
        )

        .key_by(lambda t: t[0])
        .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
        
        .process(AggregateByDeviceType(), output_type=Types.STRING())
        .sink_to(build_kafka_sink())
    )

    env.execute("IoT Aggregation — 1-minute Tumbling Window")


if __name__ == "__main__":
    main()
