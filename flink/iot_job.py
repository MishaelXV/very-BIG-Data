"""
IoT Streaming Aggregation Job — PyFlink 1.18 DataStream API

Pipeline:
  KafkaSource (iot_events, raw JSON strings)
    → WatermarkStrategy: for_bounded_out_of_orderness(5 s)
      + IotTimestampAssigner: parses event_time from raw JSON → epoch ms
    → map: parse JSON + enrich with device_type_name in ONE step
      output type: TUPLE[INT, DOUBLE, DOUBLE, STRING]
                   (device_type_id, temperature, humidity, device_type_name)
    → key_by: device_type_id  (tuple index 0)
    → TumblingEventTimeWindows(60 s)
    → ProcessWindowFunction: avg_temperature, median_humidity, events_count
    → KafkaSink (iot_aggregated, JSON strings)

Why the original pipeline broke
  The original design serialised enrich_event's output as a JSON string (Types.STRING)
  between two Python map operators. PyFlink 1.18 passes data between Python operators
  through the JVM serialisation layer. When dill cannot round-trip the closure that
  captures `device_types`, the first map silently emits malformed or missing output,
  so the second map receives a JSON dict without `device_type_id` → KeyError.

Fix: merge parse + enrich into a single map operator and use a typed Tuple instead of
     an intermediate JSON string. Only one json.loads per record (in _fn below) and
     one json.dumps at the very end (in AggregateByDeviceType.process).

Constraints honoured
  - No RichMapFunction       (not importable in PyFlink 1.18)
  - No SerializableTimestampAssigner  (does not exist — using TimestampAssigner)
  - No output_type in filter()        (not supported)
  - PostgreSQL loaded once before pipeline construction
"""

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

# Explicit Flink type for elements flowing from the map operator to the window.
# (device_type_id: INT, temperature: DOUBLE, humidity: DOUBLE, device_type_name: STRING)
# Using DOUBLE avoids 32-bit float precision loss for sensor values.
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
    _dt = dict(device_types)  # local copy; serialised with the closure by dill

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

    # Watermark strategy:
    #   for_bounded_out_of_orderness(5s) — tolerate 5-second late arrivals
    #   IotTimestampAssigner             — extracts epoch ms from raw JSON event_time
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_idleness(Duration.of_seconds(10))
        .with_timestamp_assigner(IotTimestampAssigner()))

    # Load reference table once — serialised into the map closure by dill.
    device_types = load_device_types(PG_DSN)

    (
        env

        # ① Read raw JSON strings from Kafka; stamp event-time watermarks at source.
        .from_source(
            build_kafka_source(),
            watermark_strategy,
            "Kafka Source: iot_events",
        )

        # ② Parse raw JSON + enrich with device_type_name in ONE operator.
        #    Output: (device_type_id, temperature, humidity, device_type_name)
        #    Explicit ENRICHED_TYPE prevents PyFlink type-inference ambiguity.
        .map(
            make_parse_and_enrich(device_types),
            output_type=ENRICHED_TYPE,
        )

        # ③ Partition stream by device_type_id (tuple index 0).
        .key_by(lambda t: t[0])

        # ④ Open 1-minute tumbling event-time window per key.
        #    Window closes when watermark ≥ window_end (+ 5 s bounded lateness).
        .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))

        # ⑤ Aggregate: avg_temperature, median_humidity, events_count.
        #    First json.dumps in this pipeline — only happens once per window bucket.
        .process(AggregateByDeviceType(), output_type=Types.STRING())

        # ⑥ Write JSON aggregates to Kafka.
        .sink_to(build_kafka_sink())
    )

    env.execute("IoT Aggregation — 1-minute Tumbling Window")


if __name__ == "__main__":
    main()
