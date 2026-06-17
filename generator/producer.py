import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot_events")


def on_delivery(err, msg):
    if err:
        print(f"[ERROR] delivery failed: {err}")
    else:
        print(
            f"[OK] {msg.topic()} [{msg.partition()}] offset={msg.offset()} "
            f"key={msg.key().decode()}"
        )


def now_iso() -> str:
    now = datetime.now(timezone.utc)
    ms = now.microsecond // 1000
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


def generate_event() -> dict:
    return {
        "device_type_id": random.randint(1, 5),
        "event_time": now_iso(),
        "temperature": round(random.uniform(15.0, 35.0), 1),
        "humidity": round(random.uniform(30.0, 90.0), 1),
    }


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    print(
        f"[generator] starting → topic='{KAFKA_TOPIC}' "
        f"bootstrap='{KAFKA_BOOTSTRAP_SERVERS}'"
    )

    try:
        while True:
            event = generate_event()
            producer.produce(
                topic=KAFKA_TOPIC,
                key=str(event["device_type_id"]),
                value=json.dumps(event),
                callback=on_delivery,
            )
            producer.poll(0)
            print(f"[event] {event}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("[generator] interrupted, flushing ...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
