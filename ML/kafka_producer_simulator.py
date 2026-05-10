"""
=============================================================
  Kafka Sensor Data Producer (Simulation)
  Publishes fake sensor readings to Kafka topic
  Used for testing the full pipeline end-to-end
=============================================================
  Run this BEFORE starting sensor_anomaly_detection.py
=============================================================
"""

import json
import time
import random
from datetime import datetime

try:
    from kafka import KafkaProducer
except ImportError:
    raise SystemExit("❌ Install kafka-python: pip install kafka-python")

# ── Config ─────────────────────────────────────────────────
KAFKA_BROKER  = "localhost:9092"
KAFKA_TOPIC   = "sensor-readings"
SENSOR_IDS    = ["SENSOR_1", "SENSOR_2", "SENSOR_3", "SENSOR_4"]
INTERVAL_SEC  = 2    # seconds between messages
ANOMALY_PROB  = 0.15 # 15% chance of injecting an anomaly reading


def generate_reading(sensor_id: str, prev_temp: float) -> dict:
    """
    Generate a single sensor reading.
    With ANOMALY_PROB probability injects an anomalous value.
    """
    roll = random.random()

    if roll < ANOMALY_PROB:
        # Inject anomaly: sudden spike or drop
        anomaly_type = random.choice(["spike", "drop", "critical_high", "critical_low"])
        if anomaly_type == "spike":
            temperature = prev_temp + random.uniform(35, 60)
        elif anomaly_type == "drop":
            temperature = prev_temp - random.uniform(35, 60)
        elif anomaly_type == "critical_high":
            temperature = random.uniform(155, 200)
        else:
            temperature = random.uniform(1, 8)
    else:
        # Normal variation: ±10 °C
        temperature = prev_temp + random.uniform(-10, 10)

    # Clamp to physically reasonable bounds
    temperature = max(1.0, min(399.0, temperature))

    return {
        "sensor_id":   sensor_id,
        "temperature": round(temperature, 2),
        "timestamp":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def main():
    print(f"🚀 Connecting to Kafka broker: {KAFKA_BROKER}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        acks="all",                  # wait for all replicas to confirm
    )

    print(f"✅ Connected. Publishing to topic: {KAFKA_TOPIC}")
    print(f"   Sensors: {SENSOR_IDS}")
    print(f"   Interval: {INTERVAL_SEC}s  |  Anomaly rate: {ANOMALY_PROB*100:.0f}%")
    print("-" * 60)

    # Track last temperature per sensor for realistic variation
    prev_temps = {s: random.uniform(60, 100) for s in SENSOR_IDS}

    try:
        while True:
            for sensor_id in SENSOR_IDS:
                reading = generate_reading(sensor_id, prev_temps[sensor_id])
                prev_temps[sensor_id] = reading["temperature"]

                producer.send(KAFKA_TOPIC, value=reading)

                flag = "🚨 ANOMALY" if (
                    reading["temperature"] >= 150 or
                    reading["temperature"] <= 10 or
                    abs(reading["temperature"] - prev_temps[sensor_id]) > 30
                ) else "  normal "

                print(f"[{flag}] {sensor_id}  temp={reading['temperature']:>7.2f}°C"
                      f"  @ {reading['timestamp']}")

            producer.flush()
            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\n🛑 Producer stopped by user.")
    finally:
        producer.close()
        print("✅ Kafka producer closed.")


if __name__ == "__main__":
    main()
