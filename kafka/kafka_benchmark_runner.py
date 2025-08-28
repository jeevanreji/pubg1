# -------------------------
# Author: Jeevan Reji
# Date: 2025-08-28
# -------------------------
"""
Kafka Benchmark Runner (parallel to Sprint 5 kafkalite runner).
Publishes messages with timestamps, consumes them, and records pub/consume metrics.
"""

import os, time, json, threading
from confluent_kafka import Producer, Consumer, KafkaException
from utils.metrics import Metrics

BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "bench-topic")
GROUP_ID = os.environ.get("GROUP_ID", "bench")

DURATION_SEC = float(os.environ.get("DURATION_SEC", 40))
TARGET_RPS = float(os.environ.get("TARGET_RPS", 100))

metrics = Metrics()

def consumer_worker(stop_evt: threading.Event):
    consumer = Consumer({
        "bootstrap.servers": BROKERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    while not stop_evt.is_set():
        try:
            msg = consumer.poll(1.0)
            if msg is None: 
                continue
            if msg.error():
                print(f"[ConsumerError] {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                client_ts = payload.get("client_ts") or payload.get("timestamp")
                if client_ts is not None:
                    metrics.record_consume(time.time() - float(client_ts))
                else:
                    metrics.record_consume(-1.0)
            except Exception as e:
                print(f"[ConsumerParseError] {e}")
                metrics.record_consume(-1.0)
        except KafkaException as e:
            print(f"[KafkaException] {e}")
            metrics.record_consume(-1.0)

    consumer.close()

def main():
    print(f"[KafkaBench] Brokers={BROKERS}, topic={TOPIC}, group={GROUP_ID}")
    producer = Producer({"bootstrap.servers": BROKERS})

    stop_evt = threading.Event()
    t_cons = threading.Thread(target=consumer_worker, args=(stop_evt,), daemon=True)
    t_cons.start()

    t0 = time.time()
    deadline = t0 + DURATION_SEC
    msg_id = 0

    while time.time() < deadline:
        payload = {"id": msg_id, "client_ts": time.time()}
        data = json.dumps(payload).encode("utf-8")

        t_pub = time.time()
        try:
            producer.produce(TOPIC, data)
            producer.flush()
            metrics.record_pub(time.time() - t_pub)
        except KafkaException as e:
            print(f"[ProducerError] {e}")
            metrics.record_pub(-1.0)

        msg_id += 1
        time.sleep(max(0.0, 1.0 / TARGET_RPS))

    stop_evt.set()
    t_cons.join(timeout=2.0)

    # Report
    s = metrics.summary()
    print("\n=== Kafka Benchmark Summary ===")
    print(f"Elapsed: {s['elapsed_sec']:.2f}s")
    print(f"Publish: count={s['publish']['count']} errors={s['publish']['errors']} "
          f"thr={s['publish']['throughput']:.1f}/s "
          f"lat_avg={None if s['publish']['latency']['avg'] is None else round(s['publish']['latency']['avg']*1000,1)} ms "
          f"p95={None if s['publish']['latency']['p95'] is None else round(s['publish']['latency']['p95']*1000,1)} ms")
    print(f"Consume: count={s['consume']['count']} errors={s['consume']['errors']} "
          f"thr={s['consume']['throughput']:.1f}/s "
          f"e2e_avg={None if s['consume']['latency']['avg'] is None else round(s['consume']['latency']['avg']*1000,1)} ms "
          f"p95={None if s['consume']['latency']['p95'] is None else round(s['consume']['latency']['p95']*1000,1)} ms")

if __name__ == "__main__":
    main()
