import os


def _conf():
    c = {"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")}
    proto = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    c["security.protocol"] = proto
    if proto.startswith("SASL"):
        c["sasl.mechanism"] = os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")
        c["sasl.username"] = os.environ.get("KAFKA_SASL_USERNAME", "")
        c["sasl.password"] = os.environ.get("KAFKA_SASL_PASSWORD", "")
    return c


def get_producer():
    from confluent_kafka import Producer
    return Producer(_conf())


def get_consumer(group="trendpulse-consumer"):
    from confluent_kafka import Consumer
    c = _conf()
    c["group.id"] = group
    c["auto.offset.reset"] = "earliest"
    return Consumer(c)
