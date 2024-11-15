from kafka import KafkaConsumer, KafkaAdminClient
import json


def check_kafka_topics():
    # First, list all topics
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
    topics = admin_client.list_topics()
    print(f"\nAvailable topics: {topics}")

    # Check each topic that starts with 'weather'
    for topic in topics:
        if topic.startswith("weather"):
            print(f"\nChecking topic: {topic}")
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers="localhost:9092",
                auto_offset_reset="latest",
                consumer_timeout_ms=9000000,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )

            try:
                # Try to get at least one message
                message = next(consumer, None)
                if message:
                    print(f"Found message in {topic}:")
                    print(json.dumps(message.value, indent=2))
                else:
                    print(f"No messages found in {topic}")
            except Exception as e:
                print(f"Error reading from {topic}: {e}")
            finally:
                consumer.close()

    admin_client.close()


if __name__ == "__main__":
    check_kafka_topics()
