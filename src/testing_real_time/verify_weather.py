# verify_weather.py
from kafka import KafkaConsumer
import json
from datetime import datetime
import time


def monitor_analytics():
    """Monitor weather analytics output."""
    print("\nStarting analytics monitor...")

    consumer = KafkaConsumer(
        "weather_analytics",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",  # 'earliest' if you want all data
        enable_auto_commit=False,
        group_id=None,  # Don't use consumer groups for testing
    )

    try:
        while True:
            messages = consumer.poll(timeout_ms=10000)
            if messages:
                for topic_partition, records in messages.items():
                    # print(topic_partition)
                    # print(records)
                    for record in records:
                        analytics = record.value

                        # print("\nRaw Analytics Data:")
                        # print(json.dumps(analytics, indent=2))

                        print("\n" + "=" * 80)
                        print(f"[{datetime.now()}] Weather Analytics Update")
                        print(f"Venue ID: {analytics.get('venue_id')}")

                        # Temperature Analytics
                        print("\nTemperature Analytics:")
                        print(
                            f"Average Temp.: {analytics.get('avg_temperature'):.1f}°C"
                        )
                        print(f"Feels like: {analytics.get('avg_feels_like'):.2f}°C")
                        # print(f"Max: {analytics.get('max_temperature'):.1f}°C")
                        # print(f"Min: {analytics.get('min_temperature'):.1f}°C")
                        print(f"Comfort Index: {analytics.get('comfort_index')}")
                        print(f"Avg humidity: {analytics.get('avg_humidity')}")
                        print(f"Avg pressure: {analytics.get('avg_pressure')}")

                        # Wind Analytics
                        print("\nWind Analytics:")
                        print(
                            f"Average Speed: {analytics.get('avg_wind_speed'):.1f} m/s"
                        )
                        print(f"Direction: {analytics.get('last_wind_direction')}")
                        print(
                            f"Wind Impact Score: {analytics.get('wind_impact_score')}"
                        )
                        # Condition Analytics
                        print("\nCondition Analytics:")
                        print(
                            f"Weather condition: {analytics.get('current_condition')}"
                        )
                        print(
                            f"Weather description: {analytics.get('current_description')}"
                        )
                        print(f"Visibility: {analytics.get('avg_visibility')} km")
                        print(f"Cloud Cover: {analytics.get('avg_cloud_cover')}")
                        print(f"Weather Severity: {analytics.get('weather_severity')}")

                        print("=" * 80)
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping monitor...")
    finally:
        consumer.close()


if __name__ == "__main__":
    # try:
    #     shutil.rmtree("/tmp/checkpoint")
    # except FileNotFoundError:
    #     print("Checkpoint directory doesn't exist")
    monitor_analytics()
