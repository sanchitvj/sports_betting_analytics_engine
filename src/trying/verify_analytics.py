# verify_pipeline.py
from kafka import KafkaConsumer
import json
from datetime import datetime


def verify_game_analytics():
    """Monitor and verify game analytics output."""
    consumer = KafkaConsumer(
        "game_analytics",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
    )

    print("Monitoring game analytics...")
    for message in consumer:
        analytics = message.value
        print(f"\n[{datetime.now()}] Received Analytics:")
        print(f"Game ID: {analytics.get('game_id')}")
        print(f"Sport Type: {analytics.get('sport_type')}")

        # Score Analytics
        score = analytics.get("score_analytics", {})
        print("\nScore Analytics:")
        print(f"Home Scoring Rate: {score.get('home_scoring_rate')}")
        print(f"Away Scoring Rate: {score.get('away_scoring_rate')}")

        print("-" * 80)


if __name__ == "__main__":
    verify_game_analytics()
