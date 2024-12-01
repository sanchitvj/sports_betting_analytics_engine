from kafka import KafkaConsumer
import json
from datetime import datetime
import time


def monitor_cfb_analytics():
    """Monitor college football game analytics output."""
    print("\nStarting CFB analytics monitor...")

    consumer = KafkaConsumer(
        "cfb_games_analytics",  # Your output topic
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=False,
        group_id=None,
    )

    def safe_format(value, format_str=".1f"):
        """Safely format numeric values that might be None."""
        try:
            if value is None:
                return "N/A"
            return f"{float(value):{format_str}}"
        except (ValueError, TypeError):
            return "N/A"

    try:
        while True:
            messages = consumer.poll(timeout_ms=10000)
            if messages:
                for topic_partition, records in messages.items():
                    for record in records:
                        analytics = record.value
                        if analytics.get("game_state") == "in":
                            print("\n" + "=" * 80)
                            print(f"[{datetime.now()}] CFB Game Update")

                            # Game Information
                            print("\nGame Information:")
                            print(f"Game ID: {analytics.get('game_id')}")
                            print(
                                f"Game @ {analytics.get('venue_name')}, {analytics.get('venue_city')}, {analytics.get('venue_state')}"
                            )
                            print(f"Broadcasts: {analytics.get('broadcasts')}")
                            print(f"Status: {analytics.get('status_detail')}")
                            print(f"Period: {analytics.get('current_period')}")
                            print(f"Time Remaining: {analytics.get('time_remaining')}")

                            # Score
                            print("\nScore:")
                            print(
                                f"{analytics.get('home_team_name')}: {analytics.get('home_team_score')}"
                            )
                            print(
                                f"{analytics.get('away_team_name')}: {analytics.get('away_team_score')}"
                            )
                            print(f"Home Record: {analytics.get('home_team_record')}")
                            print(f"Away Record: {analytics.get('away_team_record')}")

                            # Home Team Leaders
                            print(f"\n{analytics.get('home_team_name')} Leaders:")
                            print(f"Passing: {analytics.get('passing_leader_value')}")
                            print(f"Rushing: {analytics.get('rushing_leader_value')}")
                            print(
                                f"Receiving: {analytics.get('receiving_leader_value')}"
                            )

                            # Game Analytics
                            print("\nGame Analytics:")
                            print(
                                f"Home Scoring Run: {safe_format(analytics.get('home_scoring_run'))}"
                            )
                            print(
                                f"Away Scoring Run: {safe_format(analytics.get('away_scoring_run'))}"
                            )

                            print("=" * 80)
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping monitor...")
    finally:
        consumer.close()


if __name__ == "__main__":
    monitor_cfb_analytics()
