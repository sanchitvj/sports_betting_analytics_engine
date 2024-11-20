from kafka import KafkaConsumer
import json
from datetime import datetime
import time


def monitor_nhl_analytics():
    """Monitor NHL game analytics output."""
    print("\nStarting NHL analytics monitor...")

    consumer = KafkaConsumer(
        "nhl_analytics",
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
                        print("\n" + "=" * 80)
                        print(f"[{datetime.now()}] NHL Game Update")

                        # Game Information
                        print("\nGame Information:")
                        print(f"Game ID: {analytics.get('game_id')}")
                        print(
                            f"Game @ {analytics.get('venue_name')}, {analytics.get('venue_city')}, {analytics.get('venue_state')}"
                        )
                        print(f"Broadcasts: {analytics.get('broadcasts')}")
                        print(f"Status: {analytics.get('game_state')}")
                        print(f"Period: {analytics.get('current_period')}")
                        print(f"Time Remaining: {analytics.get('time_remaining')}")

                        # Score
                        print("\nScore:")
                        print(
                            f"{analytics.get('home_team_name')} ({analytics.get('home_team_abbrev')}): {analytics.get('home_team_score')}"
                        )
                        print(
                            f"{analytics.get('away_team_name')} ({analytics.get('away_team_abbrev')}): {analytics.get('away_team_score')}"
                        )
                        print(f"Home Record: {analytics.get('home_record')}")
                        print(f"Away Record: {analytics.get('away_record')}")

                        # Home Team Stats
                        print(f"\n{analytics.get('home_team_name')} Statistics:")
                        print(f"Saves: {safe_format(analytics.get('home_saves'))}")
                        print(
                            f"Save %: {safe_format(analytics.get('home_save_pct'), '.3f')}"
                        )
                        print(f"Goals: {safe_format(analytics.get('home_goals'))}")
                        print(f"Assists: {safe_format(analytics.get('home_assists'))}")
                        print(f"Points: {safe_format(analytics.get('home_points'))}")
                        print(
                            f"Penalties: {safe_format(analytics.get('home_penalties'))}"
                        )
                        print(
                            f"Penalty Minutes: {safe_format(analytics.get('home_penalty_minutes'))}"
                        )
                        print(
                            f"Power Plays: {safe_format(analytics.get('home_power_plays'))}"
                        )
                        print(
                            f"Power Play Goals: {safe_format(analytics.get('home_power_play_goals'))}"
                        )
                        print(
                            f"Power Play %: {safe_format(analytics.get('home_power_play_pct'), '.1f')}%"
                        )

                        # Away Team Stats
                        print(f"\n{analytics.get('away_team_name')} Statistics:")
                        print(f"Saves: {safe_format(analytics.get('away_saves'))}")
                        print(
                            f"Save %: {safe_format(analytics.get('away_save_pct'), '.3f')}"
                        )
                        print(f"Goals: {safe_format(analytics.get('away_goals'))}")
                        print(f"Assists: {safe_format(analytics.get('away_assists'))}")
                        print(f"Points: {safe_format(analytics.get('away_points'))}")
                        print(
                            f"Penalties: {safe_format(analytics.get('away_penalties'))}"
                        )
                        print(
                            f"Penalty Minutes: {safe_format(analytics.get('away_penalty_minutes'))}"
                        )
                        print(
                            f"Power Plays: {safe_format(analytics.get('away_power_plays'))}"
                        )
                        print(
                            f"Power Play Goals: {safe_format(analytics.get('away_power_play_goals'))}"
                        )
                        print(
                            f"Power Play %: {safe_format(analytics.get('away_power_play_pct'), '.1f')}%"
                        )

                        # Game Analytics
                        print("\nGame Analytics:")
                        print(
                            f"Home Scoring Run: {safe_format(analytics.get('home_scoring_run'))}"
                        )
                        print(
                            f"Away Scoring Run: {safe_format(analytics.get('away_scoring_run'))}"
                        )
                        print(
                            f"Home Power Play Efficiency: {safe_format(analytics.get('home_power_play_efficiency'), '.3f')}"
                        )
                        print(
                            f"Away Power Play Efficiency: {safe_format(analytics.get('away_power_play_efficiency'), '.3f')}"
                        )

                        print("=" * 80)
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping monitor...")
    finally:
        consumer.close()


if __name__ == "__main__":
    monitor_nhl_analytics()
