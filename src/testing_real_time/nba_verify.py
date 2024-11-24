from kafka import KafkaConsumer
import json
from datetime import datetime
import time


def monitor_basketball_analytics():
    """Monitor basketball game analytics output."""
    print("\nStarting basketball analytics monitor...")

    consumer = KafkaConsumer(
        "nba_analytics",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=False,
        group_id=None,
    )

    try:
        while True:
            messages = consumer.poll(timeout_ms=1000)
            if messages:
                for topic_partition, records in messages.items():
                    for record in records:
                        game_data = record.value

                        # print("\nRaw Analytics Data:")
                        # print(json.dumps(game_data, indent=2))
                        # if True or game_data.get("status_state") == "in":
                        print("\n" + "=" * 80)
                        print(f"[{datetime.now()}] nba Game Update")

                        # Game Information
                        print("\nGame Information:")
                        # print(f"Game ID: {game_data.get('game_id')}")
                        print(
                            f"Game @ {game_data.get('venue_name')}, {game_data.get('venue_city')}, {game_data.get('venue_state')}"
                        )

                        print(f"Broadcasts: {game_data.get('broadcasts')}")
                        print(f"Status: {game_data.get('status_state')}")
                        print(f"Time Remaining: {game_data.get('time_remaining')}")

                        # Score
                        print("\nScore:")
                        home_team = game_data.get("home_team_name")
                        away_team = game_data.get("away_team_name")
                        print(f"{home_team} (Home): {game_data.get('home_team_score')}")
                        print(f"{away_team} (Away): {game_data.get('away_team_score')}")

                        # Team Statistics
                        print("\nHome Team Stats:")
                        print(f"Field Goals: {game_data.get('home_fg_pct')}%")
                        print(f"3-Pointers: {game_data.get('home_three_pt_pct')}%")
                        # print(f"Free Throws: {game_data.get('')}%")

                        print("\nAway Team Stats:")
                        print(f"Field Goals: {game_data.get('away_fg_pct')}%")
                        print(f"3-Pointers: {game_data.get('away_three_pt_pct')}%")
                        # print(f"Free Throws: {away_team.get('free_throws', 0):.1f}%")

                        # Game Analytics
                        print("\nGame Analytics:")
                        print(
                            f"Home scoring run: {game_data.get('home_team_scoring_run')}"
                        )
                        print(
                            f"Away scoring run: {game_data.get('away_team_scoring_run')}"
                        )
                        print(f"Home rebounds: {game_data.get('home_rebounds')}")
                        print(f"Away rebounds: {game_data.get('away_rebounds')}")
                        print(f"Home assists: {game_data.get('home_assists')}")
                        print(f"Away assists: {game_data.get('away_assists')}")

                        print("=" * 80)
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping monitor...")
    finally:
        consumer.close()


def safe_format(value, format_str=".1f"):
    """Safely format numeric values that might be None."""
    try:
        if value is None:
            return "N/A"
        return f"{float(value):{format_str}}"
    except (ValueError, TypeError):
        return "N/A"


if __name__ == "__main__":
    monitor_basketball_analytics()
