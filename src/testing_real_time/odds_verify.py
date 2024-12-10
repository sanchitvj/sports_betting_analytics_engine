from kafka import KafkaConsumer
import json
from datetime import datetime
import time


def monitor_odds_analytics():
    """Monitor odds analytics output."""
    print("\nStarting Odds Analytics Monitor...")

    consumer = KafkaConsumer(
        "nfl_odds_analytics",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
    )

    def safe_format(value, format_str=".3f"):
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
                        print(f"[{datetime.now()}] Odds Update")

                        # Basic Game Info
                        print("\nGame Information:")
                        print(f"Game ID: {analytics.get('game_id')}")
                        print(
                            f"Sport: {analytics.get('sport_key')} - {analytics.get('sport_title')}"
                        )
                        print(
                            f"Teams: {analytics.get('home_team')} vs {analytics.get('away_team')}"
                        )
                        print(f"Commence Time: {analytics.get('commence_time')}")

                        # Current Odds
                        print("\nCurrent Odds:")
                        print(
                            f"Current Home Odds: {safe_format(analytics.get('current_home_odds'))}"
                        )
                        print(
                            f"Current Away Odds: {safe_format(analytics.get('current_away_odds'))}"
                        )

                        # Odds Movement
                        print("\nOdds Movement:")
                        print(
                            f"Average Home Odds: {safe_format(analytics.get('avg_home_odds'))}"
                        )
                        print(
                            f"Average Away Odds: {safe_format(analytics.get('avg_away_odds'))}"
                        )
                        print(
                            f"Max Home Odds: {safe_format(analytics.get('max_home_odds'))}"
                        )
                        print(
                            f"Max Away Odds: {safe_format(analytics.get('max_away_odds'))}"
                        )
                        print(
                            f"Min Home Odds: {safe_format(analytics.get('min_home_odds'))}"
                        )
                        print(
                            f"Min Away Odds: {safe_format(analytics.get('min_away_odds'))}"
                        )

                        # Market Efficiency
                        print("\nMarket Efficiency:")
                        print(
                            f"Home Odds Spread: {safe_format(analytics.get('home_odds_spread'))}"
                        )
                        print(
                            f"Away Odds Spread: {safe_format(analytics.get('away_odds_spread'))}"
                        )
                        print(
                            f"Home Odds Volatility: {safe_format(analytics.get('home_odds_volatility'))}"
                        )
                        print(
                            f"Away Odds Volatility: {safe_format(analytics.get('away_odds_volatility'))}"
                        )

                        # Implied Probability
                        print("\nImplied Probability:")
                        print(
                            f"Home Win Probability: {safe_format(analytics.get('implied_home_prob'), '.2%')}"
                        )
                        print(
                            f"Away Win Probability: {safe_format(analytics.get('implied_away_prob'), '.2%')}"
                        )

                        # Market Movement
                        print("\nMarket Movement:")
                        print(
                            f"Home Odds Movement: {safe_format(analytics.get('home_odds_movement_pct'), '.2%')}"
                        )
                        print(
                            f"Away Odds Movement: {safe_format(analytics.get('away_odds_movement_pct'), '.2%')}"
                        )

                        # Bookmaker Analysis
                        print("\nBookmaker Information:")
                        print(
                            f"Average Active Bookmakers: {safe_format(analytics.get('avg_bookmakers'), '.0f')}"
                        )
                        print(
                            f"Max Bookmakers: {safe_format(analytics.get('max_bookmakers'), '.0f')}"
                        )
                        # print(
                        #     f"Home Odds by Bookmaker: {analytics.get('home_odds_by_bookie')}"
                        # )
                        # print(
                        #     f"Away Odds by Bookmaker: {analytics.get('away_odds_by_bookie')}"
                        # )

                        # Time Analysis
                        print("\nUpdate Information:")
                        print(f"First Update: {analytics.get('first_update')}")
                        print(f"Latest Update: {analytics.get('latest_update')}")
                        print(f"Update Count: {analytics.get('update_count')}")

                        print("=" * 80)
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping monitor...")
    finally:
        consumer.close()


if __name__ == "__main__":
    monitor_odds_analytics()
