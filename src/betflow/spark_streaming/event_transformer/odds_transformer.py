from pyspark.sql.functions import (
    struct,
    current_timestamp,
    col,
    explode,
    when,
    collect_list,
    count_distinct,
)

# class OddsTransformer:
#     """Transform odds data from different sources."""
#
#     def transform_odds_api(
#         self, raw_data: Dict[str, Any], sport_type: str
#     ) -> Dict[str, Any]:
#         """Transform The Odds API data to standardized format."""
#         try:
#             # Generate unique odds ID
#             odds_id = self._generate_odds_id(
#                 raw_data["id"],
#                 raw_data.get("bookmaker_key", ""),
#                 raw_data.get("market_key", ""),
#             )
#
#             odds_data = {
#                 "odds_id": odds_id,
#                 "game_id": raw_data["id"],
#                 "sport_type": sport_type,
#                 "bookmaker_id": raw_data.get("bookmaker_key"),
#                 "timestamp": datetime.fromtimestamp(raw_data["last_update"]),
#                 "market_type": raw_data.get("market_key", "h2h"),
#                 "odds_value": self._extract_odds_value(raw_data),
#                 "spread_value": self._extract_spread_value(raw_data),
#                 "total_value": self._extract_total_value(raw_data),
#                 "probability": self._calculate_probability(raw_data),
#                 "volume": self._extract_volume_data(raw_data),
#                 "movement": self._extract_movement_data(raw_data),
#                 "status": raw_data.get("status", "active"),
#                 "metadata": {
#                     "bookmaker_name": raw_data.get("bookmaker_name"),
#                     "market_name": raw_data.get("market_name"),
#                     "last_update": raw_data["last_update"],
#                     "home_team": raw_data.get("home_team"),
#                     "away_team": raw_data.get("away_team"),
#                 },
#             }
#
#             # Validate against schema
#             return OddsData(**odds_data).model_dump()
#
#         except Exception as e:
#             raise ValueError(f"Failed to transform The Odds API data: {e}")
#
#     def transform_pinnacle(
#         self, raw_data: Dict[str, Any], sport_type: str
#     ) -> Dict[str, Any]:
#         """Transform Pinnacle odds data to standardized format."""
#         try:
#             odds_id = self._generate_odds_id(
#                 raw_data["eventId"], "pinnacle", raw_data.get("betType", "h2h")
#             )
#
#             odds_data = {
#                 "odds_id": odds_id,
#                 "game_id": str(raw_data["eventId"]),
#                 "sport_type": sport_type,
#                 "bookmaker_id": "pinnacle",
#                 "timestamp": datetime.fromtimestamp(raw_data["lastUpdate"]),
#                 "market_type": raw_data.get("betType", "h2h"),
#                 "odds_value": self._extract_pinnacle_odds(raw_data),
#                 "spread_value": raw_data.get("spread"),
#                 "total_value": raw_data.get("total"),
#                 "probability": self._calculate_pinnacle_probability(raw_data),
#                 "volume": {
#                     "amount": float(raw_data.get("volume", 0)),
#                     "currency": raw_data.get("currency", "USD"),
#                 },
#                 "movement": {
#                     "opening": float(raw_data.get("opening_price", 0)),
#                     "current": float(raw_data.get("price", 0)),
#                 },
#                 "status": raw_data.get("status", "active"),
#                 "metadata": {
#                     "league_id": raw_data.get("leagueId"),
#                     "period_number": raw_data.get("periodNumber"),
#                     "team_type": raw_data.get("teamType"),
#                     "bet_type": raw_data.get("betType"),
#                     "line_id": raw_data.get("lineId"),
#                 },
#             }
#
#             return OddsData(**odds_data).model_dump()
#
#         except Exception as e:
#             raise ValueError(f"Failed to transform Pinnacle data: {e}")
#
#     @staticmethod
#     def _generate_odds_id(game_id: str, bookmaker: str, market: str) -> str:
#         """Generate unique odds ID."""
#         unique_string = f"{game_id}_{bookmaker}_{market}_{datetime.now().timestamp()}"
#         return f"odds_{hashlib.md5(unique_string.encode()).hexdigest()}"
#
#     @staticmethod
#     def _extract_odds_value(data: Dict[str, Any]) -> float:
#         """Extract primary odds value."""
#         if "price" in data:
#             return float(data["price"])
#         if "odds" in data:
#             return float(data["odds"])
#         return 0.0
#
#     @staticmethod
#     def _extract_spread_value(data: Dict[str, Any]) -> Optional[float]:
#         """Extract spread value if available."""
#         if "spread" in data:
#             return float(data["spread"])
#         if "handicap" in data:
#             return float(data["handicap"])
#         return None
#
#     @staticmethod
#     def _extract_total_value(data: Dict[str, Any]) -> Optional[float]:
#         """Extract total value if available."""
#         if "total" in data:
#             return float(data["total"])
#         if "points" in data:
#             return float(data["points"])
#         return None
#
#     @staticmethod
#     def _calculate_probability(data: Dict[str, Any]) -> Optional[float]:
#         """Calculate implied probability from odds."""
#         try:
#             if "price" not in data or data["price"] is None:
#                 return None
#
#             odds = float(data["price"])
#             if odds == 0:
#                 return None
#
#             # Handle American odds
#             if abs(odds) > 2:  # Assume American odds
#                 if odds > 0:
#                     return 100 / (odds + 100)
#                 else:
#                     return abs(odds) / (abs(odds) + 100)
#             # Handle decimal odds
#             else:
#                 return 1 / odds
#
#         except (ValueError, ZeroDivisionError):
#             return None
#
#     @staticmethod
#     def _extract_volume_data(data: Dict[str, Any]) -> Optional[Dict[str, float]]:
#         """Extract volume data if available."""
#         volume = {}
#         if "volume" in data:
#             volume["total"] = float(data["volume"])
#         if "matched" in data:
#             volume["matched"] = float(data["matched"])
#         return volume if volume else None
#
#     @staticmethod
#     def _extract_movement_data(data: Dict[str, Any]) -> Dict[str, float]:
#         """Extract odds movement data."""
#         return {
#             "opening": float(data.get("opening_price", 0)),
#             "current": float(data.get("price", 0)),
#         }
#
#     @staticmethod
#     def _extract_pinnacle_odds(data: Dict[str, Any]) -> float:
#         """Extract odds value from Pinnacle data."""
#         if "price" in data:
#             return float(data["price"])
#         if "moneyline" in data:
#             return float(data["moneyline"])
#         return 0.0
#
#     @staticmethod
#     def _calculate_pinnacle_probability(data: Dict[str, Any]) -> Optional[float]:
#         """Calculate probability from Pinnacle odds."""
#         if "price" in data:
#             decimal_odds = float(data["price"])
#             if decimal_odds > 0:
#                 return 1 / decimal_odds
#         return None


class OddsTransformer:
    """Transform odds data using Spark SQL operations."""

    @staticmethod
    def transform_odds_data(df):
        try:
            # Add processing time
            df_with_time = df.withColumn("processing_time", current_timestamp())

            # First level: Explode bookmakers
            bookmakers_df = df_with_time.select(
                "*", explode("bookmakers").alias("bookmaker")
            )

            # Second level: Extract markets and outcomes in one step
            outcomes_df = (
                bookmakers_df.select(
                    "game_id",
                    "sport_key",
                    "sport_title",
                    "commence_time",
                    "home_team",
                    "away_team",
                    "processing_time",
                    "bookmaker.key",
                    "bookmaker.last_update",
                    explode("bookmaker.markets").alias("market"),
                )
                .where(col("market.key") == "h2h")
                .select("*", explode("market.outcomes").alias("outcome"))
            )

            # Aggregate with proper column handling
            final_df = outcomes_df.groupBy(
                "game_id",
                "sport_key",
                "sport_title",
                "commence_time",
                "home_team",
                "away_team",
                "processing_time",
            ).agg(
                max(
                    when(
                        col("outcome.name").cast("string")
                        == col("home_team").cast("string"),
                        col("outcome.price"),
                    )
                ).alias("best_home_odds"),
                max(
                    when(
                        col("outcome.name").cast("string")
                        == col("away_team").cast("string"),
                        col("outcome.price"),
                    )
                ).alias("best_away_odds"),
                collect_list(
                    when(
                        col("outcome.name").cast("string")
                        == col("home_team").cast("string"),
                        struct(
                            col("key").alias("bookie_key"),
                            col("outcome.price").alias("price"),
                        ),
                    )
                ).alias("home_odds_by_bookie"),
                collect_list(
                    when(
                        col("outcome.name").cast("string")
                        == col("away_team").cast("string"),
                        struct(
                            col("key").alias("bookie_key"),
                            col("outcome.price").alias("price"),
                        ),
                    )
                ).alias("away_odds_by_bookie"),
                count_distinct("key").alias("bookmakers_count"),
                max("last_update").alias("last_update"),
            )

            return final_df

        except Exception as e:
            raise ValueError(f"Failed to transform odds data: {e}")
