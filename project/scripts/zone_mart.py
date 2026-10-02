from pyspark.sql import SparkSession, functions as F, Window
from mart_core import GeoBase


class ZoneMartBuilder(GeoBase):
    """Строит витрину зон: недельные и месячные счётчики по зонам."""

    def _geocode_events(self, df, id_col):
        """Геокодирование произвольного DataFrame с lat/lon."""
        R = 6371.0
        geo_df = self.load_geo()
        geo = geo_df.select(
            F.col("id").alias("geo_id"),
            F.col("city").alias("geo_city"),
            F.col("lat").alias("geo_lat"),
            F.col("lng").alias("geo_lng"),
            F.col("timezone").alias("geo_timezone"),
        )
        joined = df.crossJoin(geo)
        joined = joined.withColumn(
            "distance",
            F.lit(2) * F.lit(R) * F.asin(F.sqrt(
                F.pow(
                    F.sin(
                        (F.radians(F.col("geo_lat")) - F.radians(F.col("lat"))) / F.lit(2)
                    ),
                    F.lit(2),
                )
                + F.cos(F.radians(F.col("lat")))
                * F.cos(F.radians(F.col("geo_lat")))
                * F.pow(
                    F.sin(
                        (F.radians(F.col("geo_lng")) - F.radians(F.col("lon"))) / F.lit(2)
                    ),
                    F.lit(2),
                )
            )),
        )
        w = Window.partitionBy(id_col).orderBy("distance")
        result = (
            joined
            .withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn", "distance", "geo_lat", "geo_lng")
            .withColumnRenamed("geo_id", "zone_id")
            .withColumnRenamed("geo_city", "city")
            .withColumnRenamed("geo_timezone", "city_timezone")
        )
        return result

    def build_zone_mart(self, raw_events, msg_geocoded):
        # --- Периоды ---
        def with_periods(df):
            return (
                df.withColumn(
                    "week",
                    F.concat(
                        F.year("date"),
                        F.lit("-W"),
                        F.lpad(F.weekofyear("date").cast("string"), 2, "0"),
                    ),
                )
                .withColumn("month", F.date_format("date", "yyyy-MM"))
            )

        # --- Сообщения (уже геокодированы) ---
        msg = with_periods(msg_geocoded)
        week_msg = msg.groupBy("week", "zone_id").agg(
            F.count("*").alias("week_message")
        )
        month_msg = msg.groupBy("month", "zone_id").agg(
            F.count("*").alias("month_message")
        )
        week_user = msg.groupBy("week", "zone_id").agg(
            F.countDistinct("user_id").alias("week_user")
        )
        month_user = msg.groupBy("month", "zone_id").agg(
            F.countDistinct("user_id").alias("month_user")
        )

        # --- Реакции ---
        react_raw = (
            raw_events
            .filter(F.col("event_type") == "reaction")
            .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
            .withColumn("event_id", F.monotonically_increasing_id())
            .withColumn("user_id", F.col("event.reaction_from"))
            .select("event_id", "user_id", "lat", "lon", "date")
        )
        react_geo = self._geocode_events(react_raw, "event_id")
        react = with_periods(react_geo)
        week_react = react.groupBy("week", "zone_id").agg(
            F.count("*").alias("week_reaction")
        )
        month_react = react.groupBy("month", "zone_id").agg(
            F.count("*").alias("month_reaction")
        )

        # --- Подписки ---
        sub_raw = (
            raw_events
            .filter(F.col("event_type") == "subscription")
            .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
            .withColumn("event_id", F.monotonically_increasing_id())
            .withColumn("user_id", F.col("event.user"))
            .select("event_id", "user_id", "lat", "lon", "date")
        )
        sub_geo = self._geocode_events(sub_raw, "event_id")
        sub = with_periods(sub_geo)
        week_sub = sub.groupBy("week", "zone_id").agg(
            F.count("*").alias("week_subscription")
        )
        month_sub = sub.groupBy("month", "zone_id").agg(
            F.count("*").alias("month_subscription")
        )

        # --- Маппинг week → month ---
        week_to_month = msg.select("week", "month").dropDuplicates(["week"])

        # --- Базовая таблица: (week, zone_id, month) ---
        base = week_msg.select("week", "zone_id").join(week_to_month, "week", "left")

        # --- Join недельных агрегатов ---
        result = (
            base
            .join(week_msg, ["week", "zone_id"], "left")
            .join(week_react, ["week", "zone_id"], "left")
            .join(week_sub, ["week", "zone_id"], "left")
            .join(week_user, ["week", "zone_id"], "left")
        )

        # --- Join месячных агрегатов ---
        result = (
            result
            .join(month_msg, ["month", "zone_id"], "left")
            .join(month_react, ["month", "zone_id"], "left")
            .join(month_sub, ["month", "zone_id"], "left")
            .join(month_user, ["month", "zone_id"], "left")
        )

        result = result.fillna(0, subset=[
            "week_message", "week_reaction", "week_subscription", "week_user",
            "month_message", "month_reaction", "month_subscription", "month_user",
        ])

        result = result.select(
            "week", "zone_id",
            "week_message", "week_reaction", "week_subscription", "week_user",
            "month", "month_message", "month_reaction", "month_subscription", "month_user",
        )

        return result

    def save(self, df):
        df.write.mode("overwrite").parquet(self.output_path)
        print(f"Витрина зон сохранена в {self.output_path}!")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("ZoneMart").getOrCreate()
    builder = ZoneMartBuilder(
        spark=spark,
        events_path="/user/master/data/geo/events",
        geo_path="/user/master/data/geo/geo.csv",
        output_path="/user/s30080726/data/marts/zone_mart",
    )
    raw_events, msg_df = builder.load_events()
    geo_df = builder.load_geo()
    msg_geocoded = builder.assign_nearest_city(msg_df, geo_df)
    zone_mart = builder.build_zone_mart(raw_events, msg_geocoded)
    zone_mart.show(20, truncate=False)
    builder.save(zone_mart)
