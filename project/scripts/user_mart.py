from pyspark.sql import SparkSession, functions as F, Window
from mart_core import GeoBase


class UserMartBuilder(GeoBase):
    """Строит витрину пользователей с геопривязкой."""

    # ----------------------------------------------------------
    # Построение витрины
    # ----------------------------------------------------------
    def build_user_mart(self, events_with_city):
        # --- ACT_CITY — город последнего сообщения ---
        w_last = Window.partitionBy("user_id").orderBy(F.col("date").desc())

        act_city_df = (
            events_with_city
            .withColumn("rn", F.row_number().over(w_last))
            .filter(F.col("rn") == 1)
            .select(
                "user_id",
                F.col("city").alias("act_city"),
                F.col("city_timezone").alias("act_timezone"),
            )
        )

        # --- HOME_CITY — последний город > 27 дней ---
        w_ordered = Window.partitionBy("user_id").orderBy("date")

        trips = (
            events_with_city
            .withColumn("prev_city", F.lag("city").over(w_ordered))
            .withColumn(
                "is_new_trip",
                F.when(F.col("city") != F.col("prev_city"), 1).otherwise(0),
            )
            .withColumn(
                "trip_id",
                F.sum("is_new_trip").over(
                    w_ordered.rowsBetween(Window.unboundedPreceding, 0)
                ),
            )
            .groupBy("user_id", "trip_id", "city")
            .agg(
                F.min("date").alias("trip_start"),
                F.max("date").alias("trip_end"),
                F.count("*").alias("msg_count"),
            )
        )

        trips = trips.withColumn(
            "trip_duration_days",
            F.datediff(F.col("trip_end"), F.col("trip_start")),
        )

        w_home = Window.partitionBy("user_id").orderBy(F.col("trip_end").desc())
        home_df = (
            trips.filter(F.col("trip_duration_days") > 27)
            .withColumn("rn", F.row_number().over(w_home))
            .filter(F.col("rn") == 1)
            .select("user_id", F.col("city").alias("home_city"))
        )

        # Запасной вариант: самый частый город
        w_freq = Window.partitionBy("user_id").orderBy(
            F.col("msg_count").desc(), F.col("trip_end").desc()
        )
        freq_city_df = (
            trips
            .withColumn("rn", F.row_number().over(w_freq))
            .filter(F.col("rn") == 1)
            .select("user_id", F.col("city").alias("freq_city"))
        )

        # --- TRAVEL_COUNT и TRAVEL_ARRAY ---
        travel_df = (
            trips
            .orderBy("user_id", "trip_start")
            .groupBy("user_id")
            .agg(
                F.count("*").alias("travel_count"),
                F.collect_list("city").alias("travel_array"),
            )
        )

        # --- LOCAL_TIME ---
        local_time_df = (
            events_with_city
            .withColumn("rn", F.row_number().over(w_last))
            .filter(F.col("rn") == 1)
            .select("user_id", "city_timezone", "event_datetime")
            .withColumn(
                "local_time",
                F.from_utc_timestamp(F.col("event_datetime"), F.col("city_timezone")),
            )
            .select("user_id", "local_time")
        )

        # --- Объединяем ---
        result = (
            act_city_df
            .join(home_df, "user_id", "left")
            .join(travel_df, "user_id", "left")
            .join(local_time_df, "user_id", "left")
            .join(freq_city_df, "user_id", "left")
        )

        result = result.withColumn(
            "home_city",
            F.coalesce(F.col("home_city"), F.col("freq_city")),
        ).drop("freq_city")

        return result

    # ----------------------------------------------------------
    # Сохранение
    # ----------------------------------------------------------
    def save(self, df):
        (df.write.mode("overwrite").parquet(self.output_path))
        print(f"Витрина пользователей сохранена в {self.output_path}!")


# ----------------------------------------------------------
# MAIN
# ----------------------------------------------------------
if __name__ == "__main__":
    spark = SparkSession.builder.appName("UserMart").getOrCreate()

    builder = UserMartBuilder(
        spark=spark,
        events_path="/user/master/data/geo/events",
        geo_path="/user/master/data/geo/geo.csv",
        output_path="/user/s30080726/data/marts/user_mart",
    )

    geo_df = builder.load_geo()
    _, msg_df = builder.load_events()
    events_with_city = builder.assign_nearest_city(msg_df, geo_df)
    user_mart = builder.build_user_mart(events_with_city)

    user_mart.show(20, truncate=False)
    builder.save(user_mart)
