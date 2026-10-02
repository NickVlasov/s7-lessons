from pyspark.sql import SparkSession, functions as F, Window
from mart_core import GeoBase


class FriendRecMartBuilder(GeoBase):
    """Строит витрину рекомендаций друзей."""

    def build_friend_rec_mart(self, raw_events, msg_geocoded):
        # 1. Последняя локация каждого пользователя
        user_loc = (
            msg_geocoded
            .select("user_id", "lat", "lon", "zone_id", "city_timezone", "event_datetime")
            .withColumn("rn", F.row_number().over(
                Window.partitionBy("user_id").orderBy(F.col("event_datetime").desc())
            ))
            .filter(F.col("rn") == 1)
            .drop("rn", "event_datetime")
        )

        # 2. Каналы каждого пользователя
        user_channels = (
            raw_events
            .filter(F.col("event_type") == "subscription")
            .select(
                F.col("event.user").alias("uid"),
                F.col("event.subscription_channel").cast("string").alias("channel_id"),
            )
            .distinct()
            .groupBy("uid")
            .agg(F.collect_set("channel_id").alias("channels"))
        )

        # 3. Прямые сообщения (оба направления)
        direct = (
            raw_events
            .filter(
                (F.col("event_type") == "message")
                & F.col("event.message_to").isNotNull()
            )
            .select(
                F.col("event.message_from").cast("string").alias("u1"),
                F.col("event.message_to").cast("string").alias("u2"),
            )
            .distinct()
        )
        direct = direct.union(
            direct.select(F.col("u2").alias("u1"), F.col("u1").alias("u2"))
        ).distinct()

        # 4. Пары рядом: self-join по zone_id, Haversine <= 1 км
        R = 6371.0
        nearby = (
            user_loc.alias("a")
            .join(user_loc.alias("b"), F.col("a.zone_id") == F.col("b.zone_id"))
            .filter(F.col("a.user_id") < F.col("b.user_id"))
            .withColumn(
                "distance",
                F.lit(2) * F.lit(R) * F.asin(F.sqrt(
                    F.pow(
                        F.sin(
                            (F.radians(F.col("b.lat")) - F.radians(F.col("a.lat"))) / F.lit(2)
                        ),
                        F.lit(2),
                    )
                    + F.cos(F.radians(F.col("a.lat")))
                    * F.cos(F.radians(F.col("b.lat")))
                    * F.pow(
                        F.sin(
                            (F.radians(F.col("b.lon")) - F.radians(F.col("a.lon"))) / F.lit(2)
                        ),
                        F.lit(2),
                    )
                )),
            )
            .filter(F.col("distance") <= 1.0)
            .select(
                F.col("a.user_id").alias("user_left"),
                F.col("b.user_id").alias("user_right"),
                F.col("a.zone_id").alias("zone_id"),
                F.col("a.city_timezone").alias("city_timezone"),
            )
        )

        # 5. Оставляем только пары с общим каналом
        nearby = (
            nearby
            .join(user_channels.alias("lc"), F.col("user_left") == F.col("lc.uid"), "inner")
            .join(user_channels.alias("rc"), F.col("user_right") == F.col("rc.uid"), "inner")
            .filter(F.size(F.array_intersect(F.col("lc.channels"), F.col("rc.channels"))) > 0)
            .select("user_left", "user_right", "zone_id", "city_timezone")
        )

        # 6. Исключаем пары, уже переписывавшиеся напрямую
        friend_rec = nearby.join(
            direct,
            (F.col("user_left") == F.col("u1"))
            & (F.col("user_right") == F.col("u2")),
            "left_anti",
        )

        # 7. Финальные колонки
        now = F.current_timestamp()
        friend_rec_mart = friend_rec.select(
            F.col("user_left"),
            F.col("user_right"),
            now.alias("processed_dttm"),
            F.col("zone_id"),
            F.from_utc_timestamp(now, F.coalesce(F.col("city_timezone"), F.lit("UTC"))).alias("local_time"),
        )

        return friend_rec_mart

    def save(self, df):
        df.write.mode("overwrite").parquet(self.output_path)
        print(f"Витрина рекомендаций друзей сохранена в {self.output_path}!")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("FriendRecMart").getOrCreate()
    builder = FriendRecMartBuilder(
        spark=spark,
        events_path="/user/master/data/geo/events",
        geo_path="/user/master/data/geo/geo.csv",
        output_path="/user/s30080726/data/marts/friend_rec_mart",
    )
    raw_events, msg_df = builder.load_events()
    geo_df = builder.load_geo()
    msg_geocoded = builder.assign_nearest_city(msg_df, geo_df)
    friend_rec_mart = builder.build_friend_rec_mart(raw_events, msg_geocoded)
    friend_rec_mart.show(20, truncate=False)
    builder.save(friend_rec_mart)
