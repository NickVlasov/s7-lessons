from pyspark.sql import functions as F, Window
from pyspark.sql.types import FloatType


_TIMEZONE_MAP = {
    "Adelaide": "Australia/Adelaide",
    "Brisbane": "Australia/Brisbane",
    "Sydney": "Australia/Sydney",
    "Melbourne": "Australia/Sydney",
    "Canberra": "Australia/Sydney",
    "Gold Coast": "Australia/Brisbane",
    "Hobart": "Australia/Sydney",
    "Launceston": "Australia/Sydney",
    "Ballarat": "Australia/Sydney",
    "Cairns": "Australia/Brisbane",
    "Mackay": "Australia/Brisbane",
    "Rockhampton": "Australia/Brisbane",
    "Toowoomba": "Australia/Brisbane",
    "Townsville": "Australia/Brisbane",
    "Wollongong": "Australia/Sydney",
    "Newcastle": "Australia/Sydney",
    "Maitland": "Australia/Sydney",
    "Ipswich": "Australia/Brisbane",
    "Bendigo": "Australia/Sydney",
    "Perth": "Australia/Perth",
    "Geelong": "Australia/Sydney",
}


class GeoBase:
    """Базовый класс для витрин: загрузка гео, событий, геопривязка."""

    def __init__(self, spark, events_path, geo_path, output_path):
        self.spark = spark
        self.events_path = events_path
        self.geo_path = geo_path
        self.output_path = output_path

    # ----------------------------------------------------------
    # Загрузка справочника городов
    # ----------------------------------------------------------
    def load_geo(self):
        geo_df = self.spark.read.csv(
            self.geo_path,
            sep=";",
            header=True,
            schema="id INT, city STRING, lat STRING, lng STRING",
        )
        geo_df = (
            geo_df
            .withColumn("lat", F.regexp_replace("lat", ",", ".").cast(FloatType()))
            .withColumn("lng", F.regexp_replace("lng", ",", ".").cast(FloatType()))
        )
        mapping_expr = F.create_map(
            [F.lit(x) for x in sum(
                [[k, v] for k, v in _TIMEZONE_MAP.items()], []
            )]
        )
        geo_df = geo_df.withColumn("timezone", mapping_expr[F.col("city")])
        return geo_df

    # ----------------------------------------------------------
    # Загрузка событий
    # ----------------------------------------------------------
    def load_events(self, use_sample=False, sample_fraction=0.1):
        df = self.spark.read.parquet(self.events_path)

        if use_sample:
            df = df.sample(withReplacement=False, fraction=sample_fraction, seed=42)

        msg_df = (
            df
            .filter(F.col("event_type") == "message")
            .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
            .select(
                F.col("event.message_id").alias("message_id"),
                F.col("event.message_from").cast("string").alias("user_id"),
                F.col("event.message_from").alias("message_from"),
                F.col("event.message_to").alias("message_to"),
                F.coalesce(F.col("event.datetime"), F.col("event.message_ts")).alias("event_datetime"),
                F.col("event.message").alias("message"),
                "event_type",
                "lat",
                "lon",
                "date",
            )
        )
        return df, msg_df

    # ----------------------------------------------------------
    # Геопривязка — формула гаверсинуса
    # ----------------------------------------------------------
    def assign_nearest_city(self, msg_df, geo_df):
        R = 6371.0

        geo = geo_df.select(
            F.col("id").alias("geo_id"),
            F.col("city").alias("geo_city"),
            F.col("lat").alias("geo_lat"),
            F.col("lng").alias("geo_lng"),
            F.col("timezone").alias("geo_timezone"),
        )

        joined = msg_df.crossJoin(geo)

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

        w = Window.partitionBy("message_id").orderBy("distance")
        result = (
            joined
            .withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn", "distance", "geo_lat", "geo_lng")
            .withColumnRenamed("geo_id", "zone_id")
            .withColumnRenamed("geo_city", "city")
            .withColumnRenamed("geo_timezone", "city_timezone")
            .withColumn("event_datetime", F.to_timestamp("event_datetime"))
        )
        return result
