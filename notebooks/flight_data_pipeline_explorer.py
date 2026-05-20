# Databricks notebook source
# MAGIC %md
# MAGIC # Flight Data Pipeline — Explorer
# MAGIC
# MAGIC Explores all tables produced by the Flight Data Pipeline, which ingests
# MAGIC real-time ADS-B flight data from the OpenSky Network.
# MAGIC
# MAGIC **Tables covered**
# MAGIC | Table | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `workspace.default.ingest_flights` | Streaming table | OpenSky microbatches (append per run) |
# MAGIC | `workspace.default.flights_current` | Materialized view | Latest batch only, cleaned for gold |
# MAGIC | `workspace.default.flight_origin` | Materialized view | % of flights by origin country |
# MAGIC | `workspace.default.flights_stats` | Table | Aggregate velocity and aircraft counts |

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Raw ingestion — `ingest_flights`
# MAGIC
# MAGIC Each row is one ADS-B state vector snapshot. Fields sourced from
# MAGIC the [OpenSky REST API](https://openskynetwork.github.io/opensky-api/rest.html).

# COMMAND ----------

df_flights = spark.table("workspace.default.ingest_flights")
df_flights.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Field reference
# MAGIC
# MAGIC | Field | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `time_ingest` | timestamp | When the row was written to the pipeline |
# MAGIC | `icao24` | string | 24-bit ICAO address — unique per physical aircraft |
# MAGIC | `callsign` | string | Flight callsign (e.g. `EJA582`) |
# MAGIC | `origin_country` | string | Country inferred from ICAO address block |
# MAGIC | `time_position` | timestamp | Time of last position report from aircraft |
# MAGIC | `last_contact` | timestamp | Time of last ADS-B signal received |
# MAGIC | `longitude` | double | WGS-84 longitude in decimal degrees |
# MAGIC | `latitude` | double | WGS-84 latitude in decimal degrees |
# MAGIC | `geo_altitude` | double | Geometric altitude in metres (GPS-derived) |
# MAGIC | `baro_altitude` | double | Barometric altitude in metres |
# MAGIC | `on_ground` | boolean | True if surface position squitter received |
# MAGIC | `velocity` | double | Ground speed in m/s |
# MAGIC | `true_track` | double | Track angle in degrees (0 = north, clockwise) |
# MAGIC | `vertical_rate` | double | Climb/descent rate in m/s (negative = descending) |
# MAGIC | `squawk` | string | Mode-C transponder code (4-digit octal) |
# MAGIC | `spi` | boolean | Special purpose indicator (emergency/priority) |
# MAGIC | `category` | int | Emitter category (0 = unknown, see ICAO Doc 9684) |
# MAGIC | `sensors` | array\<int\> | IDs of OpenSky receivers that picked up this signal |

# COMMAND ----------
# MAGIC %md
# MAGIC ### Sample rows

# COMMAND ----------

display(df_flights.limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Row count and time range

# COMMAND ----------

from pyspark.sql import functions as F

df_flights.agg(
    F.count("*").alias("total_rows"),
    F.countDistinct("icao24").alias("distinct_aircraft"),
    F.countDistinct("callsign").alias("distinct_callsigns"),
    F.min("time_ingest").alias("earliest_ingest"),
    F.max("time_ingest").alias("latest_ingest"),
).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Aircraft currently (or recently) airborne

# COMMAND ----------

display(
    df_flights
    .filter(F.col("on_ground") == False)  # noqa: E712
    .filter(F.col("velocity") > 0)
    .select("icao24", "callsign", "origin_country", "latitude", "longitude",
            "geo_altitude", "velocity", "true_track", "vertical_rate", "time_ingest")
    .orderBy(F.col("time_ingest").desc())
    .limit(50)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Altitude distribution (airborne only)

# COMMAND ----------

display(
    df_flights
    .filter(F.col("on_ground") == False)  # noqa: E712
    .filter(F.col("geo_altitude").isNotNull())
    .withColumn("altitude_band_ft",
        F.when(F.col("geo_altitude") * 3.281 < 5000, "< 5,000 ft")
         .when(F.col("geo_altitude") * 3.281 < 15000, "5,000–15,000 ft")
         .when(F.col("geo_altitude") * 3.281 < 30000, "15,000–30,000 ft")
         .otherwise("> 30,000 ft")
    )
    .groupBy("altitude_band_ft")
    .agg(F.count("*").alias("flights"))
    .orderBy("altitude_band_ft")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Squawk codes in use
# MAGIC
# MAGIC Notable codes: `7500` = hijack, `7600` = radio failure, `7700` = emergency

# COMMAND ----------

NOTABLE_SQUAWKS = {"7500": "Hijack", "7600": "Radio failure", "7700": "Emergency"}

display(
    df_flights
    .filter(F.col("squawk").isin(list(NOTABLE_SQUAWKS.keys())))
    .select("icao24", "callsign", "origin_country", "squawk", "time_ingest")
    .orderBy(F.col("time_ingest").desc())
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Country breakdown — `flight_origin`
# MAGIC
# MAGIC Materialized view: percentage of all tracked flights originating from each country.

# COMMAND ----------

df_origin = spark.table("workspace.default.flight_origin")
display(df_origin)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Top 15 countries

# COMMAND ----------

display(df_origin.limit(15))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Aggregate stats — `flights_stats`

# COMMAND ----------

display(spark.table("workspace.default.flights_stats"))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Velocity in km/h and knots
# MAGIC
# MAGIC The pipeline stores velocity in **m/s** (OpenSky native unit).

# COMMAND ----------

display(
    spark.table("workspace.default.flights_stats")
    .withColumn("max_velocity_kmh", F.round(F.col("max_velocity") * 3.6, 1))
    .withColumn("avg_velocity_kmh", F.round(F.col("avg_velocity") * 3.6, 1))
    .withColumn("max_velocity_kts", F.round(F.col("max_velocity") * 1.944, 1))
    .withColumn("avg_velocity_kts", F.round(F.col("avg_velocity") * 1.944, 1))
)
