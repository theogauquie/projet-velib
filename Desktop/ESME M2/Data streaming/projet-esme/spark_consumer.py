import os
import out as out
os.environ["SPARK_HOME"] = "/Users/goks/spark-3.2.3-bin-hadoop2.7"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /Users/goks/spark-streaming-kafka-0-10-assembly_2.12-3.2.3.jar pyspark-shell'
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11"
import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as pysqlf
import pyspark.sql.types as pysqlt
import time
from pyspark.sql.functions import col


# Todo: il faut créer un nouveau dataframe qui contient:
    # - le code postal, le nombre total de vélo disponible par code postal, le nombre total de vélo mécanique par code postal, le nombre total de vélo electrique par code postal
    # - Pousser ce nouveau dataframe vers une file kafka appéler velib-projet-clean


if __name__ == "__main__":
    # Initier spark
    spark = (SparkSession
             .builder
             .appName("velib-projet-clean")
             .master("local[1]")
             .config("spark.sql.shuffle.partitions", 1)
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3")
             .getOrCreate()
             )

    # Lire les données temps réel
    kafka_df = (spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "velib-projet")
                .option("startingOffsets", "earliest")
                .load()
                )

    # Appliquer des traitements sur les données
    schema = pysqlt.StructType([
        pysqlt.StructField("stationCode", pysqlt.StringType()),
        pysqlt.StructField("station_id", pysqlt.StringType()),
        pysqlt.StructField("num_bikes_available", pysqlt.IntegerType()),
        pysqlt.StructField("numBikesAvailable", pysqlt.IntegerType()),
        pysqlt.StructField("num_bikes_available_types",
                           pysqlt.ArrayType(pysqlt.MapType(pysqlt.StringType(), pysqlt.IntegerType()))),
        pysqlt.StructField("num_docks_available", pysqlt.IntegerType()),
        pysqlt.StructField("numDocksAvailable", pysqlt.IntegerType()),
        pysqlt.StructField("is_installed", pysqlt.IntegerType()),
        pysqlt.StructField("is_returning", pysqlt.IntegerType()),
        pysqlt.StructField("is_renting", pysqlt.IntegerType()),
        pysqlt.StructField("last_reported", pysqlt.TimestampType())
    ])

    kafka_df = (kafka_df
                .select(pysqlf.from_json(pysqlf.col("value").cast("string"), schema).alias("value"))
                .withColumn("stationCode", pysqlf.col("value.stationCode"))
                .withColumn("station_id", pysqlf.col("value.station_id"))
                .withColumn("stationCode", pysqlf.col("value.stationCode"))
                .withColumn("num_bikes_available", pysqlf.col("value.num_bikes_available"))
                .withColumn("numBikesAvailable", pysqlf.col("value.numBikesAvailable"))
                .withColumn("num_bikes_available_types", pysqlf.col("value.num_bikes_available_types"))
                .withColumn("num_docks_available", pysqlf.col("value.num_docks_available"))
                .withColumn("numDocksAvailable", pysqlf.col("value.numDocksAvailable"))
                .withColumn("is_installed", pysqlf.col("value.is_installed"))
                .withColumn("is_returning", pysqlf.col("value.is_returning"))
                .withColumn("is_renting", pysqlf.col("value.is_renting"))
                .withColumn("last_reported", pysqlf.col("value.last_reported"))
                .withColumn("mechanical ", pysqlf.col("num_bikes_available_types").getItem(0).getItem("mechanical"))
                .withColumn("ebike ", pysqlf.col("num_bikes_available_types").getItem(1).getItem("ebike"))
                )

    df_station_informations = spark.read.csv("stations_information.csv", header=True)

    kafka_df = (kafka_df
                .join(df_station_informations, on=["stationCode", "station_id"], how="left")
                )

    results_postcodes = []
    final_data = []


    def process_batch(df, epoch_id):

        postcodes = df.select("postcode").distinct().collect()
        for row in postcodes:
            results_postcodes.append(row["postcode"])
        for postcode in results_postcodes:
            postcode_df = df.filter(col("postcode") == postcode)
            postcode_df = postcode_df.dropDuplicates(subset=['stationCode'])
            print(postcode_df.show())
            nbr_total = 0
            nbr_elec = 0
            nbr_meca = 0
            print(nbr_total)

            for row in postcode_df.collect():
                nbr_total += int(row['num_bikes_available'])
                print(nbr_total)
                nbr_elec += int(row['num_bikes_available_types'][0]['mechanical'])
                nbr_meca += int(row['num_bikes_available_types'][1]['ebike'])

            final_data.append({
                "postcode": postcode,
                "nbr_total": nbr_total,
                "nbr_elec": nbr_elec,
                "nbr_meca": nbr_meca
            })


    kafka_df.writeStream \
        .outputMode("update") \
        .foreachBatch(process_batch) \
        .start()

    while True:
        if final_data != []:
            final_df = spark.createDataFrame(final_data)
            final_data = []

            col_selections = ["postcode", "nbr_total", "nbr_elec", "nbr_meca"]

            df_out = (final_df
                      .withColumn("value", pysqlf.to_json(pysqlf.struct(*col_selections)))
                      .select("value")
                      )

            df_out.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("topic", "velib-projet-final-data") \
                .save()

        time.sleep(1)

out.awaitTermination()