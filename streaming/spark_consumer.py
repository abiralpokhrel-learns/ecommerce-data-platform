"""Spark Structured Streaming: consume orders from Kafka, aggregate, write to Postgres."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, sum as _sum, avg
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType


def main():
    spark = (
        SparkSession.builder
        .appName("OrdersStreamConsumer")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.7.1")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    
    schema = (StructType()
              .add("order_id", StringType())
              .add("customer_id", StringType())
              .add("product_category", StringType())
              .add("item_count", IntegerType())
              .add("order_value", DoubleType())
              .add("freight_value", DoubleType())
              .add("state", StringType())
              .add("event_time", TimestampType()))
    
    # Read from Kafka
    df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")
          .option("subscribe", "orders_stream")
          .option("startingOffsets", "latest")
          .load())
    
    parsed = (df.select(from_json(col("value").cast("string"), schema).alias("data"))
              .select("data.*"))
    
    # 1-minute tumbling window aggregations
    windowed = (parsed
                .withWatermark("event_time", "2 minutes")
                .groupBy(window(col("event_time"), "1 minute"), col("state"))
                .agg(
                    count("*").alias("order_count"),
                    _sum("order_value").alias("total_revenue"),
                    avg("order_value").alias("avg_order_value")
                )
                .selectExpr("window.start as window_start",
                            "window.end as window_end",
                            "state", "order_count", "total_revenue", "avg_order_value"))
    
    # Write to Postgres
    def write_to_postgres(batch_df, batch_id):
        (batch_df.write
         .format("jdbc")
         .option("url", "jdbc:postgresql://postgres-warehouse:5432/warehouse")
         .option("dbtable", "streaming.orders_metrics")
         .option("user", "warehouse_user")
         .option("password", "warehouse_pass")
         .option("driver", "org.postgresql.Driver")
         .mode("append")
         .save())
    
    query = (windowed.writeStream
             .foreachBatch(write_to_postgres)
             .outputMode("update")
             .trigger(processingTime="30 seconds")
             .start())
    
    query.awaitTermination()


if __name__ == "__main__":
    main()