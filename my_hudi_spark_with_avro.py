from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from schema_class import Schema_Class
from pyspark.sql.avro.functions import from_avro
from config import KAFKA_BROKER,KAFKA_TOPIC,DATABASE_NAME



db_name = DATABASE_NAME
table_name = KAFKA_TOPIC


recordkey = 'emp_id'
pyspark_kafka_check_point_file = f"s3a://pyspark-kafka-consumers-check-point/{table_name}"
path = f"s3a://{db_name}/{table_name}"

table_type = "COPY_ON_WRITE"


hudi_options = {
    'hoodie.table.name': table_name,
    'hoodie.datasource.write.recordkey.field': recordkey,
    'hoodie.datasource.write.table.name': table_name,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'ts',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2,
    'hoodie.parquet.max.file.size':125829120,
    'hoodie.parquet.small.file.limit':	104857600
}

spark = SparkSession.builder \
    .master("local") \
    .appName("kafka-example") \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('className', 'org.apache.hudi') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9095")           \
    .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")                           \
    .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key" )                          \
    .config("spark.hadoop.fs.s3a.path.style.access", "true")                        \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")   \
    .config('spark.sql.warehouse.dir', path) \
    .getOrCreate()
    

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("includeHeaders", "true") \
    .option("failOnDataLoss", "false") \
    .load()


def process_batch_message(df, batch_id):

    _, latest_version_wikimedia = Schema_Class().get_schema_from_schema_registry()
    
    df = df.withColumn("magicByte", func.expr("substring(value, 1, 1)"))

    #  get schema id from value
    df = df.withColumn("valueSchemaId", func.expr("substring(value, 2, 4)"))

    # remove first 5 bytes from value
    df = df.withColumn("fixedValue", func.expr("substring(value, 6, length(value)-5)"))

    # creating a new df with magicBytes, valueSchemaId & fixedValue
    final_df = df.select("magicByte", "valueSchemaId", "fixedValue")
    
    fromAvroOptions = {"mode":"PERMISSIVE"}
    decoded_output = final_df.select(
                                    from_avro(
                                        func.col("fixedValue"), latest_version_wikimedia.schema.schema_str,fromAvroOptions
                                            )
                                    .alias("avro_decoded")
                                    )
    avro_decode_df = decoded_output.select("avro_decoded.*")
    avro_decode_df.printSchema()
    
    if avro_decode_df.count() >0:
        avro_decode_df.write.format("hudi"). \
            options(**hudi_options). \
            mode('append'). \
            save(path)
            
    print("batch_id : ", batch_id, avro_decode_df.show(truncate=False))

query = kafka_df.writeStream \
    .foreachBatch(process_batch_message) \
    .option("checkpointLocation",  pyspark_kafka_check_point_file) \
    .trigger(processingTime="30 second") \
    .start().awaitTermination()
    

