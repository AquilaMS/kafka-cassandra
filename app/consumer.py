from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#data schema
schema = StructType([ 
    StructField("user_id",StringType(),True), \
    StructField("name",StringType(),True), \
    StructField("email",StringType(),True), \
    StructField("location",StringType(),True), \
    StructField("country", StringType(), True), 
    StructField("job", StringType(), True), 
    StructField("spent", IntegerType(), True), 
  ])

#building a spark app session
spark = SparkSession\
   .builder\
    .appName('kafka_cass')\
    .config('spark.connection.host', 'localhost')\
    .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1')\
    .config('spark.jars.packages','com.datastax.spark:spark-cassandra-connector_2.12:3.2.0')\
    .config('spark.cassandra.connection.host', 'localhost')\
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

#read stream from source in json 
lines = spark.readStream\
    .format('kafka')\
    .option('kafka.bootstrap.servers', '127.0.0.1:9092')\
    .option('subscribe', 'source_userdata')\
    .option("startingOffsets", "earliest")\
    .option('failOnDataLoss', 'false')\
    .load()\
    .select(from_json(col('value').cast('string'), schema).alias('users'))

#select from source which data is relevant 
get_columns = lines.select(
  col('users.user_id').alias('user_id'),
  col('users.name').alias('name'),
  col('users.email').alias('email'),
  col('users.spent').alias('spent')
)
  
get_columns.printSchema()

#select usefull data, parse into json and send to other kafka's topic 
write_to_topic = get_columns\
  .select('user_id','name', 'email','spent')\
  .select(to_json(struct(col('user_id'),col('name'), col('email'), col('spent'))).alias('value'))\
  .writeStream\
  .format('kafka')\
  .option('kafka.bootstrap.servers', '127.0.0.1:9092')\
  .option('topic', 'userdata_spent_processed')\
  .option('checkpointLocation', 'cp-userdata-spent-kafka')\
  .outputMode('append')\
  .start()\

write_to_cassandra = get_columns\
    .writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .outputMode("append") \
    .option("checkpointLocation", "cp-userdata-spent-cassandra") \
    .option("keyspace", "ks_user_spent") \
    .option("table", "user_spent") \
    .start()
#kafka's checkpoints is stored in hdfs


spark.streams.awaitAnyTermination()