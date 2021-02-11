import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.master("local[*]") \
    .appName('Car sales report') \
    .getOrCreate()

# Create schema for dataframe
schema = StructType([ \
    StructField('incident_id', IntegerType(), False), \
    StructField('incident_type', StringType(), False), \
    StructField('vin', StringType(), False), \
    StructField('make', StringType(), True), \
    StructField('model', StringType(), True), \
    StructField('year', IntegerType(), True), \
    StructField('incident_date', DateType(), False), \
    StructField('description', StringType(), False) \
])

# Load dataframe and register as table
df = spark.read.format('csv') \
    .schema(schema = schema) \
    .load('/input/data.csv')
df.registerTempTable("auto_data")

# Create vin lookup dataframe
lookup_df = spark.sql("SELECT vin, MAX(make), MAX(year) FROM auto_data GROUP BY vin") \
    .withColumnRenamed('max(make)', 'make_filled') \
    .withColumnRenamed('max(year)', 'year_filled')

# Join lookup table to df to fill in missing data
joined_df = df.join(lookup_df, on = 'vin', how = 'inner') \
    .select('incident_type', 'make_filled', 'year_filled') \
    .withColumnRenamed('make_filled', 'make') \
    .withColumnRenamed('year_filled', 'year')

# Filter to accident records only, register table, and get count of accidents by make and year
accident_df = joined_df.filter(F.col('incident_type') == 'A')
accident_df.registerTempTable('accidents')
result_df = spark.sql("SELECT make, year, count(incident_type) as accidents FROM accidents GROUP BY make, year")

# Write to output csv file
result_df.coalesce(1) \
      .write \
      .option("header","true") \
      .option("sep",",") \
      .mode("overwrite") \
      .csv("/spark-output/autoinc")
