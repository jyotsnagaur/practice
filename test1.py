import pyspark as pyspark

#pip install pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lit, struct, collect_list, count, array_contains

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read JSON Files") \
    .getOrCreate()

cars_df = spark.read.option("multiline", "true").json("q1.json")
owners_df = spark.read.option("multiline", "true").json("q2.json")

# Show the DataFrame
cars_df.show(truncate=False)
owners_df.show(truncate=False)

# Explode the "type" array in the cars DataFrame
cars_df = cars_df.select(explode(col("type")).alias("car"))

# Explode the "persons" array in the owners DataFrame
owners_df = owners_df.select(explode(col("persons")).alias("person"))

cars_df.show(truncate=False)
owners_df.show(truncate=False)


# Join the DataFrames based on the common key "name"
joined_df = owners_df.join(cars_df, array_contains(col("person.owns"), col("car.name")), "inner")
joined_df.show(truncate=False)

# Group by person's name to calculate the count of cars owned by each person
output_df = joined_df.groupBy("person.name").agg(
    count("car.name").alias("count"),
    collect_list(
        struct(col("car.name").alias("name"), col("car.min").alias("min"), col("car.max").alias("max"))
    ).alias("owns")
).select(
    lit("Owner_cars").alias("id"),
    col("person.name").alias("name"),
    col("count"),
    col("owns")
)

# # Show the DataFrame
# output_df.show(truncate=False)




spark.stop()

