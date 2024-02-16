import pyspark as pyspark

#pip install pyspark
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read JSON Files") \
    .getOrCreate()

df1 = spark.read.option("multiline", "true").json("q1.json")
df2 = spark.read.option("multiline", "true").json("q2.json")
# Show the DataFrame
df1.show(truncate=False)
df2.show(truncate=False)
spark.stop()

# df1 = spark.read.json("q1.json")
# df2 = spark.read.json("q2.json")
#
# df1.show(truncate=False)
# df2.show(truncate=False)
# #joined_df = df1.join(df2, df1.common_key == df2.common_key, "inner")
# #joined_df.show()