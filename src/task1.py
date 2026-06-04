from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, trim

spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Read posts data
posts_df = spark.read.option("header", True).option("inferSchema", True).csv("input/posts.csv")

# Split hashtags into individual hashtag rows and count them
hashtag_counts = (
    posts_df
    .select(explode(split(col("Hashtags"), ",")).alias("Hashtag"))
    .select(trim(col("Hashtag")).alias("Hashtag"))
    .groupBy("Hashtag")
    .count()
    .orderBy(col("count").desc())
)

# Show output in terminal
hashtag_counts.show(truncate=False)

# Save output
hashtag_counts.coalesce(1).write.mode("overwrite").option("header", True).csv(
    "outputs/hashtag_trends.csv"
)

spark.stop()