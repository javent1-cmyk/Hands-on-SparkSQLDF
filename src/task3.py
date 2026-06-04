from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, round

spark = SparkSession.builder.appName("SentimentEngagement").getOrCreate()

# Read posts data
posts_df = spark.read.option("header", True).option("inferSchema", True).csv("input/posts.csv")

# Calculate engagement by sentiment score
sentiment_engagement = (
    posts_df
    .groupBy("SentimentScore")
    .agg(
        round(avg(col("Likes")), 2).alias("avg_likes"),
        round(avg(col("Retweets")), 2).alias("avg_retweets"),
        round(avg(col("Likes") + col("Retweets")), 2).alias("avg_total_engagement")
    )
    .orderBy("SentimentScore")
)

# Show output in terminal
sentiment_engagement.show(truncate=False)

# Save output
sentiment_engagement.coalesce(1).write.mode("overwrite").option("header", True).csv(
    "outputs/sentiment_engagement.csv"
)

spark.stop()