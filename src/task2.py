from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, round

spark = SparkSession.builder.appName("EngagementByAge").getOrCreate()

# Read input files
posts_df = spark.read.option("header", True).option("inferSchema", True).csv("input/posts.csv")
users_df = spark.read.option("header", True).option("inferSchema", True).csv("input/users.csv")

# Join posts to users
joined_df = posts_df.join(users_df, on="UserID", how="inner")

# Calculate average likes and retweets by age group
engagement_by_age = (
    joined_df
    .groupBy("AgeGroup")
    .agg(
        round(avg(col("Likes")), 2).alias("avg_likes"),
        round(avg(col("Retweets")), 2).alias("avg_retweets")
    )
    .orderBy("AgeGroup")
)

# Show results in terminal
engagement_by_age.show(truncate=False)

# Save output
engagement_by_age.coalesce(1).write.mode("overwrite").option("header", True).csv(
    "outputs/engagement_by_age.csv"
)

spark.stop()