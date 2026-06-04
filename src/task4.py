from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, lower

spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Read input files
posts_df = spark.read.option("header", True).option("inferSchema", True).csv("input/posts.csv")
users_df = spark.read.option("header", True).option("inferSchema", True).csv("input/users.csv")

# Join posts with users on UserID
joined_df = posts_df.join(users_df, on="UserID", how="inner")

# Filter for verified users
# This handles Verified values like True, true, TRUE, or boolean true
verified_df = joined_df.filter(lower(col("Verified").cast("string")) == "true")

# Calculate total engagement by verified user
top_verified_users = (
    verified_df
    .groupBy("UserID", "Username")
    .agg(
        spark_sum(col("Likes")).alias("total_likes"),
        spark_sum(col("Retweets")).alias("total_retweets"),
        spark_sum(col("Likes") + col("Retweets")).alias("total_engagement")
    )
    .orderBy(col("total_engagement").desc())
)

# Show results in terminal
top_verified_users.show(truncate=False)

# Save output
top_verified_users.coalesce(1).write.mode("overwrite").option("header", True).csv(
    "outputs/top_verified_users.csv"
)

spark.stop()