# Hands-on SparkSQL Assignment

## Assignment Title

Hands-on SparkSQL: Social Media Engagement Analysis

## Dataset Description

This assignment uses two generated CSV datasets: `posts.csv` and `users.csv`.

The `posts.csv` file contains social media post-level data, including post ID, user ID, post content, timestamp, likes, retweets, hashtags, and sentiment score.

The `users.csv` file contains user-level data, including user ID, username, age group, location, and verified status.

Together, these datasets are used to practice Spark SQL and PySpark DataFrame operations such as reading CSV files, joining datasets, grouping data, aggregating metrics, filtering records, and writing output files.

## Project Structure

```text
Hands-on-SparkSQL/
│
├── input/
│   ├── posts.csv
│   └── users.csv
│
├── outputs/
│   ├── hashtag_trends.csv
│   ├── engagement_by_age.csv
│   ├── sentiment_engagement.csv
│   └── top_verified_users.csv
│
├── src/
│   ├── task1.py
│   ├── task2.py
│   ├── task3.py
│   └── task4.py
│
├── input_generater.py
└── README.md