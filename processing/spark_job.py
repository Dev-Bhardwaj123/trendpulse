"""Apache Spark batch job: score sentiment (VADER) + aggregate trending terms.

Reads posts from Postgres (persisted store), computes per-post sentiment and
term-level average sentiment, writes spark_post_sentiment + spark_trends back to
Postgres. (Spark can also consume Kafka raw.posts directly; the persisted store
is used here so the job is reproducible after a Kafka topic reset.)
Runs locally (PySpark); swappable to Databricks at deploy via env vars.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

PG_URL = os.environ.get("SPARK_PG_URL", "jdbc:postgresql://127.0.0.1:5432/trendpulse")
PG_PROPS = {"user": "trend", "password": "trend", "driver": "org.postgresql.Driver"}

STOP = {"the", "and", "for", "with", "that", "this", "you", "are", "was", "but",
        "not", "have", "has", "from", "your", "all", "out", "get", "got", "now",
        "die", "und", "der", "das", "ich", "ist", "nicht", "ein", "eine",
        "que", "los", "las", "con", "una", "por", "para", "como", "she", "his",
        "her", "they", "them", "who", "what", "when", "why", "how", "can", "will",
        "just", "like", "about", "into", "more", "show", "game", "new", "one"}

_analyzer = SentimentIntensityAnalyzer()


@F.udf(FloatType())
def sentiment(text):
    if not text:
        return 0.0
    return float(_analyzer.polarity_scores(text)["compound"])


def main():
    spark = (SparkSession.builder
             .appName("trendpulse-spark")
             .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
             .config("spark.driver.memory", "2g")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    posts = (spark.read.jdbc(PG_URL, "api_post", properties=PG_PROPS)
             .select("source", "external_id", "title")
             .filter(F.col("title").isNotNull()))

    scored = posts.withColumn("sentiment", sentiment(F.col("title")))

    (scored.select("source", "external_id", "title", "sentiment")
     .write.jdbc(PG_URL, "spark_post_sentiment", mode="overwrite", properties=PG_PROPS))

    words = (scored.select(
                F.explode(F.split(F.lower(F.col("title")), r"[^a-z0-9#+]+")).alias("term"),
                "sentiment")
             .filter(F.length("term") >= 3))
    words = words.filter(~F.col("term").isin(list(STOP)))

    agg = (words.groupBy("term")
           .agg(F.count("*").alias("count"),
                F.round(F.avg("sentiment"), 3).alias("avg_sentiment"))
           .orderBy(F.desc("count"))
           .limit(40))

    agg.write.jdbc(PG_URL, "spark_trends", mode="overwrite", properties=PG_PROPS)

    total = scored.count()
    rows = agg.count()
    avg = scored.agg(F.round(F.avg("sentiment"), 3)).first()[0]
    print(f"[spark] posts_scored={total} trend_terms={rows} overall_avg_sentiment={avg}")
    spark.stop()


if __name__ == "__main__":
    main()
