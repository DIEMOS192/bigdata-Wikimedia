from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, avg, col, lower, sum, desc, explode, split, regexp_replace, rank
from pyspark.sql.window import Window


def _get_df(input_path):
    spark = SparkSession.builder.getOrCreate()
    return spark.read \
        .option("delimiter", " ") \
        .option("inferSchema", "true") \
        .csv(input_path) \
        .toDF("project_code", "page_title", "page_hits", "page_size")


def df_page_size_stats(input_path):
    df = _get_df(input_path)
    return df.agg(min("page_size"), max("page_size"), avg("page_size")).collect()[0]


def df_image_counts(input_path):
    df = _get_df(input_path)
    image_df = df.filter(
        lower(col("page_title")).endswith(".jpg") |
        lower(col("page_title")).endswith(".png") |
        lower(col("page_title")).endswith(".gif")
    )
    return {
        "total_image_pages": image_df.count(),
        "non_english_image_pages": image_df.filter(col("project_code") != "en").count()
    }


def df_top_terms(input_path):
    df = _get_df(input_path)
    return df.select(
        explode(split(lower(regexp_replace(col("page_title"), "[^a-z0-9_]", "")), "_")).alias("term")
    ).filter(col("term") != "") \
     .groupBy("term") \
     .count() \
     .orderBy(desc("count")) \
     .limit(10) \
     .collect()


def df_top_projects(input_path):
    df = _get_df(input_path)
    return df.groupBy("project_code") \
             .agg(sum("page_hits").alias("total_hits")) \
             .orderBy(desc("total_hits")) \
             .limit(5) \
             .collect()


def df_top_title_per_project(input_path):
    df = _get_df(input_path)
    window = Window.partitionBy("project_code").orderBy(desc("page_hits"))
    return df.withColumn("rnk", rank().over(window)) \
             .filter(col("rnk") == 1) \
             .drop("rnk") \
             .collect()