import argparse
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as psf
from pyspark.sql.window import Window

from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)

def clean(spark: SparkSession, environment: str, tag: str, user: str = "Robbe"):
    if environment == "local":
        path_data = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../../../data"
        )
    else:
        path_data = "s3a://dataminded-academy-capstone-llm-data/"
    path_data_raw   = f"{path_data}/input/{tag}/"
    path_data_clean = f"{path_data}/cleaned/{user}/{tag}/"

    questions = (
        spark.read
        .json(path_data_raw + "questions.json")
        .select(psf.explode("items").alias("item"))
        .select("item.*")
        .select("question_id", "title", "body", "accepted_answer_id")
    )

    answers = (
        spark.read
        .json(path_data_raw + "answers.json")
        .select(psf.explode("items").alias("item"))
        .select("item.*")
        .select("answer_id", "question_id", "body", "score")
    )

    answer_window = Window.partitionBy("question_id").orderBy(
        psf.when(
            psf.col("answer_id") == psf.col("accepted_answer_id"), 1
        ).otherwise(0).desc(),
        psf.col("score").desc_nulls_last(),
    )

    best_answers = (
        answers.join(
            questions.select("question_id", "accepted_answer_id"),
            on="question_id",
            how="inner", # Only keep questions with answer
        )
        .withColumn("rank", psf.row_number().over(answer_window))
        .where(psf.col("rank") == 1)
        .select(
            "question_id",
            psf.col("body").alias("response_body"),
        )
    )

    merged = (questions
        .join(best_answers, on="question_id", how="inner")
        .withColumnRenamed("body", "question_body")
        .select("question_id", "title", "question_body", "response_body")
        # .select("title", "question_body", "response_body")
    )

    # Write to one file per row
    (merged
        .repartition(merged.count())
        .write.mode("overwrite").json(path_data_clean)
    )

def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=False, default="local"
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
    }
    if args.env == "local":
        print("This is a local execution of the capestonellm project")
        builder = SparkSession.builder.appName("Spark S3 Integration").config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2"
        )
        for key, value in common_spark_config.items():
            builder = builder.config(key, value)
        session = builder.getOrCreate()
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
