import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def extract_from_catalog(database_name, table_name):
    # Script generated for node Data Catalog table
    raw_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=table_name,
    )

    # spark dataframe
    df = raw_dynamic_frame.toDF()

    return df


def clean_data(df):
    """change column type, drop unnecessary columns,
    remove nulls and duplicates"""
    df = df.dropDuplicates(['Name', 'Year', 'Genre', 'Platform', 'Publisher'])

    df = (
        df.withColumn("Year", df["Year"].cast(IntegerType()))
        .drop("Name")
        .drop("Publisher")
        .drop("Rank")
    )

    # remove nulls, and filter by year
    df_final = df.filter(
        (F.col("Year").isNotNull())
        & (F.col("Genre").isNotNull())
        & (F.col("Platform").isNotNull())
        & (df["Year"] <= 2015)
    )

    return df_final


def group_data(df):
    """group the data by platform and genre"""
    df_group = (
        df.groupBy("Year", "Platform", "Genre")
        .agg(
            F.count("*").alias("Total_games"),
            F.round(F.sum("NA_Sales"), 2).alias(
                'North_america_sales(millions)'
            ),
            F.round(F.sum("EU_Sales"), 2).alias("Europe_sales(millions)"),
            F.round(F.sum("JP_Sales"), 2).alias("Japan_sales(millions)"),
            F.round(F.sum("Other_Sales"), 2).alias(
                "Rest_of_world_sales(millions)"
            ),
            F.round(F.sum("Global_Sales"), 2).alias(
                "Worldwide_sales(millions)"
            ),
        )
        .orderBy("Year")
    )

    return df_group


def create_final_report(df):
    """select the top genre in term of sales for each year and platform"""
    sales_window = Window.partitionBy("Year", "Platform").orderBy(
        F.col("Worldwide_sales(millions)").desc()
    )

    final_df = (
        df.withColumn("genre_rank", F.rank().over(sales_window))
        .filter(F.col("genre_rank") == 1)
        .select(
            "Year",
            "Platform",
            "Genre",
            "Total_games",
            "North_america_sales(millions)",
            "Europe_sales(millions)",
            "Japan_sales(millions)",
            "Rest_of_world_sales(millions)",
            "Worldwide_sales(millions)",
        )
        .orderBy("Year")
    )
    return final_df


def load_to_s3(dynamic_df, table_name, database_name, output_path):
    """
    To save the data to S3 and create table on athena
    """
    s3 = glueContext.getSink(
        path=f"{output_path}/{table_name}",
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
        enableUpdateCatalog=True,
    )
    s3.setCatalogInfo(
        catalogDatabase=database_name, catalogTableName=table_name
    )
    s3.setFormat("glueparquet")
    s3.writeFrame(dynamic_df)


def main():
    database_name = "video-game-sales-database"
    output_table_name = "video-game-sales-report"
    output_path = "s3://video-game-etl/report"
    input_table_name = "raw_data"

    df = extract_from_catalog(database_name, input_table_name)
    df_clean = clean_data(df)
    df_group = group_data(df_clean)
    df_final = create_final_report(df_group)

    # from Spark dataframe to glue dynamic frame
    glue_dynamic_frame_final = DynamicFrame.fromDF(
        df_final, glueContext, "glue_etl_vg_sales"
    )

    load_to_s3(
        glue_dynamic_frame_final, output_table_name, database_name, output_path
    )


main()

job.commit()
