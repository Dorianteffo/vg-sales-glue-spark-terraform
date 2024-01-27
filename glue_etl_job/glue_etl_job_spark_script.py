import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Data Catalog table
DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="video-games-data",
    table_name="initial_dataset",
    transformation_ctx="DataCatalogtable_node1",
)

## spark dataframe
df = DataCatalogtable_node1.toDF()


def clean_data(df) : 
    """change column type, drop unnecessary columns, remove null records and duplicates"""
    df = df.dropDuplicates(['Name','Year','Genre','Platform', 'Publisher'])
    
    df = df.withColumn("Year", df["Year"].cast(IntegerType())) \
           .drop("Name") \
           .drop("Publisher") \
           .drop("Rank")
    
    ## remove nulls, and filter by year (only before 2015 because the data are not relevant above that)
    df_final = df.filter((F.col("Year").isNotNull()) & (F.col("Genre").isNotNull()) & (F.col("Platform").isNotNull()) 
    & (df["Year"]<=2015))
    
    return df_final
    

def group_data(df): 
    """group the data by platform and genre"""
    df_group = df \
    .groupBy("Year","Platform","Genre") \
    .agg(F.count("*").alias("Total_games"), F.round(F.sum("NA_Sales"),2).alias('North_america_sales(millions)'), \
    F.round(F.sum("EU_Sales"),2).alias("Europe_sales(millions)"), F.round(F.sum("JP_Sales"),2).alias("Japan_sales(millions)"), \
    F.round(F.sum("Other_Sales"),2).alias("Rest_of_world_sales(millions)"), F.round(F.sum("Global_Sales"),2).alias("Worldwide_sales(millions)")) \
    .orderBy("Year")
    
    return df_group
    

def create_final_report(df): 
    """select the top genre in term of sales for each year and platform"""
    sales_window = Window.partitionBy("Year","Platform").orderBy(F.col("Worldwide_sales(millions)").desc())
    
    final_df = df.withColumn("genre_rank",F.rank().over(sales_window)) \
                  .filter(F.col("genre_rank")==1) \
                  .select("Year","Platform","Genre","Total_games","North_america_sales(millions)","Europe_sales(millions)",
                            "Japan_sales(millions)","Rest_of_world_sales(millions)","Worldwide_sales(millions)") \
                  .orderBy("Year")
    return final_df
                  

              
df_clean = clean_data(df)
df_group = group_data(df_clean)
df_final = create_final_report(df_group)


#from Spark dataframe to glue dynamic frame
glue_dynamic_frame_final = DynamicFrame.fromDF(df_final, glueContext, "glue_etl_vg_sales")
    

# Script generated for node S3 bucket
S3bucket_node2 = glueContext.getSink(
    path="s3://video-game-etl/parquet-format-data/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node2",
)
S3bucket_node2.setCatalogInfo(
    catalogDatabase="video-games-data",
    catalogTableName="vg-sales-report-parquet-format",
)
S3bucket_node2.setFormat("glueparquet")
S3bucket_node2.writeFrame(glue_dynamic_frame_final)
job.commit()
