import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark():
    spark = SparkSession.builder.appName('Pyspark_Assignment3').getOrCreate()
    return spark

def dataframe(spark,dataDF,schema):
    data = spark.createDataFrame(data = dataDF, schema = schema)
    return data

def first_row(df):
    firstrow = df.select(df.department).show(1)
    return firstrow