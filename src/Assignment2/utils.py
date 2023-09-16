import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark():
    spark =  SparkSession.builder.appName("NonNestedDataFrame").getOrCreate()
    return spark

def Total_Amount(df):
    Amount = df.groupBy('product').pivot('country').agg(sum('amount'))
    return Amount

def Unpiviot(pivotdf):
    Unpiviotting = pivotdf.select('product',expr("stack(6,'china',China,'India',India,'Sweden',Sweden,'UAE',UAE,'UK',UK,'USA',USA) as (country,amount)")).where("amount is not null")
    return Unpiviotting