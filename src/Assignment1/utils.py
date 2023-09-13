import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col,max


def create_spark():
    spark =  SparkSession.builder.appName("Pyspark_Assignment1").getOrCreate()
    return spark
def dataframe(spark,dataDF,schema):
    data = spark.createDataFrame(data = dataDF, schema = schema)
    return data
def selecting_col(df,firstname,lastname,salary):
    selected =df.select(firstname,lastname,salary)
    return selected
def creating_col(df,columname,columnvalue):
    creating = df.withColumn(columname,lit(columnvalue))
    return creating
def add_salary(df,salary,value):
    adding_salary = df.withColumn(salary,col('salary')+value)
    return adding_salary
def changing_data_type(df,column,type):
    change_type = df.withColumn(column,col(column).cast(type))

    return change_type
def new_column(df,newsalary,salary,value):
    adding_sal_col = df.withColumn('newsalary',col(salary)+value)
    return adding_sal_col

def max_salary(df,salary):
    filtering_col = df.select(max(salary))
    return filtering_col
def droping_column(df,column):
    drop_column = df.drop(column)
    return drop_column

def duplicate(df,column):
    df1=df.select(column).distinct()
    return df1







