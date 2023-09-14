import pyspark
from pyspark.sql import SparkSession
from utils import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


spark = create_spark()

schema = StructType([
    StructField("product", StringType(), nullable=False),
    StructField("amount", IntegerType(), nullable=False),
    StructField("country", StringType(), nullable=False)
])

#Create the DataFrame

data = [
    ("Banana", 1000, "USA"),
    ("Carrots", 1500, "India"),
    ("Beans", 1600, "Sweden"),
    ("Orange",2000,"UK"),
    ("Orange",2000,"UAE"),
    ("Banana",400,"China"),
    ("Carrots",1200,"China")
]

df = spark.createDataFrame(data, schema=schema)
# df.show()

#Find total amount exported to each country of each product.
df1=Total_Amount(df)
df1.show()

#Perform unpivot function on output of question
Unpiviot(df1).show()
