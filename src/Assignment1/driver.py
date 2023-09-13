import pyspark
from pyspark.sql import SparkSession
from utils import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = create_spark()

dataDF = [(('James','','Smith'),'03011998','M',3000),
  (('Michael','Rose',''),'10111998','M',20000),
  (('Robert','','Williams'),'02012000','M',3000),
  (('Maria','Anne','Jones'),'03011998','F',1100),
  (('Jen','Mary','Brown'),'04101998','F',10000)
]

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
schema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('dob', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])
#createing a data frame
df = dataframe(spark,dataDF,schema)
df.show()
#Select firstname, lastname and salary from Dataframe.
selected = selecting_col(df,df.name.firstname,df.name.lastname,df.salary)
#selected.show()
#Add Country, department, and age column in the dataframe.
df = creating_col(df,'country','USA')
df = creating_col(df,'department','ECE')
df = creating_col(df,'age',22)
#df.show()
#Change the value of salary column.
df=add_salary(df,'salary','100')
#df.show()
#Change the data types of DOB and salary to String
df = changing_data_type(df,'dob','string')
df = changing_data_type(df,'salary','string')
#df.show()
#Derive new column from salary column.
df = new_column(df,'newsalary','salary','2000')
#df.show()

#Filter the name column whose salary in maximum.
df = changing_data_type(df,'salary','integer')
df = max_salary(df,'salary')
#df.show()
#Drop the department and age column.
df = droping_column(df,'department')
df = droping_column(df,'age')
#df.show()
#List out distinct value of dob and salary
df1=duplicate(df,column=('dob'))
df2=duplicate(df,column=('salary'))

