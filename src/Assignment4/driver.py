import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from Pyspark_Repo.src.Assignment4.utils import *

spark = create_spark()

dataDF = [('WashingMashine',1648770933000,20000,'Samsung','India',1),
          ('Refrigerator',1648770999000,35000,' LG',None,2),
          ('AirCooler',1648770948000,45000,' Voltas',None,3)
  ]
schema=["Product Name","Issue Date","Price","Brand","Country","Product number"]

colName1="Issue Date"
colName="Country"
ColName2="Brand"

df = dataframe(spark,dataDF,schema)
#df.show()

#Convert the Issue Date with the timestamp format.
df = format_date(df,colName1)
df.show()

#Convert timestamp to date type
df = format_type(df,colName1)
df.show()

#Remove the starting extra space in Brand column for LG and Voltas fields
df = trim_str(df,ColName2)
df.show()

#Replace null values with empty values in Country column
df = replace_null(df,colName)
df.show()





