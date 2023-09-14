import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from Pyspark_Repo.src.Assignment3.utils import *

spark = create_spark()

dataDF = [('James','sales',3000),
   ('Michael','sales',4600),
   ('Robert','sales',4100),
   ('Maria','finance',3000),
   ('Raman','finance',3000),
   ('Scott','finance',3300),
   ('Jen','finance',3900),
   ('Jeff','marketing',3000),
   ('Kumar','marketing',2000)
]

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
schema = StructType([
         StructField('employeename', StringType(), True),
         StructField('department', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])
#createing a data frame
df = dataframe(spark,dataDF,schema)
df.show()

#Select first row from each department group.
df2 = first_row(df)
df2.show()


