# Databricks notebook source
from pyspark.sql.functions import col ,coalesce
from pyspark.sql.types import StringType ,IntegerType,StructType,StructField
from delta.tables import *



#---------------source df---------------------------------------
schema=StructType(
[StructField("ID",IntegerType(),nullable=True),
StructField("Name",StringType(),nullable=True),
StructField("Location",StringType(),nullable=True),
StructField("Mobile_No",IntegerType(),nullable=True)]
)

Data=(
     [1,"Jagadesh","AndraPradesh",9999],
     [2,"Vishal","pune",7777],
     [3,"venkat","Telangana",6666],
     [4,"Kishore","Mumbai",5555],
     [4,"Kishore","Mumbai",5555],
     [2,"Vishal","pune",7777]

)

source_df=spark.createDataFrame(data=Data,schema=schema)
source_withoutdup=source_df.dropDuplicates(subset=["ID"])
source_withoutdup.write.mode("overwrite").format("delta").save("dbfs:/file/source")





#-----------------------------------target df------------------------------
schema=StructType(
[StructField("ID",IntegerType(),nullable=True),
StructField("Name",StringType(),nullable=True),
StructField("Location",StringType(),nullable=True),
StructField("Mobile_No",IntegerType(),nullable=True)]
)

Data=(
     [1,"Jagadesh","AndraPradesh",9497],
     [2,"Vishal","Delhi",7777],
     [3,"venkat","Hyderabad",969696]

)

target_df=spark.createDataFrame(data=Data,schema=schema)
target_df.write.mode("overwrite").format("delta").save("dbfs:/file/target")


# COMMAND ----------

print("source data")
source = DeltaTable.forPath(spark, "dbfs:/file/source")
source.toDF().show()
print("target data")
print("Before Merge Statment")
target = DeltaTable.forPath(spark, "dbfs:/file/target")
target.toDF().show()


source data
+---+--------+------------+---------+
| ID|    Name|    Location|Mobile_No|
+---+--------+------------+---------+
|  1|Jagadesh|AndraPradesh|     9999|
|  2|  Vishal|        pune|     7777|
|  3|  venkat|   Telangana|     6666|
|  4| Kishore|      Mumbai|     5555|
+---+--------+------------+---------+

target data
Before Merge Statment
+---+--------+------------+---------+
| ID|    Name|    Location|Mobile_No|
+---+--------+------------+---------+
|  1|Jagadesh|AndraPradesh|     9497|
|  3|  venkat|   Hyderabad|   969696|
|  2|  Vishal|       Delhi|     7777|
+---+--------+------------+---------+





# COMMAND ----------

from delta.tables import *

target = DeltaTable.forPath(spark, "dbfs:/file/target")
source = DeltaTable.forPath(spark, "dbfs:/file/source")

source_df = source.toDF()

target.alias('trg') \
  .merge(
    source_df.alias('src'),
    'trg.ID = src.ID'
  ) \
  .whenMatchedUpdate(set =
    {
      "trg.ID": "src.ID",
      "trg.Name": "src.Name",
      "trg.Location": "src.Location",
      "trg.Mobile_No": "src.Mobile_No"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "trg.ID": "src.ID",
      "trg.Name": "src.Name",
      "trg.Location": "src.Location",
      "trg.Mobile_No": "src.Mobile_No"
    }
  ) \
  .execute()

# COMMAND ----------

print(" After Merge Statment")
target = DeltaTable.forPath(spark, "dbfs:/file/target")
target.toDF().show()

# COMMAND ----------

After Merge Statment
+---+--------+------------+---------+
| ID|    Name|    Location|Mobile_No|
+---+--------+------------+---------+
|  1|Jagadesh|AndraPradesh|     9999|
|  2|  Vishal|        pune|     7777|
|  3|  venkat|   Telangana|     6666|
|  4| Kishore|      Mumbai|     5555|
+---+--------+------------+---------+




