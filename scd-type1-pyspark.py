# Databricks notebook source
# MAGIC %md scd-Type 1 with out merge

# COMMAND ----------

from pyspark.sql.functions import col ,coalesce
from pyspark.sql.types import StringType ,IntegerType,StructType,StructField



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
source_df.show()
# source_df.printSchema()




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
target_df.show()
# target_df.printSchema()



#dropDuplicates from source
source_withoutduplicates=source_df.dropDuplicates(subset=["ID"])
final_df=source_withoutduplicates.alias("src").join(target_df.alias("trg"),on="ID",how="outer")\
    .select("ID",\

            coalesce(col("src.Name"),col("trg.Name")).alias("Name"),\
            coalesce(col("src.Location"),col("trg.Location")).alias("Location"),\
            coalesce(col("src.Mobile_No"),col("trg.Mobile_No")).alias("mobile_no")\

            
            
            
            
            )
final_df.show()


+---+--------+------------+---------+
| ID|    Name|    Location|Mobile_No|
+---+--------+------------+---------+
|  1|Jagadesh|AndraPradesh|     9999|
|  2|  Vishal|        pune|     7777|
|  3|  venkat|   Telangana|     6666|
|  4| Kishore|      Mumbai|     5555|
|  4| Kishore|      Mumbai|     5555|
|  2|  Vishal|        pune|     7777|
+---+--------+------------+---------+

+---+--------+------------+---------+
| ID|    Name|    Location|Mobile_No|
+---+--------+------------+---------+
|  1|Jagadesh|AndraPradesh|     9497|
|  2|  Vishal|       Delhi|     7777|
|  3|  venkat|   Hyderabad|   969696|
+---+--------+------------+---------+

+---+--------+------------+---------+
| ID|    Name|    Location|mobile_no|
+---+--------+------------+---------+
|  1|Jagadesh|AndraPradesh|     9999|
|  2|  Vishal|        pune|     7777|
|  3|  venkat|   Telangana|     6666|
|  4| Kishore|      Mumbai|     5555|
+---+--------+------------+---------+
