# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

application_id="ad45c5c9-a8c3-4966-b939-abc17aefcd44"
secret_key=dbutils.secrets.get(scope="secretes-scope",key="screte-key")
directory_id="d463cc30-76e6-45aa-adc5-f2ae8ce39d11"
container_name="bronze"
storage_account_name="sbitsaproject1"


# COMMAND ----------

dbutils.fs.unmount("/mnt/bronze") 

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{application_id}",
          "fs.azure.account.oauth2.client.secret": secret_key,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)

# COMMAND ----------

df1=spark.read.format("csv").option("header","true").load("dbfs:/mnt/bronze/dbo/Athletes/")
df2=spark.read.format("csv").option("header","true").load("dbfs:/mnt/bronze/dbo/Coaches/")
df3=spark.read.format("csv").option("header","true").load("dbfs:/mnt/bronze/dbo/Medals/")
df4=spark.read.format("csv").option("header","true").load("dbfs:/mnt/bronze/dbo/Teams/")


# COMMAND ----------

# 1. Total Athletes by Country
total_athletes_by_country = df1.groupBy(df1.Country).agg(count("*").alias("Total_Athletes")).orderBy(col("Total_Athletes").desc())


# COMMAND ----------

# 2. Total Coaches by Discipline
total_athletes_by_discipline = df2.groupBy("Discipline").count().withColumnRenamed("count", "Coaches")


# COMMAND ----------

# 3 Medal Distribution by Discipline
medals_by_team = df3.join(df4, df3.TeamCountry == df4.Country, "inner").groupBy(col("Discipline"))\
    .agg(
 sum("Gold").alias("Total_Gold"),
 sum("Silver").alias("Total_Silver"),
 sum("Bronze").alias("Total_Bronze"),
 (sum("Gold") + sum("Silver") + sum("Bronze")).alias("Total_Medals")
).orderBy("Total_Medals", ascending=False)


# COMMAND ----------

total_athletes_by_country.coalesce(2) \
    .write \
    .mode("overwrite") \
    .format("delta") \
    .save("dbfs:/mnt/bronze/dbo/Athletes/final_data")

# COMMAND ----------

total_athletes_by_discipline.coalesce(2) \
    .write \
    .mode("overwrite") \
    .format("delta") \
    .save("dbfs:/mnt/bronze/dbo/Coaches/final_data")

# COMMAND ----------

medals_by_team.coalesce(2) \
    .write \
    .mode("overwrite") \
    .format("delta") \
    .save("dbfs:/mnt/bronze/dbo/Medals/final_data")

# COMMAND ----------


