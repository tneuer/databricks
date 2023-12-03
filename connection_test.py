# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="tn-databricks-kv")

# COMMAND ----------

dbutils.fs.ls("abfss://source@tndatabricksdata.dfs.core.windows.net/manual/test_companies.csv") # Should fail

# COMMAND ----------

dbutils.fs.ls("/mnt/tndatabricksdata/source") # Should fail

# COMMAND ----------

# MAGIC %md
# MAGIC # Mounting via SAS

# COMMAND ----------

dbutils.fs.mount(
    source = "wasbs://source@tndatabricksdata.blob.core.windows.net",
    mount_point = "/mnt/tndatabricksdata/source",
    extra_configs = {
        'fs.azure.sas.source.tndatabricksdata.blob.core.windows.net': dbutils.secrets.get(scope="tn-databricks-kv", key="tn-sas-databricks")
    }
)

# COMMAND ----------

dbutils.fs.ls("/mnt/tndatabricksdata/source/manual")

# COMMAND ----------

companies_df = (
    spark
    .read
    .format("csv")
    .option("header", "true")
    .load("/mnt/tndatabricksdata/source/manual/test_companies.csv")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # SAS Token

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.auth.type.tndatabricksdata.dfs.core.windows.net",
  "SAS"
)  
spark.conf.set(
  "fs.azure.sas.token.provider.type.tndatabricksdata.dfs.core.windows.net",
  "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
)
spark.conf.set(
  "fs.azure.sas.fixed.token.tndatabricksdata.dfs.core.windows.net",
  dbutils.secrets.get(scope="tn-databricks-kv", key="tn-sas-databricks")
)

# COMMAND ----------

dbutils.fs.ls("abfss://source@tndatabricksdata.dfs.core.windows.net/manual/test_companies.csv")

# COMMAND ----------

companies_df = (
    spark
    .read
    .format("csv")
    .option("header", "true")
    .load("abfss://source@tndatabricksdata.dfs.core.windows.net/manual/test_companies.csv")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Unity Catalog
