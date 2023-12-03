# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS test_catalog;
# MAGIC USE CATALOG test_catalog;
# MAGIC CREATE SCHEMA IF NOT EXISTS test_schema;
# MAGIC USE SCHEMA test_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA EXTENDED test_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sql_table (column1 STRING, column2 INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE sql_table VALUES ("test", 1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Pyspark

# COMMAND ----------

if spark.catalog.tableExists("sql_table"):
    print(True)
else:
    print(False)

# COMMAND ----------

df = spark.table("test_catalog.test_schema.sql_table")
df.display()
df.write.saveAsTable("test_catalog.test_schema.pyspark_table", mode="overwrite")
