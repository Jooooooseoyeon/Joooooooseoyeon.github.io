# Databricks notebook source
# MAGIC %python
# MAGIC filter(lambda x: "recordservice" in str(x), dbutils.fs.ls("file:///databricks/jars"))

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: File location and type
# MAGIC 
# MAGIC Of note, this notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.
# MAGIC 
# MAGIC First we'll need to set the location and type of the file. You set the file location when you uploaded the file. We'll do this using [widgets](https://docs.databricks.com/user-guide/notebooks/widgets.html). Widgets allow us to parameterize the exectuion of this entire notebook. First we'll create them, then we'll be able to reference them throughout the notebook.

# COMMAND ----------

dbutils.widgets.text("file_location", "/uploads/data", "Upload Location")
dbutils.widgets.dropdown("file_type", "csv", ["csv", 'parquet', 'json'])
# this can be csv, parquet, json and or any Other Spark Data source.
# See: https://docs.databricks.com/spark/latest/data-sources/index.html
# for more information.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: Reading the data
# MAGIC 
# MAGIC Now that we specified our file metadata, we can create a DataFrame. You'll notice that we use an *option* to specify that we'd like to infer the schema from the file. We can also explicitly set this to a particular schema if we have one already.
# MAGIC 
# MAGIC First, let's create a DataFrame in Python, notice how we will programmatically reference the widget values we defined above.

# COMMAND ----------

df = spark.read.format(dbutils.widgets.get("file_type")).option("header", "true").option("inferSchema", "true").load(dbutils.widgets.get("file_location"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3: Querying the data
# MAGIC 
# MAGIC Now that we created our DataFrame. We can query it. For instance, you can select some particular columns to select and display within Databricks.

# COMMAND ----------

display(df.select("EXAMPLE_COLUMN"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 4: (Optional) Create a view or table
# MAGIC 
# MAGIC If you'd like to be able to use query this data as a table, it is simple to register it as a *view* or a table.

# COMMAND ----------

df.createOrReplaceTempView("ItemizedUsage")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can query this using Spark SQL. For instance, we can perform a simple aggregation. Notice how we can use `%sql` in order to query the view from SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT EXAMPLE_GROUP, SUM(EXAMPLE_AGG) FROM YOUR_TEMP_VIEW_NAME GROUP BY EXAMPLE_GROUP

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table. You can also create a table from the DataFrame.

# COMMAND ----------

df.write.format("parquet").saveAsTable("ItemizedUsage")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This table will persist across cluster restarts as well as allow various users across different notebooks to query this data.