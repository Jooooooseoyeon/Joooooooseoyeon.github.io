# Databricks notebook source
# MAGIC %sh
# MAGIC ls -l /databricks/jars/recordservice-spark-2.0.jar

# COMMAND ----------

# MAGIC %sh
# MAGIC echo '[driver]."spark.recordservice.planner.hostports" = "10.180.45.243:50075"' >> /databricks/common/conf/cerebro.conf

# COMMAND ----------

# MAGIC %fs
# MAGIC rm /databricks/init/test-1/clear-metastore.sh

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.cerebro.recordservice.spark

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.read.format("com.cerebro.recordservice.spark").load("db.table")
# MAGIC display(df)

# COMMAND ----------

# MAGIC %sh
# MAGIC cat conf/spark-branch.conf

# COMMAND ----------

# MAGIC %sh
# MAGIC cat conf/00-custom-spark.conf

# COMMAND ----------

# MAGIC %fs
# MAGIC rm /databricks/init/test-1/add-metastore.sh

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC dbutils.fs.put(
# MAGIC     "/databricks/init/test-1/add-metastore.sh",
# MAGIC      """#!/bin/sh
# MAGIC       |# Loads environment variables to determine the correct JDBC driver to use.
# MAGIC       |source /etc/environment
# MAGIC       |# Quoting the label (i.e. EOF) with single quotes to disable variable interpolation.
# MAGIC       |cat > /databricks/hive/conf/hive-site.xml << 'EOM'
# MAGIC       
# MAGIC       |foo
# MAGIC       |<configuration>
# MAGIC       |<property>
# MAGIC       |<name>javax.jdo.option.ConnectionURL</name>
# MAGIC       |<value>jdbc:mariadb://nikedirect-db.chkweekm4xjq.us-east-1.rds.amazonaws.com:3306/organization0?trustServerCertificate=true&amp;useSSL=true</value>
# MAGIC       |<description>JDBC connect string for a JDBC metastore</description>
# MAGIC       | </property>
# MAGIC       |
# MAGIC     |<property>
# MAGIC     |<name>javax.jdo.option.ConnectionDriverName</name>
# MAGIC     |<value>org.mariadb.jdbc.Driver</value>
# MAGIC     |<description>Driver class name for a JDBC metastore</description>
# MAGIC     |</property>
# MAGIC 
# MAGIC     |<property>
# MAGIC     |<name>javax.jdo.option.ConnectionUserName</name>
# MAGIC     |<value>l2lu3DUYHtCQTdjd</value>
# MAGIC     |<description>Username to use against metastore database</description>
# MAGIC     |</property>
# MAGIC 
# MAGIC     |<property>
# MAGIC     |<name>javax.jdo.option.ConnectionPassword</name>
# MAGIC     |<value>T65nQrSRCaW3TVO4K5RTuLLP636K9nlhkv3wSkI1</value>
# MAGIC     |<description>Password to use against metastore database</description>
# MAGIC     |</property> <!-- If the following two properties are not set correctly, the metastore will attempt to initialize its schema upon startup -->
# MAGIC 
# MAGIC     |<property>
# MAGIC     |<name>datanucleus.autoCreateSchema</name>
# MAGIC     |<value>false</value>
# MAGIC     |</property>
# MAGIC 
# MAGIC     |<property>
# MAGIC     |<name>datanucleus.fixedDatastore</name>
# MAGIC     |<value>true</value>
# MAGIC     |</property>
# MAGIC 
# MAGIC     |<property>
# MAGIC     |<name>datanucleus.connectionPool.minPoolSize</name>
# MAGIC     |<value>0</value>
# MAGIC     |</property>
# MAGIC 
# MAGIC     |<property>
# MAGIC     |<name>datanucleus.connectionPool.initialPoolSize</name>
# MAGIC     |<value>0</value>
# MAGIC     |</property>
# MAGIC 
# MAGIC     |<property>
# MAGIC     |<name>datanucleus.connectionPool.maxPoolSize</name>
# MAGIC     |<value>1</value>
# MAGIC     |</property>
# MAGIC 
# MAGIC     |<property>
# MAGIC     |<name>hive.stats.autogather</name>
# MAGIC     |<value>false</value>
# MAGIC     |</property>
# MAGIC 
# MAGIC     |<property>
# MAGIC     |<name>mapred.reduce.tasks</name>
# MAGIC     |<value>100</value>
# MAGIC     |</property>
# MAGIC 
# MAGIC     |<!-- To mitigate the problem of PROD-4498 and per HIVE-7140, we need to bump the timeout. Since the default value of this property used by Impala is 3600 seconds, we will use this value for actual deployment (http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_props_cdh530_impala.html). -->
# MAGIC     |<property>
# MAGIC     |<name>hive.metastore.client.socket.timeout</name>
# MAGIC     |<value>3600</value>
# MAGIC     |</property>
# MAGIC 
# MAGIC     |<property>
# MAGIC     |<name>hive.metastore.client.connect.retry.delay</name>
# MAGIC     |<value>10</value>
# MAGIC     |</property>
# MAGIC     
# MAGIC   |<property>
# MAGIC    | <name>hive.metastore.failure.retries</name>
# MAGIC     |<value>30</value>
# MAGIC    | </property>
# MAGIC 
# MAGIC     |<property>
# MAGIC    | <name>recordservice.planner.hostports</name>
# MAGIC    | <value>10.180.45.243:50075</value>
# MAGIC    | </property>
# MAGIC 
# MAGIC    | <property>
# MAGIC    | <name>hive.metastore.rawstore.impl</name>
# MAGIC    | <value>com.cerebro.hive.metastore.CerebroObjectStore</value>
# MAGIC   |  </property>
# MAGIC 
# MAGIC |</configuration>
# MAGIC 'EOM'
# MAGIC """.stripMargin)

# COMMAND ----------

# MAGIC %fs
# MAGIC head /databricks/init/external-metastore.sh

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /databricks/hive/conf/hive-site.xml

# COMMAND ----------

# MAGIC %fs 
# MAGIC mkdirs /databricks/init/test-1

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC dbutils.fs.put(
# MAGIC     "/databricks/init/test-1/clear-metastore.sh",
# MAGIC    """#!/bin/sh
# MAGIC       |# Loads environment variables to determine the correct JDBC driver to use.
# MAGIC       |source /etc/environment
# MAGIC       |# Quoting the label (i.e. EOF) with single quotes to disable variable interpolation.
# MAGIC       |rm /databricks/driver/conf/00-custom-spark.conf
# MAGIC       
# MAGIC """
# MAGIC   )

# COMMAND ----------

# MAGIC %fs
# MAGIC head /databricks/init/test-1/clear-metastore.sh

# COMMAND ----------

