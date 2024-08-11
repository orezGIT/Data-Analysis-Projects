-- Databricks notebook source
-- MAGIC %md
-- MAGIC #TASK 1
-- MAGIC #SPARK SQL IMPELEMENTATION
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Part 2: Prepare / clean the data for SQL implementation.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # creating an RDD and removing excess punctuation marks by using lambda function from the datasets
-- MAGIC myrdd = sc.textFile('/FileStore/tables/clinicaltrial_2023.csv')
-- MAGIC myrdd2 = myrdd.map(lambda line: line.replace(',', '').replace('"', ''))
-- MAGIC myrdd2.take(2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # split the dataset base on \t delimiter
-- MAGIC myrdd3 = myrdd2.map(lambda row: row.split('\t'))
-- MAGIC myrdd3.take(2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #create a user define function 
-- MAGIC def filter_row_length(row, column_length):     
-- MAGIC     #return true if length of rows matches length of column header, else return false 
-- MAGIC     match_length = len(row) == column_length 
-- MAGIC     return match_length
-- MAGIC
-- MAGIC #count the length of column header
-- MAGIC myrdd3_header = len(myrdd3.first())
-- MAGIC
-- MAGIC #Filter rows in myrdd3 base on length of rows equals MYRRD
-- MAGIC rddsql = myrdd3.filter(lambda row: filter_row_length(row, myrdd3_header))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #conveting RDD to dataframe using toDF() method 
-- MAGIC df = rddsql.toDF()
-- MAGIC df.show(5)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #renaming the column to remove the default header
-- MAGIC df1 = df.withColumnRenamed('_1', 'Id') \
-- MAGIC     .withColumnRenamed('_2', 'Study_Title') \
-- MAGIC     .withColumnRenamed('_3', 'Acronym') \
-- MAGIC     .withColumnRenamed('_4', 'Status') \
-- MAGIC     .withColumnRenamed('_5', 'Conditions') \
-- MAGIC     .withColumnRenamed('_6', 'Interventions') \
-- MAGIC     .withColumnRenamed('_7', 'Sponsor') \
-- MAGIC     .withColumnRenamed('_8', 'Collaborators') \
-- MAGIC     .withColumnRenamed('_9', 'Enrollment') \
-- MAGIC     .withColumnRenamed('_10', 'Funder_Type') \
-- MAGIC     .withColumnRenamed('_11', 'Type') \
-- MAGIC     .withColumnRenamed('_12', 'Study_Design') \
-- MAGIC     .withColumnRenamed('_13', 'Start') \
-- MAGIC     .withColumnRenamed('_14', 'Completion')
-- MAGIC
-- MAGIC df1.show(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #removing one of the header by applying filter function and importing col
-- MAGIC from pyspark.sql.functions import col
-- MAGIC df2 = df1.filter(col('Study_Title') != 'Study Title')
-- MAGIC df2.show(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #create a tempView from df_2 
-- MAGIC df2.createOrReplaceTempView('sqlview') 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###display the tempview

-- COMMAND ----------

SELECT * FROM sqlview

-- COMMAND ----------

SHOW DATABASES


-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###store temporary view (sqlview) as a permanent table 

-- COMMAND ----------

CREATE OR REPLACE TABLE default.clinicalview AS SELECT * FROM sqlview

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create a new database to store all the tables and views 

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS sql_db 

-- COMMAND ----------

DROP TABLE IF EXISTS sql_db.clinicalview

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC file_path = 'dbfs:/user/hive/warehouse/sql_db.db/clinicalview'
-- MAGIC try:
-- MAGIC     dbutils.fs.rm('dbfs:/user/hive/warehouse/sql_db.db/clinicalview',True)
-- MAGIC     print('REMOVED') 
-- MAGIC except FileNotFoundException:
-- MAGIC     print('DO NOT EXIST')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###copy the clinicalview table to the new database

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sql_db.clinicalview AS SELECT * FROM default.clinicalview

-- COMMAND ----------

show tables

-- COMMAND ----------

SHOW TABLES IN sql_db

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #Analyse the Data
-- MAGIC

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Question 1: The number of studies in the dataset. You must ensure that you explicitly check distinct studies
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE VIEW sql_db.studies_clinicalview AS SELECT * FROM sql_db.clinicalview

-- COMMAND ----------

SELECT count(DISTINCT Study_Title) AS Distinct_Studies FROM sql_db.studies_clinicalview 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Question 2: You should list all the types (as contained in the Type column) of studies in the dataset along with the frequencies of each type. These should be ordered from most frequent to least frequent.

-- COMMAND ----------

SELECT Type, count(*) 
FROM sql_db.clinicalview 
WHERE Type IS NOT NULL AND Type != ''
GROUP BY Type
ORDER BY count(*) DESC;

-- COMMAND ----------

SELECT Type, COUNT(*) 
FROM sql_db.clinicalview 
WHERE Type IS NOT NULL AND Type != '899'
GROUP BY Type
HAVING COUNT(*) != 889
ORDER BY COUNT(*) DESC;


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Question 3: The top 5 conditions (from Conditions) with their frequencies.

-- COMMAND ----------

SELECT Conditions FROM clinicalview

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import split
-- MAGIC #Split the 'Conditions' column
-- MAGIC split_conditions_df2 = df2.withColumn('Split_Conditions', split(df2['Conditions'], '\|'))
-- MAGIC
-- MAGIC # Create a temporary view
-- MAGIC split_conditions_df2.createOrReplaceTempView("splitview")
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW explodeview AS SELECT explode(Split_Conditions) AS exploded_split FROM splitview

-- COMMAND ----------

SELECT exploded_split, count(*)
FROM explodeview
GROUP BY exploded_split
ORDER BY count(*) DESC 
LIMIT 5

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Question 4: Find the 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored. Hint: For a basic implementation, you can assume that the Parent Company column contains all possible pharmaceutical companies.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharmrdd = sc.textFile('/FileStore/tables/pharma.csv')
-- MAGIC pharmrdd.take(3)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Convert RDD to dataframe 
-- MAGIC pharmdf = spark.read.csv(
-- MAGIC     path=pharmrdd, 
-- MAGIC     header=True,
-- MAGIC     sep=',',
-- MAGIC     multiLine=True,
-- MAGIC     inferSchema=True,
-- MAGIC )
-- MAGIC #Dsiplay the first 5 rows
-- MAGIC pharmdf.show(5)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharmdf.createOrReplaceTempView("pharmview")

-- COMMAND ----------

SELECT Sponsor, COUNT(*) 
FROM clinicalview
WHERE Sponsor NOT IN (
    SELECT Parent_Company
    FROM pharmview
)
GROUP BY Sponsor
ORDER BY count(*) DESC
LIMIT 10;


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##Part 5: Plot number of completed studies for each month in 2023. You need to include your visualization as well as a table of all the values you have plotted for each month.
-- MAGIC

-- COMMAND ----------

SELECT year(Completion) AS Year, month(Completion) AS Month, COUNT(*) AS Count
FROM clinicalview
WHERE Completion LIKE '2023%' AND Status = 'COMPLETED'
GROUP BY year(Completion), month(Completion)
ORDER BY Month


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##Using Power Bi Visualization tool to show the count of completed studies for each month

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Load or create the DataFrame named clinicalview
-- MAGIC clinicalview = spark.table("clinicalview")
-- MAGIC
-- MAGIC #write the clinicalview table to a CSV file
-- MAGIC clinicalview.write.csv('/FileStore/tables/clinicalview.csv', mode='overwrite')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Run the SQL query on the permanent view and store the result in a DataFrame
-- MAGIC SQLcount_month = spark.sql("""
-- MAGIC     SELECT year(Completion) AS Year, 
-- MAGIC     month(Completion) AS Month, 
-- MAGIC     COUNT(*) AS TotalCompletedStudies
-- MAGIC     FROM clinicalview
-- MAGIC     WHERE Completion LIKE '2023%' AND Status = 'COMPLETED'
-- MAGIC     GROUP BY year(Completion), month(Completion)
-- MAGIC     ORDER BY Month
-- MAGIC """)
-- MAGIC
-- MAGIC #save the dataframe as a csv file
-- MAGIC SQLcount_month.write.csv('/FileStore/tables/SQLcount_month.csv', mode='overwrite')

-- COMMAND ----------

DROP TABLE IF EXISTS SQLcount_month ;
CREATE TABLE IF NOT EXISTS SQLcount_month
using CSV 
options(path 'dbfs:/FileStore/tables/SQLcount_month.csv', header 'False', inferSchema 'True')

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #Further Analysis 
-- MAGIC ##Top 10 collaborators with the total sum of enrollment 

-- COMMAND ----------

SELECT Collaborators, enrollment FROM clinicalview

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import split
-- MAGIC
-- MAGIC #Split the 'Collaborators' column
-- MAGIC split_collaboratorsdf = df2.withColumn('collaborators_split', split(df2['collaborators'], '\|'))
-- MAGIC
-- MAGIC # Create a temporary view
-- MAGIC split_collaboratorsdf.createOrReplaceTempView("collabview")

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW explodecollab AS SELECT explode(collaborators_split) AS exploded_collaboration, Enrollment FROM collabview

-- COMMAND ----------

SELECT exploded_collaboration AS Collaborators, SUM(Enrollment) AS TotalEnrollment
FROM explodecollab
WHERE exploded_collaboration IS NOT NULL AND exploded_collaboration != ''
GROUP BY exploded_collaboration
ORDER BY TotalEnrollment DESC
LIMIT 10
