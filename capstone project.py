# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create a DF(airlines_1987_to_2008) from this path  %fs ls dbfs:/databricks-datasets/asa/airlines
# MAGIC (There are csv files in airlines folder. It contains 1987.csv to  2008.csv files. Create only one DF from all the files )####

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

df = spark.read \
   .csv(path='dbfs:/databricks-datasets/asa/airlines',header=True,inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ####2.Create a PySpark Datatypes schema for the above DF####

# COMMAND ----------

df.columns

# COMMAND ----------

schema=[('Year',IntegerType()),
     ('Month',IntegerType()),
     ('DayofMonth',IntegerType()),
     ('DayofWeek',IntegerType()),
     ('DepTime',IntegerType()),
     ('CRSDepTime',IntegerType()),
     ('ArrTime',IntegerType()),
     ('CRSArrTime',IntegerType()),
     ('UniqueCarrier',StringType()),
    ('FlightNum',StringType()),
    ("TailNum",StringType()),
    ("ActualElapsedTime",StringType()),
    ('CRSElapsedTime',IntegerType()),
    ('AirTime',StringType()),
    ("ArrDelay",StringType()),('DepDelay',IntegerType()),
    ('Origin',StringType()),
    ('Dest',StringType()),
    ('Distance',IntegerType()),
    ('TaxiIn',StringType()),
 ('TaxiOut',StringType()),
 ('Cancelled',StringType()),
 ('CancellationCode',StringType()),
 ('Diverted',StringType()),
 ('CarrierDelay',StringType()),
 ('WeatherDelay',StringType()),
 ('NASDelay',StringType()),
 ('SecurityDelay',StringType()),
 ('LateAircraftDelay',StringType())]

# COMMAND ----------

schema = StructType([StructField (x[0], x[1], True) for x in schema])
schema

# COMMAND ----------

df1 = spark.read .csv(path='dbfs:/databricks-datasets/asa/airlines',header=True,schema=schema)

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####3.View the dataframe####

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ####4.Return count of records in dataframe####

# COMMAND ----------

rows=df1.count()
print(f'number of rows:{rows}')

# COMMAND ----------

df1.createOrReplaceTempView("airlines")


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as total_rows from airlines;

# COMMAND ----------

# MAGIC %md
# MAGIC ####5.Select the columns - Origin, Dest and Distance####

# COMMAND ----------

df1.select(df1["origin"],df1["Dest"],df1["Distance"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####6.Filtering data with 'where' method, where Year = 2001####

# COMMAND ----------

df1.where(df1['Year']==2001).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####7.Create a new dataframe (airlines_1987_to_2008_drop_DayofMonth) exluding dropped column (“DayofMonth”) ####

# COMMAND ----------

airlines_1987_to_2008_DayofMonth=df1.drop(df1["DayofMonth"])

# COMMAND ----------

# MAGIC %md
# MAGIC ####8.Display new DataFrame####

# COMMAND ----------

airlines_1987_to_2008_DayofMonth.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####9.Create column 'Weekend' and a new dataframe(AddNewColumn) and display####

# COMMAND ----------

AddNewColumn=df1.withColumn("Weekend",when(df1["DayofWeek"].isin(6,7),"Yes").otherwise("No"))
AddNewColumn.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####10.Cast ActualElapsedTime column to integer and use printschema to verify####
# MAGIC

# COMMAND ----------

df1.display()

# COMMAND ----------

df3=df1.withColumn("ActualElapsedTime",df1["ActualElapsedTime"].cast(IntegerType()))
df3.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####11.Rename 'DepTime' to 'DepartureTime'####

# COMMAND ----------

df3=df3.withColumnRenamed("DepTime","DepartureTime")
df3.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ####12.Drop duplicate rows based on Year and Month and Create new df (Drop Rows)####

# COMMAND ----------

Drop_Rows=df3.dropDuplicates(["Year","Month"])

# COMMAND ----------

Drop_Rows.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####13.Display Sort by descending order for Year Column using sort()####

# COMMAND ----------

df3.sort(df3["Year"],ascending=False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####14.Group data according to Origin and returning count####

# COMMAND ----------

df3.groupBy(df3["Origin"]).count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####15.Group data according to dest and finding maximum value for each 'Dest'####

# COMMAND ----------

df3=df3.withColumn("Time",(df3["ArrTime"]-df3["DepartureTime"]))
df3.groupBy("Dest").max("Time").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###16.Write data in Delta format###

# COMMAND ----------

df3.write.mode("overwrite").format("delta").saveAsTable("AirlinesTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from AirlinesTable limit 5

# COMMAND ----------


