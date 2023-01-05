# Databricks notebook source
import unittest
import pandas
import os, unittest
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Does the specified table exist in the specified database?

def tableExists(tableName, dbName):
    return spark.catalog._jcatalog.tableExists(tableName)   #(f"{dbName}.{tableName}")


# Does the specified column exist in the given DataFrame?
def columnExists(dataFrame, columnName):
    if columnName in dataFrame.columns:
        return True
    else:
        return False


# How many rows are there for the specified value in the specified column
# in the given DataFrame?
def numRowsInColumnForValue(dataFrame, columnName, columnValue):
    df = dataFrame.filter(col(columnName) == columnValue)
    return df.count()



# COMMAND ----------

!pip3 install pytest
import pytest
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType


# COMMAND ----------

tableName   = "nyc_taxi_1"
dbName      = "default"
columnName  = "isPaidTimeOff"
columnValue = True

# COMMAND ----------

if tableExists(tableName, dbName):

  df = spark.sql(f"SELECT * FROM {dbName}.{tableName}")

  # And the specified column exists in that table...
  if columnExists(df, columnName):
    # Then report the number of rows for the specified value in that column.
    numRows = numRowsInColumnForValue(df, columnName, columnValue)

    print(f"There are {numRows} rows in '{tableName}' where '{columnName}' equals '{columnValue}'.")
  else:
    print(f"Column '{columnName}' does not exist in table '{tableName}' in schema (database) '{dbName}'.")
else:
  print(f"Table '{tableName}' does not exist in schema (database) '{dbName}'.") 

# COMMAND ----------


# tableName   = "test_data"
# dbName      = "default"
# columnName  = "isFraud"
# columnValue = True

df = spark.sql(f"SELECT * FROM {dbName}.{tableName}")




# COMMAND ----------

# Does the table exist?


# COMMAND ----------

class test_ut:
    def test_tableExists(self):
      #assert tableExists(tableName, dbName) is True
      return (tableExists(tableName, dbName)==True)
# Does the column exist?
    def test_columnExists(self):
      #assert columnExists(df, columnName) is True
      return (columnExists(df, columnName)==True)

# Is there at least one row for the value in the specified column?
    def test_numRowsInColumnForValue(self):
      #assert numRowsInColumnForValue(df, columnName, columnValue) > 0
      return (numRowsInColumnForValue(df, columnName, columnValue) > 0)

# COMMAND ----------

unitTests = test_ut()
print(unitTests.test_tableExists())
print(unitTests.test_columnExists())
print(unitTests.test_numRowsInColumnForValue())

# COMMAND ----------

testResult = unitTests.test_tableExists() and unitTests.test_columnExists() and unitTests.test_numRowsInColumnForValue()
dbutils.notebook.exit(f"{testResult}")
Footer

# COMMAND ----------


