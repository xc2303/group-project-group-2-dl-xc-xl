# Databricks notebook source
# MAGIC %md
# MAGIC # Build Features for Q1 - Group Project

# COMMAND ----------

# Import modules
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Read in q1 dataset
q1_data = spark.read.csv("s3://group2-gr5069/processed/q1/q1_combo_f1_data.csv", header=True, inferSchema=True)

# Create binary variable to indicate 2nd place finish
q1_data = q1_data.withColumn('second_place', F.when(F.col("positionOrder") == 2, 1).otherwise(0))

# Create age as of race variable
q1_data = q1_data.withColumn("age_as_of_race", F.datediff(F.col("race_date"), F.col("dob"))/365)

# Create binary variables for constructors with top 5 most wins (excluding mercedes since they don't have that much relevant data)
q1_data = (
  q1_data
  .withColumn("ctor_ferrari", F.when(F.col("constructorRef") == "ferrari", 1).otherwise(0))
  .withColumn("ctor_mclaren", F.when(F.col("constructorRef") == "mclaren", 1).otherwise(0))
  .withColumn("ctor_williams", F.when(F.col("constructorRef") == "williams", 1).otherwise(0))
  .withColumn("ctor_team_lotus", F.when(F.col("constructorRef") == "team_lotus", 1).otherwise(0))
)

# Create binary variables for top 10 grid positions
q1_data = (
  q1_data
  .withColumn("grid_1", F.when(F.col("grid") == 1, 1).otherwise(0))
  .withColumn("grid_2", F.when(F.col("grid") == 2, 1).otherwise(0))
  .withColumn("grid_3", F.when(F.col("grid") == 3, 1).otherwise(0))
  .withColumn("grid_4", F.when(F.col("grid") == 4, 1).otherwise(0))
  .withColumn("grid_5", F.when(F.col("grid") == 5, 1).otherwise(0))
  .withColumn("grid_6", F.when(F.col("grid") == 6, 1).otherwise(0))
  .withColumn("grid_7", F.when(F.col("grid") == 7, 1).otherwise(0))
  .withColumn("grid_8", F.when(F.col("grid") == 8, 1).otherwise(0))
  .withColumn("grid_9", F.when(F.col("grid") == 9, 1).otherwise(0))
  .withColumn("grid_10", F.when(F.col("grid") == 10, 1).otherwise(0))
)

# Save
q1_data.write.csv("s3://group2-gr5069/processed/q1/q1_combo_f1_data_w_features.csv", header="true", mode="overwrite")