#!/usr/bin/env python
# coding: utf-8

# https://jaceklaskowski.github.io/spark-workshop/exercises/sql/split-function-with-variable-delimiter-per-row.html

# In[87]:


# Import PySpark
from pyspark.sql import SparkSession

#Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()


# In[88]:


# Data
records = [("50000.0#0#0#", "#"),("0@1000.0@", "@"), ("1$", "$"), ("1000.00^Test_string", "^"), 
           ("dog$cat^mouse", "^"), ("@0@1000.0@", "@")]

# Columns
columns = ["VALUES", "separator"]

# Create a spark dataframe
df_records = spark.createDataFrame(records).toDF(*columns)


# In[89]:


from pyspark.sql import functions as F

# Using the string split
df_records.createOrReplaceTempView("strings")


result = spark.sql("""
                        select VALUES, separator, 
                        length(values) as string_length,
                        (
                         CASE 
                         WHEN separator not in ('$','^') THEN SPLIT(VALUES, separator)
                         WHEN separator = '$' THEN array(split_part(VALUES, "$", 1), "")
                         WHEN separator = '^' THEN array(split_part(VALUES, "^", 1), split_part(VALUES, "^", 2))
                         END
                        ) AS VALUES_ARRAY
                        from strings;
                """)


# In[90]:


extra = result.withColumn("VALUES_FILTERED", F.udf(lambda fname: [x for x in fname if x != ""])("VALUES_ARRAY"))

extra.show(truncate=False)