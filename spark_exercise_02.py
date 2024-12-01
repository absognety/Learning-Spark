# In[18]:


# Exercise 02

# https://jaceklaskowski.github.io/spark-workshop/exercises/sql/selecting-the-most-important-rows-per-assigned-priority.html

from pyspark.sql import SparkSession

#Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

input = [(1, "MV1"),
  (1, "MV2"),
  (2, "VPV"),
  (2, "Others")]

headers = ["id", "value"]

df_input = spark.createDataFrame(input).toDF(*headers)

df_input.show(truncate=False)


# In[19]:


from pyspark.sql import functions as F
from pyspark.sql import Window

df_input = df_input.withColumn('index', F.monotonically_increasing_id())

df_input.show(truncate=False)


# In[20]:


window = Window.partitionBy("id").orderBy(F.asc("id"), F.asc("index"))
df_input = df_input.withColumn("priority", F.row_number().over(window))
result = df_input.where(df_input.priority == 1)
result.select("id", "value").show(truncate=False)


# In[ ]: