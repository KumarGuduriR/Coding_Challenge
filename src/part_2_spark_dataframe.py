#!/usr/bin/env python
# coding: utf-8

# # Part 2: Spark Dataframe API

# In[1]:


#installing PySpark

import findspark
findspark.init()


# In[2]:


#importing neccessary libraries

import csv
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *


# In[3]:


#creating the SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)


# # Part 2_Task 1

# ### Download the parquet data file from the URL: (https://github.com/databricks/LearningSparkV2/blob/master/mlflow-project-example/data/sf-airbnb-clean.parquet) and load it into a Spark data frame.

# In[4]:


#loading given airbnb.parquet dataset into Spark DataFrame

airbnbDF = spark.read.parquet("airbnb.parquet")


# In[5]:


#displaying first 5 rows of a dataFrame

airbnbDF.head(5)


# In[6]:


#displaying total number of rows in a AirBnB dataFrame

print(airbnbDF.count())


# In[7]:


#getting number of columns in a AirBnB dataFrame

len(airbnbDF.columns)


# In[8]:


#displaying all the columns in a AirBnB dataFrame 

airbnbDF.printSchema()


# In[9]:


import pyspark.sql.functions as F

df_agg = airbnbDF.agg(*[F.count(F.when(F.isnull(c), c)).alias(c) for c in airbnbDF.columns])


# In[10]:


df_agg.show()


# # Part 2_Task 2

# ### Create CSV output file: out_2_2.txt that lists the minimum price, maximum price, and total row count from the dataset. Use the following output column names in the resultant file: min_price, max_price, row_count

# In[11]:


#getting minimum and maximum price and total row count from AirBnB DataFrame

minMaxCountDF = airbnbDF.select(min("price").alias("min_price"), max("price").alias("max_price") , count("price").alias("row_count"))


# In[12]:


#displaying minimum and maximum price and total row count from AirBnB DataFrame

minMaxCountDF.show()


# In[21]:


#writing the result to a CSV output file with column names: out_2_2.txt

minMaxCountDF.repartition(1).write.mode("overwrite").format("csv").option("header", "true").save("out_2_2.txt")


# # Part 2_Task 3

# ### Calculate the average number of bathrooms and bedrooms across all the properties listed in the data set with a price of > 5000 and a review score being exactly equalt to 10. Write the results into a CSV file: out_2_3.txt with the following column headers: avg_bathrooms, avg_bedrooms

# In[24]:


#calculating the average number of bathrooms and bedrooms across all the properties listed in the data set with a 
#price of > 5000 and a review score being exactly equalt to 10.

#There are so many columns with review scores and the question didn't mentioned the exact column name.
#So assuming "review_scores_value" column as review score.

avgBathBedDF = airbnbDF.filter((airbnbDF['price'] > 5000)&(airbnbDF['review_scores_value'] == 10)).select(avg("Bathrooms").alias("avg_bathrooms"), avg("Bedrooms").alias("avg_bedrooms"))


# In[25]:


#displaying average number of bathrooms and bedrooms from AirBnB DataFrame

avgBathBedDF.show()


# In[16]:


#writing the result to a CSV output file with column names: out_2_3.txt

avgBathBedDF.repartition(1).write.mode("overwrite").format("csv").option("header", "true").save("out_2_3.txt")


# In[26]:


# Assuming the average number of bathrooms and bedrooms should be a round value. 
# However I am not writing out this result to output file.

avgBathBedDF_temp = airbnbDF.filter((airbnbDF['price'] > 5000)&(airbnbDF['review_scores_value'] == 10)).select(round(avg("Bathrooms")).alias("avg_bathrooms"), round(avg("Bedrooms")).alias("avg_bedrooms"))


# In[27]:


#displaying average number of bathrooms and bedrooms(round value) from AirBnB DataFrame

avgBathBedDF_temp.show()


# # Part 2_Task 4

# ### How many people can be accomodated by the property with the lowest price and highest rating? Write the resulting number to a text file: out_2_4.txt .

# In[17]:


#getting all the accomodates based on the price and rating.

#Assuming "review_scores_rating" column as rating column

groupingDF = airbnbDF.select("accommodates","price","review_scores_rating").groupBy("price","review_scores_rating").sum("accommodates")


# In[18]:


#getting number of people can be accomodated by the property with the lowest price and highest rating

accomodateDF = groupingDF.sort(groupingDF.price.asc(),groupingDF.review_scores_rating.desc()).limit(1)


# In[19]:


#displaying the result

accomodateDF.show()


# In[20]:


##writing only the resulting number to a text output file: out_2_3.txt

accomodateDF.select('sum(accommodates)').write.mode("overwrite").format("csv").save("out_2_4.txt")


#                                         End of Task 1, 2, 3, 4 of Part 2

#                                 Task 5 of Part 2 is in a seperate task_2_5.py file
