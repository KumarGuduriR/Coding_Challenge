#!/usr/bin/env python
# coding: utf-8

# # Part 3: Applied Machine Learning
# 
# For this section, task will be to convert a small snippet of ML Python code implemented around pandas and sklearn into a functionally equivalent Spark version which may leverage Spark's ML or MLlib packages.
# 
# We'll be working with the famous Iris data set which can be fetched from UCI's machine learning repository in CSV format: 
# 
# curl -L "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data" -o /tmp/iris.csv

# ### Problem description:
# 
# The Data Science team has provided prototype code in Python which builds a basic, Logistic Regression model on the Iris dataset. We want to predict the type of flower - denoted as class in the CSV - based upon the 4 features which relate to certain flower measurements.

# ## Part 3_Task 1

# #### Executing the given python code

# In[1]:


# importing the libraries

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression


# In[2]:


#reading the iris.data using pandas.read_csv and defining the schema

df = pd.read_csv("iris.data",
names = ["sepal_length", "sepal_width", "petal_length", "petal_width", "class"])


# In[3]:


#displaying the first 5 records

df.head()


# In[4]:


# Separate features from class.

array = df.values
X = array[:,0:4]
y = array[:,4]


# In[5]:


# Fit Logistic Regression classifier.

logreg = LogisticRegression(C=1e5)
logreg.fit(X, y)


# In[6]:


#displaying the predicted values

print(logreg.predict([[5.1, 3.5, 1.4, 0.2]]))
print(logreg.predict([[6.2, 3.4, 5.4, 2.3]]))


# # Part 3_Task 2

# Implement a Spark version of the above Python code and demonstrate that can correctly predict on the training data, similar to what was done in the preceding section.
# 
# You should be able to run your model against the pred_data data frame in the code snippet:
# 
# pred_data = spark.createDataFrame(
# [(5.1, 3.5, 1.4, 0.2),
# (6.2, 3.4, 5.4, 2.3)],
# ["sepal_length", "sepal_width", "petal_length", "petal_width"])
# 
# Write out the prediction results to a CSV file out_3_2.txt :
# class
# <class 1>
# <class 2

# In[7]:


#installing PySpark

#!pip install findspark

import findspark
findspark.init()


# In[8]:


#importing neccessary libraries

import csv
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import LogisticRegression


# In[9]:


#creating the SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)


# In[10]:


#defining schema

schema = StructType([
    StructField("sepal_length", DoubleType(), True),
    StructField("sepal_width", DoubleType(), True),
    StructField("petal_length", DoubleType(), True),
    StructField("petal_width", DoubleType(), True),
    StructField("class", StringType(), True)])

#loading iris.data into a dataFrame

irisLoad_DF = spark.read.csv("iris.data",schema=schema)


# In[11]:


irisLoad_DF.count()


# In[12]:


#displaying the first 5 rows of a dataFrame

irisLoad_DF.show(5)


# In[13]:


#renaming the column "class" as "label". Since class is a reserved keyword in Python

irisRename_DF = irisLoad_DF.withColumnRenamed("class","label")


# In[14]:


#displaying the first 5 rows of a dataFrame with renamed column "label"

irisRename_DF.show(5)


# In[15]:


#In this step we are merging sepal_length,sepal_width,petal_length,petal_width columns into 
#one vector column "features" using VectorAssembler

#VectorAssembler is a feature transformer that merges multiple columns into a vector column

vectorAssembler_features = VectorAssembler(
    inputCols=["sepal_length", "sepal_width", "petal_length", "petal_width"], outputCol="features"
)


# In[16]:


#applying VectorAssembler transformation to the Iris Dataframe

irisTrans_DF = vectorAssembler_features.transform(irisRename_DF)


# In[17]:


#displaying first 5 records after VectorAssembler transformation

irisTrans_DF.show(5)


# In[18]:


#dropping sepal_length,sepal_width,petal_length,petal_width columns. Since already have these columns as features

irisNew_DF = irisTrans_DF.drop("sepal_length", "sepal_width", "petal_length", "petal_width")

#displaying first 5 records after drop

irisNew_DF.show(5)


# In[19]:


#transforming dataframe by assigning labelIndex to every class of flower by using StringIndexer.

#StringIndexer encodes a string column of labels to a column of label indices and can encode multiple columns.

classlabel_indexer = StringIndexer(inputCol="label", outputCol="labelIndex")


# In[20]:


#applying above StringIndexer transformation to the dataFrame

irisIndexer_DF = classlabel_indexer.fit(irisNew_DF).transform(irisNew_DF)


# In[21]:


#displaying first 5 records after StringIndexer transformation


irisIndexer_DF.show(5)


# In[22]:


#defining logistic regression classifier model

logReg_model = LogisticRegression(
    labelCol="labelIndex", featuresCol="features", maxIter=100, regParam=0.001, elasticNetParam=1, standardization=True
)


# In[23]:


#splitting data to trainingData and testData

(training_Data, test_Data) = irisIndexer_DF.randomSplit([0.8, 0.2])


# In[24]:


#displaying Training Dataset and Test Dataset Count

print("Training Dataset Count: " + str(training_Data.count()))
print("Test Dataset Count: " + str(test_Data.count()))


# In[25]:


#fitting LR model to train logReg_modeling set

lr_model_temp = logReg_model.fit(training_Data)


# In[26]:


# Making predictions with the trained LR model on the test data

predictions_testData = lr_model_temp.transform(test_Data)


# In[27]:


# Displaying the predictions on test data

predictions_testData.show(5)


# ### Similar to the given python code, now training the model on the entire data. 

# In[28]:


#fitting LR model to train on entire dataset

lr_model = logReg_model.fit(irisIndexer_DF)


# In[29]:


# Creating a spark dataframe with the given values as per the task

pred_data = spark.createDataFrame(
[(5.1, 3.5, 1.4, 0.2),
(6.2, 3.4, 5.4, 2.3)],
["sepal_length", "sepal_width", "petal_length", "petal_width"])


# In[30]:


# Transofrming the input dataframe to convert the columns to vectors(features)

pred_data_new = vectorAssembler_features.transform(pred_data)


# In[31]:


# Predicting the label(flower class) for the given input dataframe

pred_data_predictions = lr_model.transform(pred_data_new)


# In[32]:


#displaying the predictions along with the features for given input data

pred_data_predictions.select("features","prediction").show()


# #### The model has predicted the class of the flowers (label) correctly for the given input dataframe as per the task.

# In[33]:


#renaming prediction as Class

pred_data_out = pred_data_predictions.select(col("prediction").alias("Class"))


# In[34]:


#displaying only Class column with the values

pred_data_out.show()


# In[36]:


#writing out the prediction results to a CSV file: out_3_2.txt

pred_data_out.repartition(1).write.mode("overwrite").format("csv").option("header", "true").save("out_3_2.txt")


#                                            The End. Thank you! :)
