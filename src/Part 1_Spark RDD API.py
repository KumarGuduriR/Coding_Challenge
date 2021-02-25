#!/usr/bin/env python
# coding: utf-8

# In[1]:


#installing PySpark

import findspark
findspark.init()


# In[2]:


#importing neccessary libraries

import csv
from pyspark import SparkContext
from pyspark.sql import SparkSession

#creating the SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)


# # Part 1_Task 1

# ### Download the data file from the given location and make it accessible to Spark. Display first 5 rows

# In[3]:


#loading given groceries.csv dataset into Spark RDD

load_rdd = sc.textFile("groceries.csv")
groceries_rdd = load_rdd.mapPartitions(lambda x: csv.reader(x))


# In[4]:


#displaying first 5 rows of a dataset

groceries_rdd.take(5)


# In[5]:


#displaying total number of rows in a dataset

groceries_rdd.count()


# # Part 1_Task 2a

# ### a.) Using Spark's RDD API, create a list of all (unique) products present in the transactions. Write out this list to a text file: out_1_2a.txt.

# In[6]:


#creating a list of all unique products in the dataset

uniqProd_rdd = groceries_rdd.flatMap(lambda x: x).distinct()


# In[7]:


#assuming: the question was asked to create list of all unique products

uniqProd_list = uniqProd_rdd.collect()


# In[8]:


#displaying a list of all unique products from the list created

print(uniqProd_list)


# In[9]:


#using Python, writing the above list to a text file: out_1_2a.txt.

with open("out_1_2a.txt", 'w') as output:
    for row in uniqProd_list:
        output.write(str(row) + '\n')


# # Part 1_Task 2b

# ### b.) Again, using Spark only, write out the total count of products to a text file: out_1_2b.txt

# In[10]:


#finding total count of unique products

uniqProd_count = uniqProd_rdd.count()


# In[11]:


#displaying total count of unique products

print(uniqProd_count)


# In[12]:


#Writing total count of unique products to a text file: out_1_2a.txt

spark.sparkContext.parallelize([f"Count:\n" + str(uniqProd_count)]).repartition(1).saveAsTextFile("out_1_2b.txt")


# # Part 1_Task 3

# ### Create an RDD and using Spark APIs, determine the top 5 purchased products along with how often they were purchased (frequency count). Write the results out in descending order of frequency into a file: out_1_3.txt.

# In[13]:


#flatMap() transformation flattens the RDD/DataFrame after applying the function on every element.

flatten_rdd = groceries_rdd.flatMap(lambda x: x)

#adding a new element with value 1 for each element using map() and merging the values of each element using reduceByKey()

wordsMap_rdd = flatten_rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)


# In[14]:


#sorting the counts by higher order to lower using sortBy()

sortedCounts_rdd = wordsMap_rdd.sortBy(lambda x: x[1], ascending=False)


# In[15]:


#getting top 5 most purchased products along with purchased count

top5purchased = sortedCounts_rdd.collect()[0:5]


# In[16]:


#Writing the top 5 results out in descending order of frequency into a file out_1_3.txt

with open("out_1_3.txt", 'w') as output:
    for row in top5purchased:
        output.write(str(row) + '\n')


#                                                   End of Part 1
