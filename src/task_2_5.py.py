#!/usr/bin/env python
# coding: utf-8

# # Part 2_Task 5

# ### Using Apache Airflow's Dummy Operator, create an Airflow Dag that runs task 1, followed by tasks 2, and 3 in parallel, followed by tasks 4, 5, 6 all in parallel.

# #### Importing various packages

# In[ ]:


# importing airflow related DAG, DummyOperator, PythonOperator and BashOperator which will define the basic setup
# we are not gonna use BashOperator and PythonOperator in this sample Airflow DAG

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# other packages
from datetime import date, timedelta, datetime


# #### Setting up default_args

# In[ ]:


#Setting up default_args

# These args will get passed on to each operator
# We can override them on a per-task basis during operator initialization
# Airflow offers a big number of default arguments that make DAG configuration even simpler

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'end_date': end_date,
    'email': ['username@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


# #### Defining our DAG, Tasks, and Operators

# In[ ]:


def task1():
 # code that writes the dataset to a Spark dataframe

def task2():
 # code that creates CSV output file under out/out_2_2.txt that lists the minimum price, 
 # maximum price, and total row count from this dataset.

def task3():
 # code that calculates the average number of bathrooms and bedrooms across all the properties 
 # listed in this data set with a price of > 5000 and a review score being exactly equalt to 10.

def task4():
 # code that calculates the accomodated people by the property with the lowest price and highest rating

def task5():
 # code that define task5 

def task6():
 # code that define task6


# #### Instantiate a DAG

# In[ ]:


#The DAG can then be instantiated as follows

sample_dag = DAG(
  dag_id='coding_challenge_dag', 
  description='Sample Airflow Dag that runs task 1, followed by tasks 2, and 3 in parallel, then by tasks 4, 5, 6 all in parallel',
  default_args=default_args
)


# #### Tasks

# In[ ]:


# This step consists of defining the tasks of DAG

task1_load = DummyOperator(task_id='task1', dag=dag)

task2_MinMax = DummyOperator(task_id='task2', dag=dag)

task3_average = DummyOperator(task_id='task3', dag=dag)

task4_accPeople = DummyOperator(task_id='task4', dag=dag)

task5_airFlow = DummyOperator(task_id='task5', dag=dag)

task6_unknown = DummyOperator(task_id='task6', dag=dag)


# #### Setting up Dependencies

# In[ ]:


# Lastly, we need to specify the dependencies. In our case, we want task_2 and task 3 to run in parallel after task_1,
# then task_4, task_5 and task_6 in parallel:

task1_load >> [task2_MinMax, task3_average]

[task2_MinMax, task3_average] >> task4_accPeople

[task2_MinMax, task3_average] >> task5_airFlow

[task2_MinMax, task3_average] >> task6_unknown


# We can also use set_downstream() or set_upstream() to create the dependencies

# #### Adding the DAG to Airflow scheduler 

# In[ ]:


#By using following commands we can start Airflow scheduler

airflow webserver
airflow scheduler


# #### Note:
# 
# The question was asked to run tasks in parallel. For that we need to use LocalExecutor.
# 
# Airflow uses a backend database to store metadata. The default database for Airflow is SQLite which supports only 1 connection at a time.
# 
# So by default Airflow uses SequentialExecutor which would execute task sequentially. In order to execute tasks in parallel, we need to create a database in MySQL or PostgresSQL and configure it in airflow.cfg and then change the executor to LocalExecutor in airflow.cfg and then we need to run airflow initdb. Now, after successful completion of task 1 job, task 2 and task 3 will run in parallel followed by Task 4, 5 and 6.
# 

# #### *
# I don't have pratical industry experience using Apache Airflow and I didn't get a chance to work on Airflow since we were using very similar tools for workflow automation and scheduling such as Skybot and Control Application(internally developed) in my previous organization.
# 
# Using my conceptual knowledge and academic experience I tried my best to answer this question by taking reference from official Airflow documentation. https://airflow.apache.org/docs/apache-airflow/stable/index.html

#                                                         End
