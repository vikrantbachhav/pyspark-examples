# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 22:42:50 2019

@author: prabha
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DateType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

sc = spark._jsc.sc()
n_workers = len([executor.host() for executor in sc.statusTracker().getExecutorInfos()]) - 1
print(n_workers)

print('Apache Spark Version :'+spark.version)
print('Apache Spark Version :'+spark.sparkContext.version)