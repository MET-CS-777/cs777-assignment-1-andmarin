"""
Andrew Marin
Class CS 777
Assignment #1
Date: 9/12/2024
This script it built to look at taxi data and accomplish 2 tasks. 1 is output the medallion number of taxis that had the top 10 number of distinct drivers. Task 2 is to figure out
the top 10 drivers in regard to average money earned per minute carrying customers. This script was built to run on pyspark. 
The data cleaning functions isfloat and correctRows were provided by the instructor.
"""

from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles, 
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

#Main
if __name__ == "__main__":
    #if len(sys.argv) != 4:
    #    print("Usage: main_task1 <file> <output> ", file=sys.stderr)
    #    exit(-1)
    
    sc = SparkContext(appName="Assignment-1")
    sqlContext = SQLContext(sc)

    testDataFrame = sqlContext.read.format('csv').options(header='false', inferSchema='true',  sep =",").load(sys.argv[1])

    testRDD = testDataFrame.rdd.map(tuple)

    # calling isfloat and correctRows functions to cleaning up data
    taxilinesCorrected = testRDD.filter(correctRows)

    #Task 1
    # Convert driver ids to a set
    taxi_driver_sets = taxilinesCorrected.map(lambda x: (x[0], x[1])).distinct().map(lambda x: (x[0], {x[1]}))

    # Aggregate driver_ids for each medallion id - idea taken from below
    # https://stackoverflow.com/questions/30549322/is-it-more-efficient-to-use-unions-rather-than-joins-in-apache-spark-or-does-it
    aggregated_driver_sets = taxi_driver_sets.reduceByKey(lambda x, y: x.union(y))

    # Count the number of distinct drivers per medallion id
    driver_counts = aggregated_driver_sets.map(lambda x: (x[0], len(x[1])))

    # Sort by count in descending order and take the top 10 ids
    top_ten_taxis = driver_counts.sortBy(lambda x: -x[1]).take(10)

    # Convert top_ten_taxis to an RDD
    top_ten_rdd = sc.parallelize(top_ten_taxis)

    # Save results to output folder
    top_ten_rdd.coalesce(1).saveAsTextFile(sys.argv[2])

    #Task 2
    # Get driver_id, (total_amount, trip_duration in minutes) - value was originally in seconds
    driver_earnings_and_time = taxilinesCorrected.map(lambda x: (x[1], (float(x[16]), float(x[4])/60)))

    # Aggregate total earnings and total minutes for each driver
    aggregated_earnings_and_time = driver_earnings_and_time.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    # Calculate money earned per minute for each driver
    driver_money_per_minute = aggregated_earnings_and_time.map(lambda x: (x[0], x[1][0] / x[1][1]))

    # Sort by money per minute in descending order and take the top 10
    top_ten_drivers = driver_money_per_minute.sortBy(lambda x: -x[1]).take(10)


    #savings output to argument
    top_ten_drivers_rdd = sc.parallelize(top_ten_drivers)
    top_ten_drivers_rdd.coalesce(1).saveAsTextFile(sys.argv[3])

    sc.stop()