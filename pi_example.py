from pyspark.sql import SparkSession
import random
import numpy as np
import pandas as pd

import sys


NUM_SAMPLES = 20000 

def inside(p):
    x, y = random.random(), random.random()
    print(f"numpy's version: {np.__version__}") 
    print(f"pandas's version: {pd.__version__}") 
    return x*x + y*y < 1


with SparkSession.builder.appName("Pi Example")\
    .master("spark://localhost:7077")\
    .config("spark.executor.memory", "500m")\
    .config("spark.executor.instances", "1")\
    .getOrCreate() as spark:
    sc = spark.sparkContext

    print(f"numpy's version: {np.__version__}") 
    print(f"pandas's version: {pd.__version__}") 

    """
    print("Python version")
    print (sys.version)
    print("Version info.")
    print (sys.version_info)
    """

    print(f"Number of sampling: {NUM_SAMPLES}")

    count = sc.parallelize(range(0, NUM_SAMPLES)) \
        .filter(inside).count()
    print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))

    rdd=sc.parallelize([1,2,3,4,5])
    rddCollect = rdd.collect()
    print("Number of Partitions: "+str(rdd.getNumPartitions()))
    print("Action: First element: "+str(rdd.first()))
    print(rddCollect)


