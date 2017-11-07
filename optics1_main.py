import sys
import math
from pyspark import SparkContext
import optics1


sc=SparkContext()
x = [[1,23],[4,5],[6,2],[3,5],[6,7],[8,9],[0,2],[3,4],[5,7],[8,2],[4,7],[4,5],[7,4]]

rdd = sc.parallelize(x, 4).cache()


optics1.run(rdd)
