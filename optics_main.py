import sys
import math
from pyspark import SparkContext
import optics


sc=SparkContext()
x = [[1,23],[4,5],[6,2],[3,5],[6,7],[8,9],[0,2],[3,4],[5,7],[8,2],[4,7],[4,5],[7,4]]

rdd = sc.parallelize(x, 2).cache()
MIN_PTS_NUM = sc.broadcast(4)
RADIUS = sc.broadcast(17)

op = optics.OPTICS(MIN_PTS_NUM,RADIUS)
result  = op.run(rdd)

print result.take(12)
