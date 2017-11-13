import sys
import math
from pyspark import SparkContext
import optics
import numpy as np
import matplotlib.pyplot as plt
import random

source = [[15,16],[50,55]]
numOfpt = 50
deviationFromPoint = 20
points =[]
for center in source:
	for pt in range(numOfpt):
	  points.append([center[i] + (-1 if random.random()>0.5 else 1)\
	  	*int(random.random() * deviationFromPoint) for i in range(len(center))]) 

pts_x = np.array([x[0] for x in points])
pts_y = np.array([y[1] for y in points])
plt.scatter(pts_x, pts_y)


#print points

sc=SparkContext()



rdd = sc.parallelize(points, 2).cache()
MIN_PTS_NUM = sc.broadcast(4)
RADIUS = sc.broadcast(17)

op = optics.OPTICS(MIN_PTS_NUM,RADIUS)
result  = op.run(rdd)

print result.takeOrdered(12, key = lambda x: x.opticsId)
plt.show()
