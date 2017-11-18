import sys
import math
from pyspark import SparkContext
from pyspark.sql import SQLContext
import optics
import numpy as np
import matplotlib.pyplot as plt
import random
from pyspark.sql import Row

source = [[15.0,16.0],[50.0,55.0]]
numOfpt = 10
deviationFromPoint = 10

def get_random_point():
	points =[]
	for center in source:
		for pt in range(numOfpt):
		  points.append([center[i] + (-1 if random.random()>0.5 else 1)\
		  	*int(random.random() * deviationFromPoint) for i in range(len(center))]) 
	return points

def plot_point(points):
	pts_x = np.array([x[0] for x in points])
	pts_y = np.array([y[1] for y in points])
	plt.scatter(pts_x, pts_y)

	return



sc=SparkContext()
random_points = get_random_point()
rdd = sc.parallelize(random_points, 2).cache()

MIN_PTS_NUM = sc.broadcast(4)
RADIUS = sc.broadcast(6)
op = optics.OPTICS(MIN_PTS_NUM,RADIUS)

# result is a rdd of Point Class Object sorted base of opticId
result  = op.run(rdd) 

print result.take(10)









'''
#below is for plotting only
plot_point(random_points)
plt.figure()

op_rDis = result.map(lambda p: p.reachDis \
				if p.reachDis <deviationFromPoint*2 else deviationFromPoint*2)
print op_rDis.take(10)
sqlc = SQLContext(sc)
row = Row("dis")
op_df = op_rDis.map(row).toDF()

print op_df.head(10)
op_np = op_df.toPandas().values.reshape(-1)#np.array(op_df.select('*'))
print op_np
print "op_np len:",op_np.size

#plt.subplots()
plt.bar(xrange(op_np.size), op_np)


plt.show()
'''