import sys
import math
from pyspark import SparkContext
from pyspark.sql import SQLContext
import optics
import numpy as np
import matplotlib.pyplot as plt
import random
from pyspark.sql import Row



def get_random_point(source,numOfpt,deviationFromPoint):
	points =[]
	dev=float(deviationFromPoint)
	for center in source:
		for pt in range(numOfpt):
		  points.append([center[i] + (-1 if random.random()>0.5 else 1)\
		  	*int(random.random() * dev) for i in range(len(center))])
	return points

def plot_point(points):
	pts_x = np.array([x[0] for x in points])
	pts_y = np.array([y[1] for y in points])
	plt.scatter(pts_x, pts_y)

	return



sc=SparkContext()
sc.setLogLevel("ERROR")
source = [[15.0,16.0],[50.0,55.0]]
numOfpt = 50
deviationFromPoint = 10
random.seed(11111111)
points = get_random_point(source,numOfpt,deviationFromPoint)
#points =[[17.0, 21.0], [8.0, 25.0], [8.0, 18.0], [22.0, 23.0], [9.0, 25.0], [15.0, 7.0], [14.0, 10.0], [18.0, 13.0], [15.0, 20.0], [14.0, 7.0], [47.0, 46.0], [49.0, 63.0], [55.0, 53.0], [57.0, 49.0], [41.0, 46.0], [54.0, 47.0], [45.0, 60.0], [58.0, 46.0], [50.0, 57.0], [57.0, 64.0]]
#this one gives good looking figures
#points  = [[10.0, 21.0], [6.0, 8.0], [20.0, 14.0], [24.0, 16.0], [12.0, 7.0], [7.0, 19.0], [18.0, 18.0], [6.0, 15.0], [21.0, 10.0], [15.0, 19.0], [22.0, 11.0], [7.0, 7.0], [18.0, 21.0], [10.0, 23.0], [10.0, 10.0], [12.0, 22.0], [10.0, 20.0], [19.0, 18.0], [16.0, 24.0], [22.0, 24.0], [22.0, 13.0], [14.0, 7.0], [24.0, 11.0], [24.0, 22.0], [22.0, 19.0], [11.0, 19.0], [17.0, 14.0], [6.0, 22.0], [6.0, 13.0], [19.0, 22.0], [14.0, 25.0], [7.0, 17.0], [13.0, 21.0], [9.0, 13.0], [7.0, 14.0], [13.0, 12.0], [20.0, 20.0], [20.0, 22.0], [12.0, 24.0], [13.0, 24.0], [10.0, 16.0], [22.0, 17.0], [24.0, 13.0], [15.0, 10.0], [15.0, 8.0], [24.0, 15.0], [18.0, 11.0], [9.0, 17.0], [11.0, 16.0], [7.0, 17.0], [48.0, 54.0], [58.0, 61.0], [48.0, 57.0], [50.0, 48.0], [45.0, 59.0], [41.0, 56.0], [50.0, 48.0], [49.0, 50.0], [46.0, 55.0], [41.0, 54.0], [43.0, 64.0], [51.0, 57.0], [52.0, 54.0], [50.0, 48.0], [52.0, 62.0], [53.0, 57.0], [51.0, 60.0], [46.0, 55.0], [51.0, 51.0], [57.0, 58.0], [49.0, 50.0], [44.0, 48.0], [48.0, 60.0], [50.0, 62.0], [44.0, 53.0], [41.0, 54.0], [58.0, 57.0], [49.0, 60.0], [49.0, 54.0], [58.0, 54.0], [56.0, 61.0], [53.0, 61.0], [44.0, 51.0], [56.0, 53.0], [50.0, 51.0], [48.0, 64.0], [41.0, 55.0], [49.0, 64.0], [50.0, 53.0], [42.0, 50.0], [55.0, 64.0], [50.0, 49.0], [49.0, 57.0], [44.0, 54.0], [59.0, 63.0], [51.0, 61.0], [46.0, 54.0], [59.0, 55.0], [48.0, 62.0], [54.0, 47.0]]

rdd = sc.parallelize(points, 2).cache()
print "\nThe original points:\n",points
MIN_PTS_NUM = sc.broadcast(4)
RADIUS = sc.broadcast(10)
op = optics.OPTICS(MIN_PTS_NUM,RADIUS)

# result is a rdd of Point Class Object sorted base of opticId
result  = op.run(rdd)
flag, lb = op.getCluter(result, 10)
print "\nSome Pionts object:\n",result.take(10)





#below is for plotting only
plot_point(points)
plt.figure()

op_rDis = result.map(lambda p: p.reachDis \
				if p.reachDis <float(deviationFromPoint)*2 else float(deviationFromPoint)*2)
#print op_rDis.take(10)
sqlc = SQLContext(sc)
row = Row("dis")
op_df = op_rDis.map(row).toDF()

#print op_df.head(10)
op_np = op_df.toPandas().values.reshape(-1)#np.array(op_df.select('*'))
print "\nSome reachable distance:\n",op_np

plt.bar(xrange(op_np.size), op_np)
plt.show()