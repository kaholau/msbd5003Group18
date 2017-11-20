import sys
import math
from pyspark import SparkContext
from pyspark.sql import SQLContext
import optics
import numpy as np
import matplotlib.pyplot as plt
import random
from pyspark.sql import Row
from time import gmtime, strftime


def get_random_point(source,numOfpt,dev):
	points =[]
	noise_portion = 0.2
	if numOfpt>=50:
		pts_x = sum([p[0] for p in source])
		pts_y = sum([p[1] for p in source])
		center_n = [pts_x/len(source),pts_y/len(source)]
		dev_n = max([math.sqrt((s[0]-center_n[0])**2+(s[1]-center_n[1])**2) for s in source])
		for pt in range(int(numOfpt*noise_portion)):
			points.append([center_n[i] + (-1 if random.random()>0.5 else 1)\
		  	*int(random.random() * 35) for i in range(len(center_n))])
		numOfpt = numOfpt*(1-noise_portion)

	for pt in range(int(numOfpt/len(source))):
		for i,center in enumerate(source):
			  points.append([center[i] + (-1 if random.random()>0.5 else 1)\
		  	*int(random.random() * dev[i]) for i in range(len(center))])
	return points

def plot_point(points,color):
	pts_x = np.array([x[0] for x in points])
	pts_y = np.array([y[1] for y in points])
	plt.scatter(pts_x, pts_y,c=color)

	return


sc=SparkContext()
sc.setLogLevel("ERROR")

Testing_pt_centers = [[40.0,25.0],[60.0,25.0],[25.0,75.0],[75.0,75.0]]
Testing_pt_centers_dev = [8.0,8.0,15.0,20.0]
num_Of_Testing_pt = 150
deviation_From_Testing_Center = max(Testing_pt_centers_dev)
random.seed(11111111)
points = get_random_point(Testing_pt_centers,num_Of_Testing_pt,Testing_pt_centers_dev)
#points =[[17.0, 21.0], [8.0, 25.0], [8.0, 18.0], [22.0, 23.0], [9.0, 25.0], [15.0, 7.0], [14.0, 10.0], [18.0, 13.0], [15.0, 20.0], [14.0, 7.0], [47.0, 46.0], [49.0, 63.0], [55.0, 53.0], [57.0, 49.0], [41.0, 46.0], [54.0, 47.0], [45.0, 60.0], [58.0, 46.0], [50.0, 57.0], [57.0, 64.0]]
#this one gives good looking figures
#points  = [[10.0, 21.0], [6.0, 8.0], [20.0, 14.0], [24.0, 16.0], [12.0, 7.0], [7.0, 19.0], [18.0, 18.0], [6.0, 15.0], [21.0, 10.0], [15.0, 19.0], [22.0, 11.0], [7.0, 7.0], [18.0, 21.0], [10.0, 23.0], [10.0, 10.0], [12.0, 22.0], [10.0, 20.0], [19.0, 18.0], [16.0, 24.0], [22.0, 24.0], [22.0, 13.0], [14.0, 7.0], [24.0, 11.0], [24.0, 22.0], [22.0, 19.0], [11.0, 19.0], [17.0, 14.0], [6.0, 22.0], [6.0, 13.0], [19.0, 22.0], [14.0, 25.0], [7.0, 17.0], [13.0, 21.0], [9.0, 13.0], [7.0, 14.0], [13.0, 12.0], [20.0, 20.0], [20.0, 22.0], [12.0, 24.0], [13.0, 24.0], [10.0, 16.0], [22.0, 17.0], [24.0, 13.0], [15.0, 10.0], [15.0, 8.0], [24.0, 15.0], [18.0, 11.0], [9.0, 17.0], [11.0, 16.0], [7.0, 17.0], [48.0, 54.0], [58.0, 61.0], [48.0, 57.0], [50.0, 48.0], [45.0, 59.0], [41.0, 56.0], [50.0, 48.0], [49.0, 50.0], [46.0, 55.0], [41.0, 54.0], [43.0, 64.0], [51.0, 57.0], [52.0, 54.0], [50.0, 48.0], [52.0, 62.0], [53.0, 57.0], [51.0, 60.0], [46.0, 55.0], [51.0, 51.0], [57.0, 58.0], [49.0, 50.0], [44.0, 48.0], [48.0, 60.0], [50.0, 62.0], [44.0, 53.0], [41.0, 54.0], [58.0, 57.0], [49.0, 60.0], [49.0, 54.0], [58.0, 54.0], [56.0, 61.0], [53.0, 61.0], [44.0, 51.0], [56.0, 53.0], [50.0, 51.0], [48.0, 64.0], [41.0, 55.0], [49.0, 64.0], [50.0, 53.0], [42.0, 50.0], [55.0, 64.0], [50.0, 49.0], [49.0, 57.0], [44.0, 54.0], [59.0, 63.0], [51.0, 61.0], [46.0, 54.0], [59.0, 55.0], [48.0, 62.0], [54.0, 47.0]]

MIN_PTS_NUM = 4
RADIUS = 15

rdd = sc.parallelize(points, 10).cache()
print "\nThe original points:\n",points
min_pts_num = sc.broadcast(MIN_PTS_NUM)
radius = sc.broadcast(RADIUS)
op = optics.OPTICS(min_pts_num,radius)

# result is a rdd of Point Class Object sorted base of opticId
result  = op.run(rdd)
flag, lb = op.getCluter(result, 6.5)
print "\nSome Pionts object:\n",result


#below is for generating csv, plotting and save the figure only
pt_new = []
pt_c = []
with open("./figure/optics_result_{}.csv".format(strftime("%Y%m%d%H%M%S", gmtime())),"w") as rfile:
	rfile.write("Pid,OPTid,Px,Py,rD,cD,cluster,notCore\n")
	for p in lb:
		rfile.write("{},{},{},{},{},{},{},{}\n"\
			.format(p.id,p.opticsId,points[p.id][0],points[p.id][1],p.reachDis,p.coreDis,p.flag,p.notCore))
		pt_new.append(points[p.id])
		pt_c.append(float(p.flag))
color = []
for c in pt_c:
	if not (c in color):
		color.append(c)
color_n =float(len(color)+1)
pt_c = [c/color_n if c <99 else 1 for c in pt_c ]
print pt_new
print pt_c
plot_point(pt_new,pt_c)

dev = deviation_From_Testing_Center
op_rDis = [p.reachDis if p.reachDis <float(dev)*2 else float(dev)*2 for p in lb ]
#print op_rDis.take(10)
#sqlc = SQLContext(sc)
#row = Row("dis")
#op_df = op_rDis.map(row).toDF()

#print op_df.head(10)
#op_np = op_df.toPandas().values.reshape(-1)#np.array(op_df.select('*'))
op_np = np.array(op_rDis)
print "\nSome reachable distance:\n",op_np
plt.savefig("./figure/figure_m{}_e{}_s{}_1_{}".format(MIN_PTS_NUM,RADIUS,num_Of_Testing_pt,strftime("%Y%m%d%H%M%S", gmtime())))
plt.figure()
plt.bar(xrange(op_np.size), op_np)
plt.savefig("./figure/figure_m{}_e{}_s{}_2_{}".format(MIN_PTS_NUM,RADIUS,num_Of_Testing_pt,strftime("%Y%m%d%H%M%S", gmtime())))
plt.show()
