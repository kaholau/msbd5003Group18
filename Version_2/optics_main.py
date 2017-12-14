import math
import sys
import os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.patches as patches
import random
from time import gmtime, strftime
import datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext
import optics
import partition 

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
    plt.scatter(pts_x, pts_y,c=color,marker='.')

def plot_partition(final):
    partitions = [a[0][1] for a in final]

    x = [a[1][0] for a in final]
    y = [a[1][1] for a in final]
    fig = plt.figure(figsize=(10, 10))
    ax = fig.add_subplot(111)
    colors = cm.spectral(np.linspace(0, 1, len(kdpart.bounding_boxes)))
    for label, box in kdpart.bounding_boxes.iteritems():
        ax.add_patch(
            patches.Rectangle(box.lower, *(box.upper - box.lower),
                              alpha=0.5, color=colors[label], zorder=0))

    plt.scatter(x, y, c=partitions)
    if not os.access('plots', os.F_OK):
        os.mkdir('plots')
    plt.savefig('./partitioning.png')
    plt.close()
    plt.scatter(x, y)
    plt.savefig('./toy_data.png')
    plt.close()

    return
def create_neighborhoods(data,bounding_boxes,eps):
    """
    Expands bounding boxes by 2 * eps and creates neighborhoods of
    items within those boxes with partition ids in key.
    """
    print 'create_neighborhoods'
    neighbors = {}
    new_data = data.context.emptyRDD()
    expanded_boxes={}
    for label, box in bounding_boxes.iteritems():
        expanded_box = box.expand(2 * eps)
        expanded_boxes[label] = expanded_box
        neighbors[label] = data.filter(
            lambda (k, v): expanded_box.contains(v)) \
            .map(lambda (k, v): ((k, label), v))
        new_data = new_data.union(neighbors[label])
    #print new_data.top(10)#((149, 7), array([ 75.,  72.]))
    return neighbors, expanded_boxes , new_data

num_Of_Testing_pt = 150

Testing_pt_centers = [[40.0,25.0],[60.0,25.0],[25.0,75.0],[75.0,75.0]]
Testing_pt_centers_dev = [8.0,8.0,15.0,20.0]

deviation_From_Testing_Center = max(Testing_pt_centers_dev)
random.seed(11111111)
points = get_random_point(Testing_pt_centers,num_Of_Testing_pt,Testing_pt_centers_dev)
#points =[[17.0, 21.0], [8.0, 25.0], [8.0, 18.0], [22.0, 23.0], [9.0, 25.0], [15.0, 7.0], [14.0, 10.0], [18.0, 13.0], [15.0, 20.0], [14.0, 7.0], [47.0, 46.0], [49.0, 63.0], [55.0, 53.0], [57.0, 49.0], [41.0, 46.0], [54.0, 47.0], [45.0, 60.0], [58.0, 46.0], [50.0, 57.0], [57.0, 64.0]]
#this one gives good looking figures
#points  = [[10.0, 21.0], [6.0, 8.0], [20.0, 14.0], [24.0, 16.0], [12.0, 7.0], [7.0, 19.0], [18.0, 18.0], [6.0, 15.0], [21.0, 10.0], [15.0, 19.0], [22.0, 11.0], [7.0, 7.0], [18.0, 21.0], [10.0, 23.0], [10.0, 10.0], [12.0, 22.0], [10.0, 20.0], [19.0, 18.0], [16.0, 24.0], [22.0, 24.0], [22.0, 13.0], [14.0, 7.0], [24.0, 11.0], [24.0, 22.0], [22.0, 19.0], [11.0, 19.0], [17.0, 14.0], [6.0, 22.0], [6.0, 13.0], [19.0, 22.0], [14.0, 25.0], [7.0, 17.0], [13.0, 21.0], [9.0, 13.0], [7.0, 14.0], [13.0, 12.0], [20.0, 20.0], [20.0, 22.0], [12.0, 24.0], [13.0, 24.0], [10.0, 16.0], [22.0, 17.0], [24.0, 13.0], [15.0, 10.0], [15.0, 8.0], [24.0, 15.0], [18.0, 11.0], [9.0, 17.0], [11.0, 16.0], [7.0, 17.0], [48.0, 54.0], [58.0, 61.0], [48.0, 57.0], [50.0, 48.0], [45.0, 59.0], [41.0, 56.0], [50.0, 48.0], [49.0, 50.0], [46.0, 55.0], [41.0, 54.0], [43.0, 64.0], [51.0, 57.0], [52.0, 54.0], [50.0, 48.0], [52.0, 62.0], [53.0, 57.0], [51.0, 60.0], [46.0, 55.0], [51.0, 51.0], [57.0, 58.0], [49.0, 50.0], [44.0, 48.0], [48.0, 60.0], [50.0, 62.0], [44.0, 53.0], [41.0, 54.0], [58.0, 57.0], [49.0, 60.0], [49.0, 54.0], [58.0, 54.0], [56.0, 61.0], [53.0, 61.0], [44.0, 51.0], [56.0, 53.0], [50.0, 51.0], [48.0, 64.0], [41.0, 55.0], [49.0, 64.0], [50.0, 53.0], [42.0, 50.0], [55.0, 64.0], [50.0, 49.0], [49.0, 57.0], [44.0, 54.0], [59.0, 63.0], [51.0, 61.0], [46.0, 54.0], [59.0, 55.0], [48.0, 62.0], [54.0, 47.0]]
points = np.asarray(points)
MIN_PTS_NUM = 4
RADIUS = 15

sc=SparkContext()
sc.setLogLevel("ERROR") 

print('Start: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
test_data = sc.parallelize(enumerate(points))
kdpart = partition.KDPartitioner(test_data, 8, 2)
neighbors, expanded_boxes , new_data = create_neighborhoods(test_data,kdpart.bounding_boxes,RADIUS)
#final = kdpart.result.collect()
#plot_partition(final)
#print test_data.top(10) #(149, array([ 75.,  72.]))

#k is the index in points, p is the parition ID, v is the point
rdd = new_data.map(lambda ((k, p), v): (p, (k, v))) \
            .partitionBy(len(kdpart.partitions)) \
            .map(lambda (p, (k, v)):((k, p), v) ).cache()
#print rdd.top(10)
params = {'min_pts_num':MIN_PTS_NUM,'radius':RADIUS}

def optics_do(iterator,params):
    pt_obj_list = []
    points = []

    for l,i in enumerate(iterator):
        pt_obj_list.append( optics.Point(l,i[0][0],i[0][1]))
        points.append( i[1])
    op = optics.OPTICS(params['min_pts_num'],params['radius'])
    result = op.run(pt_obj_list,points)
    #for p in result:
    #    p.id = OFFSET.value[index]+p.id
    numOfcluster, lb = op.getCluter(0,result, 6.5)
    print numOfcluster
    yield lb

lb = rdd.mapPartitions(lambda iterable: optics_do(iterable,params)).collect()
for l in lb:
    print l
exit()

# result is a rdd of Point Class Object sorted base of opticId

#result  = op.run(points)
#op = optics.OPTICS(MIN_PTS_NUM,RADIUS)
#cluster, lb = op.getCluter(result, 6.5)
#print ("\nSome Pionts object:\n",result)
print('Complete: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
#below is for generating csv, plotting and save the figure only
pt_new = []
pt_c = []
with open("./figure/optics_result_{}.csv".format(strftime("%Y%m%d%H%M%S", gmtime())),"w") as rfile:
    rfile.write("Pid,OPTid,Px,Py,rD,cD,cluster,notCore\n")
    for p in lb:
        rfile.write("{},{},{},{},{},{},{},{}\n"\
            .format(p.id,p.opticsId,points[p.id][0],points[p.id][1],p.reachDis,p.coreDis,p.cluster,p.notCore))
        pt_new.append(points[p.id])
        pt_c.append(float(p.cluster))
color = []
for c in pt_c:
    if not (c in color):
        color.append(c)
#print (pt_c)
color_n =max(pt_c)
pt_c = [c/color_n if c <sys.maxsize else 1 for c in pt_c ]
#print (pt_new)
#print (pt_c)
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
#print ("\nSome reachable distance:\n",op_np)
plt.savefig("./figure/figure_m{}_e{}_s{}_1_{}".format(MIN_PTS_NUM,RADIUS,num_Of_Testing_pt,strftime("%Y%m%d%H%M%S", gmtime())))
plt.figure()
plt.bar(xrange(op_np.size), op_np)
plt.savefig("./figure/figure_m{}_e{}_s{}_2_{}".format(MIN_PTS_NUM,RADIUS,num_Of_Testing_pt,strftime("%Y%m%d%H%M%S", gmtime())))
plt.show()
raw_input('Waiting a key...')

