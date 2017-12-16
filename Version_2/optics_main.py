import math
import sys
import os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.patches as patches
from itertools import izip
import random
from time import gmtime, strftime
import datetime
import optics_parallel


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


num_Of_Testing_pt = 150

Testing_pt_centers = [[40.0,25.0],[60.0,25.0],[25.0,75.0],[75.0,75.0]]
Testing_pt_centers_dev = [8.0,8.0,15.0,20.0]

deviation_From_Testing_Center = max(Testing_pt_centers_dev)
random.seed(11111111)
points = get_random_point(Testing_pt_centers,num_Of_Testing_pt,Testing_pt_centers_dev)
MIN_PTS_NUM = 4
RADIUS = 15

print('Start: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
points = np.asarray(points)
# result is a list of Point Class Object sorted base of opticId
clusters,data,bounding_boxes,expanded_boxes = optics_parallel.run(points, MIN_PTS_NUM,RADIUS)

print('Complete: {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
#below is for generating csv, plotting and save the figure only
#print clusters
#print data 
colors = cm.spectral(np.linspace(0, 1, len(bounding_boxes)))
if not os.access('plots', os.F_OK):
    os.mkdir('plots')
X = points
lim1 = 0
lim2 = 100
#print X
size = (10,10)
for i, t in enumerate(data):
    x = [X[t2[0]][0] for t2 in t]
    y = [X[t2[0]][1] for t2 in t]
    c = [int(t2[1].split(':')[1].strip('*')) for t2 in t]
    l = [int(t2[1].split(':')[0]) for t2 in t]
    box1 = bounding_boxes[l[0]]
    box2 = expanded_boxes[l[0]]
    in_box = [box2.contains([a, b]) for a, b in izip(x, y)]
    fig = plt.figure(figsize=size)
    ax = fig.add_subplot(111)
    ax.add_patch(
        patches.Rectangle(box1.lower, *(box1.upper - box1.lower),
                          alpha=0.4, color=colors[i], zorder=0))
    ax.add_patch(
        patches.Rectangle(box2.lower, *(box2.upper - box2.lower),
                          fill=False, zorder=0))
    plt.scatter(x, y, c=c, zorder=1)
    plt.xlim(-lim1, lim2)
    plt.ylim(-lim1, lim2)
    plt.savefig('plots/partition_%i.png' % i)
    plt.close()

x = X[:, 0]
y = X[:, 1]
fig = plt.figure(figsize=size)
plt.scatter(x, y, c=clusters)
plt.xlim(-lim1, lim2)
plt.ylim(-lim1, lim2)
plt.savefig('plots/clusters.png')
plt.close()
fig = plt.figure(figsize=size)

ax = fig.add_subplot(111)
for i, b in bounding_boxes.iteritems():
    ax.add_patch(
        patches.Rectangle(b.lower, *(b.upper - b.lower),
                          color=colors[i], alpha=0.5, zorder=0))

plt.scatter(x, y, c=clusters, zorder=1)
plt.xlim(-lim1, lim2)
plt.ylim(-lim1, lim2)
plt.savefig('plots/clusters_partitions.png')
plt.close()


'''pt_new = []
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

op_np = np.array(op_rDis)
plt.savefig("./figure/figure_m{}_e{}_s{}_1_{}".format(MIN_PTS_NUM,RADIUS,num_Of_Testing_pt,strftime("%Y%m%d%H%M%S", gmtime())))
plt.figure()
plt.bar(xrange(op_np.size), op_np)
plt.savefig("./figure/figure_m{}_e{}_s{}_2_{}".format(MIN_PTS_NUM,RADIUS,num_Of_Testing_pt,strftime("%Y%m%d%H%M%S", gmtime())))
plt.show()
raw_input('Waiting a key...')'''

