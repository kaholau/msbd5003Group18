import math
import sys
import os
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SQLContext
from operator import add
import optics
import partition 
from aggregator import ClusterAggregator
#from pyspark.sql.functions import *
#os.environ["PYSPARK_SUBMIT_ARGS"] = (
#    "--packages graphframes:graphframes:0.2.0-spark2.0-s_2.11 pyspark-shell"
#)

def create_neighborhoods(data,bounding_boxes,eps):
    """
    Expands bounding boxes by 2 * eps and creates neighborhoods of
    items within those boxes with partition ids in key.
    """
    #print 'create_neighborhoods'
    neighbors = {}
    new_data = data.context.emptyRDD()
    expanded_boxes={}
    for label, box in bounding_boxes.iteritems():
        expanded_box = box.expand( eps)
        expanded_boxes[label] = expanded_box
        neighbors[label] = data.filter(lambda (k, v): expanded_box.contains(v)).map(lambda (k, v): ((k, label), v))
        new_data = new_data.union(neighbors[label])
    #print new_data.top(10)#((149, 7), array([ 75.,  72.]))
    return neighbors, expanded_boxes , new_data

def map_cluster_id((key, cluster_id), broadcast_dict):
    """
    :type broadcast_dict: pyspark.Broadcast
    :param broadcast_dict: Broadcast variable containing a dictionary
        of cluster id mappings
    :rtype: int, int
    :return: key, cluster label
    Modifies the item key to include the remapped cluster label,
    choosing the first id if there are multiple ids
    """
    cluster_id = next(iter(cluster_id)).strip('*')
    cluster_dict = broadcast_dict.value

    if '-1' not in cluster_id and cluster_id in cluster_dict:
        return key, cluster_dict[cluster_id]
    else:
        return key, -1

def map_object_cluster_id(p_obj, broadcast_dict):
    """
    :type broadcast_dict: pyspark.Broadcast
    :param broadcast_dict: Broadcast variable containing a dictionary
        of cluster id mappings
    :rtype: int, int
    :return: key, cluster label
    Modifies the item key to include the remapped cluster label,
    choosing the first id if there are multiple ids
    """
    cluster_id = p_obj.cluster.strip('*')
    cluster_dict = broadcast_dict.value

    if '-1' not in cluster_id and cluster_id in cluster_dict:
        return (cluster_dict[cluster_id],p_obj.parti_id,p_obj.opticsId),p_obj
    else:
        return (sys.maxsize,p_obj.parti_id,p_obj.opticsId),p_obj

def remap_cluster_ids(data):
    """
    Scans through the data for collisions in cluster ids, creating
    a mapping from partition level clusters to global clusters.
    """

    labeled_points = data.map(lambda x:x[0])
    object_points = data.map(lambda x:x[1]).groupByKey().map(lambda x:(x[0],min(list(x[1]),key = (lambda x:x.reachDis)))).cache()
    labeled_points = labeled_points.groupByKey()
    labeled_points.cache()
    #test = labeled_points.map(lambda x:(x[0],list(x[1]))).sortByKey().collect()

    #for t in test:
    #   print "{},{}".format(t[0],t[1])
    mapper = labeled_points.aggregate(ClusterAggregator(), add, add)
    b_mapper = data.context.broadcast(mapper.fwd)
    #b_mapper = data.context.broadcast(remapByGraph(data))
    #print mapper.fwd
    #print mapper.rev
    result = labeled_points \
        .map(lambda x: map_cluster_id(x, b_mapper)) \
        .sortByKey()
    object_result = object_points.map(lambda x:map_object_cluster_id(x[1],b_mapper)).sortBy(lambda x:x[0])
    return result,object_result

def BFS(g):
    first = g.vertices.select('id').take(1)[0]['id']
    #print first
    layers = g.vertices.select('id').where("id='{}'".format(first))
    #layers = [g.vertices.select('id').where("'id'='a'")]
    visited =  layers

    while layers.count() > 0:
        # From the current layer, get all the one-hop neighbors
        d1 = layers.join(g.edges, layers['id'] == g.edges['src'])
        # Rename the column as 'id', and remove visited verices and duplicates
        layers = d1.select(d1['dst'].alias('id')) \
               .subtract(visited).distinct()
        visited = visited.union(layers)

    #visited.show()
    return visited.cache()
     

def remapByGraph(data):
    noises = data.filter(lambda x : '-1' in x[1])
    neighbors = data.filter(lambda x : '*' in x[1]).subtract(noises)
    cores = data.subtract(noises).subtract(neighbors)
    #print noises.top(10)
    #print neighbors.top(10)
    #print cores.top(10)
    cores = cores.groupByKey()
    def formEgde(pt_gid,overlap_core):
        if len(overlap_core)>1:
            edges=[]
            for i,c in enumerate(overlap_core[0:-1]):
                edges.append(('{}'.format(c),\
                                '{}'.format(overlap_core[i+1]),\
                                '{}'.format(pt_gid)))
            return edges
        else:
            return
    cores_edge = cores.map(lambda x:(x[0],list(x[1]))).flatMap(lambda x:formEgde(x[0],x[1]))
    #print cores_edge.top(10)
    edges = spark.createDataFrame(cores_edge,['src','dst','pt_gid'])
    nodeID = cores.flatMap(lambda x:list(x[1])).distinct().map(lambda x:(x,))
    #print nodeID.top(10)
    vertices = spark.createDataFrame(nodeID,['id'])
    g = GraphFrame(vertices,edges)
    cid = 0
    #g.vertices.show()
    g_sub = g.vertices.select('id')
    #g_sub.show()
    mapper = data.context.emptyRDD()
    #mapper.show()
    while g_sub.count()>0:
        #print g_sub.count()
        cluster = BFS(g)
        #print "cluster:",cluster.count()
        #cluster.show()
        g_sub.subtract(cluster)
        g_sub.show()
        g = GraphFrame(g_sub,edges)
        #g_sub = g.vertices.select('id')

        cluster = cluster.rdd.map(lambda x:x['id'],cid)
        mapper = mapper.union(cluster)
        cid+=1
    return h.collectAsMap()

def run(points,MIN_PTS_NUM,RADIUS):
    sc=SparkContext()
    sc.setLogLevel("ERROR") 
    spark = SQLContext(sc)
    #from graphframes import *
    test_data = sc.parallelize(enumerate(points))
    #print test_data.top(10)

    kdpart = partition.KDPartitioner(test_data, 8, 2)
    neighbors, expanded_boxes , new_data = create_neighborhoods(test_data,kdpart.bounding_boxes,RADIUS)
    #final = kdpart.result.collect()
    #plot_partition(final)
    #print test_data.top(10) #(149, array([ 75.,  72.]))

    #k is the index in points, p is the parition ID, v is the point
    data = new_data.map(lambda ((k, p), v): (p, (k, v))) \
                .partitionBy(len(kdpart.partitions)).cache()

    #print data.top(10)
    params = {'min_pts_num':MIN_PTS_NUM,'radius':RADIUS}

    def optics_do(iterator,params):
        pt_obj_list = []
        points = []

        for l,i in enumerate(iterator):
            pt_obj_list.append( optics.Point(l,i[1][0],i[0]))
            points.append( i[1][1])
        op = optics.OPTICS(params['min_pts_num'],params['radius'])
        result = op.run(pt_obj_list,points)
        numOfcluster, lb = op.getCluter(result, RADIUS)
        for i,p in enumerate(lb):
            flag = '*' if p.notCore else ''
            p.cluster = '{}:{}{}'.format(p.parti_id,p.cluster,flag)
            yield (p.gid,p.cluster),(p.gid,p)


    data = data.mapPartitions(lambda iterable: optics_do(iterable,params)).cache()
    #print data.top(149)
    #exit()
    clusters,object_result = remap_cluster_ids(data)
    clusters = clusters.collect()
    object_result = object_result.map(lambda x:x[1]).collect()
    #print object_result
    clusters = np.asarray(clusters)
    #print clusters
    data  = data.map(lambda x:x[0]).glom().collect()
    return clusters[:, 1],data,kdpart.bounding_boxes,expanded_boxes,object_result