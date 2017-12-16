from pyspark import SparkContext
from pyspark.sql import SQLContext
import aggregator as agg
import optics
import partition 
import numpy as np
import math
from operator import add


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

def remap_cluster_ids(data):
    """
    Scans through the data for collisions in cluster ids, creating
    a mapping from partition level clusters to global clusters.
    """
    labeled_points = data.groupByKey()
    labeled_points.cache()
    mapper = labeled_points.aggregate(agg.ClusterAggregator(), add, add)
    b_mapper = data.context.broadcast(mapper.fwd)
    result = labeled_points \
        .map(lambda x: map_cluster_id(x, b_mapper)) \
        .sortByKey()
    return result


def run(points,MIN_PTS_NUM,RADIUS):
    sc=SparkContext()
    sc.setLogLevel("ERROR") 
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
        for p in lb:
            flag = '*' if p.notCore else ''
            yield (p.gid,'{}:{}{}'.format(p.pid,p.cluster,flag))


    data = data.mapPartitions(lambda iterable: optics_do(iterable,params)).cache()
    #print data.top(149)
    #exit()
    clusters = remap_cluster_ids(data).collect()
    clusters = np.asarray(clusters)
    data  = data.glom().collect()
    return clusters[:, 1],data,kdpart.bounding_boxes,expanded_boxes