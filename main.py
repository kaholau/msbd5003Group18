import sys
import math
import os
from pyspark import SparkContext


sc=SparkContext()
x = [[1,23],[4,5],[6,2],[3,5],[6,7],[8,9],[0,2],[3,4],[5,7],[8,2],[4,7],[4,5],[7,4]]

rdd = sc.parallelize(x, 4).cache()

class Point:
    def __init__(self,id_,info_):
        self.id = id_
        self.info = info_
        self.reachDis = float("inf")
        self.coreDis = float("inf")
        self.processed = False
        self.opticsId = float("inf")#sys.maxint
        self.notCore = False

def getDistances(allPoints, coordinateValue):
    distances = []
    #sqrt((xa-xb)^2 + (ya-yb)^2 + (za-zb)^2)

    for p in allPoints:
        distance = 0
        for pp,cc in zip(p,coordinateValue):
            distance += (pp-cc)**2
        distances.append(math.sqrt(distance))
        
    return distances
        
for q in x:
    print getDistances(x,q)