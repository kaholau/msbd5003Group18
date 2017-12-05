import sys
import math
from itertools import *
import re

DEBUG_PRINT = False


def natural_sort(l):
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key) ]
    return sorted(l, key = alphanum_key)

def mprint(*str):
    if DEBUG_PRINT:
        print (str)


class Point:
    def __init__(self, ID):
        self.id = ID
        # self.info = info_
        self.reachDis = sys.maxsize # float("inf")
        self.coreDis = sys.maxsize  # float("inf")
        self.processed = False
        self.opticsId = sys.maxsize  # float("inf")
        self.notCore = False
        self.cluster = 0
        self.partition =[]
        return

    def __repr__(self):
        return "[id:{},rD:{},cD:{},optId:{},notC:{},chk:{},cluster:{}]\n" \
            .format(self.id, self.reachDis, self.coreDis, self.opticsId, self.notCore, self.processed, self.cluster)


class OPTICS:
    MIN_PTS_NUM = 0
    RADIUS = 0

    def __init__(self, minPtsNum, radius):
        self.MIN_PTS_NUM = minPtsNum
        self.RADIUS = radius
        return

    def getDistances(self, point, curPoint):
        # sqrt((xa-xb)^2 + (ya-yb)^2 + (za-zb)^2)
        distance = 0
        for pp, cc in zip(point, curPoint):
            distance += (pp - cc) ** 2
        return math.sqrt(distance)

    def createPoint(self, id):
        pt = Point(id)
        return pt

    def updatePoint(self, point, IsCore, hasNeighbor, ID, coreDis, opticsId):
        # mprint (point)
        if IsCore:
            mprint("13")
            if point[0].id == ID:
                point[0].coreDis = coreDis
                point[0].opticsId = opticsId
                point[0].processed = True

            if point[1] <= self.RADIUS and (not point[0].id == ID) and (not point[0].processed):
                mprint("14")
                #point[0].reachDis = min(point[0].reachDis, coreDis,point[1])
                if point[1] < coreDis:
                    mprint("15")
                    point[0].reachDis = min(point[0].reachDis, coreDis)
                else:
                    mprint("16")
                    point[0].reachDis = min(point[0].reachDis, point[1])
                
                point[0].opticsId = opticsId + 1
            mprint("17")
        else:
            mprint("18")
            if point[0].id == ID:
                point[0].notCore = True
                mprint("19")
                if hasNeighbor:
                    point[0].opticsId = opticsId
                    point[0].processed = True
                mprint("20")
            if hasNeighbor and point[0].opticsId == opticsId and (not point[0].processed) and (not point[0].id == ID):
                point[0].opticsId = opticsId + 1
            mprint("21")
        # mprint "!!!!!!point:{}".format(point)
        return point[0]

    def update(self, pointsInClass, distances, ID, opticsId, hasNeighbor=False):

        neiNum = len(list(filter(lambda x: x <= self.RADIUS, distances)))
        # mprint points_.take(5)
        pointsT = []
        for i in range(len(pointsInClass)):
            pointsT.append([pointsInClass[i], distances[i]])

        if neiNum >= self.MIN_PTS_NUM:
            #coreNei = distances.takeOrdered(self.MIN_PTS_NUM + 1)  # since there is 0 in the distance rdd, so plus 1
            coreNei = sorted(distances)[0:self.MIN_PTS_NUM+1]
            coreDis = coreNei[self.MIN_PTS_NUM]
            points_ = list(map(lambda p: self.updatePoint(p, True, hasNeighbor, ID, coreDis, opticsId), pointsT))
        else:
            mprint("12")
            # mprint hasNeighbor," ",ID," ",coreDis," ",opticsId
            mprint(pointsT)

            points_ = list(map(lambda p: self.updatePoint(p, False, hasNeighbor, ID, sys.maxsize, opticsId), pointsT))
        # mprint points_.take(5)


        #points_.checkpoint()#for non-local file system
        #points_.localCheckpoint()
        return points_, (neiNum > self.MIN_PTS_NUM | hasNeighbor)

    # pointsWithIndex : pointsT
    # pointsInClass : points_
    # curPoint : point
    def run(self, pt_obj_list ,points):
        mprint("	MIN_PTS_NUM :{} RADIUS :{}".format(self.MIN_PTS_NUM, self.RADIUS))

        ##pointsInClass = [self.createPoint(i) for i in range(len(points))]
        pointsInClass = pt_obj_list
        opticsId = 0

        while len(list(filter(lambda p: (not p.processed) and (not p.notCore), pointsInClass))) > 0:
            hasOut = True
            curPoint = list(filter(lambda p: (not p.processed) and (not p.notCore), pointsInClass))[0]
            point = points[curPoint.id]
            distances = list(map(lambda p: self.getDistances(p, point), points))
            temp = self.update(pointsInClass, distances, curPoint.id, opticsId)
            pointsInClass = temp[0]
            hasOut = temp[1]
            if hasOut:
                opticsId += 1
            while True:
                neigh = list(filter(lambda p: p.opticsId == opticsId and (not p.processed), pointsInClass))
                neigh_cnt = len(neigh)
                if neigh_cnt <= 0:
                    break
                hasNeighbor = True
                curPoint = sorted(neigh, key = lambda p: p.reachDis, reverse=False)[0]
                #curPoint = neigh.sortBy(lambda p: p.reachDis)[0]
                point = points[curPoint.id]
                #point = list(filter(lambda p: p[1] == curPoint.id, points))
                mprint("11")
                # mprint curPoint
                distances = list(map(lambda p: self.getDistances(p, point),points))
                mprint("12")
                # mprint distances
                pointsInClass = self.update(pointsInClass, distances, curPoint.id, opticsId, hasNeighbor)[0]
                mprint("13")
                opticsId += 1
        mprint("optics done")
        return sorted(pointsInClass, key = lambda x: x.opticsId, reverse=False)


    def getCluter(self, offset,results, r):
        noise = 0
        cluster = offset
        lb = []
        lbn = []
        lb_ = []
        for result in results:
            if result.reachDis > r:
                if result.coreDis > r:
                    noise += 1
                    result.cluster = sys.maxsize
                    lbn.append(result)
                else:
                    cluster += 1
                    if len(lb_) > 0:
                        lb.append(lb_)
                    lb_ = []
                    result.cluster = cluster
                    lb_.append(result)
            else:
                result.cluster = cluster
                lb_.append(result)

        if len(lb_) > 0:
            lb.append(lb_)
        if len(lbn) > 0:
            lb.append(lbn)

        flat = list(chain.from_iterable(lb))
        numOfcluster = cluster-offset
        print ("--------------")
        #print (flat)
        print ("--------------")

        return (numOfcluster, flat)


