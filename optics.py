import sys
import math
from pyspark import SparkContext

DEBUG_PRINT = False


def mprint(*str):
    if DEBUG_PRINT:
        print str


class Point:
    def __init__(self, ID):
        self.id = ID
        # self.info = info_
        self.reachDis = sys.maxint  # float("inf")
        self.coreDis = sys.maxint  # float("inf")
        self.processed = False
        self.opticsId = sys.maxint  # float("inf")
        self.notCore = False
        self.flag = 0
        return

    def __repr__(self):
        return "[id:{},rD:{},cD:{},optId:{},notC:{},chk:{},flag:{}]\n" \
            .format(self.id, self.reachDis, self.coreDis, self.opticsId, self.notCore, self.processed, self.flag)


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

            if point[1] <= self.RADIUS.value and (not point[0].id == ID) and (not point[0].processed):
                mprint("14")
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

        neiNum = distances.filter(lambda x: x <= self.RADIUS.value).count()
        mprint("neiNum:{}".format(neiNum))
        mprint(self.MIN_PTS_NUM.value)
        mprint("8")
        points_ = pointsInClass
        # mprint points_.take(5)
        pointsT = pointsInClass.zip(distances)
        mprint("9")
        if neiNum >= self.MIN_PTS_NUM.value:
            coreNei = distances.takeOrdered(
                self.MIN_PTS_NUM.value + 1)  # since there is 0 in the distance rdd, so plus 1
            mprint("10")
            coreDis = coreNei[self.MIN_PTS_NUM.value]
            mprint("11")
            points_ = pointsT.map(lambda p: self.updatePoint(p, True, hasNeighbor, ID, coreDis, opticsId))
        else:
            mprint("12")
            # mprint hasNeighbor," ",ID," ",coreDis," ",opticsId
            mprint(pointsT)

            points_ = pointsT.map(lambda p: self.updatePoint(p, False, hasNeighbor, ID, sys.maxint, opticsId))
        # mprint points_.take(5)


        points_.cache()
        mprint("update Points")
        return points_, (neiNum > self.MIN_PTS_NUM.value | hasNeighbor)

    # pointsWithIndex : pointsT
    # pointsInClass : points_
    # curPoint : point
    def run(self, points):
        mprint("	MIN_PTS_NUM :{} RADIUS :{}".format(self.MIN_PTS_NUM.value, self.RADIUS.value))

        pointsWithIndex = points.zipWithIndex()
        mprint(pointsWithIndex.top(20))
        pointsInClass = pointsWithIndex.map(lambda p: self.createPoint(p[1]))
        # mprint pointsInClass.top(10)
        opticsId = 0
        mprint("1")
        points.persist()
        mprint("2")
        pointsWithIndex.persist()
        mprint("3")
        pointsInClass.persist()
        mprint("4")
        while pointsInClass.filter(lambda p: (not p.processed) and (not p.notCore)).count() > 0:
            hasOut = True
            curPoint = pointsInClass.filter(lambda p: (not p.processed) and (not p.notCore)).first()
            mprint("5")
            point = pointsWithIndex.filter(lambda p: p[1] == curPoint.id).first()[0]
            mprint("6")
            # mprint curPoint
            distances = points.map(lambda p: self.getDistances(p, point))
            mprint("7")
            # mprint distances
            temp = self.update(pointsInClass, distances, curPoint.id, opticsId)
            mprint("8")
            pointsInClass = temp[0]
            hasOut = temp[1]
            if hasOut:
                opticsId += 1
            while True:
                neigh = pointsInClass.filter(lambda p: p.opticsId == opticsId and (not p.processed))
                neigh_cnt = neigh.count()
                if neigh_cnt <= 0:
                    break
                mprint("optId:", opticsId)
                mprint("9")
                hasNeighbor = True
                mprint(pointsInClass.take(5))
                curPoint = neigh.sortBy(lambda p: p.reachDis).first()
                mprint("10")
                point = pointsWithIndex.filter(lambda p: p[1] == curPoint.id).first()[0]
                mprint("11")
                # mprint curPoint
                distances = points.map(lambda p: self.getDistances(p, point))
                mprint("12")
                # mprint distances
                pointsInClass = self.update(pointsInClass, distances, curPoint.id, opticsId, hasNeighbor)[0]
                mprint("13")
                opticsId += 1
        mprint("optics done")
        return pointsInClass.sortBy(lambda x: x.opticsId)

    def getCluter(self, results, r):
        noise = 0
        flag = 0
        lb = []
        lbn = []
        lb_ = []
        for result in results.toLocalIterator():
            if result.reachDis > r:
                if result.coreDis > r:
                    noise += 1
                    result.flag = -1
                    lbn.append(result)
                else:
                    flag += 1
                    if len(lb_) > 0:
                        lb.append(lb_)
                    lb_ = []
                    result.flag = flag
                    lb_.append(result)
            else:
                result.flag = flag
                lb_.append(result)

        if len(lb_) > 0:
            lb.append(lb_)
        if len(lbn) > 0:
            lb.append(lbn)

        print "--------------"
        print lb
        print "--------------"

        return (flag, lb)


