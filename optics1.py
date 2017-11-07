import sys
import math
from pyspark import SparkContext

radius = 17
minPtsNum = 4
class Point:
	def __init__(self,ID):
		self.id = ID
		#self.info = info_
		self.reachDis = sys.maxint#float("inf")
		self.coreDis = sys.maxint#float("inf")
		self.processed = False
		self.opticsId = sys.maxint#float("inf")
		self.notCore = False
		return

	def __repr__(self):
		return "[{},{},{},{},{},{}]".format(self.id,self.reachDis,self.coreDis,self.processed,self.opticsId,self.notCore)


def getDistances(point, curPoint):
	#sqrt((xa-xb)^2 + (ya-yb)^2 + (za-zb)^2)
	distance = 0
	for pp,cc in zip(point,curPoint):
		distance += (pp-cc)**2
	return math.sqrt(distance)

def createPoint(id):
	pt= Point(id)
	return pt

def updatePoint(point,IsCore,hasNeighbor,id,coreDis,opticsId):
	if IsCore:
		if point.id == id :
			point[0].coreDis = coreDis
			point[0].opticsId = opticsId
			point[0].processed = True
	
		if point[1]<=radius and (not point[0].id == id) and (not point[0].processed):
			if point[1] < coreDis:
				point[0].reachDis = Math.min(point[0].reachDis,coreDis)
			else:
				point[0].reachDis = Math.min(point[0].reachDis,point[1])
			point[0].opticsId = opticsId + 1
	else:
		if point.id == id :
			point[0].notCore = True
			if hasNeighbor :
				point[0].opticsId = opticsId
				point[0].processed = True
		if hasNeighbor and point[0].opticsId == opticsId and (not point[0].processed) and (not point[0].id == id):
			point[0].opticsId = opticsId +1
	return

def update(pointsInClass,distances,id,opticsId,hasNeighbor = False):
	neiNum  = distances.filter( lambda x : x <= radius).count()
	points_ = pointsInClass
	pointsT = pointsInClass.zip(distances)
	if neiNum > minPtsNum:
		coreNei = distances.takeOrdered(minPtsNum+1)#since there is 0 in the distance rdd, so plus 1
		coreDis = coreNei(minPtsNum)
		points_ = pointsT.map(lambda p :updatePoint(p,True,hasNeighbor,id,coreDis,opticsId))
	else:
		points_ = pointsT.map(lambda p :updatePoint(p,False,hasNeighbor,id,coreDis,opticsId))

	points_.checkpoint()

	return points_ , (neiNum>minPtsNum | hasNeighbor)


#pointsWithIndex : pointsT
#pointsInClass : points_
#curPoint : point
def run(points):

	pointsWithIndex = points.zipWithIndex()
	#print pointsWithIndex.top(10)
	pointsInClass = pointsWithIndex.map(lambda p:createPoint(p[1]))
	#print pointsInClass.top(10)
	opticsId = 0
	points.persist()
	pointsWithIndex.persist()
	pointsInClass.persist()
	while(pointsInClass.filter(lambda p:(not p.processed) and (not p.notCore).count>0)):
		hasOut = True
		curPoint = pointsInClass.filter(lambda p:(not p.processed) and (not p.notCore)).first()
		point = pointsWithIndex.filter(lambda p:p[1]==curPoint.id).first()[0]
		#print curPoint
		distances = points.map(lambda p : getDistances(p,point))
		#print distances
		temp     = update(pointsInClass, distances, curPoint.id, opticsId)
		pointsInClass = temp[0]
		hasOut       = temp[1]
		if hasOut:
			opticsId  += 1
		while  pointsInClass.filter( lambda p : p.opticsId == opticsId).count > 0 :
			hasNeighbor = True
			curPoint    = pointsInClass.filter(lambda p:(not p.processed) and ( p.opticsId == opticsId)).sortBy(lambda p : p.reachDis).first()
			point = pointsWithIndex.filter(lambda p:p[1]==curPoint.id).first()[0]
			#print curPoint
			distances = points.map(lambda p : getDistances(p,point))
			#print distances
			pointsInClass   = update(pointsInClass, distances, curPoint.id, opticsId,hasNeighbor)[0]
			opticsId   += 1
	print points_

	return points_
	
'''		
for q in x:
	print getDistances(x,q)
'''


