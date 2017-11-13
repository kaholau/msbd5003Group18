import sys
import math
from pyspark import SparkContext




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
		return "[id:{},rD:{},cD:{},optId:{},notC:{},chk:{}],\n"\
		.format(self.id,self.reachDis,self.coreDis,self.opticsId,self.notCore,self.processed)

class OPTICS:
	MIN_PTS_NUM = 0
	RADIUS = 0
	def __init__(self,minPtsNum,radius):
		self.MIN_PTS_NUM = minPtsNum
		self.RADIUS = radius
		return 

	def getDistances(self,point, curPoint):
		#sqrt((xa-xb)^2 + (ya-yb)^2 + (za-zb)^2)
		distance = 0
		for pp,cc in zip(point,curPoint):
			distance += (pp-cc)**2
		return math.sqrt(distance)

	def createPoint(self,id):
		pt= Point(id)
		return pt

	def updatePoint(self,point,IsCore,hasNeighbor,id,coreDis,opticsId):
		#print point
		if IsCore:
			if point[0].id == id :
				point[0].coreDis = coreDis
				point[0].opticsId = opticsId
				point[0].processed = True
		
			if point[1]<=self.RADIUS.value and (not point[0].id == id) and (not point[0].processed):
				if point[1] < coreDis:
					point[0].reachDis = min(point[0].reachDis,coreDis)
				else:
					point[0].reachDis = min(point[0].reachDis,point[1])
				point[0].opticsId = opticsId + 1
		else:
			if point.id == id :
				point[0].notCore = True
				if hasNeighbor :
					point[0].opticsId = opticsId
					point[0].processed = True
			if hasNeighbor and point[0].opticsId == opticsId and (not point[0].processed) and (not point[0].id == id):
				point[0].opticsId = opticsId +1
		#print "!!!!!!point:{}".format(point)
		return point[0]

	def update(self,pointsInClass,distances,id,opticsId,hasNeighbor = False):
		neiNum  = distances.filter( lambda x : x <= self.RADIUS.value).count()
		#print "neiNum:{}".format(neiNum)
		#print self.MIN_PTS_NUM.value

		points_ = pointsInClass
		#print points_.take(5)
		pointsT = pointsInClass.zip(distances)
		if neiNum > self.MIN_PTS_NUM.value:
			coreNei = distances.takeOrdered(self.MIN_PTS_NUM.value+1)#since there is 0 in the distance rdd, so plus 1
			coreDis = coreNei[self.MIN_PTS_NUM.value]
			points_ = pointsT.map(lambda p :self.updatePoint(p,True,hasNeighbor,id,coreDis,opticsId))
		else:
			points_ = pointsT.map(lambda p :self.updatePoint(p,False,hasNeighbor,id,coreDis,opticsId))
		#print points_.take(5)
		points_.cache()

		return points_ , (neiNum>self.MIN_PTS_NUM.value | hasNeighbor)


	#pointsWithIndex : pointsT
	#pointsInClass : points_
	#curPoint : point
	def run(self,points):
		print "	MIN_PTS_NUM :{} RADIUS :{}".format(self.MIN_PTS_NUM.value,self.RADIUS.value)

		pointsWithIndex = points.zipWithIndex()
		#print pointsWithIndex.top(10)
		pointsInClass = pointsWithIndex.map(lambda p:self.createPoint(p[1]))
		#print pointsInClass.top(10)
		opticsId = 0
		points.persist()
		pointsWithIndex.persist()
		pointsInClass.persist()
		while pointsInClass.filter(lambda p:(not p.processed) and (not p.notCore)).count()>0 :
			hasOut = True
			curPoint = pointsInClass.filter(lambda p:(not p.processed) and (not p.notCore)).first()
			point = pointsWithIndex.filter(lambda p:p[1]==curPoint.id).first()[0]
			#print curPoint
			distances = points.map(lambda p : self.getDistances(p,point))
			#print distances
			temp     = self.update(pointsInClass, distances, curPoint.id, opticsId)
			pointsInClass = temp[0]
			hasOut       = temp[1]
			if hasOut:
				opticsId  += 1
			while  pointsInClass.filter( lambda p : p.opticsId == opticsId).count() > 0 :
				hasNeighbor = True
				#print pointsInClass.take(5)
				curPoint    = pointsInClass.filter(lambda p:(not p.processed) and ( p.opticsId == opticsId)).sortBy(lambda p : p.reachDis).first()
				point = pointsWithIndex.filter(lambda p:p[1]==curPoint.id).first()[0]
				#print curPoint
				distances = points.map(lambda p : self.getDistances(p,point))
				#print distances
				pointsInClass   = self.update(pointsInClass, distances, curPoint.id, opticsId,hasNeighbor)[0]
				opticsId   += 1

		return pointsInClass
		
