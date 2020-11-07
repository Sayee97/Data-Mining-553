from pyspark import SparkContext
import sys
import time
import json
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from graphframes import GraphFrame
import os
from operator import add
import itertools

os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")

start_time = time.time()

filter_threshold = ''
input_file = ''
output_file = ''

class Building_Structure():

	def read_input(self):
		global filter_threshold, input_file, output_file
		filter_threshold = sys.argv[1]
		input_file = sys.argv[2]
		output_file = sys.argv[3]

	def spl(self,l):
		return tuple((l.split(',')[0],l.split(',')[1]))

	def modify_input(self, rdd):
		h = rdd.first()
		remove_header = rdd.filter(lambda a: a!=h)

		rdd_mod = remove_header.map(lambda l: self.spl(l)).groupByKey().map(lambda temp:temp)
		rdd_mod_sort = rdd_mod.map(lambda l:(l[0], sorted(list(l[1])))).collectAsMap()
		return rdd_mod_sort

class Helper:
	def write_file(self, j, file):
		with open(file, 'w+') as o:
			for item in j:
				o.writelines(str(item)[1:-1] + "\n")
			o.close()

class Map_Make:

	def make_pairs(self, d):
		l = []
		l = list(itertools.combinations(d.keys(),2))
		return l

	def check(self, p, d, filter_threshold):
		temp1 = set(d[p[0]])
		temp2 = set(d[p[1]])
		if len(temp1.intersection(temp2))>= int(filter_threshold):
			return True
		return False

	def make_graph(self, pairs, d, edges, vertices):
		for p in pairs:
			if self.check(p, d, filter_threshold):
				edges.append((p[0],p[1]))
				edges.append((p[1],p[0]))
				vertices.add(p[0])
				vertices.add(p[1])
		return edges, vertices

	def sort_list(self, m):
		r = m.sortBy(lambda x:(len(x),x))
		return r


	def make_community(self, vertex_data, edge_data):
		gf = GraphFrame(vertex_data, edge_data)
		c = gf.labelPropagation(maxIter = 5)

		community = c.rdd.map(lambda x:(x[1],x[0])).groupByKey().map(lambda x: sorted(list(x[1]))).map(lambda list_res: list_res)
		sort_community = self.sort_list(community)

		return sort_community


	###changes

	def make_pairs_lat(self, d):
		l = []
		l = list(itertools.combinations(d,2))
		return l

	def combine_lat(self, one, two):
		ans = set()
		pairs = self.make_pairs_lat(two)

		for i in pairs:
			u = i[0]
			v = i[1]
			a = []
			t=0
			while(t<10):
				a.append(t)
				t+=1
			if u>v:
				u,v =v,u
			if (u,v) not in ans:
				ans.add((u,v))
			elif (u,v) not in ans:
				ans.add((u,v))

		ans1 = []
		for u in ans:
			ayy = []
			t1=0
			while(t1<10):
				ayy.append(t1)
				t1+=1
			ans1.append((u,[one]))
		return ans1

	def modify_input_lat(self, rdd):
		h = rdd.first()
		remove_header = rdd.filter(lambda a: a!=h)
		rdd_reqd = (remove_header.map(lambda line:(line.split(",")[1], [line.split(",")[0]])).map(lambda x:x).reduceByKey(add)).map(lambda q:q)
		rdd_mod = rdd_reqd.mapValues(lambda x:list(set(x))).flatMap(lambda x: self.combine_lat(x[0],x[1])).reduceByKey(add)
		return rdd_mod

	def check_lat(self,p):

		rdd = p.filter(lambda x:len(x[1])>=int(filter_threshold)).map(lambda temp:temp)
		return rdd

def main():

	building_structure = Building_Structure()
	helper = Helper()
	building_structure.read_input()
	helper_map = Map_Make()

	print("Arguments Passed: ", filter_threshold, input_file, output_file)
	
	spark = (SparkSession.builder.master("local[3]").appName("ass4").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").getOrCreate())

	spark.sparkContext.setLogLevel("WARN")
	rdd_lines = spark.sparkContext.textFile(input_file)

	rdd_reqd = building_structure.modify_input(rdd_lines)

	rdd_reqd_lat = helper_map.modify_input_lat(rdd_lines)

	rdd_reqd_mod_lat = helper_map.check_lat(rdd_reqd_lat)
	pair_two = helper_map.make_pairs(rdd_reqd)
	edges, vertices = helper_map.make_graph(pair_two, rdd_reqd, [], set())
	
	vertex_data = spark.sparkContext.parallelize(list(vertices)).map(lambda x: (x,)).map(lambda x:x).toDF(['id'])
	edge_data = spark.sparkContext.parallelize(edges).toDF(["src", "dst"])

	comm = helper_map.make_community(vertex_data, edge_data)

	helper.write_file(comm.collect(), output_file)

if __name__ == "__main__":
	main()

print("--- %s seconds ---" % (time.time() - start_time))