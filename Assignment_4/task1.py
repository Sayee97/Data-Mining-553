from pyspark import SparkContext
import sys
import time
import json
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from graphframes import GraphFrame
import os
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
		rdd_mod = remove_header.map(lambda l: self.spl(l)).groupByKey()
		rdd_mod_sort = rdd_mod.map(lambda l:(l[0], sorted(list(l[1])))).collectAsMap()
		return rdd_mod_sort

class Helper:
	def write_file(self, j, file):
		with open(file, 'w+') as o:
			for item in j:
				o.writelines(json.dumps(item) + "\n")
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

	def make_community(self, vertex_data, edge_data):
		gf = GraphFrame(vertex_data, edge_data)
		c = gf.labelPropagation(maxIter = 10)

		community = c.rdd.coalesce(1).map(lambda x:(x[1],x[0])).groupByKey().map(lambda x: sorted(list(x[1])))
		sort_community = community.sortBy(lambda x:(len(x), x))

		return sort_community

def main():

	building_structure = Building_Structure()
	helper = Helper()
	building_structure.read_input()
	helper_map = Map_Make()

	print("Arguments Passed: ", filter_threshold, input_file, output_file)

	conf = SparkConf().setMaster("local").setAppName("sayee").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
	sc_object = SparkContext(conf=conf)
	sparkSession = SparkSession(sc_object)

	sc_object.setLogLevel("WARN")
	rdd_lines = sc_object.textFile(input_file)

	rdd_reqd = building_structure.modify_input(rdd_lines)

	pair_two = helper_map.make_pairs(rdd_reqd)
	edges, vertices = helper_map.make_graph(pair_two, rdd_reqd, [], set())
	
	vertex_data = sc_object.parallelize(list(vertices)).map(lambda x: (x,)).toDF(['id'])
	edge_data = sc_object.parallelize(edges).toDF(["src", "dst"])

	comm = helper_map.make_community(vertex_data, edge_data)

	helper.write_file(comm.collect(), output_file)

if __name__ == "__main__":
	main()

print("--- %s seconds ---" % (time.time() - start_time))