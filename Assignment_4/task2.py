from pyspark import SparkContext, SparkConf
import sys
import time
import json
import random
import math
from itertools import combinations
from collections import deque

start_time = time.time()

case_number = ''
input_file = ''
between_output_file = ''
community_output_file = ''

class Building_Structure():

	def read_input(self):
		global case_number, input_file, between_output_file, community_output_file
		case_number = sys.argv[1]
		input_file = sys.argv[2]
		between_output_file = sys.argv[3]
		community_output_file = sys.argv[4]

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
				o.writelines(str(item)[1:-1] + "\n")
			o.close()
	def count_adj_matrix(self,edges):

		# This counts number of edges and makes adjacency matrix
		# This wont change for the calculations

		looked = set()
		adj_matrix = set()
		c = 0
		for s, e_list in edges.items():
			for e in e_list:
				# plag
				tuple_edge = ((s,e)) if s<e else ((e,s))
				adj_matrix.add(tuple_edge)
				if tuple_edge not in looked:
					looked.add(tuple_edge)
					c+=1
		return c, adj_matrix

	def get_reqd_initializations(self, vertices, edges):
		ans_v = {}
		for v in vertices:
			ans_v.setdefault(v,1)
		edge_count_m, adj_matrix = self.count_adj_matrix(edges)
		return ans_v, edge_count_m, adj_matrix

class Map_Make:

	def make_pairs(self, d):
		l = []
		l = list(combinations(d.keys(),2))
		return l

	def compute_jaccard(self,s1, s2):
		numerator = float(len(set(s1)&set(s2)))
		deno = float(len(set(s1)|set(s2)))
		ans = float(numerator/deno)
		return ans	

	def check(self, p, d):
		temp1 = set(d[p[0]])
		temp2 = set(d[p[1]])
		jacc_score = self.compute_jaccard(temp1, temp2)

		if jacc_score>=0.5:
			return True
		return False

		##### yaha thoda change karo bhai!!
		# if len(temp1&temp2)>=1:
		# 	return True
		# return False

	def make_graph(self, pairs, d, edges, vertices):
		for p in pairs:
			if self.check(p, d):
				edges.append((p[0],p[1]))
				edges.append((p[1],p[0]))
				vertices.add(p[0])
				vertices.add(p[1])
		return edges, vertices

class Similarity_Graph:

	def sort_tree(self,t):
		ans = {key: value for key, value in sorted(t.items(), key=lambda x: -x[1][0])}
		return ans

	def bfs_tree_build(self, node, edges):
		t = {}
		t[node] = (0,[])
		visited = set()
		rem = deque()
		rem.append(node)

		while(len(rem)>0):
			parent = rem.popleft()
			visited.add(parent)
			for child in edges[parent]:
				if child not in visited:
					visited.add(child)
					t[child] = (t[parent][0]+1, [parent])
					rem.append(child)

				# I think this is ki visited me aya and level upar ka match hua matlab 2 baap hai!!!
				elif t[parent][0]+1==t[child][0]:
					t[child][1].append(parent)
		ans = self.sort_tree(t)
		return ans

	def level_wise(self, bfs):
		d = {}
		# {"LEVEL": (CHILD NODE, PARENTS KA LIST)}
		for c,tuple_l_p in bfs.items():
			if tuple_l_p[0] in d:
				d[tuple_l_p[0]].append(((c, tuple_l_p[1])))
			else:
				d[tuple_l_p[0]] = [((c,tuple_l_p[1]))]

		return d

	def find_paths(self, bfs):
		#First we make level wise dict

		level_order_dict = self.level_wise(bfs)
		child_shortest_paths = {}

		# Traverse level wise to find sum of path for each node
		i = 0

		key_length = len(level_order_dict.keys())
		while(i<key_length):
			for (c,p_list) in level_order_dict[i]:
				if len(p_list)<=0:
					child_shortest_paths[c]=1
				else:
					ans = 0
					for indi_p in p_list:
						ans+=child_shortest_paths[indi_p]
					child_shortest_paths[c] = ans
			i+=1

		return child_shortest_paths

	def update_hash(self, d, key, value):
		prev = d[key]
		d[key] = float(prev + value)
		return d

	def step_2_level(self, bfs, weight_vertex):
		weight = weight_vertex.copy()
		child_shortest_paths = self.find_paths(bfs)

		ans = {}

		for k,v in bfs.items():
			if len(v)>0:
				deno = 0
				for p in v[1]:
					deno+=child_shortest_paths[p]
				for p in v[1]:
					key_reqd = ((k,p)) if k<p else ((p,k))
					numerator = float(weight[k]*child_shortest_paths[p])
					weight_edge = float(numerator/deno)
					ans[key_reqd] = weight_edge
					# Updating weights'
					weight = self.update_hash(weight, p, weight_edge)

		return ans

	def make_one(self, big, small):
		for k,v in small.items():
			if k in big:
				big = self.update_hash(big, k,v)
			else:
				big[k]=v
		return big

	def divide_by_2(self, d):
		d = dict(map(lambda k:(k[0],float(k[1]/2)), d.items()))
		return d

	def betweenness_calculate(self, vertices, edges, weight_vertex, count_edges, adj_matrix, ans_dict):

		for each_node in vertices:
			bfs_node = self.bfs_tree_build(each_node, edges)
			weights_dict = self.step_2_level(bfs_node, weight_vertex)
			ans_dict = self.make_one(ans_dict, weights_dict)
		ans_dict = self.divide_by_2(ans_dict)
		betweenness_result = sorted(ans_dict.items(), key = lambda k: (-k[1], k[0][0]))
		return betweenness_result

	def community(self, vertices_prepared, edges_mod_values, w_dict_vertices, edge_count_m, adj_matrix,betweenness_ans_reqd):
		print("yes")


def main():
	building_structure = Building_Structure()
	helper = Helper()
	map_make_helper = Map_Make()
	similarity_graph_helper = Similarity_Graph()

	building_structure.read_input()
	print("Arguments Passed: ", case_number, input_file,between_output_file,community_output_file)

	conf = SparkConf().setMaster("local").setAppName("community").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
	sc_object = SparkContext(conf=conf)
	sc_object.setLogLevel("WARN")
	rdd_lines = sc_object.textFile(input_file)

###### Similarity Graph
	if int(case_number) == 2:

		rdd_reqd = building_structure.modify_input(rdd_lines)
		pairs = map_make_helper.make_pairs(rdd_reqd)
		edges, vertices = map_make_helper.make_graph(pairs, rdd_reqd, [], set())

		vertices_sort = sorted(list(vertices))
		vertices_prepared = sc_object.parallelize(vertices_sort).collect()

		edges_int = sc_object.parallelize(edges).groupByKey()
		edges_mod_values = edges_int.map(lambda x:(x[0],sorted(list(set(x[1]))))).collectAsMap()

		# calculating number of edges and adjacency matrix for similarity graph done in helper class
		w_dict_vertices, edge_count_m, adj_matrix = helper.get_reqd_initializations(vertices_prepared, edges_mod_values)

		# PART A : CALCULATING BETWEENESSS
		betweenness_ans_reqd =  similarity_graph_helper.betweenness_calculate(vertices_prepared, edges_mod_values, w_dict_vertices, edge_count_m, adj_matrix, {})
		helper.write_file(betweenness_ans_reqd, between_output_file)

		# PART B: CALCULATING MODULARITY and COMMUNITY DETECTION
		communities_ans_reqd = similarity_graph_helper.community(vertices_prepared, edges_mod_values, w_dict_vertices, edge_count_m, adj_matrix,betweenness_ans_reqd)



if __name__ == "__main__":
	main()

print("Duration: %s s" % (time.time() - start_time))