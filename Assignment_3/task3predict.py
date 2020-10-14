from pyspark import SparkContext, SparkConf
import sys
import time
import json
import random
import math
import collections
from collections import Counter
from itertools import combinations
start_time = time.time()


train_file = ''
test_file = ''
model_file = ''
output_file=''
cf_type = ''

class User_Based:
	def make_dict_model(self, rdd_model, user_index_rdd):
		
		sim_dict = rdd_model.map(lambda x:{(user_index_rdd[x['u1']], user_index_rdd[x['u2']]):kv['sim']})
		sim_dict_filter = sim_dict.flatMap(lambda x:x.items()).collectAsMap()

		return sim_dict_filter

	def modify_input_file(self, rdd_columns,business_index_rdd, user_index_rdd):
		rdd = rdd_columns.map(lambda x:(business_index_rdd[x[1]], (user_index_rdd[x[0]],x[2]))).groupByKey()
		rdd_modify = rdd.map(lambda x:(x[0], [(y[0],y[1]) for y in list(set(x[1]))]))
		return rdd_modify

	def modify_test_file(self, test_rdd, user_index_rdd, business_index_rdd):
		rdd = test_rdd.map(lambda kv:(business_index_rdd.get(kv['business_id'],-1), user_index_rdd.get(kv['user_id'],-1)))
		rdd_filter = rdd.filter(lambda x:x[0]!=-1 and x[1]!=-1)
		return rdd_modify

	def predict(self, temp_final, model_rdd, avg_rdd, reverse_user_dict):
		tar = temp_final[0]
		index_tar = reverse_user_dict.get(tar, "UNKNOWN")
		scores_list = list(temp_final[1])
		ans = []
		

class Building_Structure():

	def read_input(self):
		global train_file, test_file, model_file, output_file, cf_type
		train_file = sys.argv[1]
		test_file = sys.argv[2]
		model_file = sys.argv[3]
		output_file = sys.argv[4]
		cf_type = sys.argv[5]

class Building_Model:

	def reverse_dict(self, d):
		return {value:key for key, value in d.items()}

	def make_dict(self, rdd_columns):
		u_dict = rdd_columns.map(lambda x:x[0]).distinct().sortBy(lambda x:x)
		u_index = u_dict.zipWithIndex().collect()
		u_index_dict = dict(u_index)
		return u_index_dict


	def make_rdd(self, rdd_lines):

		rdd_columns = rdd_lines.map(lambda data_row: (data_row['user_id'],data_row['business_id'], data_row['stars']))
		user_index_rdd = self.make_dict(rdd_columns)
		reverse_user_dict = self.reverse_dict(user_index_rdd)

		rdd_columns_mod = rdd_columns.map(lambda x: (x[1], x[0], x[2]))
		business_index_rdd = self.make_dict(rdd_columns_mod)
		reverse_business_dict = self.reverse_dict(business_index_rdd)
		return rdd_columns, user_index_rdd, reverse_user_dict, business_index_rdd, reverse_business_dict

def main():
	user_avg_path = '/HW3/user_avg.json'
	building_structure = Building_Structure()
	user_based_helper = User_Based()

	building_structure.read_input()
	print("Arguments Passed: ", train_file, test_file, model_file, output_file, cf_type)

	conf = SparkConf().setMaster("local").setAppName("task3train").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")

	sc_object = SparkContext(conf=conf)
	sc_object.setLogLevel("WARN")
	rdd_lines = sc_object.textFile(train_file).map(lambda line: json.loads(line))

	rdd_columns, user_index_rdd, reverse_user_dict, business_index_rdd, reverse_business_dict = Building_Model().make_rdd(rdd_lines)

	if cf_type == 'user_based':
		rdd = sc_object.textFile(model_file).map(lambda line:json.loads(line))
		apni_model_rdd = user_based_helper.make_dict_model(rdd, user_index_rdd)
		modify_train = user_based_helper.modify_input_file(rdd_columns,business_index_rdd, user_index_rdd)

		us_avg = sc_object.textFile(user_avg_path).map(lambda r:json.loads(r))
		us_avg_rdd = us_avg.map(lambda x:dict(k)).flatMap(lambda x:x.items()).collectAsMap()

		test_rdd = sc_object(test_file).map(lambda row:json.loads(row))
		test_rdd_dict = user_based_helper.modify_test_file(test_rdd, user_index_rdd, business_index_rdd)

		o_pair = test_rdd_dict.leftOuterJoin(modify_train).mapValues(lambda x: user_based_helper.predict(tuple(x), apni_model_rdd, us_avg_rdd, reverse_user_dict))


if __name__ == "__main__":
	main()

print("Duration: %s s" % (time.time() - start_time))