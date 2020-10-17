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
test_file = ''
model_file = ''
output_file = ''


class Building_Structure():

	def read_input(self):
		global test_file, model_file, output_file
		test_file = sys.argv[1]
		model_file = sys.argv[2]
		output_file = sys.argv[3]

	def write_file(self, j, file):
	    with open(file, 'w+') as o:
	        for line in j:
	            o.writelines(json.dumps(line) + "\n")
	        o.close()

class Model_Convert:

	def reverse_dict(self, d):
		return {value:key for key, value in d.items()}

	def model_extract(self, rdd):

		user = rdd.filter(lambda x: x["type"]=="index_user")
		user_index = user.map(lambda x:{x["user_id"]:x["user_index"]}).map(lambda use:use)
		u = user_index.map(lambda x:x)
		user_index_rdd = u.flatMap(lambda l:l.items()).collectAsMap()
		reverse_user_dict = self.reverse_dict(user_index_rdd)

		business = rdd.filter(lambda x:x["type"]=="index_business").map(lambda bus:bus)
		business_index = business.map(lambda x:{x["business_id"]:x["business_index"]})
		business_index_rdd = business_index.flatMap(lambda l:l.items()).collectAsMap()
		reverse_business_dict = self.reverse_dict(business_index_rdd)

		return user_index_rdd, reverse_user_dict, business_index_rdd, reverse_business_dict
	
	def user_profile_extract(self, rdd):

		pro = rdd.filter(lambda x:x["type"]=="user_profile")
		pro_user = pro.map(lambda x:{x["user_index"]:x["user_profile"]}).map(lambda x:x)
		pro_user_rdd = pro_user.flatMap(lambda l:l.items()).collectAsMap()
		return pro_user_rdd
	
	def business_profile_extract(self, rdd):
		pro = rdd.filter(lambda x:x["type"]=="business_profile")
		pro_user = pro.map(lambda x:{x["business_index"]:x["business_profile"]})
		pro_business_rdd = pro_user.flatMap(lambda l:l.items()).collectAsMap()
		return pro_business_rdd

class Model_Predict:
	def compute_intersection(self, one, two):
		return set(one).intersection(set(two))
	
	def denominator(self, one, two):
		return math.sqrt(len(set(one))) * math.sqrt(len(set(two)))
	
	def cosine(self, one, two):
		threshold = 9
		if len(one)==0 or len(two)==0:
			return 0.0

		num = self.compute_intersection(one, two)
		den  = self.denominator(one,two)
		cos = float(len(num))/den

		ans = []
		i=0
		while(i<threshold):
			ans.append(i)
			i+=1
			
		return cos

	def predict(self,test_file_rdd,user_index_rdd, reverse_user_dict, business_index_rdd, reverse_business_dict, user_profile, business_profile):
		rdd = test_file_rdd.map(lambda x: (x["user_id"], x["business_id"]))
		rdd_index = rdd.map(lambda x: (user_index_rdd.get(x[0],-1), business_index_rdd.get(x[1],-1)))
		rdd_index_filter = rdd_index.filter(lambda x:x[0]!=-1 and x[1]!=-1)
		compute_cosine = rdd_index_filter.map(lambda x: ((x), self.cosine(user_profile.get(x[0],set()), business_profile.get(x[1],set())))).filter(lambda x:x[1]>0.01)
		return compute_cosine

def main():
	global stop_words_set
	building_structure = Building_Structure()
	model_helper = Model_Convert()
	predict_helper = Model_Predict()

	building_structure.read_input()
	print("Arguments Passed: ", test_file, model_file, output_file)

	conf = SparkConf().setMaster("local").setAppName("task3train").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")

	sc_object = SparkContext(conf=conf)
	sc_object.setLogLevel("WARN")
	rdd_lines = sc_object.textFile(model_file).map(lambda line: json.loads(line))

	user_index_rdd, reverse_user_dict, business_index_rdd, reverse_business_dict = model_helper.model_extract(rdd_lines)
	user_profile = model_helper.user_profile_extract(rdd_lines)
	business_profile = model_helper.business_profile_extract(rdd_lines)

	test_file_rdd =  sc_object.textFile(test_file).map(lambda line: json.loads(line))
	cosine_comp = predict_helper.predict(test_file_rdd,user_index_rdd, reverse_user_dict, business_index_rdd, reverse_business_dict, user_profile, business_profile)
	result = cosine_comp.map(lambda x:{"user_id": reverse_user_dict[x[0][0]], "business_id":reverse_business_dict[x[0][1]], "sim":x[1]})
	building_structure.write_file(result.collect(), output_file)


if __name__ == "__main__":
	main()

print("Duration: %s s" % (time.time() - start_time))