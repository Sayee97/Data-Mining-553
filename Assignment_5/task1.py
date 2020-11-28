from pyspark import SparkContext, SparkConf
import sys
import time
import json
import binascii
import random
import csv
import math
from itertools import combinations
start_time = time.time()
first_json = ''
second_json = ''
output_file = ''
NUMBER_OF_HASH_FUNCTIONS = 7
vals = []

class Building_Structure():

	def read_input(self):
		global first_json, second_json, output_file
		first_json = sys.argv[1]
		second_json = sys.argv[2]
		output_file = sys.argv[3]

class Helper:

	def write_file(self, j, file):
		with open(file, 'w+', newline="") as o:
			w = csv.writer(o, delimiter=' ')
			w.writerow(j)
		o.close()

	def modify_input(self, rdd_lines):
		rdd_reqd = rdd_lines.map(lambda x_name: x_name['name']).distinct()
		rdd_reqd_fil = rdd_reqd.filter(lambda x:x!="").map(lambda x: int(binascii.hexlify(x.encode('utf8')),16))
		return rdd_reqd_fil

	def modify_input_second(self, rdd_lines):
		rdd_reqd = rdd_lines.map(lambda x_name: x_name['name'])
		return rdd_reqd

class Training:

	def hash_functions_vals(self, id1, hash_vals):
		ans = []
		for a,b,c in hash_vals:
			temp = ((a*id1+b)%23333333333)%c
			ans.append(temp)
		return ans

	def generate_hash(self):
		random.seed(a=2)
		hash_val = []
		ans = []
		u=0
		while(u<int(NUMBER_OF_HASH_FUNCTIONS-29)):
			ans.append(u)
			u+=1
		a = random.sample(range(1, 10000000-1), 50)
		b = random.sample(range(0, 10000000-1), 50)
		for i,j in zip(a,b):
			hash_val.append((i,j, 74737*50))
		return hash_val

	def hash_apply(self, rdd):
		global vals
		vals = self.generate_hash()
		rdd_mod = set(rdd.flatMap(lambda x: self.hash_functions_vals(x, vals)).collect())
		return rdd_mod

class Prediction:

	def predict(self, x, hash_output):
		i = 0
		ans = []
		while(i<NUMBER_OF_HASH_FUNCTIONS):
			ans.append(i)
			i+=1
		if x is not None and x!="":
			x_mod = int(binascii.hexlify(x.encode('utf8')),16)
			vals_compute = set( Training().hash_functions_vals(x_mod, vals))
			if vals_compute.issubset(hash_output):
				yield "T"
			else:
				yield "F"
		else:
			yield "F"

	def predict_gen(self, rdd, hash_output):
		rdd_mod = rdd.flatMap(lambda x: self.predict(x,hash_output))
		return rdd_mod


def main():
	building_structure = Building_Structure()
	helper = Helper()
	training_helper = Training()
	prediction_helper = Prediction()

	building_structure.read_input()
	print("Arguments Passed: ", first_json, second_json, output_file)

	conf = SparkConf().setMaster("local").setAppName("sayee").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
	sc_object = SparkContext(conf=conf)
	sc_object.setLogLevel("WARN")
	rdd_lines = sc_object.textFile(first_json).map(lambda line: json.loads(line))
	rdd_lines_second = sc_object.textFile(second_json).map(lambda line: json.loads(line))
	modify_rdd_first = helper.modify_input(rdd_lines)
	modify_rdd_second = helper.modify_input_second(rdd_lines_second)

	hash_output = training_helper.hash_apply(modify_rdd_first)
	prediction_output = prediction_helper.predict_gen(modify_rdd_second, hash_output)

	helper.write_file(prediction_output.collect(), output_file)


if __name__ == "__main__":
	main()

print("Duration: %s s" % (time.time() - start_time))