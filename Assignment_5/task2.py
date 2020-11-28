from pyspark import SparkContext, SparkConf
import sys
import time
import json
import binascii
import random
import csv
import math
import datetime
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from itertools import combinations
start_time = time.time()
port = ''
output_file = ''
times_count = 0

class Building_Structure():

	def read_input(self):
		global port, output_file
		port = sys.argv[1]
		output_file = sys.argv[2]

class Helper:
	def bin_val(self, h):
		b = format(h,'032b')
		return b

	def calc_bin(self,h):
		n = len(str(h))-len(str(h).rstrip('0'))
		return n

	def calc_zero(self, hash_num, bin_num):
		if hash_num==0:
			x = 0
		else:
			x = self.calc_bin(bin_num)
		return x

	def modify_input(self, rdd_lines):
		rdd_reqd = rdd_lines.map(lambda x_name: x_name['name']).distinct()
		rdd_reqd_fil = rdd_reqd.filter(lambda x:x!="").map(lambda x: int(binascii.hexlify(x.encode('utf8')),16))
		return rdd_reqd_fil

	def modify_input_second(self, rdd_lines):
		rdd_reqd = rdd_lines.map(lambda x_name: x_name['name'])
		return rdd_reqd

def FM_Algo(data):
	helper = Helper()
	global times_count
	data_stream = data.collect()
	len1 = len(data_stream)
	n = 15
	hash_functions = 30

	data_truth = len(set(data_stream))
	if times_count==0:
		ou = open(output_file,'w')
		ou.write("Time,Ground Truth,Estimation\n")
		times_count = 1
	else:
		ou = open(output_file,'a')

	t = time.time()
	start = datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')
	random.seed(11)
	a = random.sample(range(0, (2**16)-1),hash_functions)
	b = random.sample(range(0, (2**16)-1),hash_functions)
	R_trailing = []
	actual = set()
	for i in range(hash_functions):
		x = -1
		for d in data_stream:
			d1 = d.strip()
			j = json.loads(d1)
			state = j["state"]
			actual.add(state)
			int_num = int(binascii.hexlify(state.encode('utf8')),16)
			hash_num = ((a[i]*int_num+b[i])%(2**13-1))
			bin_num = helper.bin_val(hash_num)

			temp = helper.calc_zero(hash_num, bin_num)
			if temp>x:
				x=temp
		R_trailing.append(2**x)

	t = 0
	list_ans = [0]*n
	while(t<n):
		term = R_trailing[t*2:(t+1)*2]
		list_ans[t] = (float(sum(term)/len(term)))
		t+=1
	list_ans.sort()
	ou.write(str(start)+","+str(len(actual))+","+str(int(list_ans[n//2]))+"\n")
	ou.close()

def main():
	building_structure = Building_Structure()
	helper = Helper()

	conf = SparkConf().setAppName("hello").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
	sc = SparkContext(conf=conf)
	building_structure.read_input()

	print("Arguments Passed: ", port, output_file)
	sc.setLogLevel("OFF")
	steamContext = StreamingContext(sc, 5)
	lines = steamContext.socketTextStream("localhost", int(port))

	rdd_reqd = lines.window(30, 10).foreachRDD(FM_Algo)
	steamContext.start()
	steamContext.awaitTermination()


if __name__ == "__main__":
	main()

print("Duration: %s s" % (time.time() - start_time))