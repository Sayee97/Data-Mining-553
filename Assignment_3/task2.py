from pyspark import SparkContext, SparkConf
import sys
import time
import json
import random
import math
from collections import Counter
from operator import add
from itertools import combinations
start_time = time.time()
input_file = ''
model_file = ''
stop_words = ''


special_chars_set = {'.',',','!','?',';',':',')',']','(','[','"', '#', '$'}
stop_words_set={}

class Helper:

	def reformat(self, adul_word):
		adul_word = adul_word.strip()
		word_req = ""
		for character in adul_word:
			if character not in special_chars_set:
				word_req += character
		word_modified = ''.join(word_req)
		if word_modified not in stop_words_set:
			return word_modified

class Building_Structure():

	def read_input(self):
		global input_file, model_file, stop_words
		input_file = sys.argv[1]
		model_file = sys.argv[2]
		stop_words = sys.argv[3]

class Building_Model:

	def filter_karo(self,l):
		l = [word for word in l if word is not None and word!=""]
		d = Counter(l)
		return list(d.items())
		#return [(word,d[word]) for word in l]


		return [word for word in l if word is not None and word!=""]

	def make_rdd(self, rdd_lines):
		rdd_columns = rdd_lines.map(lambda data_row: (data_row['business_id'],data_row['text']))
		rdd_modify = rdd_columns.reduceByKey(add)

		rdd_split = rdd_modify.map(lambda a: (a[0], a[1].lower().split()))
		rdd_reformat = rdd_split.map(lambda a: (a[0], [Helper().reformat(i) for i in a[1]]))

		# har document ka count
		rdd_pair_filter = rdd_reformat.map(lambda a: (a[0], self.filter_karo(a[1])))

		#total coount
		rdd_count = rdd_pair_filter.map(lambda x:x[1]).flatMap(lambda x: x).reduceByKey(add).values().sum()

		print(rdd_count)


		#rdd_count_words_filter = rdd_count_words.filter(lambda a: a[1]<0.000001*count_total[0][1])
		#print(count_total)



		# print(rdd_pair_filter.filter(lambda x: x[0]=='bZMcorDrciRbjdjRyANcjA').collect())


def main():
	global stop_words_set
	building_structure = Building_Structure()

	building_structure.read_input()
	print("Arguments Passed: ", input_file, model_file, stop_words)

	conf = SparkConf().setMaster("local").setAppName("task2").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")

	#conf = SparkConf().setMaster("local").setAppName("sayee").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
	sc_object = SparkContext(conf=conf)
	sc_object.setLogLevel("WARN")
	rdd_lines = sc_object.textFile(input_file).map(lambda line: json.loads(line))
	
	file = open(stop_words,"r")
	stop_words_set = set(word.strip() for word in file)

	rdd_reqd = Building_Model().make_rdd(rdd_lines)

if __name__ == "__main__":
	main()

print("Duration: %s s" % (time.time() - start_time))