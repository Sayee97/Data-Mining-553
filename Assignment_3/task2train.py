from pyspark import SparkContext, SparkConf
import sys
import time
import json
import random
import math
from functools import reduce

import re
import collections
from collections import Counter
from itertools import combinations
start_time = time.time()
input_file = ''
model_file = ''
stop_words = ''
import operator


special_chars_set = {'.',',','!','?',';',':',')',']','(','[','"', '#', '$'}
stop_words_set={}

class Helper:
	def create_user_profile(self,rdd_columns,user_index_rdd, business_index_rdd, business_profile_final):
		u_p = rdd_columns.groupByKey().map(lambda x: (user_index_rdd[x[0]], list(set(x[1]))))
		u_p_values = u_p.map(lambda x: (x[0], [business_index_rdd[i] for i in x[1] ]))
		u_p_values_mod = u_p_values.flatMapValues(lambda x: [business_profile_final[i] for i in x]).reduceByKey(add)
		u_p_filter = u_p_values_mod.filter(lambda x:len(x[1])>1)
		u_p_dict = u_p_filter.map(lambda x:{x[0]:list(set(x[1]))})

		return u_p_dict

	def reformat(self, adul_word):
		adul_word = adul_word.strip()
		word_req = ""
		for character in adul_word:
			if character not in special_chars_set:
				word_req += character
		word_modified = ''.join(word_req)
		if word_modified not in stop_words_set:
			return word_modified

	def write_dict(self, d, model_type, temps):
		ans = []
		for k, v in d.items():
			ans.append({"model_type": model_type, temps[0]:k, temps[1]:v})

	def write_list(self, l, model_type, temps):
		ans = []
		for i in l:
			for k,v in i.items():
				ans.append({"model_type": model_type, temps[0]:k, temps[1]:v})
	
	def save_result(self,result_save, type_val, keys, key, val):

		result_save.append({"type":type_val, keys[0]:key, keys[1]:val})
		

	def save_model_form(self, input_func):
		data, type_val, keys = input_func
		result = list()
		word_val = 10
		i=0
		ans = []
		result_save = []
		while(i<(word_val)):
			ans.append(i)
			i+=1
		if isinstance(data, dict):
			for k, v in data.items():
				self.save_result(result_save, type_val, keys, k, v)
		else:
			for itemss in data:
				for k, v in itemss.items():
					self.save_result(result_save, type_val, keys, k, v)
		return result_save


	def convert_ONE_dict(self, d):
		li = []
		while(size<10):
			li.append(size)
			size+=1

		ans = collections.defaultdict(list)
		for t in d:
			i = t.keys()
			j = t.values()
			ans[list(i)[0]] = list(j)[0]
		return ans

	def write_file(self, j, file):

	    with open(file, 'w+') as o:
	        for line in j:
	            o.writelines(json.dumps(line) + "\n")
	        o.close()

class Building_Structure():

	def read_input(self):
		global input_file, model_file, stop_words
		input_file = sys.argv[1]
		model_file = sys.argv[2]
		stop_words = sys.argv[3]

class Building_Model:

	def reverse_dict(self, d):
		return {value:key for key, value in d.items()}

	def make_dict(self, rdd_columns):
		u_dict = rdd_columns.map(lambda x:x[0]).distinct().sortBy(lambda x:x)
		u_index = u_dict.zipWithIndex().collect()
		u_index_dict = dict(u_index)
		return u_index_dict


	def make_rdd(self, rdd_lines):

		rdd_columns = rdd_lines.map(lambda data_row: (data_row['user_id'],data_row['business_id']))
		user_index_rdd = self.make_dict(rdd_columns)
		reverse_user_dict = self.reverse_dict(user_index_rdd)

		rdd_columns_mod = rdd_columns.map(lambda x: (x[1], x[0]))
		business_index_rdd = self.make_dict(rdd_columns_mod)
		reverse_business_dict = self.reverse_dict(business_index_rdd)
		return rdd_columns, user_index_rdd, reverse_user_dict, business_index_rdd, reverse_business_dict

class Term_Frequency:

	def count_sayee(self, words):

		d = {}
		max_times = 0
		for word in words:
			if word in d:
				d[word]+=1
			else:
				d[word]=1
		max_times = max(d.values())
		d = dict(filter(lambda kv: (kv[1]) > 0.000001*len(words), d.items()))
		sort_d = []
		for k,v in d.items():
			sort_d.append((k,v,max_times))
		sort_d = sorted(sort_d, key = lambda x:x[1], reverse = True)
		return sort_d

	def reformat_word(self, adul_word):
		adul_word = adul_word.strip()
		word_req = ""
		i=0

		while(i<len(adul_word)):
			if adul_word[i] not in special_chars_set:
				word_req+=adul_word[i]
			w = ''.join(word_req)
			if w not in stop_words_set:
				return w
			i+=1

	def reformat(self, list_words):

		ans = []

		for t in list_words:
			ans.extend(re.split(r"[~\s\r\n]+", t))
		return ans


	def filter_karo(self,l):
		l = [word for word in l if word is not None and word!=""]
		return l


	def tf_calc(self, business_index_rdd, rdd_lines):

		reqd_rdd = rdd_lines.map(lambda x: (business_index_rdd[x['business_id']], str(x['text'].encode('utf-8')).lower())).groupByKey()
		rdd_reqd_group = reqd_rdd.map(lambda x: (x[0], self.reformat(list(x[1]))))
		rdd_reqd_group1 = rdd_reqd_group.map(lambda x: (x[0], [self.reformat_word(word) for word in x[1]]))
		rdd_pair_filter = rdd_reqd_group1.map(lambda a: (a[0], self.filter_karo(a[1])))
		rdd_count_words = rdd_pair_filter.map(lambda x:(x[0], self.count_sayee(x[1])))
		

		rdd_tf_doc = rdd_count_words.flatMap(lambda x: [((x[0], w[0]), float(w[1])/float(w[2])) for w in x[1]])

		return rdd_tf_doc

	def idf(self, rdd_tf, business_index_rdd):
		l = len(business_index_rdd)

		idf = rdd_tf.map(lambda b:(b[0][1], b[0][0]))
		idf_group = idf.groupByKey().map(lambda x: (x[0], list(set(x[1]))))
		idf_group_mod = idf_group.map(lambda y:y)
		idf_count = idf_group_mod.flatMap(lambda x:[((y, x[0]), math.log(l/len(x[1]),2)) for y in x[1]])

		return idf_count

	def calculate(self, rdd_tf, rdd_idf):

		join_rdd = rdd_tf.leftOuterJoin(rdd_idf)
		join_rdd_mod = join_rdd.mapValues(lambda x:x[0]*x[1]).map(lambda x:(x[0][0], (x[0][1], x[1]))).groupByKey()
		take_200 = join_rdd_mod.map(lambda x:(x[0], sorted(list(x[1]), reverse=True,key = lambda y:y[1])[:200]))

		take_200_vals = take_200.mapValues(lambda x:[k[0] for k in x])
		return take_200_vals

	def w_index(self, rdd):
		w = rdd.flatMap(lambda v: [(yo,1) for yo in v[1]]).groupByKey()
		w_in = w.map(lambda x:x[0]).zipWithIndex().collect()
		return dict(w_in)

def main():
	global stop_words_set
	model = []
	building_structure = Building_Structure()
	building_model_helper = Building_Model()
	term_freq_helper = Term_Frequency()
	helper = Helper()

	building_structure.read_input()
	print("Arguments Passed: ", input_file, model_file, stop_words)

	conf = SparkConf().setMaster("local").setAppName("task2").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")

	sc_object = SparkContext(conf=conf)
	sc_object.setLogLevel("WARN")
	rdd_lines = sc_object.textFile(input_file).map(lambda line: json.loads(line))
	
	file = open(stop_words,"r")
	stop_words_set = set(word.strip() for word in file)

	### Actuall
	rdd_columns, user_index_rdd, reverse_user_dict, business_index_rdd, reverse_business_dict = Building_Model().make_rdd(rdd_lines)
	rdd_tf = term_freq_helper.tf_calc(business_index_rdd, rdd_lines)

	rdd_idf = term_freq_helper.idf(rdd_tf, business_index_rdd)

	calculate_tfidf = term_freq_helper.calculate(rdd_tf, rdd_idf)

	model.extend(helper.save_model_form([user_index_rdd, "index_user", ["user_id", "user_index"]]))
	model.extend(helper.save_model_form([business_index_rdd, "index_business", ["business_id", "business_index"]]))

	word_ka_index = term_freq_helper.w_index(calculate_tfidf)

	calculate_tfidf_dict = calculate_tfidf.mapValues(lambda w: [word_ka_index[y] for y in w]).map(lambda x: {x[0]:x[1]})

	business_profile = calculate_tfidf_dict.collect()
	business_profile_final = helper.convert_ONE_dict(business_profile)
	model.extend(helper.save_model_form([business_profile, "business_profile", ["business_index", "business_profile"]]))

	user_profile = helper.create_user_profile(rdd_columns,user_index_rdd, business_index_rdd, business_profile_final)

	model.extend(helper.save_model_form([user_profile.collect(), "user_profile", ["user_index", "user_profile"]]))

	helper.write_file(model,model_file)



if __name__ == "__main__":
	main()

print("Duration: %s s" % (time.time() - start_time))