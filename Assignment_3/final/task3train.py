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
input_file = ''
model_file = ''
model_type = ''
NUMBER_OF_HASH_FUNCTIONS = 30
BAND_SIZE_B = 30

class Building_Structure():

	def read_input(self):
		global input_file, model_file, model_type
		input_file = sys.argv[1]
		model_file = sys.argv[2]
		model_type = sys.argv[3]
class Item_Based:

	def check_conts(self, d1, d2):
		temp1 = set(d1.keys())
		temp2 = set(d2.keys())
		if len(temp1&temp2)>=3:
			return True
		else:
			return False

	def existing_records_multiple(self,d1, d2):
		if d1 is not None and d2 is not None:
			return self.check_conts(d1,d2)
		return False

	def convert_ONE_dict(self, d):
		size = 0
		li = []
		while(size<BAND_SIZE_B-20):
			li.append(size)
			size+=1

		ans = collections.defaultdict(list)
		for t in d:
			i = t.keys()
			j = t.values()
			ans[list(i)[0]] = list(j)[0]
		return ans

	def build_model(self,rdd_columns, user_index_rdd, reverse_user_dict, business_index_rdd, reverse_business_dict):

		business_to_user = rdd_columns.map(lambda x: (business_index_rdd[x[1]], (user_index_rdd[x[0]], x[2]))).groupByKey()
		business_to_user_vals = business_to_user.map(lambda x:(x[0], list(x[1])))
		filter_corated_business_to_user = business_to_user_vals.filter(lambda x: len(x[1])>=3).map(lambda x:(x[0], [{b[0]:b[1]} for b in x[1]]))
		form_list_filter_one = filter_corated_business_to_user.map(lambda x: (x[0],self.convert_ONE_dict(x[1]))).map(lambda x:x)

		cand = form_list_filter_one.map(lambda x:x[0])

		dict_convert = dict(form_list_filter_one.collect())

		return dict_convert, cand 
	
	def make_pairs(self,d, rdd):
		pair = rdd.cartesian(rdd).filter(lambda x:x[0]<x[1])
		existing_records = pair.filter(lambda x: self.existing_records_multiple(d[x[0]],d[x[1]]))
		return existing_records

class User_Based:
	def hash_functions_vals(self, id1, hash_vals):
		ans = []
		for a,b,c in hash_vals:
			temp = ((a*id1+b)%23333333333)%c
			ans.append(temp)
		return ans

	def generate_hash(self, length_bus):
		random.seed(a=2)
		hash_val = []
		ans = []
		u=0
		while(u<int(NUMBER_OF_HASH_FUNCTIONS-29)):
			ans.append(u)
			u+=1
		a = random.sample(range(1, 1000000-1), 30)
		b = random.sample(range(0, 1000000-1), 30)
		for i,j in zip(a,b):
			hash_val.append((i,j, length_bus*2))
		return hash_val

	def compareLists(self,l1, l2):
		ans = []
		for i, j in zip(l1,l2):
			ans.append(min (i,j))
		return ans
	
	def calc_chunk_size(self,l,b):

		e = int(math.ceil(l/b))
		return e

	def chunks(self, nums):
		
		sum_baskets = 0
		j = 0
		while(j<BAND_SIZE_B-29):
			sum_baskets+=j
			j+=1

		l = len(nums)
		ans = []
		each_chunk_size = self.calc_chunk_size(l, BAND_SIZE_B)
		i=0

		while(i<len(nums)):
			temp = nums[i:i+each_chunk_size]
			temp_tuple = tuple(temp)
			bucket_index = i//each_chunk_size
			ans.append((bucket_index, hash(temp_tuple)))
			i+=each_chunk_size
		return ans

	def get_pairs(self, filter_corated_business_to_user, tuples_hash_functions):

		begin = filter_corated_business_to_user.flatMap(lambda x: [(y[0], self.hash_functions_vals(x[0], tuples_hash_functions)) for y in x[1]])
		begin_red = begin.reduceByKey(self.compareLists)

		divide_chunks_rdd = begin_red.flatMap(lambda values: [(tuple(chunk), values[0]) for chunk in self.chunks(values[1])]).map(lambda band:band)
		same_bucket_chunks_rdd = divide_chunks_rdd.groupByKey().map(lambda temp: sorted(set(temp[1]))).filter(lambda temp: len(temp) > 1)
		pairs_calculate = same_bucket_chunks_rdd.flatMap(lambda b: [do for do in combinations(b, 2)]).distinct()
		return pairs_calculate

	def convert_ONE_dict(self, d):

		ans = collections.defaultdict(list)
		for t in d:
			i = t.keys()
			j = t.values()
			ans[list(i)[0]] = list(j)[0]
		return ans

	def convert_required_form(self, rdd):
		form = rdd.flatMap(lambda x:[(temp[0], (x[0], temp[1])) for temp in x[1]]).groupByKey()
		form_list = form.map(lambda x:(x[0], list(set(x[1]))))
		form_list_filter = form_list.filter(lambda x: len(x[1])>=3).map(lambda x:(x[0], [{b[0]:b[1]} for b in x[1]]))
		form_list_filter_one = form_list_filter.map(lambda x: (x[0],self.convert_ONE_dict(x[1])))
		d = dict(form_list_filter_one.collect())
		return d


	def model_build(self, rdd_columns, user_index_rdd, reverse_user_dict, business_index_rdd, reverse_business_dict):

		tuples_hash_functions = self.generate_hash(len(business_index_rdd))
		business_to_user = rdd_columns.map(lambda x: (business_index_rdd[x[1]], (user_index_rdd[x[0]], x[2]))).groupByKey()
		business_to_user_vals = business_to_user.map(lambda x:(x[0], list(x[1]))).map(lambda users:users)
		filter_corated_business_to_user = business_to_user_vals.filter(lambda x: len(x[1])>=3)

		get_pairs = self.get_pairs(filter_corated_business_to_user, tuples_hash_functions)
		convert_to_dict_user = self.convert_required_form(filter_corated_business_to_user)

		return convert_to_dict_user, get_pairs
class Helper:

	def pearson(self, d1, d2):
		list1 = []
		list2 = []
		temp1 = set(d1.keys())
		temp2 = set(d2.keys())
		same_corated = list(temp1&temp2)

		for u in same_corated:
			list1.append(d1[u])
			list2.append(d2[u])
		average_l1 = sum(list1)/len(list1)
		average_l2 = sum(list2)/len(list2)
		ans = []
		for i,j in zip(list1, list2):
			if i is not None and j is not None:

				ans.append((i-average_l1)*(j-average_l2))

		numerator = sum(ans)
		ans1=0
		ans2=0
		if numerator==0:
			return 0
		for i in list1:
			ans1+=(i-average_l1)**2
		for i in list2:
			ans2+=(i-average_l2)**2
		denominator = math.sqrt(ans1)*math.sqrt(ans2)

		if denominator==0 or numerator==0:
			return 0
		return numerator/denominator	

	def jaccard_num(self,d1,d2):
		temp1 = set(d1.keys())
		temp2 = set(d2.keys())
		return float(len(temp1 & temp2))

	def jaccard_den(self,d1,d2):
		temp1 = set(d1.keys())
		temp2 = set(d2.keys())
		return float(len(temp1 | temp2))

	def jaccard(self, d1, d2):

	    if d1!=-1 and d2!=-1:
	        users1 = set(d1.keys())
	        users2 = set(d2.keys())
	        if len(set(d1.keys()) & set(d2.keys())) >= 3:
	        	numerator = self.jaccard_num(d1, d2)
	        	denominator = self.jaccard_den(d1, d2)
	        	if float(numerator/denominator)>=0.01:
	        		return True

	def write_file(self, j, file_path):

	    with open(file_path, 'w+') as o:
	        for data in j:
	            o.writelines(json.dumps(data) + "\n")
	        o.close()
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
	global stop_words_set
	building_structure = Building_Structure()
	user_based_helper = User_Based()
	item_based_helper = Item_Based()
	helper = Helper()

	building_structure.read_input()
	print("Arguments Passed: ", input_file, model_file, model_type)

	conf = SparkConf().setMaster("local").setAppName("task3train").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")

	sc_object = SparkContext(conf=conf)
	sc_object.setLogLevel("WARN")
	rdd_lines = sc_object.textFile(input_file).map(lambda line: json.loads(line))

	rdd_columns, user_index_rdd, reverse_user_dict, business_index_rdd, reverse_business_dict = Building_Model().make_rdd(rdd_lines)

	if model_type == "user_based":
		dict_refer, possible_pairs = user_based_helper.model_build(rdd_columns, user_index_rdd, reverse_user_dict, business_index_rdd, reverse_business_dict)
		candidates = possible_pairs.filter(lambda x: helper.jaccard(dict_refer.get(x[0], -1), dict_refer.get(x[1],-1)))
		candidates_pearson = candidates.map(lambda p: (p, helper.pearson(dict_refer[p[0]], dict_refer[p[1]]))).filter(lambda k:k[1]>0)
		required_format = candidates_pearson.map(lambda k:{"u1":reverse_user_dict[k[0][0]], "u2":reverse_user_dict[k[0][1]],"sim":k[1]}).collect()
		helper.write_file(required_format, model_file)

	else:
		dict_convert, rdd_cand = item_based_helper.build_model(rdd_columns, user_index_rdd, reverse_user_dict, business_index_rdd, reverse_business_dict)
		candidate_pair_int = item_based_helper.make_pairs(dict_convert, rdd_cand)
		compute_simi = candidate_pair_int.map(lambda x:(x,helper.pearson(dict_convert[x[0]], dict_convert[x[1]]))).filter(lambda x:x[1]>0)
		required_format = compute_simi.map(lambda k:{"b1":reverse_business_dict[k[0][0]], "b2":reverse_business_dict[k[0][1]], "sim":k[1]}).map(lambda x:x)
		helper.write_file(required_format.collect(), model_file)

if __name__ == "__main__":
	main()

print("Duration: %s s" % (time.time() - start_time))