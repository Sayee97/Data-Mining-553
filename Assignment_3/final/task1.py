from pyspark import SparkContext, SparkConf
import sys
import time
import json
import random
import math
from itertools import combinations
start_time = time.time()
input_file = ''
output_file = ''
BAND_SIZE_B = 30
THRESHOLD = 0.055

BUCKET = 10
class Building_Structure():

	def read_input(self):
		global input_file, output_file
		input_file = sys.argv[1]
		output_file = sys.argv[2]

class Similarity_Check:
	def computeJaccard(self,s1, s2):
		numerator = float(len(set(s1)&set(s2)))
		deno = float(len(set(s1)|set(s2)))
		ans = float(numerator/deno)
		return ans

	def check_simi(self,candidate_pairs, index_data_dict,reversed_index_dict):
		ans = list()
		temp_set = set()
		ans_check = []
		for pair in candidate_pairs:
			if pair not in temp_set:
				temp_set.add(pair)
				s = self.computeJaccard(index_data_dict.get(pair[0], set()),index_data_dict.get(pair[1], set()))
				if s >= THRESHOLD:
					pair = sorted(pair)
					ans.append({"b1": reversed_index_dict[pair[0]],"b2": reversed_index_dict[pair[1]],"sim": s})
		i=0
		while(i< BUCKET-29):
			ans_check.append(i)
			i+=1
		return ans, ans_check

class Signature_Matrix:

	def make_signature_matrix(self, rdd_user_business, hashed_calculations):

		signature_rdd = rdd_user_business.leftOuterJoin(hashed_calculations).map(lambda temp:temp[1]).flatMap(lambda j:[(b,j[1]) for b in j[0]])
		signature_rdd_reduce_by_key = signature_rdd.reduceByKey(self.compareLists)

		return signature_rdd_reduce_by_key

	def make_candidates(self, signature_rdd):

		divide_chunks_rdd = signature_rdd.flatMap(lambda values: [(tuple(chunk), values[0]) for chunk in self.chunks(values[1])])
		same_bucket_chunks_rdd = divide_chunks_rdd.groupByKey().map(lambda temp: list(temp[1])).filter(lambda temp: len(temp) > 1)
		pairs_calculate = same_bucket_chunks_rdd.flatMap(lambda b: [do for do in combinations(b, 2)])

		return pairs_calculate
	
	def calc_chunk_size(self,l,b):

		e = int(math.ceil(l/b))
		return e

	def chunks(self, nums):

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

		sum_baskets = 0
		j = 0
		while(j<BAND_SIZE_B-29):
			sum_baskets+=j
			j+=1

		return ans

	def compareLists(self,l1, l2):

		return [min(v1,v2) for v1,v2 in zip(l1,l2)]

class Helper:

	def hash_functions_vals(self, modify_rdd):

		hash_functions_list, ans = self.generate_hash(30)
		hashed_values_each_row = modify_rdd.map(lambda kv: (kv[0], [(((a*kv[0]+b)%233333333333)%c) for (a,b,c) in hash_functions_list]))
		return hashed_values_each_row

	def generate_hash(self, NUMBER_OF_HASH_FUNCTIONS):

		random.seed(a=2)
		hash_val = []
		a = random.sample(range(1, 100000-1), 30)
		b = random.sample(range(0, 100000-1), 30)
		u = 0

		ans = []
		while(u<int(NUMBER_OF_HASH_FUNCTIONS-29)):
			ans.append(u)
			u+=1
		c = 26184*2
		for i,j in zip(a,b):
			hash_val.append((i,j, c))
		return hash_val, ans 


	def modify_input(self, rdd_lines):

		rdd_columns = rdd_lines.map(lambda data_row: (data_row['business_id'],data_row['user_id']))

		business_business_id_rdd = rdd_columns.map(lambda data_row: data_row[0]).distinct().sortBy(lambda item: item).zipWithIndex().map(lambda kv: {kv[0]: kv[1]}).flatMap(lambda kv_items: kv_items.items())
		inverse_refer_by_index_business_id_rdd = business_business_id_rdd.map(lambda x: (x[1],x[0]))

		# User Ids ----> Business indexes ka list
		rdd_join = rdd_columns.join(business_business_id_rdd)
		rdd_join_user_id_business_indexes = rdd_join.map(lambda data_row: data_row[1]).groupByKey().mapValues(list)

		# user id ----> index
		user_user_id_rdd = rdd_columns.map(lambda data_row: data_row[1]).distinct().sortBy(lambda item: item).zipWithIndex().map(lambda kv: {kv[0]: kv[1]}).flatMap(lambda kv_items: kv_items.items())
		inverse_refer_by_index_user_id = user_user_id_rdd.map(lambda x:(x[1],x[0]))
		
		#user index-->business indexes list
		all_index_user_business = user_user_id_rdd.join(rdd_join_user_id_business_indexes)
		reqd_val_rdd = all_index_user_business.map(lambda x:x[1])

		return reqd_val_rdd, inverse_refer_by_index_business_id_rdd, inverse_refer_by_index_user_id

	def make_rdd(self, rdd_lines, refer_business_indexes, refer_user_indexes):
		
		rdd_columns = rdd_lines.map(lambda data_row: (data_row['user_id'],data_row['business_id']))

		inverse_rdd_business = refer_business_indexes.map(lambda x: (x[1], x[0]))
		inverse_rdd_users = refer_user_indexes.map(lambda x: (x[1], x[0]))

		join_rdd = rdd_columns.join(inverse_rdd_users)
		join_rdd_drop_key = join_rdd.map(lambda x:x[1]).groupByKey().mapValues(list)

		join_indexes = inverse_rdd_business.join(join_rdd_drop_key)
		join_indexes_drop_key = join_indexes.map(lambda x:x[1])

		return join_indexes_drop_key

	def write_file(self, j, file):
		with open(file, 'w+') as o:
			for item in j:
				o.writelines(json.dumps(item) + "\n")
			o.close()

def main():
	building_structure = Building_Structure()
	signature_helper = Signature_Matrix()
	similarity_helper = Similarity_Check()
	helper = Helper()

	building_structure.read_input()
	print("Arguments Passed: ", input_file, output_file)

	conf = SparkConf().setMaster("local").setAppName("sayee").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
	sc_object = SparkContext(conf=conf)
	sc_object.setLogLevel("WARN")
	rdd_lines = sc_object.textFile(input_file).map(lambda line: json.loads(line))


	modify_rdd, refer_business_indexes, refer_user_indexes = helper.modify_input(rdd_lines)

	hashed_values_each_row = helper.hash_functions_vals(modify_rdd)
	signature_rdd = signature_helper.make_signature_matrix(modify_rdd, hashed_values_each_row)


	pairs = signature_helper.make_candidates(signature_rdd)

	make_rdd_b_user = helper.make_rdd(rdd_lines, refer_business_indexes, refer_user_indexes)

	ans_list, ans_check = similarity_helper.check_simi(set(pairs.collect()), make_rdd_b_user.collectAsMap(), refer_business_indexes.collectAsMap())

	helper.write_file(ans_list, output_file)



if __name__ == "__main__":
	main()

print("Duration: %s s" % (time.time() - start_time))