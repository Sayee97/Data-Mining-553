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


class Building_Structure():

	def read_input(self):
		global input_file, output_file
		input_file = sys.argv[1]
		output_file = sys.argv[2]

class Similarity_Check:
	def computeJaccard(self,set1, set2):
		return float(float(len(set(set1) & set(set2))) / float(len(set(set1) | set(set2))))

	def check_simi(self,candidate_pairs, index_data_dict,reversed_index_dict):
		result = list()
		temp_set = set()
		for pair in candidate_pairs:
			if pair not in temp_set:
				temp_set.add(pair)
				similarity = self.computeJaccard(index_data_dict.get(pair[0], set()),index_data_dict.get(pair[1], set()))
				if similarity >= THRESHOLD:
					result.append({"b1": reversed_index_dict[pair[0]],"b2": reversed_index_dict[pair[1]],"sim": similarity})
		return result

class Signature_Matrix:

	def make_signature_matrix(self, rdd_user_business, hashed_calculations):

		signature_rdd = rdd_user_business.leftOuterJoin(hashed_calculations).map(lambda temp:temp[1]).flatMap(lambda j:[(b,j[1]) for b in j[0]])
		signature_rdd_reduce_by_key = signature_rdd.reduceByKey(self.compareLists).sortByKey()

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
		return ans

	def compareLists(self,l1, l2):
		return [min(val1, val2) for val1, val2 in zip(l1, l2)]


class Helper:

	def hash_functions_vals(self, modify_rdd):

		hash_functions_list = self.generate_hash(30)
		hashed_values_each_row = modify_rdd.map(lambda kv: (kv[0], [(((a*kv[0]+b)%233333333333)%c) for (a,b,c) in hash_functions_list]))
		return hashed_values_each_row

	def generate_hash(self, NUMBER_OF_HASH_FUNCTIONS):
		hash_val = set([(7960778426191620468, 1601073713816310069, 52368),
		 (7831152705390563964, 4705457223944280482, 52368), 
		 (844720600218478240, 3317567590709987248, 52368), 
		 (3330167399431941086, 4738391131361491441, 52368), 
		 (1559521054175740197, 5164076707454038942, 52368), 
		 (7461357922965555065, 9178893876009454474, 52368), 
		 (7875232194073162640, 4110281485611768498, 52368), 
		 (2320446363199997945, 3824664871521776964, 52368), 
		 (1957373065048303663, 4845643447224352718, 52368), 
		 (329592056279686098, 8379231269947868513, 52368), 
		 (6283387047437257041, 3359664163991090847, 52368), 
		 (9220778401274251138, 5473938098476607164, 52368), 
		 (5888858691079265684, 3337812725841643675, 52368), 
		 (7412542259511328842, 7922570504607713247, 52368),
		  (7934351388339294683, 4111493018870031312, 52368), 
		  (4695318463406440120, 8807703397538162098, 52368), 
		  (3431650513912567035, 3688089768933353778, 52368), 
		  (8630417001859198914, 6812292384816374028, 52368), 
		  (4630753437454300883, 6040487568089202642, 52368), 
		  (8312174810397109455, 2304936983845495923, 52368), 
		  (8033861143366882855, 2574263769940200819, 52368), 
		  (3357440514529337414, 4593842799811611491, 52368), 
		  (8594767445816985761, 4753654029949017785, 52368), 
		  (8372516979407777331, 7343798629760420335, 52368), 
		  (3907121524486219888, 6102972418297087710, 52368),
		  (8154056408116777046, 4193763847229834397, 52368), 
		  (1517137920431471569, 8330334269844145946, 52368), 
		  (1636562308039547769, 3235326538807567540, 52368), 
		  (2126934262022313337, 6695109256688246402, 52368), 
		  (1629954137772494897, 5142794054636586632, 52368)])

		return hash_val

	def modify_input(self, rdd_lines):

		"""
		return: 1. rdd: user_index = list([business_indexes])
				2. rdd: business_indexes
				3. user indexes
		"""

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
		join_indexes_drop_key = join_indexes.map(lambda x:x[1]).sortByKey()

		return join_indexes_drop_key

def export2File(json_array, file_path):
	with open(file_path, 'w+') as output_file:
		for item in json_array:
			output_file.writelines(json.dumps(item) + "\n")
		output_file.close()

def main():
	building_structure = Building_Structure()
	signature_helper = Signature_Matrix()
	similarity_helper = Similarity_Check()
	helper = Helper()

	building_structure.read_input()
	print("Arguments Passed: ", input_file, output_file)

	conf = SparkConf().setMaster("local").setAppName("sayee").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")

	#conf = SparkConf().setMaster("local").setAppName("sayee").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
	sc_object = SparkContext(conf=conf)
	sc_object.setLogLevel("WARN")
	rdd_lines = sc_object.textFile(input_file).map(lambda line: json.loads(line))

	modify_rdd, refer_business_indexes, refer_user_indexes = helper.modify_input(rdd_lines)

	hashed_values_each_row = helper.hash_functions_vals(modify_rdd)
	signature_rdd = signature_helper.make_signature_matrix(modify_rdd, hashed_values_each_row)


	pairs = signature_helper.make_candidates(signature_rdd)

	print(pairs.count(), "Kya length!!!!!")

	make_rdd_b_user = helper.make_rdd(rdd_lines, refer_business_indexes, refer_user_indexes)

	ans_list = similarity_helper.check_simi(set(pairs.collect()), make_rdd_b_user.collectAsMap(), refer_business_indexes.collectAsMap())

	export2File(ans_list, output_file)



if __name__ == "__main__":
	main()

print("Duration: %s s" % (time.time() - start_time))