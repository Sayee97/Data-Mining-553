


from pyspark import SparkContext
import sys
import time
import json
import math
from itertools import combinations
start_time = time.time()
input_file = ''
output_file = ''
BAND_SIZE_B =30


class Building_Structure():

	def read_input(self):
		global input_file, output_file
		input_file = sys.argv[1]
		output_file = sys.argv[2]

class Helper:

	def calc_chunk_size(self,l,b):
		e = int(math.ceil(l/b))
		return e

	def chunks(self, nums):
		l = len(nums)
		ans = []
		each_chunk_size = self.calc_chunk_size(l, BAND_SIZE_B)
		for index, start in enumerate(range(0, len(nums), each_chunk_size)):
			ans.append((index, hash(tuple(nums[start:start + each_chunk_size]))))
		return ans

	def generate_hash(self, NUMBER_OF_HASH_FUNCTIONS):
		ans = []
		for a in range(25):
			for b in range(4):
				ans.append((a,b,100))
		return ans

	def modify_input(self, rdd_lines):

		rdd_columns = rdd_lines.map(lambda data_row: (data_row['business_id'],data_row['user_id']))

		rdd_columns_zip = rdd_columns.map(lambda data_row: data_row[0]).distinct().sortBy(lambda item: item).zipWithIndex()
		# user_index_rdd = rdd_columns.map(lambda kv: kv[0]).distinct().sortBy(lambda item: item).zipWithIndex().map(lambda kv: {kv[0]: kv[1]}).flatMap(lambda kv_items: kv_items.items())
		

		# Storing each business ID
		mapping_business_ids = rdd_columns_zip.map(lambda x: (x[1],x[0]))


		rdd_join = rdd_columns.join(rdd_columns_zip)

		# User Id, Uske distinct business Id
		# user id -----> business ids list

		rdd_join_drop_business_id = rdd_join.map(lambda data_row: data_row[1]).groupByKey().mapValues(list)


		# user id -> index

		rdd_columns_zip_user_ids = rdd_columns.map(lambda data_row: data_row[1]).distinct().sortBy(lambda item: item).zipWithIndex()

		
		#user index-->business id
		rdd_join_with_index = rdd_columns_zip_user_ids.join(rdd_join_drop_business_id)

		rdd_join_with_index_val = rdd_join_with_index.map(lambda x:x[1])

		#USer id vs business id ka list

		return rdd_join_with_index_val
	

	def compareLists(self,l1, l2):

		return [min(s, p) for s, p in zip(l1, l2)]

def main():
	building_structure = Building_Structure()

	building_structure.read_input()
	print("Arguments Passed: ", input_file, output_file)

	#creating Object
	sc_object = SparkContext()
	rdd_lines = sc_object.textFile(input_file).map(lambda line: json.loads(line))

	# user _id --> business ids ka list
	modify_rdd = Helper().modify_input(rdd_lines)

	hash_functions_list = Helper().generate_hash(100)

	#user_id --> hash functions ka list
	hashed_values_each_row = modify_rdd.map(lambda kv: (kv[0], [((a*kv[0]+b)%c) for (a,b,c) in hash_functions_list]))


	#_______________MAKING SIGNATURE MATRIX_____________________

	signature_rdd = modify_rdd.leftOuterJoin(hashed_values_each_row).map(lambda temp:temp[1]).flatMap(lambda j:[(b,j[1]) for b in j[0]])

	# Extract minimum values!!
	signature_rdd_reduce_by_key = signature_rdd.reduceByKey(Helper().compareLists)

	divide_chunks_rdd = signature_rdd_reduce_by_key.flatMap(lambda values: [(tuple(chunk), values[0]) for chunk in Helper().chunks(values[1])])
	same_bucket_chunks_rdd = divide_chunks_rdd.groupByKey().map(lambda temp: list(temp[1])).filter(lambda temp: len(temp) >= 2)
	pairs_calculate = same_bucket_chunks_rdd.flatMap(lambda b: [do for do in combinations(b, 2)])

	print(pairs_calculate.count())

if __name__ == "__main__":
	main()

print("--- %s seconds ---" % (time.time() - start_time))