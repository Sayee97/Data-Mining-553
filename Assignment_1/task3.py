from pyspark import SparkContext
import sys
import time
import json

start_time = time.time()

input_file = ''
output_file = ''
partition_type = ''
n_partitions = 0
n = 0
ans = {}
class Building_Structure():

	def read_input(self):
		global input_file, output_file, partition_type, n_partitions, n
		input_file = sys.argv[1]
		output_file = sys.argv[2]
		partition_type = sys.argv[3]
		n_partitions = sys.argv[4]
		n = sys.argv[5]

	def process_output(self):
		# print(ans)
		f = open(output_file,"w")
		json.dump(ans,f)
		f.close()

	def convert_to_list(self, s):
		review_list = []
		for i in s:
			review_list.append(list(i))
		return review_list

def partition_function(key):
	#taking hash value of the business_id and taking mod with n_partitions
	return hash(key)%int(n_partitions)

def process(row, n):
	return row[1] > n

def calc_items(rdd_columns):
	rdd_columns_modified = rdd_columns.glom().map(len)
	ans['n_items'] = rdd_columns_modified.collect()

def calc_more_than_n(rdd_columns):
	rdd_columns_modified = rdd_columns.reduceByKey(lambda a,b :a+b)
	rdd_columns_crop = rdd_columns_modified.filter(lambda row:process(row, int(n)))
	ans['result'] = Building_Structure().convert_to_list(rdd_columns_crop.collect())

def main():

	building_structure = Building_Structure()

	building_structure.read_input()
	# print("Arguments Passed", input_file, output_file, partition_type, n_partitions, n)

	#creating Object
	sc_object = SparkContext()
	rdd_lines = sc_object.textFile(input_file).map(lambda line: json.loads(line))
	rdd_columns_reqd = rdd_lines.map(lambda row: (row['business_id'], row['review_id']))
	#key value pairs
	rdd_columns = rdd_columns_reqd.map(lambda data_row: (data_row[0],1))
	if partition_type == "customized":
		rdd_columns = rdd_columns.partitionBy(int(n_partitions), partition_function)

	ans['n_partitions'] = rdd_columns.getNumPartitions()
	calc_items(rdd_columns)
	calc_more_than_n(rdd_columns)
	building_structure.process_output()


if __name__ == "__main__":
	main()

# print("--- %s seconds ---" % (time.time() - start_time))