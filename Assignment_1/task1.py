from pyspark import SparkContext
import sys
import time
import json

start_time = time.time()

input_file = ''
output_file = ''
stop_words = ''
y = 0
m = 0
n = 0

special_chars_set = {'.',',','!','?',';',':',')',']','(','['}
stop_words_set={}

class Helper:

	def part_D_tuple_to_list(users):
		users_list = []
		for i in users:
			users_list.append(list(i))
		return users_list

	def part_E_to_list(text):
		review_list = []
		for i in text:
			review_list.append(i[0])
		return review_list

	def reformat(adul_word):
		word_req = ""
		for character in adul_word:
			if character not in special_chars_set:
				word_req += character
		word_modified = ''.join(word_req)
		if word_modified not in stop_words_set:
			return word_modified


class Review:

	def part_A(self, rdd_lines):
		row_count = rdd_lines.count()

		return row_count

	def part_B(self, rdd_lines, y):
		rdd_columns = rdd_lines.map(lambda data_row: (data_row['date'], data_row['review_id']))
		rdd_matched_years = rdd_columns.filter(lambda data_row: int(data_row[0][:4])==y)

		return rdd_matched_years.count()

	def part_C(self, rdd_lines):
		rdd_columns = rdd_lines.map(lambda data_row: data_row['business_id'])
		rdd_matched_years = rdd_columns.distinct()

		return rdd_matched_years.count()

	def part_D(self, rdd_lines, m):
		rdd_columns = rdd_lines.map(lambda data_row: (data_row['user_id'], data_row['review_id']))
		rdd_user_pair = rdd_columns.map(lambda data_row: (data_row[0], 1))
		rdd_reduce = rdd_user_pair.reduceByKey(lambda key1,key2: key1+key2).sortByKey(ascending=True)
		rdd_reduce_top_m = rdd_reduce.takeOrdered(m, key=lambda count_reviews: (-count_reviews[1], count_reviews[0]))
		required_list = Helper.part_D_tuple_to_list(rdd_reduce_top_m)

		return required_list

	def part_E(self, rdd_lines,n):
		rdd_columns = rdd_lines.map(lambda data_row: data_row['text'])
		rdd_one_list = rdd_columns.flatMap(lambda data_row: data_row.lower().split())
		rdd_text_pair = rdd_one_list.map(lambda data_row: (Helper.reformat(data_row),1))
		rdd_pair_filter = rdd_text_pair.filter(lambda key:(key[0] != None and key[0] != ""))
		rdd_reduce = rdd_pair_filter.reduceByKey(lambda key1, key2: key1+key2).sortByKey(ascending=True)
		rdd_reduce_top_n = rdd_reduce.takeOrdered(int(n), key=lambda freq:(-freq[1], freq[0]))
		required_list = Helper.part_E_to_list(rdd_reduce_top_n)

		return required_list

class Building_Structure():

	def read_input(self):
		global input_file, output_file, stop_words, y, m, n
		input_file = sys.argv[1]
		output_file = sys.argv[2]
		stop_words = sys.argv[3]
		y = sys.argv[4]
		m = sys.argv[5]
		n = sys.argv[6]

	def process_output(self, part_A, part_B, part_C, part_D, part_E):
		output_dict = {}
		output_dict['A'] = part_A
		output_dict['B'] = part_B
		output_dict['C'] = part_C
		output_dict['D'] = part_D
		output_dict['E'] = part_E

		# print(output_dict)
		f = open(output_file,"w")
		json.dump(output_dict,f)
		f.close()

def main():
	global stop_words_set 
	building_structure = Building_Structure()

	building_structure.read_input()
	# print("Arguments Passed: ", input_file, output_file, stop_words, y, m, n)

	#creating Object
	sc_object = SparkContext()
	rdd_lines = sc_object.textFile(input_file).map(lambda line: json.loads(line))

	calculate_results = Review()

	part_A_ans = calculate_results.part_A(rdd_lines)
	part_B_ans = calculate_results.part_B(rdd_lines, int(y))
	part_C_ans = calculate_results.part_C(rdd_lines)
	part_D_ans = calculate_results.part_D(rdd_lines, int(m))
	file = open(stop_words,"r")
	stop_words_set = set(word.strip() for word in file)
	part_E_ans = calculate_results.part_E(rdd_lines, int(n))

	building_structure.process_output(part_A_ans, part_B_ans, part_C_ans, part_D_ans, part_E_ans)


if __name__ == "__main__":
	main()

# print("--- %s seconds ---" % (time.time() - start_time))
