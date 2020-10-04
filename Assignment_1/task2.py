from pyspark import SparkContext
import sys
import time
import json
from collections import defaultdict

start_time = time.time()

review_file = ''
business_file = ''
output_file = ''
if_spark=''
n = 0

class Building_Structure():

	def read_input(self):
		global review_file, business_file, output_file, if_spark, n
		review_file = sys.argv[1]
		business_file = sys.argv[2]
		output_file = sys.argv[3]
		if_spark = sys.argv[4]
		n = sys.argv[5]

	def process_output(self, answer):
		global output_file
		output_dict = {}

		output_dict["result"] = [list(value) for value in answer]

		# print(output_dict)
		f = open(output_file,"w")
		json.dump(output_dict,f)
		f.close()


class Helper():
	def format_list_categories(data):
		categories = data.split(',')
		ans =[]
		for i in categories:
			ans.append(i.strip())
		return ans
	def extract_pair(review_dict):
		ans_dict = {}
		for key,value in review_dict.items():
			ans_dict[key] = (sum(value), len(value))
		return ans_dict

class PythonCode:

	def calc_avg(self, join_dict):
		avg_dict = {}
		for k,v in join_dict.items():
			avg_dict[k] = float(v[0]/v[1])
		return avg_dict

	def break_category(self, b_val, ans, value):
		i=0
		while i<len(b_val):
			if b_val[i] not in ans:
				ans[b_val[i]] = value
			else:
				ans[b_val[i]] = (value[0] + ans[b_val[i]][0], value[1] + ans[b_val[i]][1])
			i+=1
	
	def process_review_file(self, review):
		review_list = []
		for line in review:
			review_list.append(json.loads(line))
		review_dict = {}
		for r in review_list:
			if r['business_id'] in review_dict:
				review_dict[r['business_id']].append(float(r['stars']))
			else:
				review_dict[r['business_id']] = [float(r['stars'])]
		review_dict_modified = Helper.extract_pair(review_dict)

		return review_dict_modified

	def compute_join(self, review_dict_modified, business_dict):

		ans_dict = {}
		for k,v in review_dict_modified.items():
			if k in business_dict:
				self.break_category(business_dict[k], ans_dict, v)
		return ans_dict
	
	def process_business_file(self, business):

		business_list = []
		for line1 in business:
			business_list.append(json.loads(line1))
		business_dict = {}
		for b in business_list:
			if b['categories']!=None and b['categories']!="":
				if b['business_id'] in business_dict:
					business_dict[b['business_id']].extend(Helper.format_list_categories(b['categories']))
				else:
					business_dict[b['business_id']] = Helper.format_list_categories(b['categories'])
		return business_dict

	def compute_average_stars(self, n):

		review = open(review_file,'r')
		business = open(business_file, 'r')

		review_dict_modified = self.process_review_file(review)
		business_dict = self.process_business_file(business)
		join_dict = self.compute_join(review_dict_modified, business_dict)
		calc_avg = self.calc_avg(join_dict)
		sorted_val = sorted(calc_avg.items(), key = lambda row: (-row[1], row[0]) )[:int(n)]

		return sorted_val

class SparkCode:

	#store sum and count
	def transformations_in_reviews(self, rdd_reviews):
		rdd_reviews_values = rdd_reviews.groupByKey()
		rdd_reviews_values_sum_count = rdd_reviews_values.map(lambda row: (row[0],(sum(row[1]), len(row[1]))))
		return rdd_reviews_values_sum_count

	#store id and list of categories
	def transformations_in_business(self, rdd_business):

		rdd_business_values = rdd_business.filter(lambda data: data[1]!="" and data[1]!=None)
		rdd_business_values_extract = rdd_business_values.map(lambda data_row: (data_row[0],Helper.format_list_categories(data_row[1])))
		return rdd_business_values_extract
	
	def compute_join(self, business, reviews):
		return reviews.leftOuterJoin(business)

	def calc_sum(self,a,b):
		result_tuple = (a[0]+b[0], a[1]+b[1])
		return result_tuple
	def calc_avg(self, row):
		avg_stars = float(row[0]/row[1])
		return avg_stars

	def expand_category(self,row):
		ans = []
		for category in row[0]:
			ans.append((category, row[1]))
		return ans

	def category_rdd(self, rdd_join_result):

		rdd_join_result_no_id = rdd_join_result.map(lambda row: row[1])
		rdd_category = rdd_join_result_no_id.filter(lambda row: row[1] != None) 
		rdd_category_mod = rdd_category.flatMap(lambda row:self.expand_category(row))

		rdd_category_reducer = rdd_category_mod.reduceByKey(lambda a,b : self.calc_sum(a,b))
		return rdd_category_reducer

	def compute_average_stars(self, n):
		sc_object = SparkContext()
		rdd_business = sc_object.textFile(business_file).map(lambda line: json.loads(line)).map(lambda row: (row['business_id'], row['categories']))
		rdd_reviews = sc_object.textFile(review_file).map(lambda line: json.loads(line)).map(lambda row: (row['business_id'], float(row['stars'])))


		rdd_reviews_reqd = self.transformations_in_reviews(rdd_reviews)		
		rdd_business_reqd = self.transformations_in_business(rdd_business)

		rdd_join_result = self.compute_join(rdd_reviews_reqd, rdd_business_reqd)
		rdd_category_reducer = self.category_rdd(rdd_join_result)

		rdd_avg = rdd_category_reducer.map(lambda row: (row[0], self.calc_avg(row[1]))).sortByKey(ascending=True)
		rdd_avg_top = rdd_avg.takeOrdered(n, key=lambda row: (-row[1], row[0]))

		return rdd_avg_top

def main():
	global if_spark

	building_structure = Building_Structure()

	building_structure.read_input()
	# print("Arguments Passed: ", review_file, business_file, output_file, if_spark, n)

	if if_spark == "spark":
		ans_list = SparkCode().compute_average_stars(int(n))
		building_structure.process_output(ans_list)

	else:
		ans_list = PythonCode().compute_average_stars(int(n))
		building_structure.process_output(ans_list)


if __name__ == "__main__":
	main()

# print("--- %s seconds ---" % (time.time() - start_time))