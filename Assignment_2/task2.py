from pyspark import SparkContext
import sys
import time
import json
from itertools import combinations
import math

start_time = time.time()

support = ''
input_file = ''
output_file = ''
NUMBER_OF_BUCKETS = 52
NUMBER_OF_PARTITION = 2

class First_Pass_Son:

	def hash_pair(self, one, two):
		value = ord(one[0])+ord(two[0])
		hash_value = value%52
		return hash_value

	def modify_bucket(self, bucket, support_ps):
		for i in range(len(bucket)):
			if bucket[i]>=support_ps:
				bucket[i] = 1
			else:
				bucket[i] = 0
		return bucket

	def modify_items(self, items_cand, support_ps):

		d2 = {key: value for key, value in items_cand.items() if value >= support_ps}
		return d2

	def one_and_bucket(self, input_params):
		baskets_list, support_ps = input_params
		global NUMBER_OF_BUCKETS

		d = {}
		bucket = [0]*NUMBER_OF_BUCKETS
		ans_bucket = []
		int_cod  = 0

		for basket_items in baskets_list:
			for item in basket_items:
				if item in d:
					d[item] += 1
				else:
					d[item] =  1 

			for pair_items in combinations(basket_items, 2):
				hash_value = self.hash_pair(pair_items[0], pair_items[1])
				bucket[hash_value]+=1
		
		while(int_cod<support_ps):
			ans_bucket.append(int_cod)
			int_cod+=1

		frequent_singleton = self.modify_items(d, support_ps)
		bit_bucket = self.modify_bucket(bucket, support_ps)

		#print(frequent_singleton, "lolsae")

		return bit_bucket, frequent_singleton, ans_bucket

	def form_combination(self, s, size):
		ans = []
		for i in combinations(s,size):
			ans.append(i)
		return set(ans)

	def pnc(self, list_items, support_ps, frequent_singleton_items):
		
		support_ps_take = support_ps
		single_ones = frequent_singleton_items
		
		list_items_pair = []
		support_tp = []
		set_list_items_temp = set(list_items)

		if list_items!=None:
			if len(list_items)>0:
				size_items = len(list_items[0])
				i=0
				while i<=len(list_items)-2:
					for piche in list_items[i+1:]:
						if list_items[i][:-1] == piche[:-1]:
							piche_temp = set(piche)
							list_items_temp = set(list_items[i])
							list_ans = piche_temp.union(list_items_temp)
							possible_ans_tuple = tuple(sorted(list(list_ans)))

							possible_subsets = self.form_combination(possible_ans_tuple, size_items)
							if possible_subsets.issubset(set_list_items_temp):
								list_items_pair.append((possible_ans_tuple))
						else:
							break
					t = 0
					while(t<support_ps and support_ps>-1):
						support_tp.append(t)
						t+=1
					i+=1
		return list_items_pair, support_tp

	def modify_final(self, list1):
		ans = []
		for i in list1.values():
			ans.extend(i)
		return ans


	def item_set_generate(self, input_params):

		baskets, support, total = input_params  # Breaking input tuple!

		baskets_list = list(baskets)
		numerator = (len(baskets_list)*int(support))
		den = int(total)
		support_ps = math.ceil(numerator/den)
		flag_support_ps = False
		if support_ps>-1:
			flag_support_ps = True

		bit_bucket, items, ans_bucket = self.one_and_bucket((baskets_list, support_ps))
		#print(items)

		frequent_singleton_items = sorted(items.keys())

		dynamic_list = frequent_singleton_items
		# Ek list maintain karo!
		final_candidate_list = {}
		tuple_number = 1
		
		#Generate singletons
		sing = []
		for x in frequent_singleton_items:
			temp = x.split(',')
			sing.append(tuple(temp))

		final_candidate_list[str(tuple_number)] = sing

		while dynamic_list!=None and len(dynamic_list)>0:
			each_dict = {}
			tuple_number+=1
			i=0

			while(i<len(baskets_list)):

				# A-B for baskets not frequent items.
				basket_temp  = 	set(baskets_list[i])
				items_temp   = 	set(frequent_singleton_items)
				baskets_list[i]   =  sorted(basket_temp.intersection(items_temp))

				if(len(baskets_list[i])>=tuple_number):
					if tuple_number == 2:
						for item_tuple in combinations(baskets_list[i], 2):
							hash_value = self.hash_pair(item_tuple[0], item_tuple[1])
							if bit_bucket[hash_value]==1:
								if item_tuple in each_dict:
									each_dict[item_tuple]+=1
								else:
									each_dict[item_tuple]=1

					if tuple_number >=3:
						dy = 0
						while(dy<len(dynamic_list)):
							item_temp11 = set(dynamic_list[dy])
							bask_temp = set(baskets_list[i])
							if item_temp11.issubset(bask_temp):
								if dynamic_list[dy] in each_dict:
									each_dict[dynamic_list[dy]]+=1
								else:
									each_dict[dynamic_list[dy]]=1							
							dy+=1						
				i+=1

			modified_dict = self.modify_items(each_dict, support_ps)
			list_items_modified_dict = sorted(modified_dict.keys())

			dynamic_list, support_tp = self.pnc(list_items_modified_dict, support_ps, frequent_singleton_items)

			if len(list_items_modified_dict) == 0:
				if flag_support_ps == True:
					break
			final_candidate_list[str(tuple_number)] = list_items_modified_dict
			#one_list_candidate=[]
		one_list_candidate = self.modify_final(final_candidate_list)
		
		yield one_list_candidate

class Second_Pass_Son:
	def count_actual(self, file_items, candidates, support):

		d = {}
		ans =[]
		i =0
		while i<len(candidates):
			temp_set = set(candidates[i])
			temp_bask_set = set(file_items)
			if temp_set.issubset(temp_bask_set):
				if candidates[i] in d:
					d[candidates[i]]+=1
				else:
					d[candidates[i]]=1
			i+=1

		for k,v in d.items():
			temp = tuple((k,v))
			ans.append(temp)
		yield ans

class Building_Structure:

	def read_input(self):
		global support, input_file, output_file
		support = sys.argv[1]
		input_file = sys.argv[2]
		output_file = sys.argv[3]

	def modify(self, list1):
		l = sorted(list(set(list1)))
		return l

	def create_RDD(self, rdd):

		
		rdd_lines_users = rdd.map(lambda line: (line.split(',')[0], line.split(',')[1]))
		rdd_users_group = rdd_lines_users.groupByKey()
		rdd_users = rdd_users_group.map(lambda line: (line[0], self.modify(list(line[1]))))
		#print(rdd_users.collect())
		return rdd_users


	def process_output(self, cand, true_set):

		# print(output_dict)
		f = open(output_file,"w")

		cand_sort = sorted(cand, key=lambda x: (len(x),x))

		len_var = 1
		ans = ""
		ans+="Candidates:\n"
		for i in cand_sort:
			if(len(list(i))==1):
				ans += "('"+str(list(i)[0])+"'),"
			elif(len(list(i))==len_var):
				ans+=str(i)+","
			else:
				ans = ans[:-1]
				ans+="\n\n"+str(i)+","
				len_var = len(i)
		ans = ans[:-1]
		ans+="\n\nFrequent Itemsets:\n"
		i=0
		while i<len(true_set):
			if(len(list(true_set[i]))==1):
				ans += "('"+str(list(true_set[i])[0])+"'),"
			elif(len(list(true_set[i]))==len_var):
				ans+=str(true_set[i])+","
			else:
				ans = ans[:-1]
				ans+="\n\n"+str(true_set[i])+","
				len_var = len(true_set[i])	
			i+=1
		f.write(ans[:-1])
		f.close()

def main():
	global stop_words_set 
	building_structure = Building_Structure()
	son_FP = First_Pass_Son()
	son_SP = Second_Pass_Son()

	building_structure.read_input()
	#print("Arguments Passed: ", case_number, support, input_file, output_file)

	#creating Object
	sc_object = SparkContext()
	rdd_lines = sc_object.textFile(input_file, NUMBER_OF_PARTITION)

	header = rdd_lines.first()
	# removing header
	rdd_lines_reqd = rdd_lines.filter(lambda line: line!=header)
	#print(rdd_lines_reqd.take(5))
	# print(rdd_lines_reqd.count(), "bhai kitni aayi")

	rdd_process = building_structure.create_RDD(rdd_lines_reqd) 

	#just taking the values and not keys
	rdd_values = rdd_process.map(lambda values: values[1])
	totals = rdd_values.count()

	#generate candidate itemset

	first_pass_SON_output = rdd_values.mapPartitions(lambda p: son_FP.item_set_generate((p, support, totals)))
	first_pass_flatten = first_pass_SON_output.flatMap(lambda tempo:tempo)
	first_pass_SON_output_distinct = first_pass_flatten.distinct()
	first_pass_sort = first_pass_SON_output_distinct.sortBy(lambda tempo: (len(tempo), tempo)).collect()

	#print(first_pass_SON_output_distinct)

	# TRUE
	second_Pass_SON_output = rdd_values.flatMap(lambda p: son_SP.count_actual(p, first_pass_sort, support)).flatMap(lambda p:p)
	second_Pass_SON_output_reducer = second_Pass_SON_output.reduceByKey(lambda a,b:a+b)

	sec_pass_filter = second_Pass_SON_output_reducer.filter(lambda tempo:tempo[1]>=int(support)).map(lambda c:c[0])
	sec_pass_sort = sec_pass_filter.sortBy(lambda tempo: (len(tempo), tempo)).collect()

	building_structure.process_output(first_pass_sort, sec_pass_sort)

if __name__ == "__main__":
	main()

print("Duration: %s" % int(time.time() - start_time))