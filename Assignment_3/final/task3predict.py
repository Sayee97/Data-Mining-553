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


train_file = ''
test_file = ''
model_file = ''
output_file=''
cf_type = ''
neighbours = 3

class Item_Based:
	def make_dict_model(self, rdd_model, user_index_rdd):
		
		sim_dict = rdd_model.map(lambda x:{(user_index_rdd[x['b1']], user_index_rdd[x['b2']]):x['sim']})
		sim_dict_filter = sim_dict.flatMap(lambda x:x.items()).collectAsMap()

		return sim_dict_filter
	def make_list_inner(self, x):
		ans = []
		i = 0
		x1 = list(set(x))
		while(i<len(x1)):
			ans.append((x1[i][0], x1[i][1]))
			i+=1
		return ans

	def modify_input_file(self, rdd_columns,business_index_rdd, user_index_rdd):
		rdd = rdd_columns.map(lambda x:(user_index_rdd[x[0]],(business_index_rdd[x[1]],x[2]))).groupByKey()
		rdd_modify = rdd.map(lambda x:(x[0], self.make_list_inner(x[1])))
		return rdd_modify

	def modify_test_file(self, test_rdd, user_index_rdd, business_index_rdd):
		rdd = test_rdd.map(lambda kv:(user_index_rdd.get(kv['user_id'],-1), business_index_rdd.get(kv['business_id'],-1)))
		rdd_filter = rdd.filter(lambda x:x[0]!=-1 and x[1]!=-1)
		return rdd_filter

	def predict(self, temp_final, model_rdd, avg_rdd, reverse_user_dict):
		tar = temp_final[0]
		index_tar = reverse_user_dict.get(tar, "UNKNOWN")
		scores_list = list(temp_final[1])
		ans = []
		m = 1
		for s in scores_list:
			if tar<s[0]:
				if m>0:
					key_score = tuple((tar, s[0]))
			else:
				key_score = tuple((s[0],tar))

			t = (s[1], model_rdd.get(key_score,0))
			ans.append(tuple(t))

		res_score = sorted(ans, key = lambda i: i[1], reverse = True)[:3]
		num = sum(map(lambda i: i[0]*i[1],res_score))
		dem = sum(map(lambda l:abs(l[1]), res_score))
		
		t1 = (tar,avg_rdd.get(index_tar,3.83))
		s_temp = 0
		s_temp_list = []
		while(s_temp<10):
			s_temp_list.append(s_temp)
			s_temp+=1
		if num==0 or dem==0:
			return tuple(t1)

		t2 = (tar, num/dem)
		return tuple(t2)

class User_Based:
	def make_dict_model(self, rdd_model, user_index_rdd):
		
		sim_dict = rdd_model.map(lambda x:{(user_index_rdd[x['u1']], user_index_rdd[x['u2']]):x['sim']})
		sim_dict_filter = sim_dict.flatMap(lambda x:x.items()).collectAsMap()

		return sim_dict_filter

	def modify_input_file(self, rdd_columns,business_index_rdd, user_index_rdd):
		rdd = rdd_columns.map(lambda x:(business_index_rdd[x[1]], (user_index_rdd[x[0]],x[2]))).groupByKey()
		rdd_modify = rdd.map(lambda x:(x[0], [(y[0],y[1]) for y in list(set(x[1]))]))
		return rdd_modify

	def modify_test_file(self, test_rdd, user_index_rdd, business_index_rdd):
		rdd = test_rdd.map(lambda kv:(business_index_rdd.get(kv['business_id'],-1), user_index_rdd.get(kv['user_id'],-1)))
		rdd_filter = rdd.filter(lambda x:x[0]!=-1 and x[1]!=-1)
		return rdd_filter

	def num(self, ans):
		l = map(lambda i: (i[0]-i[1])*i[2],ans)
		return sum(l)
	def den(self, ans):
		l = map(lambda l:abs(l[2]), ans)
		return sum(l)

	def predict(self, temp_final, model_rdd, avg_rdd, reverse_user_dict):
		tar = temp_final[0]
		index_tar = reverse_user_dict.get(tar, "UNKNOWN")
		scores_list = list(temp_final[1])
		ans = []
		m = 1
		for s in scores_list:
			if tar<s[0]:
				if m>0:
					key_score = tuple((tar, s[0]))
			else:
				key_score = tuple((s[0],tar))
			other = reverse_user_dict.get(s[0],"UNKNOWN" )
			baki_uids = avg_rdd.get(other,3.83)
			t = (s[1],baki_uids, model_rdd.get(key_score,0))
			ans.append(tuple(t))
		
		num = self.num(ans)
		dem = self.den(ans)
		
		t1 = (tar,avg_rdd.get(index_tar,3.83))
		s_temp = 0
		s_temp_list = []
		while(s_temp<10):
			s_temp_list.append(s_temp)
			s_temp+=1

		if num==0 or dem==0:
			return tuple(t1)

		t2 = (tar, avg_rdd.get(index_tar,3.83 )+(num/dem))
		return tuple(t2)
class Helper:
	def write_file(self, j, f):

	    with open(f, 'w+') as output_file:
	        for data in j:
	            output_file.writelines(json.dumps(data) + "\n")
	        output_file.close()	

class Building_Structure():

	def read_input(self):
		global train_file, test_file, model_file, output_file, cf_type
		train_file = sys.argv[1]
		test_file = sys.argv[2]
		model_file = sys.argv[3]
		output_file = sys.argv[4]
		cf_type = sys.argv[5]

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
	user_avg_path = 'HW3/user_avg.json'
	business_avg_path = 'HW3/business_avg.json'
	building_structure = Building_Structure()
	user_based_helper = User_Based()
	item_based_helper = Item_Based()
	helper = Helper()

	building_structure.read_input()
	print("Arguments Passed: ", train_file, test_file, model_file, output_file, cf_type)

	conf = SparkConf().setMaster("local").setAppName("task3train").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")

	sc_object = SparkContext(conf=conf)
	sc_object.setLogLevel("WARN")
	rdd_lines = sc_object.textFile(train_file).map(lambda line: json.loads(line))

	rdd_columns, user_index_rdd, reverse_user_dict, business_index_rdd, reverse_business_dict = Building_Model().make_rdd(rdd_lines)

	if cf_type == 'user_based':
		rdd = sc_object.textFile(model_file).map(lambda line:json.loads(line))
		apni_model_rdd = user_based_helper.make_dict_model(rdd, user_index_rdd)
		modify_train = user_based_helper.modify_input_file(rdd_columns,business_index_rdd, user_index_rdd)

		us_avg = sc_object.textFile(user_avg_path).map(lambda r:json.loads(r)).map(lambda x:x)
		us_avg_rdd = us_avg.map(lambda x:dict(x)).flatMap(lambda x:x.items()).collectAsMap()

		test_rdd = sc_object.textFile(test_file).map(lambda row:json.loads(row))
		test_rdd_dict = user_based_helper.modify_test_file(test_rdd, user_index_rdd, business_index_rdd)

		o_pair = test_rdd_dict.leftOuterJoin(modify_train).mapValues(lambda x: user_based_helper.predict(tuple(x), apni_model_rdd, us_avg_rdd, reverse_user_dict))
		o_reqd = o_pair.map(lambda x:{"user_id": reverse_user_dict[x[1][0]], "business_id":reverse_business_dict[x[0]], "stars":x[1][1]})
		helper.write_file(o_reqd.collect(), output_file)
	else:

		rdd = sc_object.textFile(model_file).map(lambda line:json.loads(line))
		apni_model_rdd = item_based_helper.make_dict_model(rdd, business_index_rdd)
		modify_train = item_based_helper.modify_input_file(rdd_columns,business_index_rdd, user_index_rdd)
		
		us_avg = sc_object.textFile(business_avg_path).map(lambda r:json.loads(r))
		us_avg_rdd = us_avg.map(lambda x:dict(x)).flatMap(lambda x:x.items()).collectAsMap()

		test_rdd = sc_object.textFile(test_file).map(lambda row:json.loads(row))
		test_rdd_dict = item_based_helper.modify_test_file(test_rdd, user_index_rdd, business_index_rdd)
		
		o_pair = test_rdd_dict.leftOuterJoin(modify_train).mapValues(lambda x: item_based_helper.predict(tuple(x), apni_model_rdd, us_avg_rdd, reverse_business_dict))
		o_reqd = o_pair.map(lambda x:{"user_id": reverse_user_dict[x[0]], "business_id":reverse_business_dict[x[1][0]], "stars":x[1][1]})
		helper.write_file(o_reqd.collect(), output_file)

if __name__ == "__main__":
	main()

print("Duration: %s s" % (time.time() - start_time))