import sys
import time
import pandas
import pickle
from pyspark.sql import SparkSession
from pyspark import SparkContext, StorageLevel, SparkConf
from sklearn import linear_model
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import PolynomialFeatures
import xgboost as xgb
import pandas as pd
import numpy as np
import xgboost as xgb
import csv
import time
import json

start_time = time.time()
train_file = ''
user_file = ''
model_file = ''
business_file = ''
user = ''
train = ''
business = ''
ana = ''
test_file = ''
output_file = ''
test=''

column_train = {"user_id": "user_id", "business_id": "business_id", "stars": "stars"}
column_test = {"user_id": "user_id", "business_id": "business_id"}
column_user = {"user_id": "user_id","useful": "user_useful","funny": "user_funny", "cool": "user_cool", "fans": "user_fans"}
column_business = {"business_id": "business_id","latitude": "business_latitude", "longitude": "business_longitude"}

def map_user(x):
    for r in x:
        o = json.loads(r)
        yield tuple((o['user_id'], (o['useful'], o['funny'], o['fans'])))

def map_business(x):

    for r in x:
        o = json.loads(r)
        yield tuple((o['business_id'], (o['latitude'], o['longitude'])))
def map_test(x):
    for r in x:
        o = json.loads(r)
        yield tuple((o['user_id'], o['business_id']))

class Building_Structure():

    def read_input(self):
        global train_file, user_file, business_file, model_file, test_file, output_file
        train_file = "../resource/asnlib/publicdata/train_review.json"
        user_file = '../resource/asnlib/publicdata/user.json'
        business_file = '../resource/asnlib/publicdata/business.json'
        model_file = "train_model.model"
        test_file = sys.argv[1]
        output_file = sys.argv[2]

    def modify_json(self, s, d):
        for i,j in d.items():
            s = s.withColumnRenamed(i,j)
        return s

    def modify_json_one(self, s):
        d = {"_1":"user_id", "_2":"business_id", "_3":"stars"}

        for i,j in d.items():
            s = s.withColumnRenamed(i,j)
        return s   
    
    def load_json(self, r):
        return json.loads(r)

    def read_data(self, sc):
        global user,train,business, test
        user = sc.textFile(user_file).mapPartitions(map_user)
        business = sc.textFile(business_file).mapPartitions(map_business)
        test = sc.textFile(test_file).mapPartitions(map_test)
        return test, user, business

class Features:

    def to_csv(self, r):
        v = r[1]

        try:
            return (r[0][0], r[0][1], v[0][1][1][0], v[0][1][1][1], v[1][1][1][0], v[1][1][1][1])
        except:
            return (r[0][0], r[0][1], 0, 0, 0, 0)


    def combine(self, test, user, business):

        u_join = test.keyBy(lambda p:p[0]).leftOuterJoin(user)
        b_join = test.keyBy(lambda p:p[1]).leftOuterJoin(business)
        entire = u_join.keyBy(lambda p: p[1][0]).join(b_join.keyBy(lambda p:p[1][0])).map(self.to_csv)

        return entire

    def write(self, o, headers, content):

        with open(o, 'w') as f:
            helper = csv.writer(f)
            helper.writerow(headers)
            for c in content:
                helper.writerow(c)

class Model:
    def predict_model(self, test):
        uids = test.uid.values
        bids = test.bid.values

        x = test.drop(['uid','bid'], axis=1)
        x = x.values
        model = pickle.load(open(model_file, 'rb'))
        output = model.predict(x)

        return output, uids, bids

    def write_desired_output(self, output, uids, bids):
        df = pd.DataFrame()
        df["user_id"] = uids
        df["business_id"] = bids
        df["p"] = output
        with open(output_file, "w") as f:
            for _, row in df.iterrows():
                f.write("{\"user_id\": \""+str(row.user_id)+ "\", " + "\"business_id\": \""+ str(row.business_id) +"\", "+  "\"stars\": " + f"{row.p}" +" "+"}\n")

def main():
    global stop_words_set
    building_structure = Building_Structure()
    helper_feature = Features()
    model_helper = Model()

    conf = SparkConf().setMaster("local").setAppName("hello").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")

    sc = SparkContext(conf=conf)

    sc.setLogLevel("ERROR")
   
    building_structure.read_input()

    test,user,business = building_structure.read_data(sc)
    entire_join = helper_feature.combine(test, user, business)

    helper_feature.write("test_data.csv", ('uid', 'bid', 'x0', 'x1', 'x2', 'x3'), entire_join.collect())

    test = pd.read_csv("test_data.csv")
    output, uids, bids = model_helper.predict_model(test)
    model_helper.write_desired_output(output, uids, bids)


if __name__ == "__main__":
    main()

print("Duration: %s s" % (time.time() - start_time))