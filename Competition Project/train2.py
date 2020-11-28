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
# column_train = tuple((obj['user_id'], obj['business_id'], float(obj['stars'])))
# column_user = tuple((obj['user_id'], (obj['useful'], obj['funny'], obj['fans'])))
# column_business = tuple((obj['business_id'], (obj['latitude'], obj['longitude'])))
def map_user(x):
    for r in x:
        o = json.loads(r)
        yield tuple((o['user_id'], (o['useful'], o['funny'], o['fans'])))

def map_business(x):

    for r in x:
        o = json.loads(r)
        yield tuple((o['business_id'], (o['latitude'], o['longitude'])))
def map_train(x):
    for r in x:
        o = json.loads(r)
        yield tuple((o['user_id'], o['business_id'], float(o['stars'])))

class Building_Structure():

    def read_input(self):
        global train_file, user_file, business_file, model_file
        train_file = "../resource/asnlib/publicdata/train_review.json"
        user_file = '../resource/asnlib/publicdata/user.json'
        business_file = '../resource/asnlib/publicdata/business.json'
        model_file = "train_model.model"

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
        global user,train,business
        user = sc.textFile(user_file).mapPartitions(map_user)
        business = sc.textFile(business_file).mapPartitions(map_business)
        train = sc.textFile(train_file).mapPartitions(map_train)
        return train, user, business

class Features:

    def to_csv(self, r):
        v = r[1]
        return (r[0][0], r[0][1], v[0][1][1][0], v[0][1][1][1], v[1][1][1][0], v[1][1][1][1], r[0][2])

    def combine(self, train, user, business):

        u_join = train.keyBy(lambda p:p[0]).leftOuterJoin(user)
        b_join = train.keyBy(lambda p:p[1]).leftOuterJoin(business)
        entire = u_join.keyBy(lambda p: p[1][0]).join(b_join.keyBy(lambda p:p[1][0])).map(self.to_csv)

        return entire

    def write(self, o, headers, content):

        with open(o, 'w') as f:
            helper = csv.writer(f)
            helper.writerow(headers)
            for c in content:
                helper.writerow(c)

class Model:
    def build_model(self, train):
        y = train.y.values
        x = train.drop(['uid','bid','y'], axis=1)
        x = x.values
        model = xgb.XGBRegressor(n_estimators=1000, subsample=0.85, colsample_bytree=0.9, learning_rate=0.01)
        model.fit(x,y)
        return model

def main():
    global stop_words_set
    building_structure = Building_Structure()
    helper_feature = Features()
    model_helper = Model()

    conf = SparkConf().setMaster("local").setAppName("hello").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")

    sc = SparkContext(conf=conf)

    sc.setLogLevel("ERROR")
   
    building_structure.read_input()

    train,user,business = building_structure.read_data(sc)
    entire_join = helper_feature.combine(train, user, business)

    helper_feature.write("data.csv", ('uid', 'bid', 'x0', 'x1', 'x2', 'x3', 'y'), entire_join.collect())

    data = pd.read_csv("data.csv")
    model = model_helper.build_model(data)

    pickle.dump(model, open(model_file, 'wb'))

if __name__ == "__main__":
    main()

print("Duration: %s s" % (time.time() - start_time))
