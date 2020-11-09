import sys
import time
import json
import binascii
import random
import csv
import math
import string
import datetime
import tweepy
import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener

from itertools import combinations
import collections

start_time = time.time()
port = ''
output_file = ''
times_count = 0
number_tweets = 0
tweets = []

TOTAL_ALLOWED_WINDOW = 100
FREQ = 3

API_KEY = 'fvFhiMmX69DZWZELDc6AWqv5E'
API_SECRET_KEY = 'qYVmALOZrsBzJFms8uBAvx608HNMZaFG3ud9Ctub54dIqaJJ92'
ACCESS_TOKEN = '1323688688610504704-Us1hXTHv22bsSCm3m6TZ4IlZagrpoE'
ACCESS_SECRET = '2NfukbtBWStavsLDP4Diz0i90nIvLrpUHTaf6IkUneC84'

class Building_Structure():

	def read_input(self):
		global port, output_file
		port = sys.argv[1]
		output_file = sys.argv[2]

class Reservoir_Sampling(StreamListener):

	def on_error(self, status_code):
		if status_code == 420:
			return False

	def get_hashtags(self, status):
		h = status.entities.get("hashtags")
		return h

	def write(self, number_tweets, to):
		ou = open(output_file, 'a')
		out = ""
		out = "The number of tweets with tags from the beginning: " + str(number_tweets)+"\n"
		for i in to:
			out+=i[0]+" : "+str(i[1])+"\n"

		ou.write(out+"\n")
		ou.close()

	def on_status(self, status):
		global number_tweets, tweets
		d = {}
		total_hash = self.get_hashtags(status)

		if len(total_hash)>0:
			number_tweets+=1

			if number_tweets<TOTAL_ALLOWED_WINDOW:
				tweets.append(status)
			else:
				index = random.randint(0, number_tweets)
				if index<TOTAL_ALLOWED_WINDOW-1:
					tweets[index] = status

				for i in tweets:
					hash_tags = self.get_hashtags(i)
					for j in hash_tags:
						if j["text"].isalnum():
							if j["text"] not in d.keys():
								d[j["text"]] = 1
							else:
								d[j["text"]] +=1
				lexo = sorted(d.items(), key = lambda x: (-x[1], x[0]))
				
				freq_list = sorted(list(set(d.values())), reverse = True)

				freq_list_1 = freq_list[:3]

				top = []
				for i in lexo:
					if i[1] in freq_list_1:
						top.append((i[0],i[1]))
				self.write(number_tweets, top)

def main():
	global number_tweets, tweets
	building_structure = Building_Structure()

	building_structure.read_input()
	ou = open(output_file,'w')
	ou.close()
	print("Arguments Passed: ", port, output_file)
	auth = tweepy.OAuthHandler(consumer_key=API_KEY, consumer_secret=API_SECRET_KEY)
	auth.set_access_token(key=ACCESS_TOKEN, secret=ACCESS_SECRET)
	tweepy.API(auth)
	stream = Stream(auth=auth, listener=Reservoir_Sampling())
	stream.filter(track=["Biden","Harris", "President"])


if __name__ == "__main__":
	main()

print("Duration: %s s" % (time.time() - start_time))