import tweepy
import json
from tweepy.api import API
import sys
import hashlib
import re

API_KEY = 'V31UU6UBlI9zUVdwDjHWB0jZQ'
API_SECRET = 'KT5641GBHwkIRt3LvDroaBIoiDMSqMoauGoloa0QQRue0SuIJP'
ACCESS_TOKEN = '475198489-Hc523DQlVRzPFnLr9vkLn9nKSNd3OPSlcOm9YDzM'
ACCESS_TOKEN_SECRET = 'ZOWpsOEzS6H5XDL4cBm5HHjnsw7iB6HlpYh79npIdwnpv'

MIN_STATUS_COUNT = 100
MAX_FRIENDS_COUNT = 200
MIN_FOLLOWERS_COUNT = 100
MAX_STATUS_COUNT = 250000

auth = tweepy.OAuthHandler(API_KEY, API_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

mytrack_crypto = ['monero', 'bitcoin', '#crypto', 'ethereum', 'zk-snark', 'ringct', 'segwit', 'Lightning Network','dapps', 'casper', 'cryptography', 'cryptoeconomics', '#LN']
mytrack_new_tech = ['#IoT', '#AVR', '#AI' , '#data_mining', '#datamining', '#datascience', '#ML', 'neural', '#biotech', '#3dprinting', '#futurism', '#digitalrevolution', '#bigdata', 'VR', '#ANN' , '#drone']
mylangs = ['en','ru']

non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)
non_bmp_map['\n'] = '\t'

texts = []
links = []
pattern = re.compile("https\:\/\/t.co\/[a-zA-Z0-9]+")


def process_or_store(tweet):
    print(json.dumps(tweet))

def check(status):
    hashed = hashlib.sha224(status.text.encode('UTF8')).hexdigest()
    if hashed in texts:
        return False
    texts.append(hashed)    
    
    if len(texts) > 10000 :
        del texts[:200]  # delete fifo

    include =  (not status.text.startswith('RT')) and ('@spheris_io' not in status.text) and ('#Motivation' not in status.text) \
        and status.user.followers_count > MIN_FOLLOWERS_COUNT and status.user.friends_count <MAX_FRIENDS_COUNT \
        and  MIN_STATUS_COUNT < status.user.statuses_count < MAX_STATUS_COUNT

    if not include:
        return False

     #match links
    matches = pattern.findall(status.text)
    #print("Found %d match(es) " % (len(matches)))
        
    for i in matches :
        if i in links:
            print("Saw this one already")
            return False
        else:
            links.append(i)
            #print("Added link:  %s" % (i))
            with open('links3.txt', 'a') as f:
                try:
                    f.write("[" + status.user.screen_name + "][ " + status.text.translate(non_bmp_map).replace(i,'') +"][" +  i +"]\n")
                except UnicodeEncodeError:
                     f.write("[" + status.user.screen_name + "][ " + status.text.encode('UTF8').decode(sys.stdout.encoding).replace(i,'') +"][" +  i +"]\n")
    return True         
  

class Stream2Screen(tweepy.StreamListener):
    def __init__(self, api=None):
        self.api = api  or API()
        self.n = 0
        self.m = 200000

    def on_status(self, status):
        try:
            with open('tweets.json', 'a') as f:
                f.write(status.text.translate(non_bmp_map))
                
            if check(status):
                #print('\n\nUser id: %d ScreenName: %s Followers: %d  Follows: %d Statuses: %d '  % (status.user.id, status.user.screen_name, status.user.followers_count , status.user.friends_count, status.user.statuses_count))
                print ("\n\n[%s]: %s\n\n" % (status.user.screen_name, status.text.translate(non_bmp_map)))                                
                self.n = self.n+1
                
            if self.n < self.m: return True
            else:
                print ('tweets = '+str(self.n))
                return False
        except BaseException as e:
            #print("Error on_status: %s" % str(e))
            if check(status):                
                print ("\n\n[%s]: %s\n\n" % (status.user.screen_name,status.text.encode('UTF8').decode(sys.stdout.encoding)))
            return True

    def on_error(self,status):
        if check(status):
            print("Caught error")
            #print(status.text.encode('UTF8').decode(sys.stdout.encoding))
        return True
            
stream = tweepy.streaming.Stream(auth, Stream2Screen())
stream.filter(track=mytrack_new_tech, languages=mylangs)
