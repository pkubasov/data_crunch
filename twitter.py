import tweepy
import json
from tweepy.api import API
import sys
import hashlib
import re
from urllib.parse import urlparse
from http import client
import requests

API_KEY = '***********    your-key-here    **************'
API_SECRET = '********    your-secret-here    ***********'
ACCESS_TOKEN = '******    your-access-token-here    *****'
ACCESS_TOKEN_SECRET = '** your-access-token-secret-here *'

MIN_STATUS_COUNT = 100
MAX_FRIENDS_COUNT = 200
MIN_FOLLOWERS_COUNT = 100
MAX_STATUS_COUNT = 250000
MAX_RECORDS_TO_PROCESS = 500000

auth = tweepy.OAuthHandler(API_KEY, API_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

mytrack_crypto = ['monero', 'bitcoin', '#crypto', 'ethereum', 'zk-snark', 'ringct', 'segwit', 'Lightning Network','dapps', 'casper', 'cryptography', 'cryptoeconomics', '#LN']
mytrack_new_tech = ['#keras', '#openNN', '#theano', '#deeplearning4j', '#tensorflow', 'supervised learning', 'clustering', '#NLP', 'confusion matrix', '#RMSE', 'chi square',
                    '#timeseries', '#montecarlo', 'Bayesian', 'regression' ,'stochastic', '#markovchains', 'markov', 'poisson', 'xmpp', 'mqtt', 'd2d', 'd2s', 's2s', 'ergodicity', 
                    '#datamining', '#dataanalytics', '#deeplearning', '#datascience', '#ML', '#neuralnetwork', '#biotech', '#3dprinting', '#futurism', '#ANN' , '#drone']
mylangs = ['en']
blacklisted_users = ['arttechbot', 'EmpoweredHR', 'GameUP247', 'AInieuwsNL', 'BotDotSleep', 'CryptoPatron', 'DrStrange_Bot', 'a2b_bot', 'metabolic_ba']
blacklisted_words = ['Udemy', 'RT', '@spheris_io', '#Motivation', '@SemanticEarth', 'trading']

FILENAME_LINKS = 'links.txt'
FILENAME_JSON = 'tweets.json'

non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)
non_bmp_map['\n'] = '\t'

texts = []
links = []
pattern = re.compile("https\:\/\/t.co\/[a-zA-Z0-9]+")


def process_or_store(tweet):
    with open(FILENAME_JSON, 'a') as f:
        f.write(json.dumps(tweet)+ "\n")

def unshorten_url(url):    
    parsed = urlparse(url)
    h = client.HTTPConnection(parsed.netloc)
    resource = parsed.path
    if parsed.query != "":
        resource += "?" + parsed.query
    h.request('HEAD', resource )
    response = h.getresponse()
    if response.status//100 == 3 and response.getheader('Location'):
        return unshorten_url(response.getheader('Location'))  # changed to process chains of short urls
    else:
        print("URL: %s" % (url))
        return url

def unshorten_url2(url):
    r = requests.head(url, allow_redirects = True)
    parsedObj = urlparse(r.url)
    parseUrl =  parsedObj.netloc + parsedObj.path
    #print(parseUrl)
    if r:
        return parseUrl
    else:
        return url
    

def check(status):
    hashed = hashlib.sha224(status.text.encode('UTF8')).hexdigest()
    if hashed in texts:
        #print("Ignoring identical tweet")
        return False
    texts.append(hashed)    
    
    if len(texts) > 10000 :
        del texts[:200]  # delete fifo

    # check  blocked users
    for u in blacklisted_users:
        if u in status.user.screen_name:
            return False   

    # check blacklisted words
    for w in blacklisted_words:
        if w in status.text:
            return False

    include =  status.user.followers_count > MIN_FOLLOWERS_COUNT and status.user.friends_count <MAX_FRIENDS_COUNT \
        and  MIN_STATUS_COUNT < status.user.statuses_count < MAX_STATUS_COUNT

    if not include:
        return False

    #match links
    matches = pattern.findall(status.text)    
    
    output_text = "[" + status.user.screen_name + "][" + status.text.translate(non_bmp_map) + "]"
    output_links = ''    

    for i in matches :
        i_s = unshorten_url2(i)
        if 'twitter.com' in i_s:
            continue
        if i_s in links:
            print("Saw this one already")
            return False
        else:
            links.append(i_s)
            output_text = output_text.replace(i,'')
            output_links+=("[http://" +  i_s +"]")           
            
    with open(FILENAME_LINKS, 'a') as f:       
        try:
            f.write(output_text + output_links + "\n")
        except BaseException as e:           
            print("Giving up on this...")
            return False    

    return True         
  

class MyStream(tweepy.StreamListener):
    def __init__(self, api=None):
        self.api = api  or API()
        self.n = 0
        self.m = MAX_RECORDS_TO_PROCESS

    def on_status(self, status):
        try:               
            if check(status):
                process_or_store(status._json)
                print ("\n\n[%s]: %s\n\n" % (status.user.screen_name, status.text.translate(non_bmp_map)))                                
                self.n = self.n+1
                
            if self.n < self.m: return True
            else:
                print ('tweets = '+str(self.n))
                return False
        except BaseException as e:            
            if check(status):                
                print ("\n\n[%s]: %s\n\n" % (status.user.screen_name,status.text.encode('UTF8').decode(sys.stdout.encoding)))
            return True

    def on_error(self,status):
        if check(status):
            print("Caught error")            
        return True
            
stream = tweepy.streaming.Stream(auth, MyStream())
stream.filter(track=mytrack_new_tech, languages=mylangs)
