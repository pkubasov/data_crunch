import tweepy
import json
from tweepy.api import API
import sys
import hashlib
import re

API_KEY = '***********    your-key-here    **************'
API_SECRET = '********    your-secret-here    ***********'
ACCESS_TOKEN = '******    your-access-token-here    *****'
ACCESS_TOKEN_SECRET = '** your-access-token-secret-here *'

MIN_STATUS_COUNT = 100
MAX_FRIENDS_COUNT = 200
MIN_FOLLOWERS_COUNT = 100
MAX_STATUS_COUNT = 250000

auth = tweepy.OAuthHandler(API_KEY, API_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

mytrack_crypto = ['monero', 'bitcoin', '#crypto', 'ethereum', 'zk-snark', 'ringct', 'segwit', 'Lightning Network','dapps', 'casper', 'cryptography', 'cryptoeconomics', '#LN']
mytrack_new_tech = ['#keras', '#openNN', '#theano', '#deeplearning4j', '#tensorflow', 'supervised learning', 'clustering', '#NLP', 'confusion matrix', '#RMSE', 'chi square',
                    '#timeseries', '#montecarlo', 'Bayesian', 'regression' ,'stochastic', '#markovchains', 'markov', 'poisson', 'xmpp', 'mqtt', 'd2d', 'd2s', 's2s', '#AVR', '#AI' ,
                    '#datamining', '#dataanalytics', '#deeplearning', '#datascience', '#ML', '#neuralnetwork', '#biotech', '#3dprinting', '#futurism', '#VR', '#ANN' , '#drone']
mylangs = ['en']
blacklisted_users = ['arttechbot', 'EmpoweredHR', 'GameUP247', 'AInieuwsNL', 'BotDotSleep']
blacklisted_words = ['Udemy', 'RT', '@spheris_io', '#Motivation']

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
    #print("Found %d match(es) " % (len(matches)))
    
    output_text = "[" + status.user.screen_name + "][" + status.text.translate(non_bmp_map) + "]"
    output_links = ''    

    for i in matches :
        if i in links:
            print("Saw this one already")
            return False
        else:
            links.append(i)
            output_text = output_text.replace(i,'')
            output_links+=("[" +  i +"]")
            #print("Added link:  %s" % (i))
            
    with open('links3.txt', 'a') as f:       
        try:
            f.write(output_text + output_links + "\n")
        except BaseException as e:
            #f.write("[" + status.user.screen_name + "][ " + status.text.encode('UTF8').decode(sys.stdout.encoding).replace(i,'') +"][" +  i +"]\n")
            print("Giving up on this...")
            return False    

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
