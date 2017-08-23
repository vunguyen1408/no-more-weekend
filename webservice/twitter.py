import json

#import urllib3
try:
    from urllib.request import urlretrieve  # Python 3
    from urllib.error import HTTPError,ContentTooShortError
except ImportError:
    from urllib import urlretrieve  # Python 2


import sys

twitter_api = "https://api.twitter.com/1/statuses/user_timeline.json?screen_name=%s&count=5"
#endpoint = urllib2.urlopen(twitter_api % sys.argv[1])
endpoint = urlopen(twitter_api % sys.argv[1])
tweets = json.loads(endpoint.read())

#urllib2.install_opener(urllib2.build_opener(urllib2.ProxyHandler({'https':'myproxy:myproxyport'})))


for tweet in tweets:
  print ("%s\n%s\n") % (tweet["created_at"], tweet["text"])
