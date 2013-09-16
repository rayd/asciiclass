# Ray Di Ciaccio
# Python code for processing protobuf twitter data

import sys
import twitter_pb2

if len(sys.argv) != 2:
  print "Usage:", sys.argv[0], "TWITTER_DATA_FILE"
  sys.exit(-1)

# Create container class for tweets
tweets = twitter_pb2.Tweets()

# Read from file and load into protobuf-generated class
f = open(sys.argv[1], "rb")
tweets.ParseFromString(f.read())
f.close()

# Go through, check for fields and print accordingly (for debug)
deletes = 0
tweet_id_set = set()
reply_tos = 0
uid_dict = {}
places_dict = {}

for tweet in tweets.tweets:
    if tweet.is_delete:
        deletes += 1
    elif tweet.HasField('insert'):
        tweet_id_set.add(tweet.insert.id)
        if tweet.insert.uid in uid_dict:
            uid_dict[tweet.insert.uid] += 1
        else:
            uid_dict[tweet.insert.uid] = 1
        if tweet.insert.HasField('place'):
            if tweet.insert.place.id in places_dict:
                places_dict[tweet.insert.place.id] = (places_dict[tweet.insert.place.id][0] + 1, tweet.insert.place.name)
            else:
                places_dict[tweet.insert.place.id] = (1, tweet.insert.place.name)

# check for reply_tos that are in our set of tweets
for tweet in tweets.tweets:
    if tweet.HasField('insert') and tweet.insert.HasField('reply_to') and tweet.insert.reply_to in tweet_id_set:
        reply_tos += 1

# find highest five entries in uid_dict
five_highest_uids = sorted(uid_dict.items(), key=lambda x: x[1], reverse=True)[0:5]

# find highest five places in places_dict
five_highest_places = sorted(places_dict.items(), key=lambda x: x[1], reverse=True)[0:5]



print "deletes", deletes
print "reply_tos", reply_tos
print "five_highest_uids", five_highest_uids
print "five_highest_places", five_highest_places
