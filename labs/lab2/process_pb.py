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
for tweet in tweets.tweets:
    if tweet.is_delete and tweet.HasField('delete'):
        print "Delete ID: ", tweet.delete.id
    elif tweet.HasField('insert'):
        print "INSERT text:", tweet.insert.text