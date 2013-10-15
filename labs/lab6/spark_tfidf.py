from pyspark import SparkContext
import json
import string
import re

sc = SparkContext("local[4]", "rayd-tfidf-enron", pyFiles=["nltk.zip"])
# sc = SparkContext("<MASTER URI>", "rayd-tfidf-enron", pyFiles=["nltk.zip"])
enron = sc.textFile('/home/rayd/asciiclass/git/labs/lab6/lay-k.json')
# enron = sc.textFile('s3n://AKIAJFDTPC4XX2LVETGA:<AWS_PRIVATE_KEY>@6885public/enron/*.json')

import nltk
from nltk.tokenize import word_tokenize

# Define a bunch of helper functions

# split an email address into a (first_name, last_name) pair
# based on some common email patterns
# we send out a list because email addresses could be
# first_name->last_name or last_name->first_name
def split(email):
    name=email.split('@')[0]
    # split firstname lastname by . or _
    names=name.split('.')
    if(len(names) == 1):
        names=name.split('_')
    if(len(names) == 1):
        # try some common email patterns:
        # <first_initial><last_name>
        # <last_name><first_initial>
        fname1 = name[0:1]
        lname1 = name[1:len(name)]
        fname2 = name[len(name)-1:len(name)]
        lname2 = name[0:len(name)-1]
        return [((fname1,lname1),email),((fname2,lname2),email)]
    else:
        # common email pattern:
        # <first_name>(._)<last_name>
        # <last_name>(._)<first_name>
        fname=names[0]
        lname=names[len(names)-1]
    return [((fname,lname),email),((lname,fname),email)]

def jaccard_distance(string1, string2, ngram_size):
    set1 = set(ngram(string1, ngram_size))
    set2 = set(ngram(string2, ngram_size))
    return 1 - (float(len(set1.intersection(set2))) / float(len(set1.union(set2))))

def ngram(str,size):
    if(len(str) <= size):
        return [str]
    res = []
    for x in xrange(0,len(str)-size+1):
        res.append(str[x:x+size])
    return res

def compare_names(name_group):
    nameslist = name_group[1]
    all_equivalents = []
    # compare each name pair with the other pairs in its group
    for namepair in nameslist:
        firstname = namepair[0][0]
        email = namepair[1]
        this_namepair_equivalents = None
        for equivalence_set in all_equivalents:
            if email in equivalence_set:
                this_namepair_equivalents = list(equivalence_set)
                all_equivalents.remove(equivalence_set)
        if (this_namepair_equivalents == None):
            this_namepair_equivalents = []
            this_namepair_equivalents.append(email)
        for potential_match in nameslist:
            potential_firstname = potential_match[0][0]
            potential_email = potential_match[1]
            # print firstname,"->",potential_firstname
            if (firstname == potential_firstname):
                # if same firstname, then equal
                this_namepair_equivalents.append(potential_email)
            elif ((len(firstname) > 0 and len(potential_firstname) > 0) and (len(firstname) == 1 or len(potential_firstname) == 1)):
                # if one of the firstnames is an initial and it matches another firstnames first intial, assume match
                if (firstname[0] == potential_firstname[0]):
                    this_namepair_equivalents.append(potential_email)
            else:
                # check similarity, if similar enough, we'll consider them the same
                dist = jaccard_distance(firstname, potential_firstname, min(len(firstname), len(potential_firstname), 3))
                # print "jaccard_distance [",firstname,"->",potential_firstname,"]",dist
                if (dist <= .75):
                    this_namepair_equivalents.append(potential_email)
        # only add this to the list of equivalence sets if there's something else in it
        this_equivalence_set = set(this_namepair_equivalents)
        if(len(this_equivalence_set) > 1):
            all_equivalents.append(tuple(sorted(this_equivalence_set)))
    return all_equivalents

def transform_tuples(t):
    canonical_email = t[0]
    return map(lambda x: (x, canonical_email), t[1:len(t)])

def lookup_canonical_sender(email):
    senders_dict = broadcast_equivalents_dict.value
    if (email in senders_dict):
        return senders_dict[email]
    else:
        return email

stops = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should', 'now']
punct_regex = re.compile('[%s]' % re.escape(string.punctuation))
porter = nltk.PorterStemmer()

def filter_punctuation_contractions_stops(x):
    return ((re.match(punct_regex, x) == None) and (x not in ['\'s', 'n\'t', '\'ve']) and (x not in stops))

# do some preprocess for the text of each email
def preprocess_email(txt):
    words = []
    s_tokens = word_tokenize(txt)
    # remove punctuation-only and contraction tokens and stop words
    s_words = filter(filter_punctuation_contractions_stops, map(string.lower, s_tokens))
    words.extend([ porter.stem(w) for w in s_words ])
    return words

def extract_term_sender_pairs(sender_txt_pair):
    sender = lookup_canonical_sender(sender_txt_pair['sender'])
    text = sender_txt_pair['text']
    return map(lambda x: { 'sender': sender, 'term': x }, preprocess_email(text))

def reduce_sender_count(d, obj):
    sender = obj['sender']
    if(sender in d):
        d[sender] += 1
    else:
        d[sender] = 1
    return d

# Start real spark processing

# ER code
json_enron = enron.map(lambda x: json.loads(x)).cache()
distinct_senders = json_enron.map(lambda x: x['sender']).distinct().cache()
sender_pairs = distinct_senders.flatMap(split).distinct()
lastname_groups = sender_pairs.groupBy(lambda x: x[0][1])
email_equivalents_dict = lastname_groups.filter(lambda x: len(x[1]) > 1).flatMap(compare_names).distinct().flatMap(transform_tuples).collectAsMap()
# broadcast email equivalents map to all nodes so they can properly map senders
broadcast_equivalents_dict = sc.broadcast(email_equivalents_dict)

# Now start TF-IDF related calculations
# compute total number of documents (i.e. number of senders since each sender's corpus of email is being considered one document)
total_documents = distinct_senders.map(lambda x: lookup_canonical_sender(x)).distinct().count()
distinct_senders.unpersist()
# broadcast total documents number so nodes can calculate per-term idf
broadcast_total_documents = sc.broadcast(total_documents)
# count how many senders have used each term
terms_grouped_rdd = json_enron.flatMap(extract_term_sender_pairs).groupBy(lambda x: x['term']).cache()
json_enron.unpersist()
# compute per-term idf values by dividing total document count by number of documents with term (which is x[1])
per_term_idf_rdd = terms_grouped_rdd.map(lambda x: (x[0], len(set(map(lambda y: y['sender'], x[1]))))).map(lambda x: (x[0], broadcast_total_documents.value / float(x[1]))).cache()
# count the term-frequency for each sender
per_term_sender_freq_rdd = terms_grouped_rdd.flatMap(lambda x: map(lambda y: (x[0],y), reduce(reduce_sender_count, x[1], {}).items())).cache()
# join on the term and compute tfidf
# here x[0] is the term, x[1] = ((sender, tf), idf)
per_term_sender_freq_rdd.join(per_term_idf_rdd).map(lambda x: { 'term': x[0], 'sender': x[1][0][0], 'tfidf': x[1][0][1] * x[1][1]}).saveAsTextFile('spark_output')
# per_term_sender_freq_rdd.join(per_term_idf_rdd).map(lambda x: { 'term': x[0], 'sender': x[1][0][0], 'tfidf': x[1][0][1] * x[1][1]}).saveAsTextFile('s3n://AKIAJFDTPC4XX2LVETGA:<AWS_PRIVATE_KEY>@6885public/rayd/spark_output')
