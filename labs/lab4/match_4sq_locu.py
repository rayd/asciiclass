import json
import csv
import editdist
import codecs
import math

def clean_foursquare_phone(fsq_obj):
    phone = fsq_obj['phone']
    if (phone):
        phone = phone.replace('(', '')
        phone = phone.replace(')', '')
        phone = phone.replace('-', '')
        phone = phone.replace(' ', '')
        fsq_obj['phone'] = phone

def editdist_score(record1, record2, field):
    if(record1[field] == None):
        record1[field] = ''
    if(record2[field] == None):
        record2[field] = ''
    val1 = record1[field].encode('utf8').lower()
    val2 = record2[field].encode('utf8').lower()
    if (len(val1) == 0):
        return 1
    return editdist.distance(val1, val2)/float(len(val1))

def jaccard_distance(record1, record2, field, ngram_size):
    if(record1[field] == None):
        record1[field] = ''
    if(record2[field] == None):
        record2[field] = ''
    val1 = record1[field].encode('utf8').lower()
    val2 = record2[field].encode('utf8').lower()
    set1 = set(ngram(val1, ngram_size))
    set2 = set(ngram(val2, ngram_size))
    return 1 - (float(len(set1.intersection(set2))) / float(len(set1.union(set2))))

def ngram(str,size):
    if(len(str) <= size):
        return [str]
    res = []
    for x in xrange(0,len(str)-size+1):
        res.append(str[x:x+size])
    return res

# calculated avg dist: 0.0879627073485
def location_distance(record1, record2):
    lat1 = record1['latitude']
    lat2 = record2['latitude']
    lng1 = record1['longitude']
    lng2 = record2['longitude']
    if(lat1 == None or lat2 == None or lng1 == None or lng2 == None):
        return 0.0879627073485
    d = abs(math.sqrt(math.pow(lat2-lat1,2) + math.pow(lng2-lng1,2)))
    return d

foursquare_training = json.load(codecs.getreader('latin_1')(open('foursquare_train_hard.json','rU')))
locu_training = json.load(codecs.getreader('latin_1')(open('locu_train_hard.json','rU')))
truth_training = csv.DictReader(open("matches_train_hard.csv","rU"))
locu_truth = {}

for entry in truth_training:
    locu_truth[entry['locu_id']] = entry['foursquare_id']

for fsq_record in foursquare_training:
    clean_foursquare_phone(fsq_record)

true_pos = 0
false_pos = 0

for locu_record in locu_training:
    best_score = 1
    best_rec = []
    for fsq_record in foursquare_training:
        if ((locu_record['postal_code'] == fsq_record['postal_code']) or locu_record['postal_code'] == '' or fsq_record['postal_code'] == ''):
            # weights and scores
            s1 = .3, editdist_score(locu_record,fsq_record,'name')
            s2 = .1, editdist_score(locu_record,fsq_record,'street_address')
            s3 = .25, editdist_score(locu_record,fsq_record,'phone')
            s4 = .1, jaccard_distance(locu_record,fsq_record,'name', 4)
            s5 = .05, jaccard_distance(locu_record,fsq_record,'street_address', 4)
            s6 = .2, location_distance(locu_record,fsq_record)
            s = s1,s2,s3,s4,s5,s6

            weighted_avg_score = sum(map(lambda x: x[0] * x[1], s))/float(len(s))
            if (weighted_avg_score < best_score):
                best_score = weighted_avg_score
                best_rec = fsq_record
    if (best_score < .07):
        # we'll consider this a match, so check if it's right
        locu_key = locu_record['id']
        if (locu_key in locu_truth and locu_truth[locu_key] == best_rec['id']):
            true_pos += 1
        else:
            false_pos += 1

prec = true_pos / float(true_pos + false_pos)
recall = true_pos / float(len(locu_truth))
fmeas = (2.0 * prec * recall) / (prec + recall)
print "training -- precision: ", prec, " recall: ", recall, " f1-measure: ", fmeas, "true positives: ", true_pos, "false postitives: ", false_pos

# Run on test data

foursquare_test = json.load(codecs.getreader('latin_1')(open('foursquare_test_hard.json','rU')))
locu_test = json.load(codecs.getreader('latin_1')(open('locu_test_hard.json','rU')))
results = []

for locu_record in locu_test:
    best_score = 1
    best_rec = []
    for fsq_record in foursquare_test:
        if ((locu_record['postal_code'] == fsq_record['postal_code']) or locu_record['postal_code'] == '' or fsq_record['postal_code'] == ''):
            # weights and scores
            s1 = .3, editdist_score(locu_record,fsq_record,'name')
            s2 = .1, editdist_score(locu_record,fsq_record,'street_address')
            s3 = .25, editdist_score(locu_record,fsq_record,'phone')
            s4 = .1, jaccard_distance(locu_record,fsq_record,'name', 4)
            s5 = .05, jaccard_distance(locu_record,fsq_record,'street_address', 4)
            s6 = .2, location_distance(locu_record,fsq_record)
            s = s1,s2,s3,s4,s5,s6

            weighted_avg_score = sum(map(lambda x: x[0] * x[1], s))/float(len(s))
            if (weighted_avg_score < best_score):
                best_score = weighted_avg_score
                best_rec = fsq_record
    if (best_score < .07):
        results.append({'locu_id': locu_record['id'], 'foursquare_id': best_rec['id']})

dw = csv.DictWriter(open('matches_test.csv','w'), ['locu_id', 'foursquare_id'])
dw.writeheader()
for row in results:
    dw.writerow(row)

