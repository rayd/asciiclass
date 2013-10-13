import json
lay = sc.textFile('/home/rayd/asciiclass/git/labs/lab6/lay-k.json')
json_lay = lay.map(lambda x: json.loads(x)).cache()
distinct_senders = json_lay.map(lambda x: x['sender']).distinct().cache()
sender_pairs = distinct_senders.flatMap(split).distinct().cache()
lastname_groups = sender_pairs.groupBy(lambda x: x[0][1]).cache()
email_equivalents = lastname_groups.filter(lambda x: len(x[1]) > 1).flatMap(compare_names).distinct().cache();

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
			elif (len(firstname) == 1 or len(potential_firstname) == 1):
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
			all_equivalents.append(tuple(this_equivalence_set))
	return all_equivalents

def transform_tuples(t):

