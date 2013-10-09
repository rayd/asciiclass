import nltk
from nltk.tokenize import word_tokenize
import string
import re
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.protocol import PickleProtocol
import math
import os

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


class tfidf_emails(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol

    # A document will be considered all of an employees email text.
    # The collection will be all of the emails in the corpus.

    # used to combine values of the form { 'sender': name, 'count': num}
    # that are produced in the mapper_idf step
    def combine_sender_count_list(self, sender_count_list):
        sender_count_dict = {}
        for entry in sender_count_list:
            sender_key = entry['sender']
            if (sender_key in sender_count_dict):
                sender_count_dict[sender_key] += entry['count']
            else:
                sender_count_dict[sender_key] = entry['count']
        combined_list = []
        for key in sender_count_dict:
            combined_list.append({ 'sender': key, 'count': sender_count_dict[key] })
        return combined_list

    # First we'll compute tf per user by doing a word count across all of a user's emails.
    def mapper_count_words(self, key, email):
        words = preprocess_email(email['text'])
        # let's normalize the senders by dropping the domain and removing any
        # punctuation from the email.
        sender = re.sub(punct_regex, '', email['sender'].split('@')[0].lower())
        for w in words:
            yield (w,sender),1

    def combiner_sum_words(self, (word,sender), counts):
        yield (word,sender),sum(counts)

    def reducer_sum_words(self, (word, sender), counts):
        yield None,{ 'term': word, 'sender': sender, 'count': sum(counts) }

    def mapper_idf(self, _, sender_term_obj):
        # print "term:",sender_term_obj['term'],'sender:',sender_term_obj['sender']
        self.increment_counter('idf_mapper_stage', 'terms')
        yield sender_term_obj['term'],{ 'sender': sender_term_obj['sender'], 'count': sender_term_obj['count'] }

    def combiner_idf(self, term, senders):
        combined_list = self.combine_sender_count_list(senders)
        yield term,combined_list
        # print "yield",term,combined_list

    def reducer_idf_init(self):
        self.terms = {}
        self.total_senders = set()

    def reducer_idf(self, term, s_count_list):
        # s_count_list is a list of lists because this is the reducer
        senders = []
        for s_list in s_count_list:
            senders.extend(s_list)
        combined_sender_list = self.combine_sender_count_list(senders)
        self.terms[term] = combined_sender_list
        self.total_senders = self.total_senders.union(set(map(lambda x: x['sender'], combined_sender_list)))
        self.increment_counter('idf_reduce_stage', 'terms')

    def reducer_idf_final(self):
        for termkey in self.terms:
            sender_list = self.terms[termkey]
            # compute idf
            idf = math.log(len(self.total_senders)/len(sender_list))
            # print termkey,idf,sender_list
            self.increment_counter('idf_reduce_final_stage', 'terms')
            yield None,{ 'term': termkey, 'idf': idf, 'senders': sender_list }
        #print "total_senders:",len(self.total_senders)
        #print "sender_list:",self.total_senders

    def mapper_tfidf(self, _, term_obj):
        # transform into (term,sender),tf*idf
        for sender_count_obj in term_obj['senders']:
            sender = sender_count_obj['sender']
            count = sender_count_obj['count']
            # compute tf-idf by multiplying raw freq with term idf
            yield (term_obj['term'],sender),term_obj['idf']*count

    def reducer_tfidf(self, (term,sender), tfidf_vals):
        self.increment_counter('tf-idf_reduce_stage', 'tf-idf-values')
        yield None,{ 'term': term, 'sender': sender, 'tf-idf': tfidf_vals.next() }

    def steps(self):
        return [
            self.mr(mapper=self.mapper_count_words,
                combiner=self.combiner_sum_words,
                reducer=self.reducer_sum_words),
            self.mr(mapper=self.mapper_idf,
                combiner=self.combiner_idf,
                reducer_init=self.reducer_idf_init,
                reducer=self.reducer_idf,
                reducer_final=self.reducer_idf_final),
            self.mr(mapper=self.mapper_tfidf,
                reducer=self.reducer_tfidf)
        ]


if __name__ == '__main__':
    #print "starting job"
    tfidf_emails.run()
