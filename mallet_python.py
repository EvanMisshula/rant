# set up logging so we see what's going on
import logging
import os
from gensim import corpora, models, utils
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
 
def iter_documents(reuters_dir):
    """Iterate over Reuters documents, yielding one document at a time."""
    for fname in os.listdir(reuters_dir):
        # read each document as one big string
        document = open(os.path.join(reuters_dir, fname)).read()
        # parse document into a list of utf8 tokens
        yield utils.simple_preprocess(document)


def iter_documents_pd(df3,titles):
    for docs in titles:
        posts_for_this_doc_list = df3['comments'][df3['document']==titles[idx:(idx+1)][0]].values.tolist()
        document = ' '.join(posts_for_this_doc_list)
        yield utils.simple_preprocess(document)

        
class ReutersCorpus(object):
    def __init__(self, reuters_dir):
        self.reuters_dir = reuters_dir
        self.dictionary = corpora.Dictionary(iter_documents(reuters_dir))
        self.dictionary.filter_extremes()  # remove stopwords etc
 
    def __iter__(self):
        for tokens in iter_documents(self.reuters_dir):
            yield self.dictionary.doc2bow(tokens)


class RantCorpus(object):
    def __init__(self,df3,titles):
        self.DataFrame = df3
        self.titles = titles
        self.dictionary = corpora.Dictionary(iter_documents_pd(df3,titles))
#        self.dictionary.filter_extremes(no_below=1, no_above=0.5, keep_n=100000)  # remove stopwords etc

    def __iter__(self):
        for tokens in iter_documents_pd(self.DataFrame, self.titles):
            yield self.dictionary.doc2bow(tokens)
        
        # set up the streamed corpus
corpus = ReutersCorpus('/Users/emisshula/Documents/insight/nltk_data/corpora/reuters/training/')

corpus = RantCorpus(df1,titles)
# train 10 LDA topics using MALLET
mallet_path = '/Users/emisshula/Documents/insight/mallet/bin/mallet'
model = models.wrappers.LdaMallet(mallet_path, corpus, num_topics=5, id2word=corpus.dictionary)

# now use the trained model to infer topics on a new document
doc = "Don't sell coffee, wheat nor sugar; trade gold, oil and gas instead."
bow = corpus.dictionary.doc2bow(utils.simple_preprocess(doc))
print model[bow]  # print list of (topic id, topic weight) pairs

