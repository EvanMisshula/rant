import string
import re
import sqlalchemy
import os
import pandas as pd
import mysql.connector
from spider import RantSpider as Rs
import time
import random
import platform

# create documents
import textmining
tdm = textmining.TermDocumentMatrix()
from stop_words import get_stop_words
from collections import Counter

# use LDA
import numpy as np
import lda

if platform.system() == 'Linux':
    os.chdir('/home/app/rant')
    f1 = open("/home/app/rant/db_settings.csv",'r')
else:
    os.chdir('/Users/emisshula/Documents/insight/lscrape')
    f1 = open("/Users/emisshula/Documents/insight/lscrape/db_settings.csv",'r')

sql_params = f1.read()
f1.close()

db = sqlalchemy.create_engine(sql_params)
conn = db.connect()


## Get the comment database  ##
df1=pd.read_sql_table(table_name='comments',con=db,schema=None)
df1 = df1[['document', 'authors', 'comments','post_time']]

stop_words = get_stop_words('english')

c=Counter(df1['document'].values.tolist())
for idx,val in enumerate(list(c)):
    posts_for_this_doc_list = df1['comments'][df1['document']==list(c)[idx:(idx+1)][0]].values.tolist()
    doc = ' '.join(posts_for_this_doc_list)
    filteredtext = [t.lower() for t in doc.split() if t.lower() not in stop_words ]
    doc = ' '.join(filteredtext)
    tdm.add_doc(doc)

matrix_loc = '/Users/emisshula/Documents/insight/lscrape/matrix.csv'
tdm.write_csv(matrix_loc,cutoff=2)

df3 = pd.read_csv(matrix_loc,delimiter=',')
X = pd.read_csv(matrix_loc,delimiter=',').values
vocab = df3.columns
titles = list(c)

X.shape

model = lda.LDA(n_topics=5, n_iter=1500, random_state=1)
model.fit(X) 


topic_word = model.topic_word_  # model.components_ also works
n_top_words = 8

for i, topic_dist in enumerate(topic_word):
        topic_words = np.array(vocab)[np.argsort(topic_dist)][:-n_top_words:-1]
        print('Topic {}: {}'.format(i, ' '.join(topic_words)))

print("\n**These are the 'document titles':")
for n, title in enumerate(titles):
    print("title {}: {}".format(n+1, title.encode('utf-8').strip())

    
post_author =  [r.xpath('.//a/text()')[0] for r in forumSpider.authorNodes]
post_comment = [r.xpath('.//div[@class="post-body"]/text ()')[0] in forumSpider.commentNodes]

df= pd.DataFrame([post_authors,post_comments,post_time],columns=headers)
                  =["authors","posts","timeStr"]

# <div class="next">Next&raquo;</div>

# <div class="next active"><a href="/topic/76652/Black-Female-Wants-To-Be-NYPD-PO-Daily-News?page=2" title="Next page [n]" accesskey="n">Next&raquo;</a></div>

d = {'one' : Series([1., 2., 3.], index=['a', 'b', 'c']),
   ....:      'two' : Series([1., 2., 3., 4.], index=['a', 'b', 'c', 'd'])}
conn.close()
db.dispose()
 
