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

if platform.system() == 'Linux':
    os.chdir('/home/app/rant')
    f1 = open("/home/app/rant/db_settings.csv",'r')
else:
    os.chdir('/Users/emisshula/Documents/insight/lscrape')
    f1 = open("/Users/emisshula/Documents/insight/lscrape/db_settings.csv",'r')

sql_params = f1.read()
f1.close()

baseForumURL = 'http://theerant.yuku.com'

db = sqlalchemy.create_engine(sql_params)
conn = db.connect()
    #some simple data operations
df1=pd.read_sql_table(table_name='topics',con=db,schema=None)
df1 = df1[['Topic', 'forum_link', 'topicLastPostTime','topic_replies', 'views']]

topic_head_link =df1['forum_link'].values.tolist()
topic_titles = df1['Topic'].values.tolist()

for topic_idx,topic_val in enumerate(topic_head_link):
    print("topic_idx: %d" % topic_idx)
    next_url_list = [topic_head_link[topic_idx]]
    print("This is the topic title: %s" % topic_titles[topic_idx].encode('utf-8').strip())
    pages_on_this_topic = 0
    while next_url_list != []:
        print("The url to get is:\n %s" % next_url_list[0])
        forumSpider = Rs.RantSpider(next_url_list[0])
#        response = forumSpider.get_response()
        forumSpider.doc = forumSpider.get_parsed_doc()
        post_authors =  [unicode(r.xpath('.//a/text()')[0]) for r in forumSpider.doc.findall('.//td[@class="th firstcol poster-name"]')]
        post_time = [ unicode(t.get('datetime')) for t in forumSpider.doc.xpath('.//time[@class="timeago"]')]
        if len(post_time) != len(post_authors):
            post_time=[unicode(r.text) for r in forumSpider.doc.xpath('.//span[@class="date"]')]
        post_comments = [ unicode(re.sub('[\\n\\t\']','',s.text_content())) for s in forumSpider.doc.xpath('.//div[starts-with(@class,"post-body")]/div[@class="scrolling"]/div')]
        doc_title = [ topic_titles[topic_idx] ] * len(post_comments)
        print("Collected: %d pages of data on this topic." % (pages_on_this_topic+1))
        # get post date
        next_url_list = forumSpider.doc.xpath('.//div[@class="next active"]/a')
        if next_url_list != []:
            next_url_list[0] = baseForumURL + next_url_list[0].get('href')
            print("There is another active page on this topic")
        else:
            print("changing topics")

        print("len(post_authors): %d", len(post_authors))
        print("len(post_comments): %d", len(post_comments))
        print("len(post_time): %d", len(post_time))
        print("len(doc_title): %d", len(doc_title))                        

        min_comm = min(len(post_time), len(post_authors), len(post_comments), len(doc_title))
        max_comm = max(len(post_time), len(post_authors), len(post_comments), len(doc_title))
        
        if min_comm != max_comm:
            print("min(com): %d", min_comm)
            auth_series = pd.Series(post_authors[:min_comm], index=range(min_comm))
            comment_series = pd.Series(post_comments[:min_comm], index=range(min_comm))
            time_series = pd.Series(post_time[:min_comm], index=range(min_comm))
            doc_series = pd.Series(doc_title[:min_comm], index=range(min_comm))
        else:
            auth_series = pd.Series(post_authors, index=range(len(post_time)))
            comment_series = pd.Series(post_comments, index=range(len(post_time)))
            time_series = pd.Series(post_time, index=range(len(post_time)))
            doc_series = pd.Series(doc_title, index=range(len(doc_title)))

        
        print("len(auth_series):", len(auth_series))
        print("len(comment_series):", len(comment_series))
        print("len(time_series):", len(time_series))
        print("len(doc_series):", len(doc_series))

        
        d = { 'authors' : auth_series, 'comments' : comment_series, 'post_time' : time_series, 'document' : doc_series}
        df=pd.DataFrame(d)
        print("There are %d rows in the dataframe we are appending" % len(df.index))
        print("There are %d columns in the dataframe we are appending." % len(df.columns))

        if topic_idx==0 or pages_on_this_topic==0:
            print('This is the first page of comments')
            df.to_sql(name='comments',con=db,schema=None,if_exists='replace',index=True,index_label=None)
            df1=df
            del df
        else:
            print("There are %d rows in the dataframe scraped so far." % len(df1.index))
            print("There are %d columns in the dataframe we scraped so far." % len(df1.columns))
            df1=pd.concat([df1,df])
            df1.to_sql(name='comments',con=db,schema=None,if_exists='replace',index=True,index_label=None)
            del df
        print("saved df1 with %d records." % len(df1.index))
        pages_on_this_topic = pages_on_this_topic + 1
        del forumSpider
            
conn.close()
db.dispose()

