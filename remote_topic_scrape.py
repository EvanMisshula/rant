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

db = sqlalchemy.create_engine(sql_params)
conn = db.connect()

mySpider = Rs.RantSpider("http://theerant.yuku.com/forums/58/THEE-RANT/THEE-RANT")
#response = mySpider.get_response()
#headers= response.headers
mySpider.doc = mySpider.get_parsed_doc()

print("retrieved webpage")
mySpider.topicNodes = mySpider.doc.findall('.//td[@class="topic-titles"]')
num_pageTopics = len(mySpider.topicNodes)

print("There are %d initial topic titles." % num_pageTopics)


mySpider.authorNodes = mySpider.doc.findall('.//td[@class="author"]')
num_pageAuthors = len(mySpider.authorNodes)

mySpider.repliesNodes = mySpider.doc.findall('.//td[@class="replies"]')
num_pageReplies = len(mySpider.repliesNodes)

mySpider.viewsNodes = mySpider.doc.findall('.//td[@class="views"]')
num_pageViews = len(mySpider.viewsNodes)

mySpider.latestNodes = mySpider.doc.findall('.//td[@class="latest lastcol"]')
num_pageLatest =len(mySpider.latestNodes)

mySpider.nextActiveNode = mySpider.doc.find('.//div[@class="next active"]')

topic_titles = []
topic_authors = []
topic_replies = []
topic_views = []
topic_lastPost_time = []
topic_head_link = []
fLink_predicate = "http://theerant.yuku.com"

topic_titles = [r.xpath('./a')[0].text for r in mySpider.topicNodes]
topic_head_link = [(fLink_predicate+r.xpath('./a')[0].get('href')) for r in mySpider.topicNodes]
topic_authors = [r.xpath('./p/a')[0].text for r in mySpider.authorNodes]
topic_replies = [int(r.text) for r in mySpider.repliesNodes]
topic_views = [int(r.text) for r in mySpider.viewsNodes]
topic_lastPost_time = [ r.xpath('./p[@class="date"]/text()')[0] for r in mySpider.latestNodes]
topic_lastPost_time = [ filter(lambda x: x in string.letters + string.digits + string.punctuation + ' ', myStr) for myStr in topic_lastPost_time]
topic_lastPost_time = [ ' '.join(r.split()) for r in topic_lastPost_time ]

d = { 'Topic' : pd.Series(topic_titles, index=range(len(topic_lastPost_time))), 'topicLastPostTime' : pd.Series(topic_lastPost_time, index=range(len(topic_lastPost_time))), 'topic_replies' : pd.Series(topic_replies, index=range(len(topic_lastPost_time))), 'views' : pd.Series(topic_views, index=range(len(topic_lastPost_time))), 'forum_link' : pd.Series(topic_head_link, index=range(len(topic_lastPost_time)))}
df1=pd.DataFrame(d)
f1=open("progress.log",'a')

print("There are %d rows in the dataframe scraped so far." % len(df1.index))
print("There are %d columns in the dataframe we scraped so far." % len(df1.columns))

f1.write("There are %d rows in the dataframe scraped so far." % len(df1.index))
f1.write("There are %d columns in the dataframe we scraped so far." % len(df1.columns))
df1.to_sql(name='topics',con=db,schema=None,if_exists='replace',index=True,index_label=None)
print("saved df1 with %d records on the %s machine" % (len(df1.index), platform.system()) )
f1.write("saved df1 with %d records on the %s machine" % (len(df1.index), platform.system()) )


# http://theerant.yuku.com/forums/58?page=1#.VYskrBNVikp
nextForumURL = "http://theerant.yuku.com" + mySpider.nextActiveNode.find('a').get('href')
noMoreForumPages = False
forum_page = 2

while (noMoreForumPages == False):
    print("Retrieving page: %d of the forum posts." % forum_page)
    time.sleep(random.randint(10,30))
    mySpider = Rs.RantSpider(nextForumURL)
#response = mySpider.get_response()
    mySpider.doc = mySpider.get_parsed_doc()

    print("retrieved webpage")
    f1.write("retrieved webpage")    
    mySpider.topicNodes = mySpider.doc.findall('.//td[@class="topic-titles"]')
    num_pageTopics = len(mySpider.topicNodes)

    print("There are %d topic titles." % num_pageTopics)
    f1.write("There are %d topic titles." % num_pageTopics)    

    mySpider.authorNodes = mySpider.doc.findall('.//td[@class="author"]')
    num_pageAuthors = len(mySpider.authorNodes)
    
    mySpider.repliesNodes = mySpider.doc.findall('.//td[@class="replies"]')
    num_pageReplies = len(mySpider.repliesNodes)

    mySpider.viewsNodes = mySpider.doc.findall('.//td[@class="views"]')
    num_pageViews = len(mySpider.viewsNodes)

    mySpider.latestNodes = mySpider.doc.findall('.//td[@class="latest lastcol"]')
    num_pageLatest =len(mySpider.latestNodes)

    mySpider.nextActiveNode = mySpider.doc.find('.//div[@class="next active"]')

    curr_topic_titles = [r.xpath('./a')[0].text for r in mySpider.topicNodes]
    topic_titles = topic_titles + curr_topic_titles

    curr_topic_head_links = [(fLink_predicate+r.xpath('./a')[0].get('href')) for r in mySpider.topicNodes]
    topic_head_link = topic_head_link + curr_topic_head_links

    curr_replies_count = [int(r.text) for r in mySpider.repliesNodes]
    topic_replies = topic_replies + curr_replies_count
    
    curr_topic_views = [int(r.text) for r in mySpider.viewsNodes]
    topic_views = topic_views + curr_topic_views
    
    curr_topic_lastPost_time = [ r.xpath('./p[@class="date"]/text()')[0] for r in mySpider.latestNodes]
    curr_topic_lastPost_time = [ filter(lambda x: x in string.letters + string.digits + string.punctuation + ' ', myStr) for myStr in curr_topic_lastPost_time]
    curr_topic_lastPost_time = [ ' '.join(r.split()) for r in curr_topic_lastPost_time ]
    topic_lastPost_time = topic_lastPost_time + curr_topic_lastPost_time
    forum_page = forum_page + 1
    nextForumURL_ant = mySpider.doc.xpath('.//div[@class="next active"]/a')[0].get('href')
    if nextForumURL_ant == '':
        noMoreForumPages = True
    else:
        nextForumURL = "http://theerant.yuku.com" + nextForumURL_ant
    d = { 'Topic' : pd.Series(topic_titles, index=range(len(topic_lastPost_time))), 'topicLastPostTime' : pd.Series(topic_lastPost_time, index=range(len(topic_lastPost_time))), 'topic_replies' : pd.Series(topic_replies, index=range(len(topic_lastPost_time))), 'views' : pd.Series(topic_views, index=range(len(topic_lastPost_time))), 'forum_link' : pd.Series(topic_head_link, index=range(len(topic_lastPost_time)))}
    df=pd.DataFrame(d)
    print("There are %d rows in the dataframe we are appending" % len(df.index))
    print("There are %d rows in the dataframe scraped so far." % len(df1.index))
    print("There are %d columns in the dataframe we are appending." % len(df.columns))
    print("There are %d columns in the dataframe we scraped so far." % len(df1.columns))

    f1.write("There are %d rows in the dataframe we are appending" % len(df.index))
    f1.write("There are %d rows in the dataframe scraped so far." % len(df1.index))
    f1.write("There are %d columns in the dataframe we are appending." % len(df.columns))
    f1.write("There are %d columns in the dataframe we scraped so far." % len(df1.columns))
    df1=pd.concat([df1,df])
    df1.to_sql(name='comments',con=db,schema=None,if_exists='replace',index=True,index_label=None)
    print("saved df1 with %d records." % len(df1.index))
    f1.write("saved df1 with %d records." % len(df1.index))    

print("Loop finished. Gathered %d pages of posts." % forum_page)
f1.write("Loop finished. Gathered %d pages of posts." % forum_page)

conn.close()
db.dispose()
