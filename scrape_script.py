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
    #some simple data operations
mySpider = Rs.RantSpider("http://theerant.yuku.com/forums/58/THEE-RANT/THEE-RANT", headers = [])
response = mySpider.get_response()
headers= response.headers
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


# http://theerant.yuku.com/topic/76233/Ruger-LCP-SafetyRecall-Notice#.VX7MAedJZTN
# links and titles
#for idx, val in enumerate(mySpider.topicNodes):
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

print("There are %d rows in the dataframe scraped so far." % len(df1.index))
print("There are %d columns in the dataframe we scraped so far." % len(df1.columns))
df1.to_sql(name='topics',con=db,schema=None,if_exists='replace',index=True,index_label=None)
print("saved df1 with %d records." % len(df1.index))


# http://theerant.yuku.com/forums/58?page=1#.VYskrBNVikp
nextForumURL = "http://theerant.yuku.com" + mySpider.nextActiveNode.find('a').get('href')
noMoreForumPages = False
forum_page = 2

while (noMoreForumPages == False):
    print("Retrieving page: %d of the forum posts." % forum_page)
    time.sleep(random.randint(10,30))
    mySpider = Rs.RantSpider(nextForumURL, headers = [])
#response = mySpider.get_response()
    mySpider.doc = mySpider.get_parsed_doc()

    print("retrieved webpage")
    mySpider.topicNodes = mySpider.doc.findall('.//td[@class="topic-titles"]')
    num_pageTopics = len(mySpider.topicNodes)

    print("There are %d topic titles." % num_pageTopics)

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
    d = { 'Topic' : pd.Series(topic_titles, index=range(len(topic_lastPost_time))), 'topicLastPostTime' : pd.Series(topic_lastPost_time, index=range(len(topic_lastPost_time))), 'topic_replies' : pd.Series(topic_replies, index=range(len(topic_lastPost_time))), 'views' : pd.Series(topic_views, index=range(len(doc_title))), 'forum_link' : pd.Series(topic_head_link, index=range(len(doc_title)))}
    df=pd.DataFrame(d)
    print("There are %d rows in the dataframe we are appending" % len(df.index))
    print("There are %d rows in the dataframe scraped so far." % len(df1.index))
    print("There are %d columns in the dataframe we are appending." % len(df.columns))
    print("There are %d columns in the dataframe we scraped so far." % len(df1.columns))
    df1=pd.concat([df1,df])
    df1.to_sql(name='topics',con=db,schema=None,if_exists='replace',index=True,index_label=None)
    print("saved df1 with %d records." % len(df1.index))

print("Loop finished. Gathered %d pages of posts." % forum_page)

# for idx, val in enumerate(mySpider.topicNodes):
#     print("Retrieving the posts for the %d topic of %d known topics." % (idx, num_pageTopics))
#     curr_topic_node = mySpider.topicNodes[idx]
#     curr_topic_title = curr_topic_node.find('a').text
#     curr_topic_link = curr_topic_node.find('a').get('href')
#     curr_topic_fLink = "http://theerant.yuku.com" + curr_topic_link
#     topic_titles.append(curr_topic_title)
#     topic_head_link.append(curr_topic_fLink)
#     # authors
#     curr_author_node = mySpider.authorNodes[idx]
#     original_post_author = curr_author_node.find('p/a').text
#     topic_authors.append(original_post_author)
#     # replies    
#     curr_replies_node = mySpider.repliesNodes[idx]
#     curr_replies_count = int(curr_replies_node.text)
#     topic_replies.append(curr_replies_count)
#     # views
#     curr_views_node = mySpider.viewsNodes[idx]
#     curr_views_count = int(curr_views_node.text)
#     topic_views.append(curr_views_count)
#     # latest post
#     curr_latest_node = mySpider.latestNodes[idx]
#     myStr = curr_latest_node.xpath('./p[@class="date"]/text()')[0]
#     curr_latest_date_str = filter(lambda x: x in string.letters + string.digits + string.punctuation + ' ', myStr)
#     curr_latest_date_str = ' '.join(curr_latest_date_str.split())
#     topic_lastPost_time.append(curr_latest_date_str)

    
    
print "The topics are: "
 print topic_titles
print "The original authors were: "
print topic_authors
print "The number of replies are: "
print topic_replies
print "The number of views were: "
print topic_views
print "The latest post on this topic was: "
print topic_lastPost_time

nextForumURL = "http://theerant.yuku.com" + mySpider.nextActiveNode.find('a').get('href')

# http://theerant.yuku.com/topic/76233/Ruger-LCP-SafetyRecall-Notice
# scrape a topic
# for idx, val in enumerate(topic_head_link):

baseForumURL = 'http://theerant.yuku.com'

page_idx = 0
page_val = None

index = ['authors', 'comments', 'post_time','document']
df1 = pd.DataFrame(columns=index)


for topic_idx,topic_val in enumerate(topic_head_link):
    print("topic_idx: %d" % topic_idx)
    next_url_list = [topic_head_link[topic_idx]]
    print("this is the topic title: %s" % topic_titles[topic_idx])
    pages_on_this_topic = 0
    while next_url_list != []:
        print("The url to get is: %s" % next_url_list[0])
        forumSpider = RantSpider(next_url_list[0])
         response = forumSpider.get_response()
        forumSpider.doc = forumSpider.get_parsed_doc()
        post_authors =  [unicode(r.xpath('.//a/text()')[0]) for r in forumSpider.doc.findall('.//td[@class="th firstcol poster-name"]')]
        post_time = [ unicode(t.get('datetime')) for t in forumSpider.doc.xpath('.//time[@class="timeago"]')]
        if len(post_time) != len(post_authors):
            post_time=[unicode(r.text) for r in forumSpider.doc.xpath('.//span[@class="date"]')]
        post_comments = [ unicode(re.sub('[\\n\\t\']','',s.text_content())) for s in forumSpider.doc.xpath('.//div[starts-with(@class,"post-body")]/div[@class="scrolling"]/div')]
        doc_title = [ topic_titles[topic_idx] ] * len(post_comments)
        print("collected: %d pages of data on this topic." % (pages_on_this_topic+1))
        # get post date
        next_url_list = forumSpider.doc.xpath('.//div[@class="next active"]/a')
        if next_url_list != []:
            next_url_list[0] = baseForumURL + next_url_list[0].get('href')
            print("There is another active page on this topic")
        else:
            print("changing topics")
        d = { 'authors' : pd.Series(post_authors, index=range(len(post_time))), 'comments' : pd.Series(post_comments, index=range(len(post_time))), 'post_time' : pd.Series(post_time, index=range(len(post_time))), 'document' : pd.Series(doc_title, index=range(len(doc_title)))}
        df=pd.DataFrame(d)
        print("There are %d rows in the dataframe we are appending" % len(df.index))
        print("There are %d rows in the dataframe scraped so far." % len(df1.index))
        print("There are %d columns in the dataframe we are appending." % len(df.columns))
        print("There are %d columns in the dataframe we scraped so far." % len(df1.columns))
        if topic_idx==0 or pages_on_this_topic==0:
            df.to_sql(name='comments',con=db,schema=None,if_exists='replace',index=True,index_label=None)
            df1=df
        elif:
            df1=pd.concat([df1,df])
            df1.to_sql(name='comments',con=db,schema=None,if_exists='replace',index=True,index_label=None)
        print("saved df1 with %d records." % len(df1.index))
        pages_on_this_topic = pages_on_this_topic + 1
 
            

# create documents
import textmining
tdm = textmining.TermDocumentMatrix()
from stop_words import get_stop_words


stop_words = get_stop_words('english')

c=Counter(df1['document'].values.tolist())
for idx,val in enumerate(list(c)):
    posts_for_this_doc_list = df1['comments'][df1['document']==list(c)[idx:(idx+1)][0]].values.tolist()
    doc = ' '.join(posts_for_this_doc_list)
    filteredtext = [t.lower() for t in doc.split() if t.lower() not in stop_words ]
    doc = ' '.join(filteredtext)
    tdm.add_doc(doc)

tdm.write_csv('/Users/emisshula/Documents/insight/lscrape/matrix.csv',cutoff=2)

topic_word = model.topic_word_



for i, topic_dist in enumerate(topic_word):
        topic_words = np.array(vocab)[np.argsort(topic_dist)][:-n_top_words:-1]
        print('Topic {}: {}'.format(i, ' '.join(topic_words)))

print("\n**These are the 'document titles':")
for n, title in enumerate(titles):
    print("title {}: {}".format(n+1, title))

    
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
 


