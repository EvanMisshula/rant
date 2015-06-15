from spider.rant_scrape import RantSpider
import string

mySpider = RantSpider("http://theerant.yuku.com/forums/58/THEE-RANT/THEE-RANT")
response = mySpider.get_response()
mySpider.doc = mySpider.get_parsed_doc()

print("retrieved webpage")
mySpider.topicNodes = mySpider.doc.findall('.//td[@class="topic-titles"]')
num_pageTopics = len(mySpider.topicNodes)

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

# links and titles
for idx, val in enumerate(mySpider.topicNodes):
    curr_topic_node = mySpider.topicNodes[idx]
    curr_topic_title = curr_topic_node.find('a').text
    curr_topic_link = curr_topic_node.find('a').get('href')
    curr_topic_fLink = "http://theerant.com" + curr_topic_link
    topic_titles.append(curr_topic_title)
    topic_head_link.append(curr_topic_fLink)
    # authors
    curr_author_node = mySpider.authorNodes[idx]
    original_post_author = curr_author_node.find('p/a').text
    topic_authors.append(original_post_author)
    # replies    
    curr_replies_node = mySpider.repliesNodes[idx]
    curr_replies_count = int(curr_replies_node.text)
    topic_replies.append(curr_replies_count)
    # views
    curr_views_node = mySpider.viewsNodes[idx]
    curr_views_count = int(curr_views_node.text)
    topic_views.append(curr_views_count)
    # latest post
    curr_latest_node = mySpider.latestNodes[idx]
    myStr = curr_latest_node.xpath('./p[@class="date"]/text()')[0]
    curr_latest_date_str = filter(lambda x: x in string.letters + string.digits + string.punctuation + ' ', myStr)
    curr_latest_date_str = ' '.join(curr_latest_date_str.split())
    topic_lastPost_time.append(curr_latest_date_str)

    
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

nextForumURL = "http://theerant.com" + mySpider.nextActiveNode.find('a').get('href')

# http://theerant.yuku.com/topic/76233/Ruger-LCP-SafetyRecall-Notice
