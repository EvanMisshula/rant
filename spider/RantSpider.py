import string
import re
import sqlalchemy
import os
import pandas as pd
import mysql.connector

import requests # This command allows us to fetch URLs
from lxml import html # This module will allow us to parse the returned HTML/XML
import pandas # To create a dataframe
from urllib import urlencode
import json
import sys
import unirest
from collections import Counter
import re


class RantSpider(object):

    def __init__(self)
        self.name = "badCop"
        self.start_urls = start_urls
        self.base_url = "http://theerant.com"
        self.doc = None

    def get_response(self):
        response = requests.get(self.start_urls)
        return response

    def get_parsed_doc(self):
        response = self.get_response()
        self.doc = html.fromstring(response.text)
        return self.doc

    def get_topic_nodes(self):
        self.topicNodes = self.doc.findall('.//td[@class="topic-titles"]')
        return self.topicNodes

    def close(self):
        print "killing this forumSpider"

    def get_headers(self):
        return self.headers

    def __del__(self):
        self.close
