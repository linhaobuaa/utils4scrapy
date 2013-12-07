# -*- coding: utf-8 -*-
"""OAuth 1.0 for scrapy weibo"""

from weibopy.auth import OAuthHandler
import webbrowser


class OAuth4Scrapy(object):

    def __init__(self, consumer_key, consumer_secret):
        self.consumer_key, self.consumer_secret = consumer_key, consumer_secret

    def getAtt(self, key):
        try:
            return self.obj.__getattribute__(key)
        except Exception, e:
            print e
            return ''

    def getAttValue(self, obj, key):
        try:
            return obj.__getattribute__(key)
        except Exception, e:
            print e
            return ''
    
    def auth(self):
        """用于获取sina微博  access_token 和access_token_secret"""

        if len(self.consumer_key) == 0:
            print "Please set consumer_key"
            return
        
        if len(self.consumer_secret) == 0:
            print "Please set consumer_secret"
            return
        
        self.auth = OAuthHandler(self.consumer_key, self.consumer_secret)
        auth_url = self.auth.get_authorization_url()
        webbrowser.open(auth_url)
        print 'Please authorize: ' + auth_url
        verifier = raw_input('PIN: ').strip()
        access_token = self.auth.get_access_token(verifier)
        return access_token


    def setToken(self, token, tokenSecret):
        """通过oauth协议验证身份"""
        self.auth = OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.setToken(token, tokenSecret)
        
    def getAuthHeaders(self, url):
        return self.auth.get_headers(url)

    def getAuthUser(self):
    	return self.auth.get_username_uid()    