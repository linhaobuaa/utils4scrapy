# -*- coding: utf-8 -*-
import time

TK_ALIVE_HASH = "{api_key}:tokensalive"
TK_ALIVE_HASH_v1 = "{api_key}_{api_secret}:v1_tokensalive"
ACCESS_TOKEN_KEY_SECRET_v1 = "{token_key}_{token_secret}"


class TkAlive(object):
    def __init__(self, server, api_key):
        self.server = server
        self.key = TK_ALIVE_HASH.format(api_key=api_key)

    def hset(self, token, expired_in):
        self.server.hset(self.key, token, expired_in)

    def isalive(self, token, hourly=False):
        if hourly:
            return float(self.server.hget(self.key, token)) > time.time() + 3600
        else:
            return float(self.server.hget(self.key, token)) > time.time()

    def drop_tk(self, token):
        if self.isalive(token):
            self.server.hset(self.key, token, time.time())


class TkAliveV1(object):
    def __init__(self, server, api_key, api_secret):
        self.server = server
        self.key = TK_ALIVE_HASH_v1.format(api_key=api_key, api_secret=api_secret)

    def hset(self, token_key, token_secret, expired_in):
        self.server.hset(self.key, ACCESS_TOKEN_KEY_SECRET_v1.format(token_key=token_key, token_secret=token_secret), expired_in)

    def isalive(self, token, hourly=False):
        if hourly:
            return float(self.server.hget(self.key, token)) > time.time() + 3600
        else:
            return float(self.server.hget(self.key, token)) > time.time()

    def drop_tk(self, token):
        if self.isalive(token):
            self.server.hset(self.key, token, time.time())