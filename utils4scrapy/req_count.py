# -*- coding: utf-8 -*-

REQ_COUNT_HASH = "{api_key}:tokens"
REQ_COUNT_HASH_v1 = "{api_key}_{api_secret}:v1_tokens"
REQ_COUNT_HASH_IP = "ip_address_v1_reqcount"
ACCESS_TOKEN_KEY_SECRET_v1 = "{token_key}_{token_secret}"
INITIALI_VALUE = 0


class ReqCount(object):
    def __init__(self, server, api_key):
        self.server = server
        self.key = REQ_COUNT_HASH.format(api_key=api_key)

    def one_token(self):
        member = self.server.zrange(self.key, 0, 0)
        if member == []:
            return None, None

        pipe = self.server.pipeline()
        pipe.multi()
        pipe.zincrby(self.key, member[0], 1).zscore(self.key, member[0])
        _, score = pipe.execute()
        return member[0], score

    def set(self, token, count):
        self.server.zadd(self.key, token, count)

    def all_tokens(self):
        return self.server.zrange(self.key, 0, -1)

    def delete(self, token):
        self.server.zrem(self.key, token)

    def notexisted(self, token):
        return self.server.zrank(self.key, token) is None


class ReqCountV1(object):
    def __init__(self, server, api_key, api_secret):
        self.server = server
        self.key = REQ_COUNT_HASH_v1.format(api_key=api_key, api_secret=api_secret)

    def one_token(self):
        member = self.server.zrange(self.key, 0, 0)
        if member == []:
            return None, None

        pipe = self.server.pipeline()
        pipe.multi()
        pipe.zincrby(self.key, member[0], 1).zscore(self.key, member[0])
        _, score = pipe.execute()
        return member[0], score

    def set(self, token_key, token_secret, count):
        self.server.zadd(self.key, ACCESS_TOKEN_KEY_SECRET_v1.format(token_key=token_key, token_secret=token_secret), count)

    def all_tokens(self):
        return self.server.zrange(self.key, 0, -1)

    def delete(self, token):
        self.server.zrem(self.key, token)

    def notexisted(self, token):
        return self.server.zrank(self.key, token) is None


class ReqCountIp(object):
    def __init__(self, server):
        self.server = server
        self.key = REQ_COUNT_HASH_IP

    def one_ip_request(self, field):
        value = self.incr(field)
        return field, value

    def all_ips(self):
        ip_count_pairs = self.server.hgetall(self.key)
        return ip_count_pairs

    def incr(self, field, increment=1):
        value = self.server.hincrby(self.key, field, increment)
        return int(value)

    def decr(self, field, decrement=1):
        value = self.server.hincrby(self.key, field, 0-decrement)
        return int(value)

    def set(self, field, value):
        self.server.hset(self.key, field, value)

    def get(self, field):
        value = self.server.hget(self.key, field)
        return INITIALI_VALUE if value is None else int(value)

    def reset(self, field):
        self.set(field, INITIALI_VALUE)