# -*- coding: utf-8 -*-

NOT_EXIST_USERS_HASH = '{spider}:not_exist_users'


class NotExistUser(object):
    def __init__(self, server, spider):
    	self.server = server
        self.key = NOT_EXIST_USERS_HASH.format(spider=spider)

    def incre(self, user, increment=1):
    	pipe = self.server.pipeline()
        pipe.multi()
    	if not pipe.zscore(self.key, user).execute():
    	    pipe.zadd(self.key, user, 1).zscore(self.key, user)
        else:
	    pipe.zincrby(self.key, user, increment).zscore(self.key, user)
	_, score = pipe.execute()
        return user, score

    def set(self, user, count):
    	self.server.zadd(self.key, user, count)

    def all_users(self):
    	return self.server.zrange(self.key, 0, -1)

    def delete(self, user):
    	self.server.zrem(self.key, user)
