# -*- coding: utf-8 -*-

from scrapy.exceptions import CloseSpider
from scrapy import log
from tk_alive import TkAlive, TkAliveV1
from req_count import ReqCount, ReqCountV1, ReqCountIp
from not_exist_users import NotExistUser
from OAuth4Scrapy import OAuth4Scrapy
import redis
import pymongo
import simplejson as json
import urllib2
import urllib3
import time
import socket
import sys

LIMIT_URL = 'https://api.weibo.com/2/account/rate_limit_status.json?access_token={access_token}'
LIMIT_URL_v1 = 'http://api.t.sina.com.cn/account/rate_limit_status.json?source={source_api_key}'
EXPIRED_TOKEN = 21327
INVALID_ACCESS_TOKEN = 21332
REACH_IP_LIMIT = 10022
REACH_PER_TOKEN_LIMIT = 10023
REACH_PER_TOKEN_LIMIT_1 = 10024
REACH_PER_APP_PER_USER_LIMIT = 40310
INVALID_SIGNATURE = 40107
USER_DOES_NOT_EXIST = 40023
TOKEN_REJECTED = 40113
MAX_TOKENS_IN_REDIS = 16
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
MONGOD_HOST = 'localhost'
MONGOD_PORT = 27017
USEDB = 'mongo4scrapy'

# 假设OAuthToken 1.0永不过期，假定为2020.1.1
EXPIRES_IN = 1577808000

# prod
'''
PER_TOKEN_HOURS_LIMIT = 1000
AT_LEAST_TOKEN_COUNT = 6
API_KEY = '4131380600'
'''

# dev
PER_TOKEN_HOURS_LIMIT = 2000
AT_LEAST_TOKEN_COUNT = 1
API_KEY = '3105114937'
API_SECRET = '985e8f106a5db148d1a96abfabcd9043'


def _default_mongo(host=MONGOD_HOST, port=MONGOD_PORT, usedb=USEDB):
    # 强制写journal，并强制safe
    connection = pymongo.MongoClient(host=host, port=port, j=True, w=1)
    db = connection.admin
    db.authenticate('root', 'root')
    db = getattr(connection, usedb)
    return db


def _default_redis(host=REDIS_HOST, port=REDIS_PORT):
    return redis.Redis(host, port)


def _default_req_count(r, api_key=API_KEY):
    return ReqCount(r, api_key)


def _v1_req_count(r, api_key=API_KEY, api_secret=API_SECRET):
    return ReqCountV1(r, api_key, api_secret)


def _ip_req_count(r):
    return ReqCountIp(r)


def _default_not_exist_users(r, spider):
    return NotExistUser(r, spider)


def _default_tk_alive(r, api_key=API_KEY):
    return TkAlive(r, api_key)


def _v1_tk_alive(r, api_key=API_KEY, api_secret=API_SECRET):
    return TkAliveV1(r, api_key, api_secret)


def api_key_status(api_key, api_secret, token_key, token_secret):
    retry = 0
    while 1:
        retry += 1
        if retry > 3:
            raise 'UNKNOWN CHECK FAILURE'
        
        try:
            log.msg("[Api Key] api_key: {api_key}, token_key: {token_key}, sleep one second, wait to check".format(api_key=api_key, token_key=token_key), level=log.INFO)
            time.sleep(1)
            log.msg("[Api Key] now check", level=log.INFO)

            oauth4Scrapy = OAuth4Scrapy(api_key, api_secret)
            oauth4Scrapy.setToken(token_key, token_secret)
            check_url = LIMIT_URL_v1.format(source_api_key=api_key)
            headers = oauth4Scrapy.getAuthHeaders(check_url)
            req = urllib2.Request(check_url, headers=headers)
            try:
                resp = urllib2.urlopen(req)
            except:
                return 'UNKNOWN CHECK FAILURE'
            resp = json.loads(resp.read())
            if 'error' in resp:
                if resp['error'] == u'40107:signature_invalid!':
                    log.msg("[Api Key] invalid signature check", level=log.INFO)
                    return INVALID_SIGNATURE
                if resp['error'] == u'40113:token_rejected!':
                    log.msg("[Api Key] invalid signature check", level=log.INFO)
                    return TOKEN_REJECTED
                else:
                    return 'UNKNOWN CHECK FAILURE'
            reset_time_in = resp['reset_time_in_seconds']
            remaining = resp['remaining_hits']
            return reset_time_in, remaining
        except socket.gaierror:
            pass


def token_status(token):
    retry = 0
    while 1:
        retry += 1
        if retry > 3:
            raise CloseSpider('CHECK LIMIT STATUS FAIL')

        try:
            log.msg("[Token Status] token: {token}, sleep one second, wait to check".format(token=token), level=log.INFO)
            time.sleep(1)
            log.msg("[Token Status] now check", level=log.INFO)

            http = urllib3.PoolManager(timeout=10)
            resp = http.request('GET', LIMIT_URL.format(access_token=token))
            resp = json.loads(resp.data)

            if 'error' in resp:
                if resp['error'] == 'expired_token':
                    return EXPIRED_TOKEN
                elif resp['error'] == 'invalid_access_token':
                    return INVALID_ACCESS_TOKEN
                else:
                    raise CloseSpider('UNKNOWN TOKEN STATUS ERROR')

            reset_time_in = resp['reset_time_in_seconds']
            remaining = resp['remaining_user_hits']
            return reset_time_in, remaining
        except (socket.gaierror, urllib3.exceptions.TimeoutError):
            pass


def one_valid_token(req_count, tk_alive):
    while 1:
        token, used = req_count.one_token()
        if not token:
            return None
        elif not tk_alive.isalive(token):
            req_count.delete(token)
            tk_alive.drop_tk(token)
            continue

        return token, used


def one_ip_request_count(req_ip_count, ip_addr):
    ip_addr, used = req_ip_count.one_ip_request(ip_addr)
    return ip_addr, used


def reset_ip_req_count(req_ip_count):
    ips_in_redis = req_ip_count.all_ips()

    for ip in ips_in_redis:
        req_ip_count.reset(ip)


def record_not_exist_user(user_not_exist, user):
    user, score = user_not_exist.incre(user)
    return user, score


def calibration(req_count, tk_alive, per_token_hours_limit):
    log.msg('[Token Maintain] begin calibration', level=log.INFO)
    tokens_in_redis = req_count.all_tokens()
    for token in tokens_in_redis:
        tk_status = token_status(token)
        if tk_status in [EXPIRED_TOKEN, INVALID_ACCESS_TOKEN]:
            req_count.delete(token)
            tk_alive.drop_tk(token)
            continue

        _, remaining = tk_status
        req_count.set(token, per_token_hours_limit - remaining)

    log.msg('[Token Maintain] end calibration', level=log.INFO)


def calibration_v1(api_key, api_secret, req_count, tk_alive, per_token_hours_limit, logbk=None):
    if logbk:
        logbk.info('[Token Maintain] begin calibration')

    tokens_in_redis = req_count.all_tokens()
    if logbk:
        logbk.info('Tokens in redis count: %s' % len(tokens_in_redis))

    for token in tokens_in_redis:
        token_key, token_secret = token.split('_')
        tk_status = api_key_status(api_key, api_secret, token_key, token_secret)
        if tk_status in [INVALID_SIGNATURE, TOKEN_REJECTED, 'UNKNOWN CHECK FAILURE']:
            req_count.delete(token)
            tk_alive.drop_tk(token)
            continue

        _, remaining = tk_status
        req_count.set(token_key, token_secret, per_token_hours_limit - remaining)      

    if logbk:
        logbk.info('[Token Maintain] end calibration')


def maintain(at_least=1, hourly=False, logbk=None):
    r = _default_redis()
    mongo = _default_mongo()
    req_count = _default_req_count(r)
    tk_alive = _default_tk_alive(r)

    log.msg('[Token Maintain] begin maintain', level=log.INFO)

    # 从导入所有未过期的token，并初始使用次数为0，相应的alive为True
    for user in mongo.users.find():
        if user['expires_in'] > time.time():
            req_count.set(user['access_token'], 0)
            tk_alive.hset(user['access_token'], user['expires_in'])

    tokens_in_redis = req_count.all_tokens()
    print 'before alive:', len(tokens_in_redis)
    if logbk:
        logbk.info('before alive: %s' % len(tokens_in_redis))  # 清理之前

    alive_count = 0
    for token in tokens_in_redis:
        if tk_alive.isalive(token, hourly=hourly):
            alive_count += 1
        else:
            req_count.delete(token)
            tk_alive.drop_tk(token)

    tokens_in_redis = req_count.all_tokens()
    print 'after alive:', len(tokens_in_redis)
    if logbk:
        logbk.info('after alive: %s' % len(tokens_in_redis))  # 清理之后

    if alive_count < at_least:
        raise CloseSpider('TOKENS COUNT NOT REACH AT_LEAST')

    log.msg('[Token Maintain] end maintain', level=log.INFO)


def add_without_reset_req_count(max_tokens_redis_limit, logbk=None):
    r = _default_redis()
    mongo = _default_mongo()
    req_count = _v1_req_count(r)
    tk_alive = _v1_tk_alive(r)

    tokens_in_redis = req_count.all_tokens()
    if logbk:
        logbk.info('before alive: %s' % len(tokens_in_redis))

    if len(tokens_in_redis) >= max_tokens_redis_limit:
        if logbk:
            logbk.info('Token in Redis reach max size: %s, stop maintain' % len(tokens_in_redis))
        sys.exit('Tokens in Redis reach max size')
    
    tokens_count = len(tokens_in_redis)
    for user in mongo.users.find():
        token = user['token_key'] + '_' + user['token_secret']
        if user['expires_in'] > time.time() and token not in tokens_in_redis:
            if tokens_count >= max_tokens_redis_limit:
                if logbk:
                    logbk.info('Token in Redis reach max size: %s, stop maintain' % tokens_count)
                sys.exit('Token in Redis reach max size')
            req_count.set(user['token_key'], user['token_secret'], 0)
            tk_alive.hset(user['token_key'], user['token_secret'], user['expires_in'])
            tokens_count += 1

    tokens_in_redis = req_count.all_tokens()
    print 'after alive:', len(tokens_in_redis)


def maintain_v1(max_tokens_redis_limit, at_least=1, hourly=False, logbk=None, startoffset=0, endoffset=14):
    r = _default_redis()
    mongo = _default_mongo()
    req_count = _v1_req_count(r)
    tk_alive = _v1_tk_alive(r)

    tokens_in_redis = req_count.all_tokens()
    print 'before alive:', len(tokens_in_redis)

    for token in tokens_in_redis:
        req_count.delete(token)    

    # 从导入所有未过期的token，并初始使用次数为0，相应的alive为True
    user_count = 0
    for user in mongo.users.find():
        if user_count >= startoffset and user_count <= endoffset:
            if user['expires_in'] > time.time():
                req_count.set(user['token_key'], user['token_secret'], 0)
                tk_alive.hset(user['token_key'], user['token_secret'], user['expires_in'])

        user_count += 1

    tokens_in_redis = req_count.all_tokens()
    print 'before alive:', len(tokens_in_redis)
    if logbk:
        logbk.info('before alive: %s' % len(tokens_in_redis))  # 清理之前

    alive_count = 0
    for token in tokens_in_redis:
        if tk_alive.isalive(token, hourly=hourly):
            alive_count += 1
        else:
            req_count.delete(token)
            tk_alive.drop_tk(token)

    tokens_in_redis = req_count.all_tokens()
    print 'after alive:', len(tokens_in_redis)
    if logbk:
        logbk.info('after alive: %s' % len(tokens_in_redis))  # 清理之后

    if alive_count < at_least:
        raise CloseSpider('TOKENS COUNT NOT REACH AT_LEAST')


def generate_api_access_token(logbk):
    mongo = _default_mongo()
    if logbk:
        logbk.info('before mongo tokens count: %s' % mongo.users.find().count())

    oauth4Scrapy = OAuth4Scrapy(API_KEY, API_SECRET)
    access_token = oauth4Scrapy.auth()
    oauth4Scrapy.setToken(str(access_token.key), str(access_token.secret))
    screen_name, uid = oauth4Scrapy.getAuthUser()
    if logbk:
        logbk.info('%s auth api key %s success' % (screen_name, API_KEY))

    mongo.users.update({'uid': uid},
                       {'$set': {'uid': uid, 'api_key': API_KEY, 'api_secret': API_SECRET,
                       'expires_in': EXPIRES_IN, 'screen_name': screen_name,
                       'token_key': str(access_token.key), 'token_secret': str(access_token.secret), 
                       }}, upsert=True, safe=True)

    if logbk:
        logbk.info('after mongo tokens count: %s' % mongo.users.find().count())
    

def calibration_by_hand():
    print 'calibration start'
    r = _default_redis()
    req_count = _v1_req_count(r)
    tk_alive = _v1_tk_alive(r)

    tokens_in_redis = req_count.all_tokens()
    print 'tokens count in redis: ', len(tokens_in_redis)

    calibration_v1(API_KEY, API_SECRET, req_count, tk_alive, PER_TOKEN_HOURS_LIMIT)
    print 'calibration end'


if __name__ == '__main__':
    '''Usage: python tk_maintain.py --log hehe.log
    '''

    from logbook import FileHandler
    from logbook import Logger
    from argparse import ArgumentParser
    import sys
    parser = ArgumentParser()
    logpath = './log/'
    parser.add_argument('--log', nargs=1, help='log path')
    parser.add_argument('--version', nargs=1, help='maintain version')
    args = parser.parse_args(sys.argv[1:])
    logfilepath = logpath + args.log[0]
    maintain_version = args.version[0]
    log_handler = FileHandler(logfilepath)
    logbk = Logger('Token Maintain')

    with log_handler.applicationbound():
        logbk.info('maintain prepare')

        at_least = AT_LEAST_TOKEN_COUNT
        max_tokens_redis_limit = MAX_TOKENS_IN_REDIS

        logbk.info('maintain begin')

        # 认证新用户，并将access_token加入mongodb，redis从mongodb导入新token，不重置已有token 的 req_count
        if maintain_version == 'addatoken':
            print 'generate new token, write to mongo, push to redis without reset request count'
            generate_api_access_token(logbk)
            add_without_reset_req_count(max_tokens_redis_limit, logbk)

        # 将mongodb中所有access_token加入redis，并重置已有token 的 req_count
        if maintain_version == 'addalltoken':
            print 'push all tokens from mongo to redis and reset request count'
            maintain_v1(max_tokens_redis_limit, at_least=at_least, hourly=True, logbk=logbk)

        # 手动重新校准redis中所有token的实际req_count，可考虑改为每小时初时的自动任务
        if maintain_version == 'calibration':
            print 'calibration by hand'
            logbk.info('calibration by hand')
            calibration_by_hand()

        if maintain_version == 'fifteentokens':
            print 'push 15 tokens from mongo to redis and reset request count'
            maintain_v1(max_tokens_redis_limit, at_least=1, hourly=False, logbk=None, startoffset=45, endoffset=89)

        logbk.info('maintain end')