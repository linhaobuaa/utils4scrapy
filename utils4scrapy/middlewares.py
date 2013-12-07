# -*- coding: utf-8 -*-

from scrapy import log
from scrapy.exceptions import CloseSpider
from tk_maintain import token_status, one_valid_token, calibration, \
    _default_redis, _default_req_count, _default_tk_alive, record_not_exist_user, \
    EXPIRED_TOKEN, INVALID_ACCESS_TOKEN, REACH_IP_LIMIT, _ip_req_count, \
    REACH_PER_TOKEN_LIMIT, REACH_PER_TOKEN_LIMIT_1, USER_DOES_NOT_EXIST, \
    REACH_PER_APP_PER_USER_LIMIT, api_key_status, one_ip_request_count, \
    calibration_v1, _v1_req_count, _v1_tk_alive, _default_not_exist_users
from raven.handlers.logging import SentryHandler
from raven import Client
from raven.conf import setup_logging
from scrapy.utils.reqser import request_to_dict
from OAuth4Scrapy import OAuth4Scrapy
from utils import localIp
import urlparse
import cPickle
import logging
import simplejson as json
import time
import sys


BUFFER_SIZE = 100
RESET_TIME_CHECK = 60
SLEEP_TIME_CHECK = 10

"""
raise ShouldNotEmptyError() in spider or spidermiddleware's process_spider_input()
********************
raise error
** ** ** ** ** ** ** ** ** **
SentrySpiderMiddleware process_spider_exception
** ** ** ** ** ** ** ** ** **
RetryErrorResponseMiddleware process_spider_exception
2013-01-25 00:46:56+0800 [public_timeline] DEBUG: Retrying <GET https://api.weibo.com/2/statuses/public_timeline.json?count=200> (failed 1 times):
2013-01-25 00:46:56+0800 [public_timeline] DEBUG: Request token: used: 526.0
2013-01-25 00:46:58+0800 [public_timeline] DEBUG: Crawled (200) <GET https://api.weibo.com/2/statuses/public_timeline.json?count=200> (referer: None)
********************
raise error
** ** ** ** ** ** ** ** ** **
SentrySpiderMiddleware process_spider_exception
** ** ** ** ** ** ** ** ** **
RetryErrorResponseMiddleware process_spider_exception
2013-01-25 00:46:59+0800 [public_timeline] DEBUG: Retrying <GET https://api.weibo.com/2/statuses/public_timeline.json?count=200> (failed 2 times):
2013-01-25 00:46:59+0800 [public_timeline] DEBUG: Request token: used: 527.0
2013-01-25 00:47:01+0800 [public_timeline] DEBUG: Crawled (200) <GET https://api.weibo.com/2/statuses/public_timeline.json?count=200> (referer: None)
********************
raise error
** ** ** ** ** ** ** ** ** **
SentrySpiderMiddleware process_spider_exception
** ** ** ** ** ** ** ** ** **
RetryErrorResponseMiddleware process_spider_exception
2013-01-25 00:47:01+0800 [public_timeline] DEBUG: Gave up retrying <GET https://api.weibo.com/2/statuses/public_timeline.json?count=200> (failed 3 times):
"""


class InvalidTokenError(Exception):
    """token过期或不合法"""
    def __init__(self, value=None):
        self.value = value

    def __str__(self):
        if self.value:
            return repr(self.value)
        else:
            return 'InvalidTokenError'


class UserDoesNotExistError(Exception):
    """用户信息不存在"""
    def __init__(self, value=None):
        self.value = value

    def __str__(self):
        if self.value:
            return repr(self.value)
        else:
            return 'UserDoesNotExistError'


class PerUserPerAppLimitError(Exception):
    """用户级应用调用次数超过上限"""
    def __init__(self, value=None):
        self.value = value

    def __str__(self):
        if self.value:
            return repr(self.value)
        else:
            return 'PerUserPerAppLimitError'

class UnknownResponseError(Exception):
    """未处理的错误"""
    def __init__(self, value=None):
        self.value = value

    def __str__(self):
        if self.value:
            return repr(self.value)
        else:
            return 'UnknownResponseError'


class ShouldNotEmptyError(Exception):
    """返回不应该为空，但是为空了，在spider里抛出"""
    def __init__(self, value=None):
        self.value = value

    def __str__(self):
        if self.value:
            return repr(self.value)
        else:
            return 'ShouldNotEmptyError'


class RequestTokenMiddleware(object):
    def __init__(self, host, port, api_key, per_token_hours_limit, buffer_size):
        r = _default_redis(host, port)
        self.req_count = _default_req_count(r, api_key=api_key)
        self.tk_alive = _default_tk_alive(r, api_key=api_key)
        self.per_token_hours_limit = per_token_hours_limit 
        self.buffer_size = buffer_size

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        host = settings.get('REDIS_HOST')
        port = settings.get('REDIS_PORT')
        api_key = settings.get('API_KEY')
        per_token_hours_limit = settings.get('PER_TOKEN_HOURS_LIMIT')
        buffer_size = settings.get('BUFFER_SIZE')
        return cls(host, port, api_key, per_token_hours_limit, per_ip_hours_limit, buffer_size)

    def process_request(self, request, spider):
        token_and_used = one_valid_token(self.req_count, self.tk_alive)
        if token_and_used is None:
            log.msg(format='No token alive',
                    level=log.INFO, spider=spider)

            raise CloseSpider('No Token Alive')
        token, used = token_and_used

        if used > self.per_token_hours_limit - self.buffer_size:
            calibration(self.req_count, self.tk_alive, self.per_token_hours_limit)
            token, _ = one_valid_token(self.req_count, self.tk_alive)
            tk_status = token_status(token)
            reset_time_in, remaining = tk_status
            if remaining < BUFFER_SIZE:
                log.msg(format='REACH API REQUEST BUFFER, SLEEP %(reset_time_in)s SECONDS',
                        level=log.WARNING, spider=spider, reset_time_in=reset_time_in)

                time.sleep(reset_time_in)

        log.msg(format='Request token: %(token)s used: %(used)s',
                level=log.INFO, spider=spider, token=token, used=used)
        request.headers['Authorization'] = 'OAuth2 %s' % token


class RequestApiv1AuthMiddleware(object):
    def __init__(self, host, port, api_key, api_secret, per_token_hours_limit, per_ip_hours_limit, buffer_size, ip_address):
        r = _default_redis(host, port)
        self.req_count = _v1_req_count(r, api_key=api_key, api_secret=api_secret)
        self.req_ip_count = _ip_req_count(r)
        self.localIp = ip_address
        self.tk_alive = _v1_tk_alive(r, api_key=api_key, api_secret=api_secret)
        self.per_token_hours_limit = per_token_hours_limit
        self.per_ip_hours_limit = per_ip_hours_limit
        self.buffer_size = buffer_size
        self.api_key = api_key
        self.api_secret = api_secret
    
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        host = settings.get('REDIS_HOST')
        port = settings.get('REDIS_PORT')
        api_key = settings.get('API_KEY')
        api_secret = settings.get('API_SECRET')
        per_token_hours_limit = settings.get('PER_TOKEN_HOURS_LIMIT')
        per_ip_hours_limit = settings.get('PER_IP_HOURS_LIMIT')
        buffer_size = settings.get('BUFFER_SIZE')
        ip_address = localIp()
        return cls(host, port, api_key, api_secret, per_token_hours_limit, per_ip_hours_limit, buffer_size, ip_address)

    def process_request(self, request, spider):
        token_and_used = one_valid_token(self.req_count, self.tk_alive)
        if token_and_used is None:
            log.msg(format='No token alive',
                    level=log.INFO, spider=spider)

            raise CloseSpider('No Token Alive')
        token, used = token_and_used

        if used > self.per_token_hours_limit - self.buffer_size:
            while 1:
                token, used = one_valid_token(self.req_count, self.tk_alive)
                remaining = self.per_token_hours_limit - used
                token_key, token_secret = token.split('_')   

                if remaining < self.buffer_size:
                    log.msg(format='[Token] %(token)s REACH API REQUEST BUFFER, SLEEP %(reset_time_in)s SECONDS',
                            level=log.WARNING, spider=spider, token=token, reset_time_in=SLEEP_TIME_CHECK)
                
                    time.sleep(SLEEP_TIME_CHECK)
                else:#当remaining恢复时，跳出sleep循环
                    break

        ip_addr, ip_used = one_ip_request_count(self.req_ip_count, self.localIp)
        if ip_used > self.per_ip_hours_limit - self.buffer_size:
            while 1:
                ip_addr, ip_used = one_ip_request_count(self.req_ip_count, self.localIp)
                if ip_used < self.per_ip_hours_limit - self.buffer_size:
                    break
                else:
                    log.msg(format='[Token] %(ip)s REACH IP REQUEST BUFFER, SLEEP %(reset_time_in)s SECONDS',
                            level=log.WARNING, spider=spider, ip=ip_addr, reset_time_in=SLEEP_TIME_CHECK)
                
                    time.sleep(SLEEP_TIME_CHECK)

        log.msg(format='Request token: %(token)s used: %(used)s, Request IP: %(ip)s used: %(ip_used)s',
                level=log.INFO, spider=spider, token=token, used=used, ip=ip_addr, ip_used=ip_used)    

        token_key, token_secret = token.split('_')
        oauth4Scrapy = OAuth4Scrapy(self.api_key, self.api_secret)
        oauth4Scrapy.setToken(token_key, token_secret)  
        headers = oauth4Scrapy.getAuthHeaders(request.url)
        request.headers.update(headers)


class ErrorRequestMiddleware(object):
    def __init__(self, host, port, api_key):
        r = _default_redis(host, port)
        self.req_count = _default_req_count(r, api_key=api_key)
        self.tk_alive = _default_tk_alive(r, api_key=api_key)
        self.users_not_exist = _default_not_exist_users(r, 'user_info_spider') 

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        host = settings.get('REDIS_HOST')
        port = settings.get('REDIS_PORT')
        api_key = settings.get('API_KEY')
        return cls(host, port, api_key)

    def process_spider_input(self, response, spider):
        resp = json.loads(response.body)
        if response.status == 403:
            reason = resp.get('error')
            if resp.get('error_code') in [EXPIRED_TOKEN, INVALID_ACCESS_TOKEN]:
                token = response.request.headers['Authorization'][7:]
                self.req_count.delete(token)
                self.tk_alive.drop_tk(token)
                log.msg(format='Drop token: %(token)s %(reason)s',
                        level=log.INFO, spider=spider, token=token, reason=reason)
                raise InvalidTokenError('%s %s' % (token, reason))

            if resp.get('error_code') in [REACH_IP_LIMIT, REACH_PER_TOKEN_LIMIT, REACH_PER_TOKEN_LIMIT_1]:
                log.msg(format='REACH API LIMIT, SLEEP 60*60 SECONDS %(error)s %(error_code)s',
                        level=log.WARNING, spider=spider, error=resp.get('error'), error_code=resp.get('error_code'))

                time.sleep(3600)
                raise InvalidTokenError('%s %s' % (token, reason))

            if resp.get('error_code') == u'403' and int(resp.get('error')[0:5]) in [REACH_PER_APP_PER_USER_LIMIT]:
                now_ts = time.time()
                extra_sleep_ts = 10      
                remaining_ts = 3600 - now_ts % 3600 + extra_sleep_ts
                log.msg(format='REACH API PER APP PER USER LIMIT, SLEEP %(sleep_time)s SECONDS %(error)s %(error_code)s',
                        level=log.WARNING, spider=spider, sleep_time=remaining_ts, error=resp.get('error'), error_code=resp.get('error_code'))

                time.sleep(remaining_ts)
                raise PerUserPerAppLimitError('%s %s' % (resp.get('error'), resp.get('error_code')))

            raise UnknownResponseError('%s %s' % (resp.get('error'), resp.get('error_code')))
        
        elif response.status == 400:
            if resp.get('error_code') == u'400' and int(resp.get('error')[0:5]) in [USER_DOES_NOT_EXIST]:
                log.msg(format='USER_DOES_NOT_EXIST, DONT SLEEP AND DONT RETRY, %(error)s %(error_code)s',
                        level=log.WARNING, spider=spider, error=resp.get('error'), error_code=resp.get('error_code'))
                parse_result = urlparse.urlparse(response.request.url)
                params = urlparse.parse_qs(parse_result.query, True)
                user = int(params['user_id'][0])
                record_not_exist_user(self.users_not_exist, user)
                raise UserDoesNotExistError('%s %s' % (user, resp.get('error')))

            raise UnknownResponseError('%s %s' % (resp.get('error'), resp.get('error_code')))

        elif 'error' in resp and resp.get('error'):
            log.msg(format='UnknownResponseError: %(error)s %(error_code)s',
                    level=log.ERROR, spider=spider, error=resp.get('error'), error_code=resp.get('error_code'))

            raise UnknownResponseError('%s %s' % (resp.get('error'), resp.get('error_code')))


class RetryErrorResponseMiddleware(object):
    def __init__(self, retry_times):
        self.retry_times = retry_times

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        retry_times = settings.get('RETRY_TIMES', 2)
        return cls(retry_times)

    def _retry(self, request, reason, spider):
        retries = request.meta.get('retry_times', 0) + 1
        if retries <= self.retry_times:
            log.msg(format="Retrying %(request)s (failed %(retries)d times): %(reason)s",
                    level=log.WARNING, spider=spider, request=request, retries=retries, reason=reason)
            retryreq = request.copy()
            retryreq.meta['retry_times'] = retries
            retryreq.dont_filter = True
            return retryreq
        else:
            log.msg(format="Gave up retrying %(request)s (failed %(retries)d times): %(reason)s",
                    level=log.ERROR, spider=spider, request=request, retries=retries, reason=reason)

    def process_spider_exception(self, response, exception, spider):
        if 'dont_retry' not in response.request.meta and \
                isinstance(exception, InvalidTokenError) or isinstance(exception, UnknownResponseError) \
                or isinstance(exception, ShouldNotEmptyError) or isinstance(exception, PerUserPerAppLimitError):
            return [self._retry(response.request, exception, spider)]

        if isinstance(exception, UserDoesNotExistError):
            # UserDoesNotExistError放弃重试
            pass


class SentrySpiderMiddleware(object):
    def __init__(self, sentry_dsn):
        client = Client(sentry_dsn, string_max_length=sys.maxint)

        handler = SentryHandler(client)
        setup_logging(handler)
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        sentry_dsn = settings.get('SENTRY_DSN')
        return cls(sentry_dsn)

    def process_spider_exception(self, response, exception, spider):
        self.logger.error('SentrySpiderMiddleware %s [%s]' % (exception, spider.name), exc_info=True, extra={
            'culprit': 'SentrySpiderMiddleware/%s [spider: %s]' % (type(exception), spider.name),
            'stack': True,
            'data': {
                'response': cPickle.dumps(response.body),
                'request': cPickle.dumps(request_to_dict(response.request, spider)),
                'exception': exception,
                'spider': spider,
            }
        })


class SentryDownloaderMiddleware(object):
    def __init__(self, sentry_dsn):
        client = Client(sentry_dsn, string_max_length=sys.maxint)

        handler = SentryHandler(client)
        setup_logging(handler)
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        sentry_dsn = settings.get('SENTRY_DSN')
        return cls(sentry_dsn)

    def process_exception(self, request, exception, spider):
        self.logger.error('SentryDownloaderMiddleware %s [%s]' % (exception, spider.name), exc_info=True, extra={
            'culprit': 'SentryDownloaderMiddleware/%s [spider: %s]' % (type(exception), spider.name),
            'stack': True,
            'data': {
                'request': cPickle.dumps(request_to_dict(request, spider)),
                'exception': exception,
                'spider': spider,
            }
        })
