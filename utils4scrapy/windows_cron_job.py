#!/usr/bin/env python
# -*- coding: UTF-8 -*-
 
import sched, time
from threading import Thread, Timer
import subprocess
 
#s = sched.scheduler(time.time, time.sleep)


class Job(Thread):
    def run(self):
        '''主要业务执行方法'''
        print_time()
        print "-------------- compiling --------------"
        cmdline.execute("scrapy crawl user_info_v1 -a since_id=1 -a max_id=100 --loglevel=INFO".split())
        #subprocess.Popen("scrapy crawl user_info_v1 --loglevel=INFO")
 
def each_day_time(hour,min,sec,next_day=True):
    '''返回当天指定时分秒的时间'''
    struct = time.localtime()
    if next_day:
        day = struct.tm_mday + 1
    else:
        day = struct.tm_mday
    return time.mktime((struct.tm_year,struct.tm_mon,day,
        hour,min,sec,struct.tm_wday, struct.tm_yday,
        struct.tm_isdst))
    
def print_time(name="None"):
    print name, ":","From print_time",\
        time.time()," :", time.ctime()
 
def do_somthing():
    job = Job()
    job.start()
 
def echo_start_msg():
    print "-------------- auto compile begin running --------------"

 
def main():
    #指定时间点执行任务
    s.enterabs(each_day_time(1,15,0,False), 1, echo_start_msg, ())
    s.run()
    while(True):
        Timer(0, do_somthing, ()).start()
        time.sleep(60)

def test():
    from twisted.internet import reactor
    from scrapy.crawler import Crawler
    from scrapy import log, signals
    #from scrapy_weibo.spiders.user_info_v1_spider import UserInfoSpiderV1
    from testspiders.spiders.followall import FollowAllSpider
    from scrapy.utils.project import get_project_settings

    #spider = UserInfoSpiderV1(0, 100)
    spider = FollowAllSpider(domain='scrapinghub.com')
    settings = get_project_settings()
    crawler = Crawler(settings)
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
    crawler.configure()
    crawler.crawl(spider)
    crawler.start()
    log.start()
    reactor.run() # the script will block here until the spider_closed signal was sent
 
if __name__ == "__main__":
    #main()
    test()