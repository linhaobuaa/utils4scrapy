# -*- coding: utf-8 -*-

from tk_maintain import reset_ip_req_count, _ip_req_count, _default_redis

r = _default_redis()
req_ip_count = _ip_req_count(r)
reset_ip_req_count(req_ip_count)


# windows cron jobs
# schtasks /create /sc hourly /st 00:00:00 /tn PythonIpResetCountTask /TR "C:\python27\python.exe E:\scrapy_weibo_v1\install\utils4scrapy-master\utils4scrapy\auto_reset_ip_count.py"
# schtasks /delete /tn PythonIpResetCountTask

# linux cron jobs
# cd /etc/crontab;
# vim /etc/crontab;
# 0 * * * * root cd /home/mirage/linhao/scrapy_weibo/install/utils4scrapy-master/utils4scrapy;python auto_reset_ip_req_count.py