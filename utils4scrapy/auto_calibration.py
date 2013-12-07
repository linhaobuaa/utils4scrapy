# -*- coding: utf-8 -*-

from tk_maintain import _default_redis, _v1_req_count, _v1_tk_alive, calibration_v1, \
                        API_KEY, API_SECRET, PER_TOKEN_HOURS_LIMIT


r = _default_redis()
req_count = _v1_req_count(r)
tk_alive = _v1_tk_alive(r)
calibration_v1(API_KEY, API_SECRET, req_count, tk_alive, PER_TOKEN_HOURS_LIMIT)

# windows cron jobs
# schtasks /create /sc minute /mo 5 /tn PythonTokenCalibrationTask /TR "C:\python27\python.exe E:\scrapy_weibo_v1\install\utils4scrapy-master\utils4scrapy\auto_calibration.py"
# schtasks /delete /tn PythonTokenCalibrationTask

# linux cron jobs
# cd /etc/crontab;
# vim /etc/crontab;
# */2 * * * * root cd /home/mirage/linhao/scrapy_weibo/install/utils4scrapy-master/utils4scrapy;python auto_calibration.py