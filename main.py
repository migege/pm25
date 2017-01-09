#!/usr/bin/env python
# -*- coding:utf8 -*-
import os, sys, time
from datetime import datetime, timedelta
import feedparser
import json
import MySQLdb

reload(sys)
sys.setdefaultencoding('utf8')


def chunks(l, n):  # 切片
    for i in xrange(0, len(l), n):
        yield l[i:i + n]


def slice_list(l, n):  # 切分list
    return list(chunks(l, n))


def write(table, keys, vals):
    with MySQLdb.connect(host="127.0.0.1", user="air", passwd="air", db="air", port=3306) as db:
        val_fmt = "(" + ",".join(["'%s'"] * len(keys)) + ")"
        rows_list = slice_list(vals, 500)
        for rows in rows_list:
            sql = "insert into {table} ({keys}) values {vals} on duplicate key update {updates}".format(table=table, keys=",".join(keys), vals=",".join([val_fmt % row for row in rows]), updates=",".join(["{k}=values({k})".format(k=k) for k in keys]))
            print sql
            db.execute(sql)
            time.sleep(0.1)


def main():
    feed = "http://www.stateair.net/web/rss/1/1.xml"
    res = feedparser.parse(feed)

    print res['feed']['subtitle']
    keys = "city", "time", "aqi", "pm25"
    vals = []
    for entry in res['entries']:
        try:
            aqi = entry['aqi']
            pm25 = entry['conc']
            if float(aqi) <= 0.0:
                aqi = 0.0
            if float(pm25) <= 0.0:
                pm25 = 0.0
        except:
            continue
        vals.append(("beijing", datetime.strftime(datetime.strptime(entry['readingdatetime'], "%m/%d/%Y %I:%M:%S %p"), "%Y-%m-%d %H:%M:%S"), aqi, pm25))

    write("air_of_city", keys, vals)


if __name__ == '__main__':
    main()
