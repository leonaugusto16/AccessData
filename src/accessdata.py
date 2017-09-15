# coding: utf-8

import sys
import os
import re
import datetime,time
import io,json
from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from datetime import date

conf = SparkConf().setAppName("AccessData")
sc = SparkContext(conf=conf)
sqlCtx = SQLContext(sc)

month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
    'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

# A regular expression pattern to extract fields from the log line
APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'

def parse_apache_time(s):
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))

def parseApacheLogLine(logline):
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = long(0)
    else:
        size = long(match.group(9))
    return (Row(
        host          = match.group(1),
        client_identd = match.group(2),
        user_id       = match.group(3),
        date_time     = parse_apache_time(match.group(4)),
        method        = match.group(5),
        endpoint      = match.group(6),
        protocol      = match.group(7),
        response_code = int(match.group(8)),
        content_size  = size
    ), 1)


def parseLogs():
    parsed_logs = (sc
                   .textFile(logFile)
                   .map(parseApacheLogLine)
                   .cache())
    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())

    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))
    failed_logs_count = failed_logs.count()
    if failed_logs_count > 0:
        print 'Number of invalid logline: %d' % failed_logs.count()
        for line in failed_logs.take(20):
            print 'Invalid logline: %s' % line

    print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs.count(), access_logs.count(), failed_logs.count())
    return parsed_logs, access_logs, failed_logs

def contentSize(access):  
    file = open("/DATA/result/content_size","w")

    content_sizes = access.map(lambda log: log.content_size).cache()
    file.write('Content Size Avg: %i, Min: %i, Max: %s\n' % (
        content_sizes.reduce(lambda a, b : a + b) / content_sizes.count(),
        content_sizes.min(),
        content_sizes.max()))
    file.close()

def toJSON(fileSave, obj):
    file = open(fileSave,"w")
    obj_json = json.dumps(obj)
    file.write(obj_json)
    file.close

def response(access):
    responseCodeToCount = (access.map(lambda log: (log.response_code, 1))
                                 .reduceByKey(lambda a, b : a + b)
                                 .cache())
    responseCodeToCountList = responseCodeToCount.take(100)
    toJSON("/DATA/result/response.json",responseCodeToCountList)

def accessMoreInTime(access, time, lenght):
    # time = number de access URL 
    # lenght = number list return

    hostCountPairTuple = access.map(lambda log: (log.host, 1))
    hostSum = hostCountPairTuple.reduceByKey(lambda a, b : a + b)
    hostMoreThan = hostSum.filter(lambda s: s[1] > time)
    hostsPick = (hostMoreThan
                 .map(lambda s: s[0])
                 .take(lenght))
    toJSON("/DATA/result/access_more.json",hostsPick)

def topEndpoints(access,lenght):
    endpointCounts = (access.map(lambda log: (log.endpoint, 1))
                            .reduceByKey(lambda a, b : a + b))
    topEndpoints = endpointCounts.takeOrdered(lenght, lambda s: -1 * s[1])
    toJSON("/DATA/result/top_endpoints.json",topEndpoints)

def topFailedEndpoints(access, lenght):
    notCode = access.filter(lambda log: log.response_code != 200)
    endpointCountPairTuple = notCode.map(lambda log: (log.endpoint, 1))
    endpointSum = endpointCountPairTuple.reduceByKey(lambda a, b: a + b)
    topErrURLs = endpointSum.takeOrdered(lenght, lambda s: -1 * s[1])
    toJSON("/DATA/result/top_failed_endpoints.json",topErrURLs)

def numberUniqueHosts(access):
    hosts = access.map(lambda log: log.host)
    uniqueHosts = hosts.distinct()
    uniqueHostCount = uniqueHosts.count()
    toJSON("/DATA/result/top_unique_hosts.json",uniqueHostCount)

def event404(access):
    event404 = access_logs.filter(lambda log: log.response_code==404).cache()
    return event404

def endpoints404(access):
    badEndpoints = access.map(lambda log: log.endpoint)
    badUniqueEndpoints = badEndpoints.distinct()
    badUniqueEndpointsPick = badUniqueEndpoints.take(40)
    toJSON("/DATA/result/uri_404.json",badUniqueEndpointsPick)

def topEndpoints404(access,lenght):
    badEndpointsCountPairTuple = access.map(lambda log: (log.endpoint, 1)).reduceByKey(lambda a, b: a+b)
    badEndpointsSum = badEndpointsCountPairTuple.sortBy(lambda x: x[1], False)
    badEndpointsTop = badEndpointsSum.take(lenght)
    toJSON("/DATA/result/top_uri_404.json",badEndpointsTop)

def topHostsError(access,lenght):
    errHostsCountPairTuple = access.map(lambda log: (log.host, 1)).reduceByKey(lambda a, b: a+b)
    errHostsSum = errHostsCountPairTuple.sortBy(lambda x: x[1], False)
    errHostsTop = errHostsSum.take(lenght)
    toJSON("/DATA/result/top_hosts_error.json",errHostsTop)

def errorPerDay404(access):
    errDateCountPairTuple = access.map(lambda log: (log.date_time.day, 1))
    errDateSum = errDateCountPairTuple.reduceByKey(lambda a, b: a+b)
    errDateSorted = errDateSum.sortByKey().cache()
    errByDate = errDateSorted.collect()
    toJSON("/DATA/result/error_per_day.json",errByDate)

def topDays404(access,days):
    errDateCountPairTuple = access.map(lambda log: (log.date_time.day, 1))
    errDateSum = errDateCountPairTuple.reduceByKey(lambda a, b: a+b)
    errDateSorted = errDateSum.sortByKey().cache()
    topErrDate = errDateSorted.sortBy(lambda x: -x[1]).take(days)
    toJSON("/DATA/result/top_days_404.json",topErrDate)

def topHours404(access):
    hourCountPairTuple = access.map(lambda log: (log.date_time.hour, 1))
    hourRecordsSum = hourCountPairTuple.reduceByKey(lambda a, b: a+b)
    hourRecordsSorted = hourRecordsSum.sortByKey().cache()
    errHourList = hourRecordsSorted.collect()
    toJSON("/DATA/result/top_hours_404.json",errHourList)

if __name__ == "__main__":
    baseDir = os.path.join('DATA')
    inputPath = os.path.join('logs')
    logFile = os.path.join(baseDir, inputPath)
    parsed_logs, access_logs, failed_logs = parseLogs()
    
    #contentSize(access_logs)
    response(access_logs)
    accessMoreInTime(access_logs, 5, 20)
    topEndpoints(access_logs, 20)
    topFailedEndpoints(access_logs,20)
    numberUniqueHosts(access_logs)
    
    access404 = event404(access_logs)
    endpoints404(access404)
    topEndpoints404(access404,40)
    topHostsError(access404,40)      
    errorPerDay404(access404)
    topDays404(access404,5)
    topHours404(access404)
    
