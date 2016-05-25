# -*- coding: utf-8 -*-
import sys
import datetime
import locale
import os
import optparse
import src
import ConfigParser
from src import read
from src import options
from src import const
from src import db
from src import engine
from src.utility import DateUtility
from src.utility import Utility
from src.preprocessing import PreProcessing

reload(sys)
sys.setdefaultencoding('utf-8')

# running time 계산을 위한 시작시간.
start_time = datetime.datetime.now()

print '\n'
print '%s' % ('{:*^60}'.format(''))
print '%s%s' % ('{:<30}'.format('*******'), '{:>30}'.format('*******'))
print '%s' % ('{:*^60}'.format(
    '   Indstry Risk Early Warning Engine V.1.0    '))
print '%s%s' % ('{:<30}'.format('*******'), '{:>30}'.format('*******'))
print '%s' % ('{:*^60}'.format(''))
print '\n'

opts = options.get_options()    # 옵션값
const = const.Const(opts.fix)   # 상수세팅

# db config
configParser = ConfigParser.RawConfigParser()
configParser.read(r'config.txt')
config = {
    'user': configParser.get('db-config', 'user'),
    'password': configParser.get('db-config', 'password'),
    'host': configParser.get('db-config', 'host'),
    'database': configParser.get('db-config', 'database'),
}

dbs = db.DbHelper(config)
qr = db.queries(dbs, const)
params = qr.getSetup(opts.userId, opts.seq, opts.dv)

util = Utility()

# print parameters
util.printLine()
for x in params:
    util.printKeyValue(x, params[x], open=False)
util.printLine()

# run engine
engine_time = datetime.datetime.now()
util.printLine()
util.printKeyValue('Engine Start', '')
engine = engine.Engine(qr)
result = engine.start(params, opts)

# Engine running time 출력
util.printKeyValue('Engine Time diff', (datetime.datetime.now() - engine_time))
util.printLine()

# db output
output_time = datetime.datetime.now()
util.printLine()
util.printKeyValue('Output Start', '')
dbHelper = src.db.OutputToDB(params, const, dbs)
dbHelper.insert_report(result)

# Output running time 출력
util.printKeyValue('Output Time diff', (datetime.datetime.now() - output_time))
util.printLine()

# running time 출력
util.printLine()
util.printKeyValue('Total Time diff', (datetime.datetime.now() - start_time))
util.printLine()
