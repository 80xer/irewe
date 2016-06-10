# -*- coding: utf-8 -*-
import sys
import datetime
import locale
import os
import optparse
import src
import ConfigParser
import logging
import logging.config
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

opts = options.get_options()    # 옵션값
const = const.Const(opts.fix)   # 상수세팅
secs = start_time.second
mins = start_time.minute
hours = start_time.hour
days = start_time.day
months = start_time.month
years = int(str(start_time.year)[2:])

timevalue = ('%02d' % (years,)) + \
            ('%02d' % (months,)) + \
            ('%02d' % (days,)) + \
            ('%02d' % (hours,)) + \
            ('%02d' % (mins,)) + \
            ('%02d' % (secs,))
logFileName = 'logs/%s_%s.log'%(opts.userId, timevalue)

logging.config.fileConfig(
    'logging.conf',
    defaults={'logfilename': logFileName}
)
logger = logging.getLogger('irewe')

logger.info('%s' % ('{:*^60}'.format('')))
logger.info('%s%s' % ('{:<30}'.format('*******'), '{:>30}'.format('*******')))
logger.info('%s' % ('{:*^60}'.format(
    '   Indstry Risk Early Warning Engine V.1.0    ')))
logger.info('%s%s' % ('{:<30}'.format('*******'), '{:>30}'.format('*******')))
logger.info('%s' % ('{:*^60}'.format('')))


# db config
configParser = ConfigParser.RawConfigParser()
configParser.read(r'config')
config = {
    'user': configParser.get('db-config', 'user'),
    'password': configParser.get('db-config', 'password'),
    'host': configParser.get('db-config', 'host'),
    'database': configParser.get('db-config', 'database'),
}

dbs = db.DbHelper(config)
qr = db.queries(dbs, const)
util = Utility(opts.debug)
util.setLogger(logger)

if opts.dv is None and opts.loop is True:
    dvs = qr.getDvs(opts.userId)
else:
    dvs = ((opts.seq, opts.dv),)

cnt = 0
try:
    for dv in dvs:
        cnt = cnt + 1
        seq = dv[0]
        dv = dv[1]
        params = ()
        params = qr.getSetup(opts.userId, seq, dv)

        logger.info(' %s >> SEQ: %s  DV: %s ' % (cnt, seq, dv))

        # print parameters
        util.printLine()
        for x in params:
            util.printKeyValue(x, params[x], open=False)
        util.printLine()

        # continue

        # run engine
        engine_time = datetime.datetime.now()
        util.printLine()
        util.printKeyValue('Engine Start', '')
        irEngine = engine.Engine(qr, params, opts, logger)
        result = irEngine.start()

        # Engine running time 출력
        util.printKeyValue('Engine Time diff', (datetime.datetime.now() - engine_time), ' ', True, True)
        util.printLine()

        # db output
        output_time = datetime.datetime.now()
        util.printLine()
        util.printKeyValue('Output Start', '')
        dbHelper = src.db.OutputToDB(params, const, dbs, logger)
        dbHelper.insert_report(result)

        # Output running time 출력
        util.printKeyValue('Output Time diff', (datetime.datetime.now() - output_time), ' ', True, True)
        util.printLine()
except Exception as inst:
    logger.exception(inst)
    sys.exit(1)
# running time 출력
util.printLine()
util.printKeyValue('%s DV Total Time diff' % len(dvs),
                   (datetime.datetime.now() -
                                          start_time), ' ', True, True)
util.printLine()
sys.exit()