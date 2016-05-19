# -*- coding: utf-8 -*-
import sys
import datetime
import locale
import os
import optparse
import src
from src import read
from src import options
from src import const
from src import db
from src import engine
from src.utility import DateUtility
from src.preprocessing import PreProcessing

reload(sys)
sys.setdefaultencoding('utf-8')

# running time 계산을 위한 시작시간.
start_time = datetime.datetime.now()

print '\n'
print '%s' %('{:*^60}'.format(''))
print '%s%s' %('{:<30}'.format('*******'), '{:>30}'.format('*******'))
print '%s' %('{:*^60}'.format('   Indstry Risk Early Warning Engine V.1.0    '))
print '%s%s' %('{:<30}'.format('*******'), '{:>30}'.format('*******'))
print '%s' %('{:*^60}'.format(''))
print '\n'

opts = options.get_options()
const = const.Const(opts.fix)
qr = db.queries(const)
params = qr.getSetup(opts.userId, opts.seq)

# print parameters
print '{:*^60}'.format('')
for x in params:
    print "%s : %s" % ('{:>27}'.format(x), params[x])
print '{:*^60}'.format('')

# run engine
engine_time = datetime.datetime.now()
print '{:*^60}'.format('')
print 'Engine Start'
engine = engine.Engine()
result = engine.run(params, opts, qr)

# Engine running time 출력
print 'Engine Time difference : {difftime}'\
    .format(difftime=(datetime.datetime.now() - engine_time))
print '{:*^60}'.format('')

# db output
output_time = datetime.datetime.now()
print '{:*^60}'.format('')
print 'Output Start'
dbHelper = src.db.OutputToDB(params, const)
dbHelper.insert_report(result)

# Output running time 출력
print 'Output Time difference : {difftime}'\
    .format(difftime=(datetime.datetime.now() - output_time))
print '{:*^60}'.format('')

# running time 출력
print '{:*^60}'.format('')
print 'Total Time difference : {difftime}'\
    .format(difftime=(datetime.datetime.now() - start_time))
print '{:*^60}'.format('')
