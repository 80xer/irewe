# -*- coding: utf-8 -*-
import sys
import optparse
from src.utility import Utility

reload(sys)
sys.setdefaultencoding('utf-8')


# options 설정
def set_options():

    parser = optparse.OptionParser('usage run.py <options>')

    parser.add_option(
        '-d', '--debug',
        action='store_true',
        dest='debug',
        default=False,
        help='set debug falg'
    )

    parser.add_option(
        '-f', '--fix',
        action='store_true',
        dest='fix',
        default=False,
        help='use fixed conditions'
    )

    parser.add_option(
        '-i', '--id',
        dest='userId',
        default='system',
        help='set user id'
    )

    parser.add_option(
        '-s', '--seq',
        dest='seq',
        default='1',
        help='set sequence'
    )

    parser.add_option(
        '-v', '--dv',
        dest='dv',
        help='choice dependent variable'
    )

    parser.add_option(
        '-l', '--loop',
        action='store_true',
        dest='loop',
        default=False,
        help='set loop flag'
    )

    parser.add_option(
        '--shift',
        action='store_true',
        dest='shift',
        default=False,
        help='shift date series'
    )

    return parser


# options 반환
def get_options():
    parser = set_options()
    (options, args) = parser.parse_args()

    util = Utility()

    if options.fix is False and options.userId is '':
        print 'insert fix or userId options'
        print u'fix 또는 id 옵션을 설정하세요.'
        sys.exit()

    if options.fix:
        options.userId = 'system'

    if options.dv is '' or options.dv is None and \
            options.loop is False or options.loop is None:
        util.printKeyValue('Insert DV code', '')
        sys.exit()


    # print options
    util.printLine()
    util.printKeyValue('debug', options.debug, open=False)
    util.printKeyValue('fix', options.fix, open=False)
    util.printKeyValue('userId', options.userId, open=False)
    util.printKeyValue('seq', options.seq, open=False)
    util.printKeyValue('dv', options.dv, open=False)
    util.printKeyValue('loop', options.loop, open=False)
    util.printKeyValue('shift', options.shift, open=False)
    util.printLine()

    return options
