# -*- coding: utf-8 -*-
import sys
import optparse

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
        default='',
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
        default='dv',
        help='choice dependent variable'
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
    if options.fix is False and options.userId is '':
        print 'insert fix or userId options'
        print u'fix 또는 id 옵션을 설정하세요.'
        sys.exit()

    if options.fix:
        options.userId = 'system'

    # print options
    print '%s' %('{:*^60}'.format(''))
    print '%s : %s' %('{:>27}'.format('debug'), options.debug)
    print '%s : %s' %('{:>27}'.format('fix'), options.fix)
    print '%s : %s' %('{:>27}'.format('userId'), options.userId)
    print '%s : %s' %('{:>27}'.format('seq'), options.seq)
    print '%s : %s' %('{:>27}'.format('dv'), options.dv)
    print '%s : %s' %('{:>27}'.format('shift'), options.shift)
    print '%s' %('{:*^60}'.format(''))

    return options
