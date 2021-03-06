# -*- coding: utf-8 -*-
import sys, xlrd, datetime, calendar
reload(sys)
sys.setdefaultencoding('utf-8')

class DateUtility():
    
    # 두 날짜 사이의 month 리스트 반환
    def get_montly_span(self, date_t0, date_t1):
        month_cnt = self.diff_month(date_t1, date_t0) + 1
        first_month = datetime.date(date_t0.year, date_t0.month, 1)
        
        result_str = []
        result_str.append(self.convert2yyyymm(first_month))
        
        result_date = []        
        result_date.append(self.convert2months(first_month))

        for i in range(month_cnt)[1:]:
            result_str.append(self.convert2yyyymm(self.add_months(first_month, i)))

        for i in range(month_cnt)[1:]:
            result_date.append(self.convert2months(self.add_months(first_month, i)))

        return result_str, result_date

    # 두 날짜 사이의 月 개수 반환
    def diff_month(self, d1, d2):
        return (d1.year - d2.year)*12 + d1.month - d2.month

    def add_months(self, sourcedate,months):
        month = sourcedate.month - 1 + months
        year = sourcedate.year + month / 12
        month = month % 12 + 1
        day = min(sourcedate.day,calendar.monthrange(year,month)[1])
        return datetime.date(year,month,day)

    def subtract_months(self, sourcedate, months):
        month = sourcedate.month - months
        year = sourcedate.year
        if month < 0:
            year = year - 1
            month = 12 + month
        day = 1
        return datetime.date(year, month, day)

    # datetime.datetime --> YYYYMM
    def convert2yyyymm(self, dt):
        return dt.strftime('%Y%m')

    # datetime.datetime --> integer date = year*12 + month
    def convert2months(self, dt):
        return dt.year * 12 + dt.month


    def getCol_values(self, listData, col, start_row, end_row):
        results = []
        for i in range(start_row, end_row):
            # print "listData[%s][col]:%s" %(i, listData[i][col])
            results.append(listData[i][col])
        return results


class Utility:
    # code --> 00000
    def __init__(self, debug=False):
        self.debug = debug
        return

    def setLogger(self, logger):
        self.logger = logger

    def convert_code(self, code):
        try:
            str_code = "{0:d}".format(int(code))
        except Exception:
            str_code = code

        pre_fix = "0"*(5 - len(str_code))
        str_code = pre_fix + str_code
        return str_code


    def printKeyDotValue(self, key, value):
        self.printKeyValue(key, value, '.')

    def printKeyValue(self, key, value, dot=' ', open=True, use=False):
        # if self.debug is False: return
        if hasattr(self, 'logger'):
            self.printKeyValueWithLogger(key, value, dot, open, use)
            return

        if open:
            kp = '<'
            vp = '>'

        else:
            kp = '>'
            vp = '<'

        keyFormat = '{:' + dot + kp + '30}'
        valueFormat = '{:' + dot + vp + '29}'

        # print '%s %s' %(keyFormat.format(key),valueFormat.format(str(value)))

        if type(value) is datetime.timedelta:
            secs = value.seconds
            mics = int(str(value.microseconds)[:2])
            mins = secs / 60
            secs = secs % 60
            value = '0:' + \
                    ('%02d' % (mins,)) + ':' + \
                    ('%02d' % (secs,)) + '.' + \
                    ('%02d' % (mics,))
        print '%s %s' %(keyFormat.format(key),valueFormat.format(
            str(value)))

    def printKeyValueWithLogger(self, key, value, dot=' ', open=True,
                               use=False):
        if open:
            kp = '<'
            vp = '>'

        else:
            kp = '>'
            vp = '<'

        keyFormat = '{:' + dot + kp + '30}'
        valueFormat = '{:' + dot + vp + '29}'

        # print '%s %s' %(keyFormat.format(key),valueFormat.format(str(value)))

        if type(value) is datetime.timedelta:
            secs = value.seconds
            mics = int(str(value.microseconds)[:2])
            mins = secs / 60
            secs = secs % 60
            value = '0:' + \
                    ('%02d' % (mins,)) + ':' + \
                    ('%02d' % (secs,)) + '.' + \
                    ('%02d' % (mics,))
        self.logger.info('%s %s' %(keyFormat.format(key),valueFormat.format(
            str(value))))


    def printLine(self):
        if hasattr(self, 'logger'):
            self.logger.info('{:*^60}'.format(''))
        else:
            print '{:*^60}'.format('')