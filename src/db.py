# -*- coding:cp949 -*-
import sys
import os
import MySQLdb
import datetime
import warnings
from src.utility import DateUtility
from src.utility import Utility
from src.read import Series
from src.io import IO

reload(sys)
sys.setdefaultencoding('utf-8')


class DbHelper():
    def __init__(self, config):
        self.__wbsDbConfig = config
        self.conn = None
    def getConn(self):
        config = self.__wbsDbConfig
        if self.conn is None or self.conn.open is 0:
            self.conn = MySQLdb.connect(host=config["host"], port=3306, user=config["user"], passwd=config["password"], db=config["database"], charset ='utf8')
        return self.conn

    def exeData(self, query):
        conn = self.getConn()
        cur = conn.cursor()
        try:
            cur.execute(query)
        except Exception as inst:
            print type(inst)
            print inst.args

        results = cur.fetchall()

        conn.close()
        return results

    def executeMany(self, query, data):
        conn = self.getConn()
        cur = conn.cursor()

        try:
           cur.executemany(query, data)
           conn.commit()
        except Exception as inst:
            print type(inst)
            print inst.args
            conn.rollback()

        conn.close()

class queries():
    def __init__(self, db, const):
        self.db = db
        self.utility = Utility()
        self.CONST = const

    def getDvs(self, id):
        dataTuples = self.db.exeData(
            self.CONST.QR_SELECT_ALL_DV % id)
        return dataTuples

    def getSetup(self, id, seq, dvcd):

        # 변수별 컬럼 값
        ID_NM = 0
        SEQ = 1
        DV = 2
        START_DT = 3
        END_DT = 4
        LEARN_DT = 5
        NTS = 6
        FILTER = 7
        PCA = 8
        LAG = 9
        SCALING = 10
        LAG_CUT = 11
        SHIFT = 12
        DIR = 13
        THRESHOLD = 14

        dataTuples = self.db.exeData(
            self.CONST.QR_SELECT_DV_SETUP % (dvcd, id, seq))
        dbData = dataTuples[0]

        result = {}
        result['id_nm'] = dbData[ID_NM]
        result['seq'] = dbData[SEQ]
        result['nts_thres'] = dbData[NTS]
        result['t0'] = datetime.datetime.strptime(str(dbData[START_DT]) + '01', '%Y%m%d').date()
        result['t1'] = datetime.datetime.strptime(str(dbData[END_DT]) + '01', '%Y%m%d').date()
        result['t2'] = datetime.datetime.strptime(str(dbData[LEARN_DT]) + '01', '%Y%m%d').date()
        result['pca_thres'] = dbData[PCA]
        result['intv'] = int(dbData[LAG])
        result['lag_cut'] = int(dbData[LAG_CUT])
        result['scaling'] = dbData[SCALING]
        result['hp_filter'] = dbData[FILTER]
        result['dv'] = dbData[DV]
        result['dv_dir'] = dbData[DIR]
        result['thres_cut'] = 0.2  # .2 고정
        result['dv_thres'] = dbData[THRESHOLD]
        result['shift'] = dbData[SHIFT]
        self.params = result
        return result

    def getDv(self, dv):
        result = []
        dataTuples = self.db.exeData(self.CONST.QR_SELECT_DV % dv)
        dbData = self.extract_from_list(dataTuples)
        result.extend(dbData)
        return result

    def getITemsFromDV(self, dv):
        items = []
        return items

    def getItems(self, id, seq, dvcd):

        items = self.db.exeData(self.CONST.QR_SELECT_ITEM % (dvcd))

        itemCdSelect = []
        itemNmSelect = []
        pathSelect = []
        dataSelect = []
        cnt = 0

        itemCdSelect.append("select '', ")
        itemNmSelect.append("select '', ")
        pathSelect.append("select 'TRD_DT', ")
        dataSelect.append("select concat(a.trd_dt,'01'), ")

        for item in items:
            itemCdSelect.append(
                "MAX(iF(a.item_cd = '" + item[0]
                + "', a.item_cd, null)) 'I'")
            itemNmSelect.append(
                "MAX(iF(a.item_cd = '" + item[0]
                + "', concat(a.item_nm, '_', a.unit), null)) ")
            pathSelect.append(
                "MAX(iF(a.item_cd = '" + item[0]
                + "', a.path, null)) ")
            dataSelect.append(
                "MAX(iF(a.item_cd = '" + item[0]
                + "', a.amount, null)) ")
            if cnt < len(items) - 1:
                itemCdSelect.append(', ')
                itemNmSelect.append(', ')
                pathSelect.append(', ')
                dataSelect.append(', ')
            cnt = cnt + 1

        itemCdSelect.append(
            "from iwbs_ind_var_mast a, iwbs_indust_mast b "
            "where b.dv_cd = '" + dvcd + "' and a.item_cd = b.item_cd")
        itemNmSelect.append(
            "from iwbs_ind_var_mast a, iwbs_indust_mast b "
            "where b.dv_cd = '" + dvcd + "' and a.item_cd = b.item_cd")
        pathSelect.append(
            "from iwbs_ind_var_mast a, iwbs_indust_mast b "
            "where b.dv_cd = '" + dvcd + "' and a.item_cd = b.item_cd")
        dataSelect.append(
            "from iwbs_ind_var_data a, iwbs_indust_mast b "
            "where b.dv_cd = '" + dvcd + "' and a.item_cd = b.item_cd "
            "group by a.trd_dt")

        allSelect = []
        allSelect.append(''.join(itemCdSelect))
        allSelect.append(" union all ")
        allSelect.append(''.join(itemNmSelect))
        allSelect.append(" union all ")
        allSelect.append(''.join(pathSelect))
        allSelect.append(" union all ")
        allSelect.append(''.join(dataSelect))

        result = []
        dataTuples = self.db.exeData(''.join(allSelect))
        dbData = self.extract_from_list(dataTuples)
        result.extend(dbData)
        return result

    def extract_from_list(self, data):
        series_result = []
        date_result = []
        du = DateUtility()
        date_col = 0
        id_row = 0
        nm_row = 1
        unit_row = 2
        start_col = 1
        start_row = 3
        date_values = du.getCol_values(data, date_col, start_row, len(data))

        for i in range(len(date_values)):
            date_str = str(int(date_values[i]))
            date_result.append(datetime.datetime.strptime(date_str, '%Y%m%d').date())
            pass

        col_cnt = len(data[id_row])
        io_type = 'I'
        for i in range(col_cnt)[start_col:]:
            name = data[nm_row][i]
            code = self.utility.convert_code(data[id_row][i])
            unit = data[unit_row][i]
            series = Series(self.params)
            series.io_type = io_type
            series.code = code
            series.name = name
            series.group = unit
            series.value = du.getCol_values(data, i, start_row, len(data))
            series.date = date_result
            series.data_cleansing(self.params['t0'], self.params['t1'])
            series.set_freq()

            if len(series.date) > 0 and series.date[0] <= self.params['t0'] and series.date[-1] >= self.params['t1']:
                series_result.append(series)

        return series_result


class OutputToDB:
    def __init__(self, params, const, dbs, logger):
        self.params = params
        self.CONST = const
        self.db = dbs
        self.logger = logger

    def insert_report(self, data):
        util = Utility()

        util.printKeyValue('in output', '', open=True)
        atime = datetime.datetime.now()
        self.insert_iv(data)
        util.printKeyValue('    iv Time diff',
                           datetime.datetime.now() - atime, ' ', True, True)

        btime = datetime.datetime.now()
        self.insert_factor(data)
        util.printKeyValue('    factor Time diff',
                           datetime.datetime.now() - btime)

        ctime = datetime.datetime.now()
        self.insert_factor_weight(data)
        util.printKeyValue('    factor_weight Time diff',
                           datetime.datetime.now() - ctime)

        dtime = datetime.datetime.now()
        self.insert_factor_parent()
        util.printKeyValue('    factor parent Time diff',
                           datetime.datetime.now() - dtime)

        etime = datetime.datetime.now()
        self.insert_warning_board_idx(data)
        util.printKeyValue('    index Time diff',
                           datetime.datetime.now() - etime)

    def insert_iv(self, data):
        iv_sh = data['df_iv']
        iv_sh_digit = data['df_iv_digit']
        iv_info = data['iv_info_dict']

        insertData = []

        for col in iv_sh.columns:
            if col != 'YYYYMM' and col != 'DATE' and col != 'DV':
                for j in range(len(iv_sh[col])):
                    if datetime.datetime.strptime(
                        iv_sh['YYYYMM'][j] + '01', '%Y%m%d'
                    ).date() == self.params['t1']:
                        if iv_info[col]['adf_test']:
                            adf_gb = '1'
                        else:
                            adf_gb = '0'

                        elem = (str(self.params['id_nm']),
                                str(self.params['seq']),
                                str(iv_sh['YYYYMM'][j]),
                                str(self.params['dv']),
                                str(col),
                                float(iv_sh[col][j]),
                                adf_gb,
                                iv_info[col]['dir'],
                                iv_info[col]['nts'],
                                iv_info[col]['thres'],
                                iv_info[col]['a'],
                                iv_info[col]['b'],
                                iv_info[col]['c'],
                                iv_info[col]['d'],
                                int(iv_sh_digit[col][j])
                                )
                    else:
                        elem = (str(self.params['id_nm']),
                                str(self.params['seq']),
                                str(iv_sh['YYYYMM'][j]),
                                str(self.params['dv']),
                                str(col),
                                float(iv_sh[col][j]),
                                None,
                                None,
                                None,
                                None,
                                None,
                                None,
                                None,
                                None,
                                int(iv_sh_digit[col][j])
                                )

                    insertData.append(elem)

        conn = self.db.getConn()
        cur = conn.cursor()

        io = IO()
        file = io.print_df(
            'data_%s_%s_%s'%(
                self.params['id_nm'], self.params['seq'], self.params['dv']
            ),
            insertData
        )
        try:
            warnings.filterwarnings('always', category=MySQLdb.Warning)
            with warnings.catch_warnings(record=True) as w:
                cur.execute(self.CONST.QR_DELETE_IND_VAR_INFO,
                            (self.params['id_nm'], self.params['seq'],
                             self.params['dv']))

                cur.execute(self.CONST.QR_INSERT_IND_VAR_INFO, file)
                # todo change to file
                # if len(w) > 0: self.logger.warning(w[-1].message)
                conn.commit()
                io.remove_file(file)
        except Exception as e:
            conn.rollback()
            conn.close()
            io.remove_file(file)
            raise Exception(e)

        conn.close()

    def insert_factor(self, data):
        iv_sh = data['df_factor_yyyymm']
        iv_info = data['factor_info_dict']
        fw = data['factor_weight']
        fracs = fw['fracs']

        insertData = []

        code_ordered = []
        code_ordered.append('YYYYMM')
        for c in iv_sh.columns:
            if c != 'DV' and c != 'YYYYMM':
                code_ordered.append(c)
        code_ordered.append('DV')

        id_info = (str(self.params['id_nm']), str(self.params['seq']))

        for col in code_ordered:
            if col != 'YYYYMM' and col != 'DATE' and col != 'DV':
                num = col.replace('FAC', '')
                for j in range(len(iv_sh[col])):
                    if iv_info[col]['dir'] == 'U':
                        crisis_gb = 1 if iv_sh[col][j] > iv_info[col]['thres'] else 0
                    elif iv_info[col]['dir'] == 'D':
                        crisis_gb = 1 if iv_sh[col][j] < iv_info[col]['thres'] else 0
                    if datetime.datetime.strptime(
                        iv_sh['YYYYMM'][j] + '01', '%Y%m%d'
                    ).date() == self.params['t1']:
                        elem = (str(iv_sh['YYYYMM'][j]),
                                str(col),
                                iv_sh[col][j],
                                fracs[int(num)],
                                iv_info[col]['nts'],
                                iv_info[col]['a'],
                                iv_info[col]['b'],
                                iv_info[col]['c'],
                                iv_info[col]['d'],
                                crisis_gb,
                                iv_info[col]['dir'],
                                iv_info[col]['thres'],
                                self.params['dv']
                               )
                    else:
                        elem = (str(iv_sh['YYYYMM'][j]),
                                str(col),
                                iv_sh[col][j],
                                None,
                                None,
                                None,
                                None,
                                None,
                                None,
                                crisis_gb,
                                iv_info[col]['dir'],
                                iv_info[col]['thres'],
                                self.params['dv']
                               )

                    elem = id_info + elem

                    insertData.append(elem)

        conn = self.db.getConn()
        cur = conn.cursor()

        try:
            cur.execute(
                self.CONST.QR_DELETE_FACT_INFO,
                (self.params['id_nm'], self.params['seq'], self.params['dv']))
            cur.executemany(self.CONST.QR_INSERT_FACT_INFO, insertData)
            conn.commit()
        except Exception:
            conn.rollback()
            conn.close()
            raise Exception('error insert fact')

        conn.close()

    def insert_factor_weight(self, data):
        fw = data['factor_weight']
        iv_list = fw['col_list']
        weight = fw['weight']

        insertData = []
        id_info = (str(self.params['id_nm']), str(self.params['seq']))

        for i in range(len(weight)):
            for j in range(len(iv_list)):

                elem = (str(self.params['t1'].strftime('%Y%m')),
                        'FAC%s' %i,
                        iv_list[j],
                        weight[i][j],
                        str(self.params['dv']),
                        iv_list[j]
                       )

                elem = id_info + elem

                insertData.append(elem)

        conn = self.db.getConn()
        cur = conn.cursor()

        try:
            cur.execute(
                self.CONST.QR_DELETE_FACT_WT,
                (self.params['id_nm'], self.params['seq'], self.params['dv']))

            cur.executemany(self.CONST.QR_INSERT_FACT_WT, insertData)
            conn.commit()
        except Exception:
            conn.rollback()
            conn.close()
            raise Exception('error insert fact weight')

        conn.close()

    def insert_factor_parent(self):
        elem = (
                self.params['id_nm'],
                self.params['seq'],
                self.params['dv'],
                str(self.params['t1'].year) + ('%02d' % self.params['t1'].month)
            )
        sortedFactors = self.db.exeData(
            self.CONST.QR_SELECT_FACT_SORTED % (elem)
        )

        commData = (
            self.params['id_nm'],
            self.params['seq'],
            self.params['dv']
        )

        conn = self.db.getConn()
        cur = conn.cursor()

        try:

            cur.execute(
                self.CONST.QR_DELETE_FACT_FORM,
                (self.params['id_nm'], self.params['seq'], self.params['dv']))

            for factor in sortedFactors:
                elem = (
                    factor[0],
                )
                if factor[2] == 'U':
                    elem = elem + ('>',)
                    ord = ('desc',)
                else:
                    elem = elem + ('<',)
                    ord = ('asc',)

                elem = commData + elem + commData + ord + ord

                cur.execute(
                    self.CONST.QR_INSERT_FACT_FORM % elem)
                conn.commit()
        except Exception:
            conn.rollback()
            conn.close()
            raise Exception('error insert fact parent')

        conn.close()

    def insert_warning_board_idx(self, data):
        iv_sh = data['df_warning_idx']

        insertData = []
        id_info = (str(self.params['id_nm']), str(self.params['seq']))

        for col in iv_sh.columns:
            if col == 'IDX':
                for j in range(len(iv_sh[col])):
                    elem = (str(iv_sh['YYYYMM'][j]),
                            self.params['dv'],
                            iv_sh[col][j]
                           )

                    elem = id_info + elem

                    insertData.append(elem)

        conn = self.db.getConn()
        cur = conn.cursor()

        try:
            cur.execute(
                self.CONST.QR_DELETE_IDX,
                (self.params['id_nm'], self.params['seq'], self.params['dv']))

            cur.executemany(self.CONST.QR_INSERT_IDX, insertData)
            conn.commit()
        except Exception:
            conn.rollback()
            conn.close()
            raise Exception('error insert idx')

        conn.close()