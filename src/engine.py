# -*- coding: utf-8 -*-
import sys
import datetime
from src import read
from src.utility import DateUtility
from src.utility import Utility
from src.preprocessing import PreProcessing
from src.nts import NtsCaldulator
from src.pca import PcaCalculator
import copy

reload(sys)
sys.setdefaultencoding('utf-8')


class Engine:
    def __init__(self, qr, params, options, logger):
        self.qr = qr
        self.params = params
        self.options = options
        self.logger = logger
        return

    def start(self):
        util = Utility(self.options.debug)
        util.setLogger(self.logger)
        iv_total = []

        # 디비에서 독립변수 받기

        atime = datetime.datetime.now()
        items = self.qr.getItems(
            self.options.userId,
            self.params['seq'],
            self.params['dv']
        )
        # 유저 셋팅
        #  아이템 받기
        iv_total.extend(items)

        # 디비에서 종속변수 받기
        dv = self.qr.getDv(self.params['dv'])

        # debug 용 데이터 축소
        # if options.debug:
        #     iv_total = iv_total[:12]
        #     print "length of iv_total is %s" % len(iv_total)

        util.printKeyValue(
            '    GetItems Time diff',
            datetime.datetime.now() - atime)

        du = DateUtility()

        interpolated_time = datetime.datetime.now()
        # t0와 t1 월별날짜 리스트 계산
        month_list_str, month_list_months = du.get_montly_span(
            self.params['t0'],
            self.params['t1'])

        # out of sample months
        month_list_str_out, month_list_months_out = du.get_montly_span(
            self.params['t0'],
            self.params['t2'])

        iv_total_out = copy.deepcopy(iv_total)

        iv_info_dict = {}

        iv_total_out_time = datetime.datetime.now()

        for iv in iv_total:
            iv.set_monthly_data()  # 같은월에 여러 데이터중 최신 데이터만
            # 내삽
            iv.set_interpolated_data(month_list_months, month_list_str)
            iv_info_dict[iv.code] = {}
            iv_info_dict[iv.code]['group'] = iv.group

        util.printKeyValue(
            '    interpolated Time diff',
            datetime.datetime.now() - interpolated_time)

        for iv in iv_total_out:
            iv.set_monthly_data()
            iv.set_interpolated_data(month_list_months_out,
                                     month_list_str_out)
        # --------------------------------------------------

        util.printKeyValue(
            '    iv_total_out Time diff', datetime.datetime.now() -
                                        iv_total_out_time)

        dv[0].set_monthly_data()
        dv[0].set_interpolated_data(month_list_months, month_list_str)

        dv_out = copy.deepcopy(dv)
        dv_out[0].set_monthly_data()
        dv_out[0].set_interpolated_data(month_list_months_out,
                                          month_list_str_out)

        # 월중 최신데이터만 선택, 내삽 완료.

        df_iv_time = datetime.datetime.now()
        df_iv = read.convert_series_list_to_dataframe(iv_total)

        # out of sample months
        df_iv_out = read.convert_series_list_to_dataframe(
            iv_total_out)
        # --------------------------------------------------

        util.printKeyValue(
            '    df_iv, df_iv_out Time diff', datetime.datetime.now() -
                                        df_iv_time)

        # 전처리 작업 구동
        pp = PreProcessing()

        df_time = datetime.datetime.now()
        # ADF 테스트 후 차분
        df_iv, df_iv_out = pp.get_adf_test_after_df(df_iv,
                                                    df_iv_out,
                                                    iv_info_dict)
        util.printKeyValue(
            '    adf_test Time diff',
            datetime.datetime.now() - df_time)

        filter_time = datetime.datetime.now()
        # Hp Filter
        df_iv = pp.get_hp_filter(df_iv, self.params['hp_filter'])
        util.printKeyValue(
            '    df_iv_filter Time diff',
            datetime.datetime.now() - filter_time)

        df_iv_time = datetime.datetime.now()
        # out of sample months ------------------------------------------------
        df_iv_out = pp.get_hp_filter(df_iv_out, self.params['hp_filter'])
        util.printKeyValue(
            '    df_iv_out_filter Time diff',
            datetime.datetime.now() - df_iv_time)
        # ---------------------------------------------------------------------

        df_dv_time = datetime.datetime.now()
        # 종속변수
        df_dv = read.convert_series_list_to_dataframe(dv)
        df_dv_out = read.convert_series_list_to_dataframe(dv_out)

        df_dv = df_dv[1:].reset_index(drop=True)  # 맨 앞 데이터 차분
        df_dv_out = df_dv_out[1:].reset_index(drop=True)

        if int(self.params['scaling']) == 1:
            df_iv, df_iv_out = pp.scale_iv(df_iv, df_iv_out)

        df_iv['DV'] = df_dv[df_dv.columns[2]]
        df_iv_out['DV'] = df_dv_out[df_dv_out.columns[2]]    # out of sample
        util.printKeyValue(
            '    df_dv_out Time diff',
            datetime.datetime.now() - df_dv_time)

        nts_time = datetime.datetime.now()
        # nts 계산
        nts_module = NtsCaldulator()
        dv_crisis_digit_list, dv_thres = \
            nts_module.cal_nts_total(
                df_iv, iv_info_dict,
                self.params['intv'],
                self.params['thres_cut'],
                self.params['dv_thres'],
                self.params['lag_cut'],
                self.params['dv_dir']
            )
        # iv_info_dict 에 nts 관련 정보 적재  (2016.03.10) nts 계산에서 \
        # 선행기간 내 위기식별 구간 제한 추가작업 lag_cut
        # nts_module.cal_nts_by_digit(df_iv, dv_crisis_digit_list)
        util.printKeyValue(
            '    cal_nts_total Time diff',
            datetime.datetime.now() - nts_time)

        df_iv_digit_time = datetime.datetime.now()
        # nts 에 따른 thres와 digit 저장
        df_iv_digit = nts_module.get_iv_sh_digit(df_iv, iv_info_dict,
                                                    self.params['dv_thres'],
                                                    self.params['dv_dir'])
        util.printKeyValue(
            '    get_iv_sh_digit Time diff',
            datetime.datetime.now() - df_iv_digit_time)

        srt_time = datetime.datetime.now()
        srted = sorted(iv_info_dict.iteritems(),
                       key=self.get_value,
                       reverse=False)
        filtered = [s for s in srted if s[1]['nts'] < self.params['nts_thres']]
        util.printKeyValue(
            '    sorted Time diff',
            datetime.datetime.now() - srt_time)

        factor_time = datetime.datetime.now()
        code_list = []
        for f in filtered:
            code_list.append(f[0])

        pca_module = PcaCalculator()

        y, wt, fracs, df_factor, df_factor_out = \
            pca_module.run_cap(
                df_iv[code_list],
                df_iv_out[code_list],
                self.params['pca_thres']
            )

        factor_weight = {}
        factor_weight['col_list'] = df_iv[code_list].columns.tolist()
        factor_weight['weight'] = wt
        factor_weight['fracs'] = fracs

        # df_factor_yyyymm 출력용
        df_factor_series = df_factor.copy()
        df_factor_series['YYYYMM'] = df_iv['YYYYMM'].tolist()
        df_factor_series['DV'] = df_iv['DV'].tolist()

        # df_factor_yyyymm 출력용
        df_factor_series_out = df_factor_out.copy()
        df_factor_series_out['YYYYMM'] = df_iv['YYYYMM'].tolist()
        df_factor_series_out['DV'] = df_iv['DV'].tolist()

        factor_info_dict = {}
        for col in df_factor.columns:
            factor_info_dict[col] = {}

        # df_factor['DV'] = df_dv_sh[df_dv_sh.columns[2]]
        nts_module.cal_nts_total(
            df_factor_series,
            factor_info_dict,
            self.params['intv'],
            self.params['thres_cut'],
            self.params['dv_thres'],
            self.params['lag_cut'],
            self.params['dv_dir']
        )
        # (2016.03.10) nts 계산에서 선행기간 내 위기식별 구간 제한 추가작업 lag_cut

        for i in range(len(df_factor.columns.tolist())):
            factor_info_dict[df_factor.columns.tolist()[i]]['weight'] = \
                factor_weight['fracs'][i]
        util.printKeyValue(
            '    factor Time diff',
            datetime.datetime.now() - factor_time)

        idx_time = datetime.datetime.now()
        # 위기지수 계산
        df_warning_idx = self.cal_warning_idx(factor_info_dict,
                                              df_factor_series)
        df_warning_idx_out = \
            self.cal_warning_idx(factor_info_dict, df_factor_series_out)

        result = {}
        # result['params'] = params
        result['iv_raw'] = iv_total
        # result['iv_code'] = iv_code
        result['iv_info_dict'] = iv_info_dict
        result['df_iv'] = df_iv
        result['df_iv_digit'] = df_iv_digit
        result['factor_info_dict'] = factor_info_dict
        result['df_factor_yyyymm'] = df_factor_series
        result['df_warning_idx'] = df_warning_idx
        result['df_warning_idx_out'] = df_warning_idx_out
        result['dv_thres'] = dv_thres
        result['factor_weight'] = factor_weight
        util.printKeyValue(
            '    cal idx Time diff',
            datetime.datetime.now() - idx_time)
        return result

    def get_value(self, item):
        return item[1]['nts']

    def list_to_dict(self, list):
        result = {}
        for i in range(len(list)):
            result[list[i][0]] = list[i][1]
        return result

    def cal_warning_idx(self, factor_info_dict, df_factor_series):

        # NTS 가 0일경우 위기지수 계산시 division by 0 에러가 발생하므로
        # NTS 가 0일경우 NTS가 0이 아닌 NTS들의 최소값으로 대체하여 계산한다.
        nts_list = []
        for k, v in factor_info_dict.iteritems():
            nts_list.append(v['nts'])
        nts_min = min(nts_list)

        factor_info_dict_2 = factor_info_dict.copy()
        weight_sum = 0.0
        for k, v in factor_info_dict_2.iteritems():
            if v['nts'] == 0:
                v['weight'] = 1/nts_min
            else:
                v['weight'] = 1/v['nts']
            weight_sum += v['weight']

        df_warning_idx = df_factor_series.copy()
        for i in df_warning_idx.index:
            idx_sum = 0.0
            for k, v in factor_info_dict_2.iteritems():
                dir = v['dir']
                thres = v['thres']
                nts = v['nts']
                if dir == 'U':
                    if df_warning_idx[k][i] > thres:    # 위기여부 1
                        idx_sum += v['weight'] * 1.0
                elif dir == 'D':
                    if df_warning_idx[k][i] < thres:    # 위기여부 1
                        idx_sum += v['weight'] * 1.0
            warning_idx = idx_sum / weight_sum
            df_warning_idx.set_value(i, 'IDX', warning_idx)

        return df_warning_idx
