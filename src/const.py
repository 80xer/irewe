#-*- coding: utf-8 -*-
def constant(f):
    def fset(value):
        raise TypeError
    def fget(self):
        return f(self)
    return property(fget, fset)

class Const():
    def __init__(self, fixed):
        self.fixed = fixed

    def isFixed(self):
        return self.fixed

    # 쿼리문 상수
    @constant
    def QR_SELECT_ALL_DV(self):
        return "select  cre_seq, dv_cd " \
                "from  iwbs_dv_setup " \
                "where id_nm = '%s' " \
                "order by cre_seq, dv_cd ;"

    @constant
    def QR_SELECT_DV_SETUP(self):  # 파라미터 셋업 조건 조회
        return "select	b.id_nm, b.cre_seq, a.dv_cd, b.strt_dt, b.end_dt, " \
               "b.learn_dt, b.nts, b.hp_filter, b.pca, b.lag, b.scaling, " \
               "b.lead_term_lmt, b.trd_dt_link_gb, a.up_dn, a.threshold " \
               "from	iwbs_dv_mast a, " \
               "iwbs_dv_setup b " \
               "where	a.dv_cd = '%s' " \
               "and		a.node_yn = 'N' " \
               "and		a.use_yn = 'Y' " \
               "and		a.dv_cd = b.dv_cd " \
               "and		b.id_nm = '%s' " \
               "and		b.cre_seq = '%s';"

    @constant
    def QR_SELECT_ITEM(self):   # 독립변수 아이템 조회
        return "select b.* from " \
               "iwbs_indust_mast a, iwbs_ind_var_mast b " \
               "where a.dv_cd = '%s' " \
               "and a.item_cd = b.item_cd"

    @constant
    def QR_SELECT_DV(self):  # DV 값 조회
        return "select '', '' union all " \
               "select '', '99999' union all " \
               "select 'trd_dt', 'IDX' union all " \
               "select concat(a.trd_dt, '01') trd_dt, a.diff_amount " \
               "from iwbs_dv_data a " \
               "where a.dv_cd = '%s'"

    @constant
    def QR_DELETE_IND_VAR_INFO(self):  # 독립변수 값들 삭제
        return """DELETE from iwbs_ind_var_info where id_nm = %s and cre_seq
        = %s and dv_cd = %s """

    @constant
    def QR_INSERT_IND_VAR_INFO(self):  # 독립변수 값들 생성
        return """INSERT INTO iwbs_ind_var_info
            (ID_NM, CRE_SEQ, TRD_DT, DV_CD, ITEM_CD, DIFF_AMOUNT, CRISIS_GB,
            UP_DN,
            NTS,
            THRESHOLD, VAR_A, VAR_B, VAR_C, VAR_D, ADF_GB)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s)"""

    @constant
    def QR_DELETE_FACT_INFO(self):    # 팩터 값들 삭제
        query = """DELETE from iwbs_fact_info where id_nm = %s and cre_seq = %s and dv_cd = %s """

        return query

    @constant
    def QR_INSERT_FACT_INFO(self):    # 팩터 값들 생성
        query = """INSERT INTO iwbs_fact_info
            (ID_NM, CRE_SEQ, TRD_DT, FACT_NM, AMOUNT, FACT_WT, FACT_NTS,
            VAR_A, VAR_B, VAR_C, VAR_D, CRISIS_GB, UP_DN, FACT_THRESHOLD,
            DV_CD )
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        return query

    @constant
    def QR_DELETE_FACT_WT(self):  # 독립변수 비중 값들 삭제
        query = """DELETE from iwbs_fact_wt where id_nm = %s and cre_seq =
        %s and dv_cd = %s """

        return query

    @constant
    def QR_INSERT_FACT_WT(self):  # 독립변수 비중 값들 생성

        query = """INSERT INTO iwbs_fact_wt (ID_NM, CRE_SEQ, TRD_DT,
        FACT_NM, PARENT_ITEM_CD, ITEM_CD, ITEM_WT, DV_CD)
        select %s, %s, %s, %s, parent_item_cd, %s, %s, %s
        from iwbs_ind_var_mast where item_cd = %s"""

        return query

    @constant
    def QR_SELECT_FACT_SORTED(self):  # 팩터 wt 값으로 소팅

        query = "select fact_nm, fact_wt, up_dn " \
                "from iwbs_fact_info " \
                "where id_nm = '%s' " \
                "and cre_seq = '%s' " \
                "and dv_cd = '%s' " \
                "and trd_dt = '%s' " \
                "order by fact_wt desc"

        return query

    @constant
    def QR_DELETE_FACT_FORM(self):  # 독립변수 비중 값들 삭제
        query = """DELETE from iwbs_fact_form where id_nm = %s and cre_seq = %s and dv_cd = %s """

        return query

    @constant
    def QR_INSERT_FACT_FORM(self):  # 팩터 wt 값으로 소팅

        query = "insert into iwbs_fact_form (id_nm, cre_seq, trd_dt, " \
                "fact_nm, parent_item_cd, dv_cd) "\
                "select id_nm, cre_seq, trd_dt, " \
                        "fact_nm, " \
                        "parent_item_cd, " \
                        "dv_cd " \
                "from " \
                "( " \
                "select parent_item_cd, n, item_wt, " \
                        "id_nm, " \
                        "cre_seq, " \
                        "trd_dt, " \
                        "fact_nm, " \
                        "dv_cd " \
                "from   " \
                "( select @nm := '', @n := 0 ) init " \
                "join " \
                "( " \
                "SELECT @n := if(parent_item_cd != @nm, 1, @n + 1) as n, " \
                    "@nm := parent_item_cd " \
                    "parent_item_cd, " \
                    "item_wt, " \
                    "id_nm, " \
                    "cre_seq, " \
                    "trd_dt, " \
                    "fact_nm, " \
                    "dv_cd " \
                "FROM   iwbs.iwbs_fact_wt a " \
                "where  a.id_nm = '%s'  " \
                "and        a.cre_seq = '%s'  " \
                "and        a.dv_cd = '%s'  " \
                "and        a.fact_nm = '%s'  " \
                "and        a.item_wt %s 0 " \
                "and        a.parent_item_cd not in (  " \
                               "select  parent_item_cd  " \
                               "from    iwbs.iwbs_fact_form  " \
                               "where   id_nm = '%s'  " \
                               "and     cre_seq = '%s'  " \
                               "and     dv_cd = '%s'  " \
                               ") " \
                "order by a.parent_item_cd asc, item_wt %s " \
                        ") x " \
                    "where   n <= 20 " \
                    "order by parent_item_cd, n         " \
                ") a " \
                "group by parent_item_cd, id_nm, cre_seq, trd_dt, fact_nm, dv_cd " \
                "order by sum(item_wt) %s " \
                "limit 1"

        return query

    @constant
    def QR_DELETE_IDX(self):  # 위기지수 삭제
        query = """DELETE from iwbs_idx_data where id_nm = %s and cre_seq =
        %s and dv_cd = %s """

        return query

    @constant
    def QR_INSERT_IDX(self):  # 위기지수 생성

        query = """INSERT INTO iwbs_idx_data
            (ID_NM, CRE_SEQ, TRD_DT, DV_CD, AMOUNT)
            VALUES(%s, %s, %s, %s, %s)"""

        return query
