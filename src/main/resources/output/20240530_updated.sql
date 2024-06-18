CREATE OR REPLACE PROCEDURE p_report_cisp_wdb_amp_bsjyb_1(
    pi_end_date IN NUMBER --加载日期
)
    IS
    v_pi_end_date_t1 NUMBER(8);
    v_pi_end_date_t2 NUMBER(8);
BEGIN

    --删除重复数据
    DELETE FROM rdm.wdb_amp_bsjyb_1 WHERE insert_time = pi_end_date;
    COMMIT;

    --T+1数据日期
    SELECT sk_date
    INTO v_pi_end_date_t1
    FROM dw.dim_time
    WHERE wkdayno = (SELECT wkdayno FROM dw.dim_time WHERE sk_date = pi_end_date) - 1
      AND isworkday = 1;

    --T+2数据日期
    SELECT sk_date
    INTO v_pi_end_date_t2
    FROM dw.dim_time
    WHERE wkdayno = (SELECT wkdayno FROM dw.dim_time WHERE sk_date = pi_end_date) - 2
      AND isworkday = 1;

    --插入数据到结果报表
    INSERT INTO rdm.wdb_amp_bsjyb_1
    ( gzdm -- 规则代码
    , sjrq -- 数据日期
    , cpdm -- 产品代码
    , insert_time --插入时间
    , fxdj -- 风险等级 0-严重 1-警告
    )
    /*====================================================================================================
    # 规则代码: AM00001
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: 公司无QDII产品的情况下，不应当有境外托管人
             公司无QDII产品的情况下，不应当有境外投资顾问
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    SELECT DISTINCT 'AM00001'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 产品类别:
      --    公募基金: 119-股票型QDII基金、129-混合型QDII基金、139-债券型QDII基金、158-FOF型QDII基金、159-MOM型QDII基金、198-其他QDII基金
      --    大集合: 219-股票型QDII、229-混合型QDII、239-债券型QDII、258-FOF型QDII、259-MOM型QDII、298-其他QDII
      AND t1.cplb NOT IN ('119', '129', '139', '158', '159', '198', '219', '229', '239', '258', '259', '298')
      AND (t1.jwtgrzwmc IS NOT NULL OR t1.jwtzgwzwmc IS NOT NULL OR t1.jwtzgwywmc IS NOT NULL)

    /*====================================================================================================
    # 规则代码: AM00002
    # 目标接口: J1026-资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: 股票型基金股票的投资比例应大于等于80%
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1002-产品基本信息 J1009-产品净值信息
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前 T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00002'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , tt1.zclb
               , tt3.cplb
               , sum(tt1.qmsz) OVER (PARTITION BY tt1.sjrq, tt1.cpdm, tt1.zclb) AS qmsz1
               , max(tt2.zczz) OVER (PARTITION BY tt1.sjrq, tt1.cpdm)           AS qmsz2
          FROM report_cisp.wdb_amp_inv_assets tt1
                   LEFT JOIN report_cisp.wdb_amp_prod_nav tt2 ON tt1.jgdm = tt2.jgdm
              AND tt1.status = tt2.status
              AND tt1.sjrq = tt2.sjrq
              AND tt1.cpdm = tt2.cpdm
                   LEFT JOIN report_cisp.wdb_amp_prod_baseinfo tt3 ON tt2.jgdm = tt3.jgdm
              AND tt2.status = tt3.status
              AND tt2.sjrq = tt3.sjrq
              AND tt2.cpdm = tt3.cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 资产类别: 101-股票
      AND t1.zclb = '101'
      -- 产品类别:
      --    公募基金: 111-股票型基金、119-股票型QDII基金
      --    大集合: 211-股票型、219-股票型QDII
      AND t1.cplb IN ('111', '119', '211', '219')
      AND round(t1.qmsz1 / t1.qmsz2, 2) < 0.8

    /*====================================================================================================
    # 规则代码: AM00003
    # 目标接口: J1026-资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: 债券型基金债券的投资比例应大于等于80%
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1002-产品基本信息 J1009-产品净值信息
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前 T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00003'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , tt1.zclb
               , tt3.cplb
               , sum(tt1.qmsz) OVER (PARTITION BY tt1.sjrq, tt1.cpdm, tt1.zclb) AS qmsz1
               , max(tt2.zczz) OVER (PARTITION BY tt1.sjrq, tt1.cpdm)           AS qmsz2
          FROM report_cisp.wdb_amp_inv_assets tt1
                   LEFT JOIN report_cisp.wdb_amp_prod_nav tt2 ON tt1.jgdm = tt2.jgdm
              AND tt1.status = tt2.status
              AND tt1.sjrq = tt2.sjrq
              AND tt1.cpdm = tt2.cpdm
                   LEFT JOIN report_cisp.wdb_amp_prod_baseinfo tt3 ON tt2.jgdm = tt3.jgdm
              AND tt2.status = tt3.status
              AND tt2.sjrq = tt3.sjrq
              AND tt2.cpdm = tt3.cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 资产类别: 103-债券
      AND t1.zclb = '103'
      -- 产品类别:
      --    公募基金: 131-债券基金、139-债券型QDII基金
      --    大集合: 231-债券型、239-债券型QDII
      AND t1.cplb IN ('131', '139', '231', '239')
      AND round(t1.qmsz1 / t1.qmsz2, 2) < 0.8

    /*====================================================================================================
    # 规则代码: AM00004
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: 产品起始日期应小于或等于当前系统日期
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00004'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND to_date(t1.hzrq, 'yyyymmdd') > to_date(t1.sjrq, 'yyyymmdd')

    /*====================================================================================================
    # 规则代码: AM00005
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {注册批文变更日期}不为空时，{核准日期}<={注册批文变更日期},且{注册批文变更日期}<={数据日期}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00005'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.zcpwbgrq IS NOT NULL
      AND (to_date(t1.hzrq, 'yyyymmdd') > to_date(t1.zcpwbgrq, 'yyyymmdd') OR
           to_date(t1.zcpwbgrq, 'yyyymmdd') > to_date(t1.sjrq, 'yyyymmdd'))

    /*====================================================================================================
    # 规则代码: AM00006
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {正式转型日期}不为空时，{正式转型日期}<={数据日期}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00006'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.zszxrq IS NOT NULL
      AND to_date(t1.zszxrq, 'yyyymmdd') > to_date(t1.sjrq, 'yyyymmdd')

    /*====================================================================================================
    # 规则代码: AM00007
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {转型前产品正式代码}和{正式转型日期}有绑定关系
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00007'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND (t1.zxqcpzsdm IS NOT NULL AND t1.zszxrq IS NULL OR t1.zxqcpzsdm IS NULL AND t1.zszxrq IS NOT NULL)

    /*====================================================================================================
    # 规则代码: AM00008
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: 指数基金受托责任为被动投资,其中指数增强受托责任为两者兼具
    # 规则来源: 证监会-FAQ
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00008'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 指数基金类型: 1-ETF、2-ETF联接、3-指数增强型基金、9-其他指数基金
      -- 受托责任: 1-主动投资、2-被动投资、3-两者兼具
      AND (t1.zsjjlx IN ('1', '2', '9') AND t1.stzr <> '2' OR t1.zsjjlx = '3' AND t1.stzr <> '3')

    /*====================================================================================================
    # 规则代码: AM00009
    # 目标接口: J1011-产品收益分配信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 分配收益为负值时，需要在{备注}中写明原因
    # 规则来源: 证监会-FAQ
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00009'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_divid t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.yfpsy < 0 OR t1.sjfpsy < 0 OR t1.dwcpfpsy < 0)
      AND t1.bz IS NULL

    /*====================================================================================================
    # 规则代码: AM00010
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {证券代码}无需包含“SH”“SZ”
    # 规则来源: 证监会-FAQ
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00010'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stock t1
    WHERE t1.jgdm = 70610000
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.zqdm IN ('%SZ%', '%SH%')

    /*====================================================================================================
    # 规则代码: AM00011
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {债券代码}不能包含字符“SH”、“SZ”、“IB”
    # 规则来源: 证监会-FAQ
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00011'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
    WHERE t1.jgdm = 70610000
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.zqdm IN ('%SZ%', '%SH%', '%IB%')

    /*====================================================================================================
    # 规则代码: AM00012
    # 目标接口: J1037-协议回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易对手方产品代码}第7位为"1"的，{交易对手方类型}应为“银行非保本理财";
             {交易对手方产品代码}第7位为"2"的，{交易对手方类型}应为“信托公司资管产品";
             {交易对手方产品代码}第7位为"3"的，{交易对手方类型}应为“证券公司及其子公司资管产品";
             {交易对手方产品代码}第7位为"4"的，{交易对手方类型}应为“基金管理公司及其子公司专户";
             {交易对手方产品代码}第7位为"5"的，{交易对手方类型}应为“期货公司及其子公司资管产品";
             {交易对手方产品代码}第7位为"6"的，{交易对手方类型}应为“保险资管产品";
             {交易对手方产品代码}第7位为"8"的，{交易对手方类型}应为“公募基金";
    # 规则来源: 人行
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00012'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_agrmrepo t1
    WHERE t1.jgdm = 70610000
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (
        -- 交易对手方类型: 201-银行理财产品、202-银行子公司公募理财、203-银行子公司理财产品
        (substr(t1.jydsfcpdm, 7, 1) = '1' AND t1.jydsflx NOT IN ('201', '202', '203'))
            -- 交易对手方类型: 204-信托计划
            OR (substr(t1.jydsfcpdm, 7, 1) = '2' AND t1.jydsflx <> '204')
            -- 交易对手方类型: 208-证券公司公募基金、209-证券公司资产管理计划、210-证券公司子公司公募基金、211-证券公司子公司资产管理计划
            OR (substr(t1.jydsfcpdm, 7, 1) = '3' AND t1.jydsflx NOT IN ('208', '209', '210', '211'))
            -- 交易对手方类型: 212-基金管理公司公募基金、214-基金管理公司子公司资产管理计划
            OR (substr(t1.jydsfcpdm, 7, 1) = '4' AND t1.jydsflx NOT IN ('212', '214'))
            -- 交易对手方类型: 215-期货公司资产管理计划、216-期货公司子公司资产管理计划
            OR (substr(t1.jydsfcpdm, 7, 1) = '5' AND t1.jydsflx NOT IN ('215', '216'))
            -- 交易对手方类型: 205-保险产品、206-保险公司资产管理计划、229-保险公司子公司公募基金、207-保险公司子公司资产管理计划
            OR (substr(t1.jydsfcpdm, 7, 1) = '6' AND t1.jydsflx NOT IN ('205', '206', '229', '207'))
            -- 交易对手方类型: 212-基金管理公司公募基金、214-基金管理公司子公司资产管理计划
            OR (substr(t1.jydsfcpdm, 7, 1) = '8' AND t1.jydsflx NOT IN ('212', '214'))
        )

    /*====================================================================================================
    # 规则代码: AM00013
    # 目标接口: J1047-债券借贷投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易对手方产品代码}第7位为"1"的，{交易对手方类型}应为“银行非保本理财";
             {交易对手方产品代码}第7位为"2"的，{交易对手方类型}应为“信托公司资管产品";
             {交易对手方产品代码}第7位为"3"的，{交易对手方类型}应为“证券公司及其子公司资管产品";
             {交易对手方产品代码}第7位为"4"的，{交易对手方类型}应为“基金管理公司及其子公司专户";
             {交易对手方产品代码}第7位为"5"的，{交易对手方类型}应为“期货公司及其子公司资管产品";
             {交易对手方产品代码}第7位为"6"的，{交易对手方类型}应为“保险资管产品";
             {交易对手方产品代码}第7位为"8"的，{交易对手方类型}应为“公募基金";
    # 规则来源: 人行
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00013'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_loan t1
    WHERE t1.jgdm = 70610000
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (
        -- 交易对手方类型: 201-银行理财产品、202-银行子公司公募理财、203-银行子公司理财产品
        (substr(t1.jydsfcpdm, 7, 1) = '1' AND t1.jydsflx NOT IN ('201', '202', '203'))
            -- 交易对手方类型: 204-信托计划
            OR (substr(t1.jydsfcpdm, 7, 1) = '2' AND t1.jydsflx <> '204')
            -- 交易对手方类型: 208-证券公司公募基金、209-证券公司资产管理计划、210-证券公司子公司公募基金、211-证券公司子公司资产管理计划
            OR (substr(t1.jydsfcpdm, 7, 1) = '3' AND t1.jydsflx NOT IN ('208', '209', '210', '211'))
            -- 交易对手方类型: 212-基金管理公司公募基金、214-基金管理公司子公司资产管理计划
            OR (substr(t1.jydsfcpdm, 7, 1) = '4' AND t1.jydsflx NOT IN ('212', '214'))
            -- 交易对手方类型: 215-期货公司资产管理计划、216-期货公司子公司资产管理计划
            OR (substr(t1.jydsfcpdm, 7, 1) = '5' AND t1.jydsflx NOT IN ('215', '216'))
            -- 交易对手方类型: 205-保险产品、206-保险公司资产管理计划、229-保险公司子公司公募基金、207-保险公司子公司资产管理计划
            OR (substr(t1.jydsfcpdm, 7, 1) = '6' AND t1.jydsflx NOT IN ('205', '206', '229', '207'))
            -- 交易对手方类型: 212-基金管理公司公募基金、214-基金管理公司子公司资产管理计划
            OR (substr(t1.jydsfcpdm, 7, 1) = '8' AND t1.jydsflx NOT IN ('212', '214'))
        )

    /*====================================================================================================
    # 规则代码: AM00014
    # 目标接口: J3006-基金经理信息
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: 基金经理的{任职日期}与{公告日期}应该相同，剔除已经离职的基金经理
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00014'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.sjrq
               , tt1.cpdm
               , tt1.zjhm
               , tt1.xm
               , tt1.rzrq
               , tt1.ggrq
               , tt1.jgdm
               , tt1.status
               , tt2.lzrq
               , max(tt2.lzrq) OVER (PARTITION BY tt1.zjhm,tt1.xm) AS lzrq1
          FROM report_cisp.wdb_amp_prod_fundmngr tt1
                   LEFT JOIN report_cisp.hd_product_fundmngr_info tt2
                             ON tt1.cpdm = tt2.cpdm
                                 AND tt1.zjhm = tt2.zjhm
                                 AND tt1.xm = tt2.xm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.lzrq1 IS NULL OR to_date(t1.sjrq, 'yyyymmdd') < to_date(t1.lzrq1, 'yyyymmdd'))
      AND t1.rzrq <> t1.ggrq

    /*====================================================================================================
    # 规则代码: AM00015
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 基金报告期间+数据报送日期，与基金已清盘标志强关联。在该基金已清盘的当月，不需要报送该产品
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00015'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 报告期间: 3-月
      AND t1.bgqj = 3
      AND t1.htyzzbz = 1

    /*====================================================================================================
    # 规则代码: AM00016
    # 目标接口: J1004-产品侧袋信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: 产品侧袋基本信息中的{当前状态}与{涉及投资者数量}，呈强相关。例如，{涉及投资者数量}为0的情况下，{当前状态}才可以是已终止。
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00016'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpzdm    AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_pocket t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.sjtzzsl <> 0
      AND t1.dqzt = '1'

    /*====================================================================================================
    # 规则代码: AM00017
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {本年累计费用总和}={本年累计业绩报酬}+{本年累计销售服务费}+{本年累计托管费}+{本年累计管理费}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00017'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND bnljfyzh - bnljyjbc - bnljxsfwf - bnljtgf - bnljglf <> 0

    /*====================================================================================================
    # 规则代码: AM00018
    # 目标接口: J1009-产品净值信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {资产净值}={总份额}*{单位净值}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00018'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_nav t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND abs(t1.zcjz - t1.zfe * t1.dwjz) > 100

    /*====================================================================================================
    # 规则代码: AM00019
    # 目标接口: J1009-产品净值信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: ([产品基本信息].{产品类别}为“货币基金”、“货币型”)时，{每万份收益}不能为空
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00019'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_nav t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 产品类别:
      --    公募基金: 141-货币基金
      --    大集合: 241-货币型
      AND t2.cplb IN ('141', '241')
      AND t1.mwfsy IS NULL

    /*====================================================================================================
    # 规则代码: AM00020
    # 目标接口: J1010-QDII及FOF产品净值信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ([产品基本信息].{产品类别}为“货币基金”、“货币型”)时，{每万份收益}不能为空
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00020'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_qd_nav t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 产品类别:
      --    公募基金: 141-货币基金
      --    大集合: 241-货币型
      AND t2.cplb IN ('141', '241')
      AND t1.mwfsy IS NULL

    /*====================================================================================================
    # 规则代码: AM00021
    # 目标接口: J1011-产品收益分配信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 权益登记日期不能在分配日期之前。
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00021'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_divid t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qydjrq < t1.fprq

    /*====================================================================================================
    # 规则代码: AM00022
    # 目标接口: J1011-产品收益分配信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [产品基本信息].{净值型产品标志}为“是”时，[产品收益分配信息].{实际分配收益}>=0，[产品收益分配信息].{现金分配金额}>=0，[产品收益分配信息].{再投资金额}>=0
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00022'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_divid t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.jzxcpbz = '1'
      AND (t1.sjfpsy < 0 OR t1.xjfpje < 0 OR t1.ztzje < 0)

    /*====================================================================================================
    # 规则代码: AM00023
    # 目标接口: J1011-产品收益分配信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [产品基本信息].{净值型产品标志}为“否”时，[产品收益分配信息].{应分配本金}>=0
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00023'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_divid t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.jzxcpbz = '0'
      AND t1.yfpbj < 0

    /*====================================================================================================
    # 规则代码: AM00024
    # 目标接口: J1011-产品收益分配信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {应分配收益}={分配基数}*{单位产品分配收益}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00024'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_divid t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND abs(t1.yfpsy - t1.fpjs * t1.dwcpfpsy) > 100

    /*====================================================================================================
    # 规则代码: AM00025
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {项目编号}为｛2000-营业总支出、2001-管理人报酬、200101-其中：暂估管理人报酬、2002-托管费、2003-销售服务费、2004-投资顾问费、2005-利息支出、
             200501-卖出回购金融资产利息支出、2006-信用减值损失、2007-税金及附加、2099-其他费用｝时，本月金额不能为负数
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00025'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_fin_profit t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 利润项目: 2000-营业总支出、2001-管理人报酬、200101-其中：暂估管理人报酬、2002-托管费、2003-销售服务费、2004-投资顾问费、
      --          2005-利息支出、200501-卖出回购金融资产利息支出、2006-信用减值损失、2007-税金及附加、2099-其他费用
      AND t1.xmbh IN ('2000', '2001', '200101', '2002', '2003', '2004', '2005', '200501', '2006', '2007', '2099')
      AND t1.byje < 0

    /*====================================================================================================
    # 规则代码: AM00026
    # 目标接口: J1014-账户汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末账户数}={期末有持仓的户数}+{期末无持仓的户数}
             {期末账户数}={截至期末从未有交易的户数}+{截至期末曾经有交易的户数}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00026'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.ztlb     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_acctnum_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.qmzhs <> t1.qmyccdhs + t1.qmwccdhs OR t1.qmzhs <> t1.jzqmcwyjydhs + t1.jzqmcjyjydhs)

    /*====================================================================================================
    # 规则代码: AM00027
    # 目标接口: J1014-账户汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末账户数}=(上期).{期末账户数}+{本期开户数}-{本期销户数}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00027'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.ztlb     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_acctnum_sum t1
             LEFT JOIN report_cisp.wdb_amp_sl_acctnum_sum t2
                       ON t1.djjgmc = t2.djjgmc
                           AND t1.djjgdm = t2.djjgdm
                           AND t1.ztlb = t2.ztlb
                           AND t1.tzzlx = t2.tzzlx
                           AND t1.sjrq = (SELECT max(tt1.sjrq)
                                          FROM report_cisp.wdb_amp_sl_acctnum_sum tt1
                                          WHERE tt1.sjrq < t1.sjrq)
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmzhs <> t2.qmzhs + t1.bqkhs - t1.bqxhs

    /*====================================================================================================
    # 规则代码: AM00028
    # 目标接口: J1015-投资者年龄结构
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {年龄分段}不能为空
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00028'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.nlfd     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_agestruct t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.nlfd IS NULL

    /*====================================================================================================
    # 规则代码: AM00029
    # 目标接口: J1015-投资者年龄结构
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {有效投资者数量}>0
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00029'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.nlfd     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_agestruct t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.yxtzzsl <= 0

    /*====================================================================================================
    # 规则代码: AM00030
    # 目标接口: J1015-投资者年龄结构
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {持有市值}>0
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00030'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.nlfd     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_agestruct t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cysz <= 0

    /*====================================================================================================
    # 规则代码: AM00031
    # 目标接口: J1016-投资者份额结构
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {有效个人投资者数量}>=0
             {有效机构投资者数量}>=0
             {有效产品投资者数量}>=0
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00031'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.nlfd     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_shrstruct t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.yxgrtzzsl < 0 OR t1.yxjgtzzsl < 0 OR t1.yxcptzzsl < 0)

    /*====================================================================================================
    # 规则代码: AM00032
    # 目标接口: J1017-交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: 场内交易第17-23项应该填写0
             17{手续费}、18{手续费（归管理人）}、19{手续费（归销售机构）}、20{手续费（归产品资产）}、21{后收费}、22{后收费（归管理人）}、23{后收费（归销售机构）}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00032'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.sxf <> '0' OR t1.sxfgglr <> '0' OR t1.sxfgxsjg <> '0' OR t1.sxfgcpzc <> '0' OR t1.hsf <> '0' OR
           t1.hsfgglr <> '0' OR t1.hsfgxsjg <> '0')
      AND t1.cpdm NOT IN ('501059', '502000')

    /*====================================================================================================
    # 规则代码: AM00033
    # 目标接口: J1018-QDII及FOF交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 场内交易第17-23项应该填写0
             17{手续费}、18{手续费（归管理人）}、19{手续费（归销售机构）}、20{手续费（归产品资产）}、21{后收费}、22{后收费（归管理人）}、23{后收费（归销售机构）}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00033'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.sxf <> '0' OR t1.sxfgglr <> '0' OR t1.sxfgxsjg <> '0' OR t1.sxfgcpzc <> '0' OR t1.hsf <> '0' OR
           t1.hsfgglr <> '0' OR t1.hsfgxsjg <> '0')

    /*====================================================================================================
    # 规则代码: AM00034
    # 目标接口: J1019-净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {净申赎金额}={申购金额}-{赎回金额}
             {净申赎份数}={申购份数}-{赎回份数}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00034'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_bignetrdm t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.jssje <> t1.sgje - t1.shje OR t1.jssfs <> t1.sgfs - t1.shfs)

    /*====================================================================================================
    # 规则代码: AM00035
    # 目标接口: J1020-QDII及FOF净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {净申赎金额}={申购金额}-{赎回金额}
             {净申赎份数}={申购份数}-{赎回份数}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00035'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_bignetrdm t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.jssje <> t1.sgje - t1.shje OR t1.jssfs <> t1.sgfs - t1.shfs)

    /*====================================================================================================
    # 规则代码: AM00036
    # 目标接口: J1021-份额汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {持有份额}>0
             {持有投资者数量}>0
             {持有市值}>=0
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00036'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_shr_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND (t1.cyfe <= 0 OR t1.cytzzsl <= 0 OR t1.cysz < 0)

    /*====================================================================================================
    # 规则代码: AM00037
    # 目标接口: J1022-QDII及FOF份额汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {持有份额}>0
             {持有投资者数量}>0
             {持有市值}>=0
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00037'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_shr_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.cyfe <= 0 OR t1.cytzzsl <= 0 OR t1.cysz < 0)

    /*====================================================================================================
    # 规则代码: AM00038
    # 目标接口: J1023-产品关系人购买情况
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 如果{投资者与本产品关系}非空，则{持有市值} > 0
             如果{投资者与本产品关系}非空，则{持有份额} <> 0
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00038'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_stkhldr_hold t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.tzzybcpgx IS NOT NULL
      AND (t1.cysz <= 0 OR t1.cyfe = 0)

    /*====================================================================================================
    # 规则代码: AM00039
    # 目标接口: J1025-客户情况
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {持有投资者数量}={个人投资者数量}+{机构投资者数量}+{产品投资者数量}
             {持有投资者数量}={单笔委托300万（含）以上的投资者数量}+{单笔委托300万以下的投资者数量}
             {持有投资者数量}>{质押客户数量}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00039'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_stkhldr_hold t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.cytzzsl <> t1.grtzzsl + t1.jgtzzsl + t1.cptzzsl
        OR t1.cytzzsl <> t1.dbwt300wysdtzzsl + t1.dbwt300wyxdtzzsl
        OR t1.cytzzsl <= t1.zykhsl)

    /*====================================================================================================
    # 规则代码: AM00040
    # 目标接口: J1026-资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: 任何一项，{期末市值}>0
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00040'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_assets t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.qmsz <= 0

    /*====================================================================================================
    # 规则代码: AM00041
    # 目标接口: J1027-QDII及FOF资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 任何一项，{期末市值}>0
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00041'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_qd_assets t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsz <= 0

    /*====================================================================================================
    # 规则代码: AM00042
    # 目标接口: J1027-QDII及FOF资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {资产类别}不能为其他非标准化资产
             如果{资产类别} = 其他标准化资产，则{备注}不能为空
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00042'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_qd_assets t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 299-其他非标准化资产
      AND (t1.zclb = '299'
        -- 199-其他标准化资产
        OR (t1.zclb = '199' AND t1.bz IS NULL))

    /*====================================================================================================
    # 规则代码: AM00043
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码} <> 全国中小企业股份转让系统
             {交易场所代码} <> 区域股权市场
             {交易场所代码} <> 银行间市场
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00043'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stock t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码: 106-全国中小企业股份转让系统、107-区域股权市场、101-银行间市场
      AND t1.jycsdm IN ('106', '107', '101')

    /*====================================================================================================
    # 规则代码: AM00044
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}=(上期).{期末数量}+{买入数量}-{卖出数量}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00044'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stock t1
             LEFT JOIN report_cisp.wdb_amp_inv_stock t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_stock tt1 WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
                           AND t1.zqdm = t2.zqdm
                           AND t1.zqmc = t2.zqmc
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl <> t2.qmsl + t1.mrsl - t1.mcsl

    /*====================================================================================================
    # 规则代码: AM00045
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {非流通股份数量}={流通受限股份数量}+{其他流通受限股份数量}
             {非流通股份市值}={流通受限股份市值}+{其他流通受限股份市值}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00045'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stock t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.fltgfsl <> t1.ltsxgfsl + t1.qtltsxgfsl OR t1.fltgfsz <> t1.ltsxgfsz + t1.qtltsxgfsz)

    /*====================================================================================================
    # 规则代码: AM00046
    # 目标接口: J1030-优先股投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码} <> 全国中小企业股份转让系统
             {交易场所代码} <> 区域股权市场
             {交易场所代码} <> 银行间市场
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00046'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_prestock t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码: 106-全国中小企业股份转让系统、107-区域股权市场、101-银行间市场
      AND t1.jycsdm IN ('106', '107', '101')

    /*====================================================================================================
    # 规则代码: AM00047
    # 目标接口: J1030-优先股投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}=(上期).{期末数量}+{买入数量}-{卖出数量}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00047'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_prestock t1
             LEFT JOIN report_cisp.wdb_amp_inv_prestock t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_prestock WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl <> t2.qmsl + t1.mrsl - t1.mcsl

    /*====================================================================================================
    # 规则代码: AM00048
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}=(上期).{期末数量}+{买入数量}-{卖出数量}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00048'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
             LEFT JOIN report_cisp.wdb_amp_inv_bond t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_bond WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl <> t2.qmsl + t1.mrsl - t1.mcsl

    /*====================================================================================================
    # 规则代码: AM00049
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 如果{影子价值}有数据，{产品类型}应当为货币基金或者短期债券类型基金（短期债需要从{债项评级}中判断）
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 0
    # 备注: 搁置,规则说明有歧义
    ====================================================================================================*/
    /*UNION ALL
    SELECT DISTINCT 'AM00049'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.yzjz IS NOT NULL
      -- 产品类别: 03-货币市场基金
      -- AND t2.cplb <>'03'
      AND t2.cplb <> '141'
      -- 债项评级: 短期债券有101-A-1、102-A-2、103-A-3、104-B、105-C、106-D
      AND t1.zxpj NOT IN ('101-A-1', '102-A-2', '103-A-3', '104-B', '105-C', '106-D')*/

    /*====================================================================================================
    # 规则代码: AM00050
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 如果资产支持证券数据存在，那么债券类别应当为资产支持证券类产品
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00050'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.zczczqlb IS NOT NULL
      -- 债券类别: 16-资产支持证券（在交易所挂牌）
      AND t1.zqlb <> '16'

    /*====================================================================================================
    # 规则代码: AM00051
    # 目标接口: J1035-同业存单投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 同业存单投资只能在银行间市场进行
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00051'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_ncd t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码: 101-银行间市场
      AND t1.jycsdm <> '101'

    /*====================================================================================================
    # 规则代码: AM00052
    # 目标接口: J1036-债券回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 敲定债券回购只能在银行间市场进行
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00052'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bdrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码: 101-银行间市场
      AND t1.jycsdm <> '101'

    /*====================================================================================================
    # 规则代码: AM00053
    # 目标接口: J1037-协议回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 敲定协议回购只能在银行间市场进行
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00053'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_agrmrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码: 101-银行间市场
      AND t1.jycsdm <> '101'

    /*====================================================================================================
    # 规则代码: AM00054
    # 目标接口: J1039-商品现货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}应该为"125-大连商品交易所"或者"126-郑州商品交易所"
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00054'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comspt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码: 125-大连商品交易所、126-郑州商品交易所
      AND t1.jycsdm NOT IN ('125', '126')

    /*====================================================================================================
    # 规则代码: AM00055
    # 目标接口: J1039-商品现货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {产品类别}应该为"171-商品基金（黄金）"或"172-商品基金（其他商品）"
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00055'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comspt t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 产品类别:
      --    公募基金: 171-商品基金（黄金）、172-商品基金（其他商品）
      AND t2.cplb NOT IN ('171', '172')

    /*====================================================================================================
    # 规则代码: AM00056
    # 目标接口: J1040-商品现货延期交收投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}应该为"125-大连商品交易所"或者"126-郑州商品交易所"
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00056'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comsptdly t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码: 125-大连商品交易所、126-郑州商品交易所
      AND t1.jycsdm NOT IN ('125', '126')

    /*====================================================================================================
    # 规则代码: AM00057
    # 目标接口: J1040-商品现货延期交收投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {产品类别}应该为"171-商品基金（黄金）"或"172-商品基金（其他商品）"
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00057'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comsptdly t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 产品类别:
      --    公募基金: 171-商品基金（黄金）、172-商品基金（其他商品）
      AND t2.cplb NOT IN ('171', '172')

    /*====================================================================================================
    # 规则代码: AM00058
    # 目标接口: J1041-金融期货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码} = "121-中国金融期货交易所"
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00058'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_finftr t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码: 121-中国金融期货交易所
      AND t1.jycsdm <> '121'

    /*====================================================================================================
    # 规则代码: AM00059
    # 目标接口: J1041-金融期货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}=(上期).{期末数量}+{开仓数量}-{平仓数量}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00059'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_finftr t1
             LEFT JOIN report_cisp.wdb_amp_inv_finftr t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_finftr WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
                           AND t1.hydm = t2.hydm
                           AND t1.mmfx = t2.mmfx
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl <> (t2.qmsl + t1.kcsl - t1.pcsl)

    /*====================================================================================================
    # 规则代码: AM00060
    # 目标接口: J1043-场内期权投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}应该为"102-上海证券交易所"或"103-深圳证券交易所"
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00060'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_opt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码: 102-上海证券交易所、103-深圳证券交易所
      AND t1.jycsdm NOT IN ('102', '103')

    /*====================================================================================================
    # 规则代码: AM00061
    # 目标接口: J1043-场内期权投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量} <> (上期).{期末数量}+{开仓数量}-{平仓数量}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00061'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_opt t1
             LEFT JOIN report_cisp.wdb_amp_inv_opt t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_opt WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl <> (t2.qmsl + t1.kcsl - t1.pcsl)

    /*====================================================================================================
    # 规则代码: AM00062
    # 目标接口: J1046-转融券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}应该为"102-上海证券交易所"或"103-深圳证券交易所"
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00062'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_refinance t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码: 102-上海证券交易所、103-深圳证券交易所
      AND t1.jycsdm NOT IN ('102', '103')

    /*====================================================================================================
    # 规则代码: AM00063
    # 目标接口: J1046-转融券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量} <> (上期).{期末数量}+{开仓数量}-{平仓数量}
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00063'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_refinance t1
             LEFT JOIN report_cisp.wdb_amp_inv_refinance t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_refinance WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl <> (t2.qmsl + t1.kcsl - t1.pcsl)

    /*====================================================================================================
    # 规则代码: AM00064
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: 本表必须存在记录
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00064'        AS gzdm        -- 规则代码
                  , v_pi_end_date_t1 AS sjrq        -- 数据日期
                  , 'NoDataFound'    AS cpdm        -- 产品代码
                  , pi_end_date      AS insert_time -- 插入时间，若测试，请注释本行
                  , 0                AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE NOT exists(SELECT 1
                     FROM report_cisp.wdb_amp_prod_baseinfo tt1
                     WHERE tt1.jgdm = '70610000'
                       AND tt1.status NOT IN ('3', '5')
                       AND tt1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
    )

    /*====================================================================================================
    # 规则代码: AM00065
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {管理人统一社会信用代码}的长度必须为18位
             {托管人统一社会信用代码}的长度必须为18位
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00065'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND (length(t1.glrtyshxydm) <> 18 OR length(t1.tgrtyshxydm) <> 18)

    /*====================================================================================================
    # 规则代码: AM00066
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: ｛投资顾问统一社会信用代码｝不为空时，｛投资顾问统一社会信用代码｝的长度必须为18位
             ｛律师事务所统一社会信用代码｝不为空时，｛律师事务所统一社会信用代码｝的长度必须为18位
             ｛会计师事务所统一社会信用代码｝不为空时，｛会计师事务所统一社会信用代码｝的长度必须为18位
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00066'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND ((t1.tzgwtyshxydm IS NOT NULL AND length(t1.tgrtyshxydm) <> 18)
        OR (t1.lsswstyshxydm IS NOT NULL AND length(t1.lsswstyshxydm) <> 18)
        OR (t1.kjsswstyshxydm IS NOT NULL AND length(t1.kjsswstyshxydm) <> 18))

    /*====================================================================================================
    # 规则代码: AM00067
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {产品类别}的值在<产品类别表>中必须存在
             {运作方式}的值在<运作方式表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00067'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 产品类别:
      --    公募基金: 111-股票型基金、119-股票型QDII基金、121-偏股混合型基金、122-偏债混合型基金、123-混合型基金（灵活配置或其他）、129-混合型QDII基金、131-债券基金、139-债券型QDII基金、141-货币基金、151-FOF基金、152-MOM基金、153-ETF联接、158-FOF型QDII基金、159-MOM型QDII基金、161-同业存单基金、171-商品基金（黄金）、172-商品基金（其他商品）、179-其他另类基金、180-REITS基金、198-其他QDII基金、199-以上范围外的公募基金
      --    大集合: 211-股票型、219-股票型QDII、221-偏股混合型、222-偏债混合型、223-混合型（灵活配置或其他）、229-混合型QDII、231-债券型、239-债券型QDII、241-货币型、251-FOF、252-MOM、253-ETF联接、258-FOF型QDII、259-MOM型QDII、261-同业存单型、271-商品（黄金）、272-商品（其他商品）、279-其他另类、280-REITS、298-其他QDII、299-以上范围外的大集合
      AND (t1.cplb NOT IN
           ('111', '119', '121', '122', '123', '129', '131', '139', '141', '151', '152', '153', '158', '159', '161',
            '171', '172', '179', '180', '198', '199',
            '211', '219', '221', '222', '223', '229', '231', '239', '241', '251', '252', '253', '258', '259', '261',
            '271', '272', '279', '280', '298', '299')
        -- 运作方式: 1-开放式、2-定期开放式、3-封闭式、4-开放式（其他）
        OR (t1.yzfs NOT IN ('1', '2', '3', '4')))

    /*====================================================================================================
    # 规则代码: AM00068
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {运作方式}为”定期开放式”时，{开放频率}的值在<开放频率表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00068'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 运作方式: 2-定期开放式
      AND t1.yzfs = '2'
      -- 开放频率: 2-周、3-月、4-季/三月、5-半年、6-年、7-一年以上、三年以下、8-三年以上、9-其他
      AND t1.kfpl NOT IN ('1', '2', '3', '4', '5', '6', '7', '8', '9')

    /*====================================================================================================
    # 规则代码: AM00069
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {受托责任}不为空时，{受托责任}的值在<受托责任表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00069'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.stzr IS NOT NULL
      -- 受托责任: 1-主动投资、2-被动投资、3-两者兼具
      AND t1.stzr NOT IN ('1', '2', '3')

    /*====================================================================================================
    # 规则代码: AM00070
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {结算币种}的值在<币种代码表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00070'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.stzr IS NOT NULL
      -- 币种代码: CNY-人民币、USD-美元、EUR-欧元、JPY-日元、HKD-港币、GBP-英镑、AUD-澳元、NZD-新西兰元、SGD-新加坡元、CHF-瑞士法郎、CAD-加拿大元、MYR-马来西亚林吉特、RUB-俄罗斯卢布
      AND t1.jsbz NOT IN ('CNY', 'USD', 'EUR', 'JPY', 'HKD', 'GBP', 'AUD', 'NZD', 'SGD', 'CHF', 'CAD', 'MYR', 'RUB')

    /*====================================================================================================
    # 规则代码: AM00071
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {管理费算法}的值在<管理费算法表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00071'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.stzr IS NOT NULL
      -- 管理费算法: 1-浮动管理费率、2-固定管理费率、3-固定管理费、4-无管理费
      AND t1.glfsf NOT IN ('1', '2', '3', '4')

    /*====================================================================================================
    # 规则代码: AM00072
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {管理费算法}为”固定管理费率”时，{管理费率}不能为空
             {管理费算法}为”固定管理费”时，{管理费率}为空
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00072'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.stzr IS NOT NULL
      -- 管理费算法: 1-浮动管理费率、2-固定管理费率、3-固定管理费、4-无管理费
      AND ((t1.glfsf = '2' AND t1.glfl IS NULL)
        OR (t1.glfsf = '3' AND t1.glfl IS NOT NULL))

    /*====================================================================================================
    # 规则代码: AM00073
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {产品类别}为“MOM基金”、“MOM型QDII基金”、“MOM”和“MOM型QDII”时，{子资产单元标志}必须只包含”0”或”1”
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00073'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.stzr IS NOT NULL
      -- 产品类别:
      --    公募基金: 152-MOM基金、159-MOM型QDII基金
      --    大集合: 252-MOM、253-ETF联接、259-MOM型QDII
      AND t1.cplb IN ('152', '159', '252', '259')
      AND t1.sfzzcdybz NOT IN ('0', '1')

    /*====================================================================================================
    # 规则代码: AM00074
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {子资产单元标志}为”是”时，{管理人中管理人产品代码}不能为空
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00074'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.stzr IS NOT NULL
      AND t1.sfzzcdybz = '1'
      AND t1.glrzglrcpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00075
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {净值型产品标志}必须只包含”0”或”1”
             {净值型产品标志}为”是”时，{估值方法}的值在<估值方法表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00075'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.stzr IS NOT NULL
      AND ((t1.jzxcpbz NOT IN ('0', '1'))
        -- 估值方法: 1-摊余成本法、2-市值法、3-成本法、4-摊余成本法和市值法混合估值
        OR (t1.jzxcpbz = '1' AND t1.gzff NOT IN ('1', '2', '3')))

    /*====================================================================================================
    # 规则代码: AM00076
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {养老目标基金标志}必须只包含”0”或”1”
             {内地互认基金标志}必须只包含”0”或”1”
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00076'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND ((t1.ylmbjjbz NOT IN ('0', '1'))
        OR (t1.ndhrjjbz NOT IN ('0', '1')))

    /*====================================================================================================
    # 规则代码: AM00077
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {避险策略基金标志}必须只包含”0”或”1”
             {避险策略基金标志}为”是”时，{保障义务人代码}的长度必须为18位，{保障义务人名称}不能为空
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00077'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.stzr IS NOT NULL
      AND ((t1.bxcljjbz NOT IN ('0', '1'))
        OR (t1.bxcljjbz = '1' AND (length(t1.bzywrdm) <> 18 OR t1.bzywrmc IS NULL)))

    /*====================================================================================================
    # 规则代码: AM00078
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {采用量化策略标志}必须只包含”0”或”1”
             {采用对冲策略标志}必须只包含”0”或”1”
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00078'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND ((t1.cylhclbz NOT IN ('0', '1'))
        OR (t1.cydcclbz NOT IN ('0', '1')))

    /*====================================================================================================
    # 规则代码: AM00079
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {上市交易标志}必须只包含”0”或”1”
             {上市基金类型}不为空时，{上市基金类型}的值在<上市基金类型表>中必须存在
             {上市交易标志}为”是”时，{上市交易场所}的值在<交易场所代码表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00079'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.stzr IS NOT NULL
      AND ((t1.ssjybz NOT IN ('0', '1'))
        -- 上市基金类型: 1-LOF、2-ETF、3-封闭式基金
        OR (t1.ssjjlx IS NOT NULL AND t1.ssjjlx NOT IN ('1', '2', '3'))
        -- 交易场所代码:
        --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
        --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
        OR (t1.ssjybz = '1' AND t1.ssjycs NOT IN ('101', '102', '103', '104', '105', '106',
                                                  '107', '108', '111', '112', '113', '121', '122', '123', '124',
                                                  '125', '126', '131', '132', '133', '134', '135', '136', '137',
                                                  '138', '138', '199', '200', '210', '220', '230', '240', '250',
                                                  '299')))

    /*====================================================================================================
    # 规则代码: AM00080
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {指数基金标志}必须只包含”0”或”1”
             {指数基金标志}为”是”时，{指数基金类型}的值在<指数基金类型表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00080'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.stzr IS NOT NULL
      AND ((t1.zsjjbz NOT IN ('0', '1'))
        -- 指数基金类型: 1-ETF、2-ETF联接、3-指数增强型基金、9-其他指数基金
        OR (t1.zsjjbz = '1' AND t1.zsjjlx NOT IN ('1', '2', '3', '9')))

    /*====================================================================================================
    # 规则代码: AM00081
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {产品类别}为“ETF联接”时，{目标基金代码}不能为空，{目标基金名称}不能为空
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00081'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.stzr IS NOT NULL
      -- 产品类别:
      --    公募基金: 153-ETF联接
      AND t1.cplb = '153'
      AND (t1.mbjjdm IS NULL OR t1.mbjjmc IS NULL)

    /*====================================================================================================
    # 规则代码: AM00082
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {启用侧袋标志}必须只包含”0”或”1
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00082'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.qycdbz NOT IN ('0', '1')

    /*====================================================================================================
    # 规则代码: AM00083
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {正式转型日期}、{转型前产品正式代码}不为空时，上一交易日<{正式转型日期}<={数据日期}，{转型前产品正式代码}的值在当期[产品基本信息].{产品代码}中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注: 简化,规则说明有歧义
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00083'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND (t1.zszxrq IS NOT NULL AND t1.zxqcpzsdm IS NOT NULL AND
           to_date(t1.zszxrq, 'yyyymmdd') > to_date(t1.sjrq, 'yyyymmdd'))

    /*====================================================================================================
    # 规则代码: AM00084
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {正式转型日期}与{转型前产品正式代码}同时为空或不为空
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00084'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
        AND t1.status NOT IN ('3', '5')
        AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
        AND (t1.zszxrq IS NULL AND t1.zxqcpzsdm IS NOT NULL)
       OR (t1.zszxrq IS NOT NULL AND t1.zxqcpzsdm IS NULL)

    /*====================================================================================================
    # 规则代码: AM00085
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: [产品基本信息]中存在的产品，必须报送[产品净值信息]（FOF、QDII等T+4批次报送的产品除外）。请核对并确保[产品基本信息]不存在募集期、未成立、已清盘、已终止的基金。对于暂停运作等特殊原因需要持续报送的基金，产品净值信息应当按照实际资产填报，可以填报0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1009-产品净值信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注: 简化,规则说明有歧义
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00085'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
             LEFT JOIN report_cisp.wdb_amp_prod_nav t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 产品类别:
      --    公募基金: 119-股票型QDII基金、129-混合型QDII基金、139-债券型QDII基金、151-FOF基金、152-MOM基金、153-ETF联接、158-FOF型QDII基金、159-MOM型QDII基金、198-其他QDII基金
      --    大集合:219-股票型QDII、229-混合型QDII、239-债券型QDII、251-FOF、252-MOM、253-ETF联接、258-FOF型QDII、259-MOM型QDII、298-其他QDII
      AND t1.cplb NOT IN
          ('119', '129', '139', '151', '152', '153', '158', '159', '198', '219', '229', '239', '251', '252', '253',
           '258', '259', '298')
      AND t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00086
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: [产品基本信息表].{目标基金代码}未在已报送{产品代码}名单中（需要维护一个全量的已报送产品代码名单），如果确认{目标基金代码}存在，请发邮件给cisp邮箱（cisp@csrc.gov.cn</span>）说明情况
             {目标基金代码}必须在{产品代码}里
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00086'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpzdm = t2.cpdm
                           AND t1.mbjjdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.mbjjdm IS NOT NULL
      AND t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00087
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: [产品基本信息表].{指数基金类型}与[产品基本信息表].{产品类别}冲突，{指数基金类型}为“ETF连接”的，{产品类别}必须为“153-ETF联接”或“253-ETF联接”
              [产品基本信息表].{产品类别}与[产品基本信息表].{指数基金类型}冲突，{产品类别}为“153-ETF联接”或“253-ETF联接”的，{指数基金类型}必须为“ETF连接”
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00087'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 指数基金类型: 1-ETF、2-ETF联接、3-指数增强型基金、9-其他指数基金
      -- 产品类别:
      --    公募基金: 153-ETF联接
      --    大集合: 253-ETF联接
      AND ((t1.zsjjlx = '2' AND t1.cplb NOT IN ('153', '253'))
        OR (t1.cplb IN ('153', '253') AND t1.zsjjlx <> '2'))

    /*====================================================================================================
    # 规则代码: AM00088
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {产品类别}不为“119-股票型QDII基金”、“129-混合型QDII基金”、“139-债券型QDII基金”、“158-FOF型QDII基金”、“159-MOM型QDII基金”、“198-其他QDII基金”、“219-股票型QDII”、“229-混合型QDII”、“239-债券型QDII”、“258-FOF型QDII”、“259-MOM型QDII”、“298-其他QDII”时，{境外托管人中文名称}、{境外投资顾问中文名称}和{境外投资顾问英文名称}必须为空
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00088'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 产品类别:
      --    公募基金: 119-股票型QDII基金、129-混合型QDII基金、139-债券型QDII基金、158-FOF型QDII基金、159-MOM型QDII基金、198-其他QDII基金
      --    大集合: 219-股票型QDII、229-混合型QDII、239-债券型QDII、258-FOF型QDII、259-MOM型QDII、298-其他QDII
      AND t1.cplb NOT IN ('119', '129', '139', '158', '159', '198', '219', '229', '239', '258', '259', '298')
      AND (t1.jwtgrzwmc IS NOT NULL OR t1.jwtzgwzwmc IS NOT NULL OR t1.jwtzgwywmc IS NOT NULL)

    /*====================================================================================================
    # 规则代码: AM00089
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {核准日期}<=当前系统日期
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00089'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND to_date(t1.hzrq, 'YYYYMMDD') >= to_data(t1.sjrq, 'YYYYMMDD')

    /*====================================================================================================
    # 规则代码: AM00090
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {注册批文变更日期}不为空时，{核准日期}<={注册批文变更日期}
             {注册批文变更日期}不为空时，{注册批文变更日期}<=当前系统日期
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00090'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.zcpwbgrq IS NOT NULL
      AND ((to_date(t1.hzrq, 'YYYYMMDD') > to_data(t1.zcpwbgrq, 'YYYYMMDD'))
        OR (to_date(t1.zcpwbgrq, 'YYYYMMDD') > to_data(t1.sjrq, 'YYYYMMDD')))

    /*====================================================================================================
    # 规则代码: AM00091
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {正式转型日期}不为空时，{正式转型日期}<=当前系统日期
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00091'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.zszxrq IS NOT NULL
      AND to_date(t1.zszxrq, 'YYYYMMDD') > to_data(t1.sjrq, 'YYYYMMDD')

    /*====================================================================================================
    # 规则代码: AM00092
    # 目标接口: J1002-产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {指数基金类型}为“1-ETF”、“2-ETF联接”、“9-其他指数基金”时，{受托责任}为“2-被动投资”
             {指数基金类型}为“3-指数增强型基金”时，受托责任为“3-两者兼具”
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00092'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 指数基金类型: 1-ETF、2-ETF联接、3-指数增强型基金、9-其他指数基金
      -- 受托责任: 1-主动投资、2-被动投资、3-两者兼具
      AND ((t1.zsjjlx IN ('1', '2', '9') AND t1.stzr <> '2')
        OR (t1.zsjjlx = '3' AND t1.stzr <> '3'))

    /*====================================================================================================
    # 规则代码: AM00093
    # 目标接口: J1003-下属产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {产品主代码}的值在[产品基本信息].{产品代码}中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00093'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_subprod_baseinfo t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpzdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00094
    # 目标接口: J1003-下属产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {上市交易标志}必须只包含”0”或”1”
             {上市交易标志}为”是”时，{上市交易场所}的值在<交易场所代码表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00094'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_subprod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND (t1.ssjybz NOT IN ('1', '0')
        -- 交易场所代码:
        --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
        --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
        OR (t1.ssjybz = '1' AND t1.ssjycs NOT IN ('101', '102', '103', '104', '105', '106',
                                                  '107', '108', '111', '112', '113', '121', '122', '123', '124',
                                                  '125', '126', '131', '132', '133', '134', '135', '136', '137',
                                                  '138', '138', '199', '200', '210', '220', '230', '240', '250',
                                                  '299')))

    /*====================================================================================================
    # 规则代码: AM00095
    # 目标接口: J1003-下属产品基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {内地互认基金份额标志}必须只包含”0”或”1”
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00095'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_subprod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.ndhrjjfebz NOT IN ('0', '1')

    /*====================================================================================================
    # 规则代码: AM00096
    # 目标接口: J1004-产品侧袋基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {产品主代码}的值在[产品基本信息].{产品代码}中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00096'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_pocket t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpzdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00097
    # 目标接口: J1004-产品侧袋基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {当前状态}必须只包含”0”或”1”
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00097'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_pocket t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.dqzt NOT IN ('0', '1')

    /*====================================================================================================
    # 规则代码: AM00098
    # 目标接口: J1004-产品侧袋基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {侧袋结束日期}不为空时，{侧袋结束日期}>={侧袋启用日期}
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00098'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_pocket t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.cdjsrq IS NOT NULL
      AND t1.cdjsrq < t1.cdqyrq

    /*====================================================================================================
    # 规则代码: AM00099
    # 目标接口: J1004-产品侧袋基本信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {当前状态}为“1”时，{涉及投资者数量}=0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00099'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_pocket t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.dqzt = '1'
      AND t1.sjtzzsl <> 0

    /*====================================================================================================
    # 规则代码: AM00100
    # 目标接口: J1005-产品投资比例限制
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {产品代码}的值在[产品基本信息].{产品代码}中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00100'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_invlmt t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00101
    # 目标接口: J1005-产品投资比例限制
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {股票类资产投资最低比例}<={股票类资产投资最高比例}<=1
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00101'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_invlmt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND (t1.gplzctzzdbl > t1.gplzctzzgbl OR t1.gplzctzzdbl > 1 OR t1.gplzctzzgbl > 1)

    /*====================================================================================================
    # 规则代码: AM00102
    # 目标接口: J1005-产品投资比例限制
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {香港市场的股票类资产投资最高比例}<=1
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00102'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_invlmt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.xgscgplzctzzgbl > 1

    /*====================================================================================================
    # 规则代码: AM00103
    # 目标接口: J1005-产品投资比例限制
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {债券类资产投资最低比例}<={债券类资产投资最高比例}<=1
             {货币类资产投资最低比例}<={货币类资产投资最高比例}<=1
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00103'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_invlmt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND ((t1.zqlzctzzdbl > t1.zqlzctzzgbl OR t1.zqlzctzzdbl > 1 OR t1.zqlzctzzgbl > 1)
        OR (t1.hblzctzzdbl > t1.hblzctzdgbl OR t1.hblzctzzdbl > 1 OR t1.hblzctzdgbl > 1))

    /*====================================================================================================
    # 规则代码: AM00104
    # 目标接口: J1005-产品投资比例限制
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: [产品基本信息].{产品类别}为“151-FOF基金”时，产品不用在本表报送
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00104'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_invlmt t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 产品类别:
      --    公募基金: 151-FOF基金
      AND t2.cplb = '151'

    /*====================================================================================================
    # 规则代码: AM00105
    # 目标接口: J3006-基金经理信息
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {产品代码}的值在[产品基本信息].{产品代码}中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00105'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_fundmngr t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00106
    # 目标接口: J3006-基金经理信息
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {证件类型}的值在<证件类型表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00106'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_fundmngr t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 证件类型:
      --    个人证件类型: 0101-身份证、0102-护照、0103-港澳居民来往内地通行证、0104-台湾居民来往大陆通行证、0105-军官证、0106-士兵证、010601-解放军士兵证、010602-武警士兵证、0107-户口本、0108-文职证、010801-解放军文职干部证、010802-武警文职干部证、0109-警官证、0110-社会保障号、0111-外国人永久居留证、0112-外国护照、0113-临时身份证、0114-港澳：回乡证、0115-台：台胞证、0116-港澳台居民居住证、0199-其他人员证件
      --    机构证件类型: 0201-组织机构代码、0202-工商营业执照、0203-社团法人注册登记证书、0204-机关事业法人成立批文、0205-批文、0206-军队凭证、0207-武警凭证、0208-基金会凭证、0209-特殊法人注册号、0210-统一社会信用代码、0211-行政机关、0212-社会团体、0213-下属机构（具有主管单位批文号）、0299-其他机构证件号
      --    产品证件类型: 0301-营业执照、0302-登记证书、0303-批文、0304-产品正式代码、0399-其它
      AND t1.zjlx NOT IN
          ('0101', '0102', '0103', '0104', '0105', '0106', '010601', '010602', '0107', '0108', '010801', '010802',
           '0109', '0110', '0111', '0112', '0113', '0114', '0115', '0116', '0199',
           '0201', '0202', '0203', '0204', '0205', '0206', '0207', '0208', '0209', '0210', '0211', '0212', '0213',
           '0299', '0301', '0302', '0303', '0304', '0399')

    /*====================================================================================================
    # 规则代码: AM00107
    # 目标接口: J3006-基金经理信息
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: [产品基本信息].{产品类别}首字符为“1”时，{公告日期}不能为空
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00107'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_fundmngr t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND substr(t2.cplb, 1, 1) = '1'
      AND t1.ggrq IS NULL

    /*====================================================================================================
    # 规则代码: AM00108
    # 目标接口: J3006-基金经理信息
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {数据日期}>={任职日期}
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00108'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_fundmngr t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND to_date(t1.sjrq, 'YYYYMMDD') < to_date(t1.rzrq, 'YYYYMMDD')

    /*====================================================================================================
    # 规则代码: AM00109
    # 目标接口: J3006-基金经理信息
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {公告日期}不为空时，{任职日期}={公告日期}或{任职日期}={公告日期}+1
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00109'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_fundmngr t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.ggrq IS NOT NULL
      AND ((to_date(t1.rzrq, 'YYYYMMDD') <> to_date(t1.ggrq, 'YYYYMMDD'))
        OR (to_date(t1.rzrq, 'YYYYMMDD') <> to_date(t1.ggrq, 'YYYYMMDD') + 1))

    /*====================================================================================================
    # 规则代码: AM00110
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {产品代码}的值在[产品基本信息].{产品代码}中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00110'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00111
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {合同已终止标志}的值必须为“0”或“1”
             {产品暂停运作标志}的值必须为“0”或“1”
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00111'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND ((t1.htyzzbz NOT IN ('1', '0'))
        OR (t1.cpztyzbz NOT IN ('1', '0')))

    /*====================================================================================================
    # 规则代码: AM00112
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {合同已终止标志}为“是“时，{合同终止日期}不能为空，{数据日期}>={合同终止日期}
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00112'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.htyzzbz = '1'
      AND (t1.htzzrq IS NULL OR t1.sjrq < t1.htzzrq)

    /*====================================================================================================
    # 规则代码: AM00113
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易状态}不为空时，{交易状态}的值在<交易状态表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00113'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.jyzt IS NOT NULL
      -- 交易状态: 1-可认购、2-停止认购、3-可申购赎回、4-可申购不可赎回、5-不可申购可赎回、6-停止申购赎回
      AND t1.jyzt NOT IN ('1', '2', '3', '4', '5', '6')

    /*====================================================================================================
    # 规则代码: AM00114
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 所有[下属产品运行信息].{交易状态}相同时，{交易状态}不能为空，{交易状态}==[下属产品运行信息].{交易状态}
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 0
    # 备注: 搁置,规则说明有歧义
    ====================================================================================================*/
    /*UNION ALL
    SELECT DISTINCT 'AM00114'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
             LEFT JOIN report_cisp.wdb_amp_subprod_oprt t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.jyzt IS NULL*/

    /*====================================================================================================
    # 规则代码: AM00115
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ([产品基本信息].{运作方式}为“定期开放式”，{交易状态}为“可申购不可赎回”)时，{申购开始日期}不能为空，{赎回开始日期}为空
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00115'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 运作方式： 1-开放式、2-定期开放式、3-封闭式、4-开放式（其他）
      AND t2.yzfs = '2'
      -- 交易状态: 1-可认购、2-停止认购、3-可申购赎回、4-可申购不可赎回、5-不可申购可赎回、6-停止申购赎回
      AND t1.jyzt = '4'
      AND ((t1.sgksrq IS NULL)
        OR (t1.shksrq IS NOT NULL))

    /*====================================================================================================
    # 规则代码: AM00116
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ([产品基本信息].{运作方式}为“定期开放式”，{交易状态}为“不可申购可赎回”)时，{赎回开始日期}不能为空
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00116'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 运作方式： 1-开放式、2-定期开放式、3-封闭式、4-开放式（其他）
      AND t2.yzfs = '2'
      -- 交易状态: 1-可认购、2-停止认购、3-可申购赎回、4-可申购不可赎回、5-不可申购可赎回、6-停止申购赎回
      AND t1.jyzt = '5'
      AND t1.shksrq IS NULL

    /*====================================================================================================
    # 规则代码: AM00117
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ([产品基本信息].{运作方式}为“定期开放式”，{交易状态}为“可申购赎回”)时，{赎回开始日期}不能为空，{申购开始日期}不能为空
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00117'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 运作方式： 1-开放式、2-定期开放式、3-封闭式、4-开放式（其他）
      AND t2.yzfs = '2'
      -- 交易状态: 1-可认购、2-停止认购、3-可申购赎回、4-可申购不可赎回、5-不可申购可赎回、6-停止申购赎回
      AND t1.jyzt = '3'
      AND ((t1.sgksrq IS NULL) OR (t1.sgksrq IS NULL))

    /*====================================================================================================
    # 规则代码: AM00118
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [产品基本信息].{上市交易标志}为“是”时，“上市日期”不能为空
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00118'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.ssjybz = '1'
      AND t1.ssrq IS NULL

    /*====================================================================================================
    # 规则代码: AM00119
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ([产品基本信息].{产品类别}首字符为“1"，{产品暂停运作标志}为“否”，{合同已终止标志}为“否”)时，{总份数}>0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00119'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND substr(t2.cplb, 1, 1) = '1'
      AND t1.cpztyzbz = '0'
      AND t1.htyzzbz = '0'
      AND t1.zfs <= 0

    /*====================================================================================================
    # 规则代码: AM00120
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 上期存在未终止的产品在本表必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00120'   AS gzdm        -- 规则代码
                  , t2.sjrq     AS sjrq        -- 数据日期
                  , t2.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
             RIGHT JOIN report_cisp.wdb_amp_prod_oprt t2
                        ON t1.jgdm = t2.jgdm
                            AND t1.status = t2.status
                            AND t1.sjrq =
                                (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_prod_oprt WHERE tt1.sjrq < t1.sjrq)
                            AND t1.cpdm = t2.cpdm
    WHERE t2.jgdm = '70610000'
      AND t2.status NOT IN ('3', '5')
      AND t2.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.htyzzbz = '0'
      AND t1.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00121
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ({管理费率}>0，[产品基本信息].{管理费算法}为“固定管理费率”)时，{管理费率}==[产品基本信息].{管理费率}
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00121'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.glfl > 0
      -- 管理费算法: 1-浮动管理费率、2-固定管理费率、3-固定管理费、4-无管理费
      AND t2.glfsf = '2'
      AND t1.glfl <> t2.glfl

    /*====================================================================================================
    # 规则代码: AM00122
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ([产品基本信息].{估值完成天数}<=1，[产品基本信息].{子资产单元标志}不为“是”，{合同已终止标志}为“否”，{总份数}>0)时，在[份额汇总]中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1002-产品基本信息 J1021-份额汇总
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前 T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00122'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_sl_shr_sum t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.gzwcts <= 1
      AND t2.sfzzcdybz <> 1
      AND t1.htyzzbz = 0
      AND t1.zfs > 0
      AND t3.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00123
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ([产品基本信息].{产品类别}为“货币基金”、“货币型”，{合同已终止标志}=“否”)时，在[货币市场基金监控]中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1002-产品基本信息 J1049-货币市场基金监控
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前 T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00123'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_rsk_monitor t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 产品类别:
      --    公募基金: 141-货币基金
      --    大集合: 241-货币型
      AND t2.cplb IN ('141', '241')
      AND t1.htyzzbz = 0
      AND t3.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00124
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ({合同已终止标志}=“否”)时，[产品净值信息].{产品代码}+[QDII及FOF产品净值信息].{产品代码}与本表{产品代码}一致
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1009-产品净值信息 J1010-QDII及FOF产品净值信息
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前 T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00124'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
             LEFT JOIN report_cisp.wdb_amp_prod_nav t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_prod_qd_nav t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND ((t2.cpdm IS NULL)
        OR (t3.cpdm IS NULL))

    /*====================================================================================================
    # 规则代码: AM00125
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [产品基本信息].{产品代码}的值在本表中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00125'   AS gzdm        -- 规则代码
                  , t2.sjrq     AS sjrq        -- 数据日期
                  , t2.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
             RIGHT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                        ON t1.jgdm = t2.jgdm
                            AND t1.status = t2.status
                            AND t1.sjrq = t2.sjrq
                            AND t1.cpdm = t2.cpdm
    WHERE t2.jgdm = '70610000'
      AND t2.status NOT IN ('3', '5')
      AND t2.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00126
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [产品基本信息].{产品类别}不为“119”、“129”、“139”、“151”、“152”、“153”、“158”、“159”、“198”、“219”、“229”、“239”、“251”、“252”、“253”、“258”、“259”、“298”，且{产品暂停运作标志}为“否”时，{产品代码}在[产品净值信息]中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1009-产品净值信息 J1002-产品基本信息
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前 T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00126'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_prod_nav t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 产品类别:
      --    公募基金: 119-股票型QDII基金、129-混合型QDII基金、139-债券型QDII基金、151-FOF基金、152-MOM基金、158-FOF型QDII基金、159-MOM型QDII基金、198-其他QDII基金
      --    大集合: 219-股票型QDII、229-混合型QDII、239-债券型QDII、251-FOF、252-MOM、253-ETF联接、258-FOF型QDII、259-MOM型QDII、298-其他QDII
      AND t2.cplb NOT IN ('119', '129', '139', '151', '152', '153', '158', '159', '198', '219', '229',
                          '239', '251', '252', '253', '258', '259', '298')
      AND t1.cpztyzbz = 0
      AND t3.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00127
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {合同已终止标志}为“1-是”时，{数据日期}<={合同终止日期}+1个交易日
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00127'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.htyzzbz = '1'
      AND to_date(t1.sjrq, 'YYYYMMDD') > to_date(t1.htzzrq, 'YYYYMMDD') + 1

    /*====================================================================================================
    # 规则代码: AM00128
    # 目标接口: J1008-下属产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {产品主代码}+{产品代码}的值在[下属产品基本信息]中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1003-下属产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00128'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_subprod_oprt t1
             LEFT JOIN report_cisp.wdb_amp_subprod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
                           AND t1.cpzdm = t2.cpzdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00129
    # 目标接口: J1008-下属产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {产品主代码}的值在[产品运行信息]中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1007-产品运行信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00129'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_subprod_oprt t1
             LEFT JOIN report_cisp.wdb_amp_prod_oprt t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
                           AND t1.cpzdm = t2.cpzdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00130
    # 目标接口: J1008-下属产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {合同已终止标志}的值必须为“0”或“1”
             {产品暂停运作标志}的值必须为“0”或“1”
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00130'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_subprod_oprt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND ((t1.htyzzbz NOT IN ('1', '0'))
        OR (t1.cpztyzbz NOT IN ('1', '0')))

    /*====================================================================================================
    # 规则代码: AM00131
    # 目标接口: J1008-下属产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {合同已终止标志}为“是“时，{合同终止日期}不能为空
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00131'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_subprod_oprt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.htyzzbz = '1'
      AND t1.htzzrq IS NULL

    /*====================================================================================================
    # 规则代码: AM00132
    # 目标接口: J1008-下属产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易状态}的值在<交易状态表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00132'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_subprod_oprt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易状态: 1-可认购、2-停止认购、3-可申购赎回、4-可申购不可赎回、5-不可申购可赎回、6-停止申购赎回
      AND t1.jyzt NOT IN ('1', '2', '3', '4', '5', '6')

    /*====================================================================================================
    # 规则代码: AM00133
    # 目标接口: J1008-下属产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {总份数}>=0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00133'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_subprod_oprt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.zfs < 0

    /*====================================================================================================
    # 规则代码: AM00134
    # 目标接口: J1008-下属产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [下属产品基本信息].{上市交易标志}为“是”时，{上市日期}不能为空
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1003-下属产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00134'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_subprod_oprt t1
             LEFT JOIN report_cisp.wdb_amp_subprod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
                           AND t1.cpzdm = t2.cpzdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.ssjybz = '1'
      AND t1.ssrq IS NULL

    /*====================================================================================================
    # 规则代码: AM00135
    # 目标接口: J1008-下属产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ({合同已终止标志}为“否”，[产品收益分配信息].{产品代码}不等于[产品收益分配信息].{产品主代码})时，[产品收益分配信息].{产品主代码}+[产品收益分配信息].{产品代码}的值在[下属产品基本信息]中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1003-下属产品基本信息 J1011-产品收益分配信息
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前 T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00135'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_subprod_oprt t1
             LEFT JOIN report_cisp.wdb_amp_prod_divid t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
                           AND t1.cpzdm = t2.cpzdm
             LEFT JOIN report_cisp.wdb_amp_subprod_baseinfo t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
                           AND t1.cpzdm = t3.cpzdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.htyzzbz = '0'
      AND t2.cpdm <> t2.cpzdm
      AND (t3.cpdm IS NULL OR t3.cpzdm IS NULL)

    /*====================================================================================================
    # 规则代码: AM00136
    # 目标接口: J1009-产品净值信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: 本表必须存在记录
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00136'        AS gzdm        -- 规则代码
                  , v_pi_end_date_t1 AS sjrq        -- 数据日期
                  , 'NoDataFound'    AS cpdm        -- 产品代码
                  , pi_end_date      AS insert_time -- 插入时间，若测试，请注释本行
                  , 0                AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_nav t1
    WHERE NOT exists(SELECT 1
                     FROM report_cisp.wdb_amp_prod_nav tt1
                     WHERE tt1.jgdm = '70610000'
                       AND tt1.status NOT IN ('3', '5')
                       AND tt1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
    )

    /*====================================================================================================
    # 规则代码: AM00137
    # 目标接口: J1009-产品净值信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {产品代码}的值在[产品基本信息].{产品代码}中必须存在，且[产品基本信息].{估值完成天数}<=1
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00137'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_nav t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND (t2.cpdm IS NULL OR t2.gzts > 1)

    /*====================================================================================================
    # 规则代码: AM00138
    # 目标接口: J1009-产品净值信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: ([产品基本信息].{产品类别}为“货币基金”、“货币型”)时，{每万份收益}不能为空
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00138'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_nav t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 产品类别:
      --    公募基金: 141-货币基金
      --    大集合: 241-货币型
      AND t2.cplb IN ('141', '241')
      AND t1.mwfsy IS NULL

    /*====================================================================================================
    # 规则代码: AM00139
    # 目标接口: J1009-产品净值信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: 产品代码必须唯一
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00139'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_nav t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
    GROUP BY t1.sjrq, t1.cpdm
    HAVING count(t1.cpdm) > 1

    /*====================================================================================================
    # 规则代码: AM00140
    # 目标接口: J1009-产品净值信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {产品代码}的值在[资产组合].{产品代码}中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1026-资产组合
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00140'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_nav t1
             LEFT JOIN report_cisp.wdb_amp_inv_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00141
    # 目标接口: J1010-QDII及FOF产品净值信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {产品代码}的值在[产品基本信息].{产品代码}中必须存在，且[产品基本信息].{估值完成天数}>=1
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00141'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_qd_nav t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t2.cpdm IS NULL OR t2.gzwcts < 1)

    /*====================================================================================================
    # 规则代码: AM00142
    # 目标接口: J1010-QDII及FOF产品净值信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ([产品基本信息].{产品类别}为“货币基金”、“货币型”)时，{每万份收益}不能为空
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00142'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_qd_nav t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 产品类别:
      --    公募基金: 141-货币基金
      --    大集合: 241-货币型
      AND t2.cplb IN ('141', '241')
      AND t1.mwfsy IS NULL

    /*====================================================================================================
    # 规则代码: AM00143
    # 目标接口: J1010-QDII及FOF产品净值信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 产品代码必须唯一
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00143'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_qd_nav t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
    GROUP BY t1.sjrq, t1.cpdm
    HAVING count(t1.cpdm) > 1

    /*====================================================================================================
    # 规则代码: AM00144
    # 目标接口: J1010-QDII及FOF产品净值信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {资产总值}<{资产净值}时，{备注}不能为空
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00144'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_qd_nav t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.zczz < t1.zcjz
      AND t1.bz IS NULL

    /*====================================================================================================
    # 规则代码: AM00145
    # 目标接口: J1010-QDII及FOF产品净值信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF产品净值信息]存在的产品，不能同时在[产品净值信息]中报送
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1009-产品净值信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00145'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_qd_nav t1
             LEFT JOIN report_cisp.wdb_amp_prod_nav t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.cpdm IS NOT NULL

    /*====================================================================================================
    # 规则代码: AM00146
    # 目标接口: J1010-QDII及FOF产品净值信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [产品基本信息].{产品代码}的值在[产品净值信息]不存在，且[产品运行信息].{产品暂停运作标志}为“否”时，产品在本表中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 3
    # 其他接口: J1002-产品基本信息 J1009-产品净值信息 J1007-产品运行信息
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前 T+1日24:00前 T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00146'   AS gzdm        -- 规则代码
                  , t2.sjrq     AS sjrq        -- 数据日期
                  , t2.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_qd_nav t1
             RIGHT JOIN (SELECT tt1.jgdm
                              , tt1.status
                              , tt1.sjrq
                              , tt1.cpdm
                         FROM report_cisp.wdb_amp_prod_baseinfo tt1
                                  LEFT JOIN report_cisp.wdb_amp_prod_nav tt2
                                            ON tt1.jgdm = tt2.jgdm
                                                AND tt1.status = tt2.status
                                                AND tt1.sjrq = tt2.sjrq
                                                AND tt1.cpdm = tt2.cpdm
                                  LEFT JOIN report_cisp.wdb_amp_prod_oprt tt3
                                            ON tt1.jgdm = tt3.jgdm
                                                AND tt1.status = tt3.status
                                                AND tt1.sjrq = tt3.sjrq
                                                AND tt1.cpdm = tt3.cpdm
                         WHERE tt2.cpdm IS NOT NULL
                           AND tt3.cpztyzbz = '0') t2
                        ON t1.jgdm = t2.jgdm
                            AND t1.status = t2.status
                            AND t1.sjrq = t2.sjrq
                            AND t1.cpdm = t2.cpdm
    WHERE t2.jgdm = '70610000'
      AND t2.status NOT IN ('3', '5')
      AND t2.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00147
    # 目标接口: J1010-QDII及FOF产品净值信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {产品代码}的值在[QDII及FOF资产组合].{产品代码}中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1027-QDII及FOF资产组合
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00147'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_qd_nav t1
             LEFT JOIN report_cisp.wdb_amp_inv_qd_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00148
    # 目标接口: J1011-产品收益分配信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {产品主代码}的值在[产品基本信息].{产品代码}中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00148'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_divid t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpzdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00149
    # 目标接口: J1011-产品收益分配信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {分配日期}<={数据日期}
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00149'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_divid t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND to_date(t1.fprq, 'YYYYMMDD') > to_date(t1.sjrq, 'YYYYMMDD')

    /*====================================================================================================
    # 规则代码: AM00150
    # 目标接口: J1011-产品收益分配信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ([产品基本信息].{产品类别}首字符为“1"，[产品基本信息].{净值型产品标志}为“是”)时，{权益登记日期}<={分配日期}
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00150'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_divid t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND substr(t2.cplb, 1, 1) = '1'
      AND t2.jzxcpbz = '1'
      AND to_date(t1.qydjrq, 'YYYYMMDD') > to_date(t1.sjrq, 'YYYYMMDD')

    /*====================================================================================================
    # 规则代码: AM00151
    # 目标接口: J1011-产品收益分配信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {分配基数}>0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00151'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_divid t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.fpjs <= 0

    /*====================================================================================================
    # 规则代码: AM00152
    # 目标接口: J1011-产品收益分配信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [产品基本信息].{净值型产品标志}为“是”时，{实际分配收益}>=0，{现金分配金额}>=0，{再投资金额}>=0
             [产品基本信息].{净值型产品标志}为“否”时，{应分配本金}>=0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00152'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_divid t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND ((t2.jzxcpbz = '1' AND (t1.sjfpsy < 0 OR t1.xjfpsy < 0, t1.ztzje < 0))
        OR (t2.jzxcpbz = '0' AND t1.yfpbj < 0))

    /*====================================================================================================
    # 规则代码: AM00153
    # 目标接口: J1011-产品收益分配信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {应分配收益}<0或{实际分配收益}<0或{单位产品分配收益}<0时，｛备注｝不能为空
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00153'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_divid t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.sjfpsy < 0 OR t1.xjfpsy < 0, t1.ztzje < 0)
      AND t1.bz IS NULL

    /*====================================================================================================
    # 规则代码: AM00154
    # 目标接口: J3012-资产负债表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {产品代码}的值在[产品基本信息].{产品代码}中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00154'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_fin_balsht t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00155
    # 目标接口: J3012-资产负债表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: ([产品运行信息].{合同已终止标志}为“否”,[产品运行信息].{总份数}>0)时，在[资产负债表]中必须存在记录
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1007-产品运行信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00155'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_fin_balsht t1
             RIGHT JOIN report_cisp.wdb_amp_prod_oprt t2
                        ON t1.jgdm = t2.jgdm
                            AND t1.status = t2.status
                            AND t1.sjrq = t2.sjrq
                            AND t1.cpdm = t2.cpdm
    WHERE t2.jgdm = '70610000'
      AND t2.status NOT IN ('3', '5')
      AND t2.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.htyzzbz = '0'
      AND t2.zfs > 0
      AND t1.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00156
    # 目标接口: J3012-资产负债表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {项目编号}的值在<资产负债项目表>中必须存在
             <资产负债项目表>中的每个项目在[资产负债表]中都必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注: 暂简化
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00156'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_fin_balsht t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产负债项目:
      --    1000-资产合计、1001-银行存款、1002-结算备付金、1003-存出保证金、1004-衍生金融资产、1005-应收清算款、1006-应收利息、1007-应收股利、1008-应收申购款、1009-买入返售金融资产、1010-发放贷款和垫款、1011-交易性金融资产、101101-股票投资、101102-基金投资、101103-贵金属投资、1012-债权投资、101201-债券投资、101202-未挂牌资产支持证券投资、1013-其他债权投资、1014-其他权益工具投资、1015-长期股权投资、1099-其他资产
      --    2000-负债合计、2001-短期借款、2002-交易性金融负债、2003-衍生金融负债、2004-卖出回购金融资产款、2005-应付管理人报酬、2006-应付托管费、2007-应付销售服务费、2008-应付投资顾问费、2009-应交税费、2010-应付清算款、2011-应付赎回款、2012-应付利息、2013-应付利润、2099-其他负债
      --    3000-所有者权益合计、3001-实收资金、3002-其他综合收益、3003-未分配利润
      --    4000-负债和所有者权益合计
      AND t1.xmbh NOT IN
          ('1000', '1001', '1002', '1003', '1004', '1005', '1006', '1007', '1008', '1009', '1010', '1011', '101101',
           '101102', '101103', '1012', '101201', '101202', '1013', '1014', '1015', '1099', '2000', '2001', '2002',
           '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2099', '3000',
           '3001', '3002', '3003', '4000')

    /*====================================================================================================
    # 规则代码: AM00157
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: ([产品运行信息].{合同已终止标志}为“否”,[产品运行信息].{总份数}>0)时，在[利润表]中必须存在记录
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1007-产品运行信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00157'   AS gzdm        -- 规则代码
                  , t2.sjrq     AS sjrq        -- 数据日期
                  , t2.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_fin_profit t1
             RIGHT JOIN report_cisp.wdb_amp_prod_oprt t2
                        ON t1.jgdm = t2.jgdm
                            AND t1.status = t2.status
                            AND t1.sjrq = t2.sjrq
                            AND t1.cpdm = t2.cpdm
    WHERE t2.jgdm = '70610000'
      AND t2.status NOT IN ('3', '5')
      AND t2.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.htyzzbz = '0'
      AND t2.zfs > 0
      AND t1.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00158
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {产品代码}的值在[产品基本信息].{产品代码}中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00158'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_fin_profit t1
             LEFT JOIN dw.dim_time t2
                       ON t1.sjrq = t2.sk_date
             LEFT JOIN dw.dim_time t3
                       ON t2.wkdayno = t3.wkdayno
                           AND t3.isworkday = 1 --关联到最近的工作
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t4
                       ON t1.jgdm = t4.jgdm
                           AND t1.status = t4.status
                           AND t3.sk_date = t4.sjrq
                           AND t1.cpdm = t4.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- and t1.sjrq >= '202208031' --t4表数据从20220831开始
      AND t4.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00159
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {项目编号}的值在<利润项目表>中必须存在
             <利润项目表>中的每个项目在[利润表]中都必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注: 暂简化
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00159'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_fin_profit t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 利润项目:
      --    1000-营业总收入、1001-利息收入、1002-投资收益、100201-其中：以摊余成本计量的金融资产终止确认产生的收益、100211-股票投资收益、100212-基金投资收益、100213-债券投资收益、100214-资产支持证券投资收益、100215-贵金属投资收益、100216-衍生工具收益、100217-股利收益、100218-差价收入增值税抵减、100219-其他投资收益、1003-公允价值变动收益、1004-汇兑收益、1099-其他业务收入
      --    2000-营业总支出、2001-管理人报酬、200101-其中：暂估管理人报酬、2002-托管费、2003-销售服务费、2004-投资顾问费、2005-利息支出、200501-卖出回购金融资产利息支出、2006-信用减值损失、2007-税金及附加、2099-其他费用
      --    3000-利润总额、300001-所得税费用
      --    4000-净利润
      --    5000-其他综合收益的税后净额
      --    6000-综合收益总额
      AND t1.xmbh NOT IN
          ('1000', '1001', '1002', '100201', '100211', '100212', '100213', '100214', '100215', '100216', '100217',
           '100218', '100219', '1003', '1004', '1099',
           '2000', '2001', '200101', '2002', '2003', '2004', '2005', '200501', '2006', '2007', '2099',
           '3000', '300001',
           '4000',
           '5000',
           '6000')

    /*====================================================================================================
    # 规则代码: AM00160
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {项目编号}为“2000-营业总支出”、“2001-管理人报酬”、“200101-其中：暂估管理人报酬”、“2002-托管费”、“2003-销售服务费”、“2004-投资顾问费”、“2005-利息支出”、“200501-卖出回购金融资产利息支出”、“2006-信用减值损失”、“2007-税金及附加”、“2099-其他费用”时，{本月金额}>=0，{本年累计金额}>=0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00160'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_fin_profit t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 利润项目:
      --    2000-营业总支出、2001-管理人报酬、200101-其中：暂估管理人报酬、2002-托管费、2003-销售服务费、2004-投资顾问费、2005-利息支出、200501-卖出回购金融资产利息支出、2006-信用减值损失、2007-税金及附加、2099-其他费用
      AND t1.xmbh IN ('2000', '2001', '200101', '2002', '2003', '2004', '2005', '200501', '2006', '2007', '2099')
      AND (t1.byje < 0 OR t1.bnljje < 0)

    /*====================================================================================================
    # 规则代码: AM00161
    # 目标接口: J1014-账户汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {主体类别}的值在<主体类别代码表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00161'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_acctnum_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 主体类别代码: 01-个人、02-机构、03-产品
      AND t1.ztlb NOT IN ('01', '02', '03')

    /*====================================================================================================
    # 规则代码: AM00162
    # 目标接口: J1014-账户汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {投资者类型}的值在<投资者类型表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00162'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_acctnum_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 投资者类型:
      --    个人: 001-个人
      --    机构: 101-银行、102-银行子公司、103-保险公司、104-保险公司子公司、105-信托公司、106-财务公司、107-证券公司、108-证券公司子公司、109-基金管理公司、110-基金管理公司子公司、111-期货公司、112-期货公司子公司、113-私募基金管理人、114-其他金融机构、118-地方政府、119-国有企业、115-境内非金融机构、116-境外金融机构、117-境外非金融机构
      --    产品: 201-银行理财产品、202-银行子公司公募理财、203-银行子公司理财产品、204-信托计划、205-保险产品、206-保险公司资产管理计划、229-保险公司子公司公募基金、207-保险公司子公司资产管理计划、208-证券公司公募基金、209-证券公司资产管理计划、210-证券公司子公司公募基金、211-证券公司子公司资产管理计划、212-基金管理公司公募基金、213-基金管理公司资产管理计划、214-基金管理公司子公司资产管理计划、215-期货公司资产管理计划、216-期货公司子公司资产管理计划、217-私募投资基金、218-政府引导基金、219-全国社保基金、220-地方社保基金、221-基本养老保险基金、222-养老金产品、223-企业年金、224-中央职业年金、225-省属职业年金、226-社会公益基金（慈善基金、捐赠基金等）、227-境外资金（QFII）、228-境外资金（RQFII）、299-其他产品
      AND t1.tzzlx NOT IN ('001',
                           '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113',
                           '114', '118', '119', '115', '116', '117',
                           '201', '202', '203', '204', '205', '206', '229', '207', '208', '209', '210', '211', '212',
                           '213', '214',
                           '215', '216', '217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227',
                           '228', '299')

    /*====================================================================================================
    # 规则代码: AM00163
    # 目标接口: J1015-投资者年龄结构
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {年龄分段}的值在<年龄分段表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00163'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_agestruct t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 年龄分段: 1-30岁以下、2-[30岁～40岁）、3-[40岁～50岁）、4-[50岁～60岁）、5-60岁及60岁以上、9-未知
      AND t1.nlfd NOT IN ('1', '2', '3', '4', '5', '9')

    /*====================================================================================================
    # 规则代码: AM00164
    # 目标接口: J1015-投资者年龄结构
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {有效投资者人数}>0
            {持有市值}>0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00164'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_agestruct t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND ((t1.yxtzzrs <= 0)
        OR (t1.cysz <= 0))

    /*====================================================================================================
    # 规则代码: AM00165
    # 目标接口: J1016-投资者份额结构
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {持有市值分段}的值在<持有市值分段表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00165'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_shrstruct t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 持有市值分段: 1-1万元以下、2-[1万元～5万元）、3-[5万元～10万元）、4-[10万元～50万元）、5-[50～100万元）、6-[100～300万元）、7-[300～500万元）、8-[500～1000万元）、9-1000万元及1000万元以上
      AND t1.cyszfd NOT IN ('1', '2', '3', '4', '5', '6', '7', '8', '9')

    /*====================================================================================================
    # 规则代码: AM00166
    # 目标接口: J1016-投资者份额结构
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {有效个人投资者数量}>=0
             {有效机构投资者数量}>=0
             {有效产品投资者数量}>=0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00166'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_shrstruct t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.yxgrtzzsl < 0 OR t1.yxjgtzzsl < 0 OR t1.yxcptzzsl < 0)

    /*====================================================================================================
    # 规则代码: AM00167
    # 目标接口: J1017-交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {产品代码}的值在[产品基本信息].{产品代码}中必须存在，且[产品基本信息].{估值完成天数}<=1
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00167'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_tran_sum t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t2.cpdm IS NULL OR t2.gzwcts > 1)

    /*====================================================================================================
    # 规则代码: AM00168
    # 目标接口: J1017-交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {场内场外标志}的值必须为“0”或“1”
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00168'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cncwbz NOT IN ('1', '0')

    /*====================================================================================================
    # 规则代码: AM00169
    # 目标接口: J1017-交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {主体类别}的值在<主体类别代码表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00169'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 主体类别代码: 01-个人、02-机构、03-产品
      AND t1.ztlb NOT IN ('01', '02', '03')

    /*====================================================================================================
    # 规则代码: AM00170
    # 目标接口: J1017-交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {投资者类型}的值在<投资者类型表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00170'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 投资者类型:
      --    个人: 001-个人
      --    机构: 101-银行、102-银行子公司、103-保险公司、104-保险公司子公司、105-信托公司、106-财务公司、107-证券公司、108-证券公司子公司、109-基金管理公司、110-基金管理公司子公司、111-期货公司、112-期货公司子公司、113-私募基金管理人、114-其他金融机构、118-地方政府、119-国有企业、115-境内非金融机构、116-境外金融机构、117-境外非金融机构
      --    产品: 201-银行理财产品、202-银行子公司公募理财、203-银行子公司理财产品、204-信托计划、205-保险产品、206-保险公司资产管理计划、229-保险公司子公司公募基金、207-保险公司子公司资产管理计划、208-证券公司公募基金、209-证券公司资产管理计划、210-证券公司子公司公募基金、211-证券公司子公司资产管理计划、212-基金管理公司公募基金、213-基金管理公司资产管理计划、214-基金管理公司子公司资产管理计划、215-期货公司资产管理计划、216-期货公司子公司资产管理计划、217-私募投资基金、218-政府引导基金、219-全国社保基金、220-地方社保基金、221-基本养老保险基金、222-养老金产品、223-企业年金、224-中央职业年金、225-省属职业年金、226-社会公益基金（慈善基金、捐赠基金等）、227-境外资金（QFII）、228-境外资金（RQFII）、299-其他产品
      AND t1.tzzlx NOT IN ('001',
                           '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113',
                           '114', '118', '119', '115', '116', '117',
                           '201', '202', '203', '204', '205', '206', '229', '207', '208', '209', '210', '211', '212',
                           '213', '214',
                           '215', '216', '217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227',
                           '228', '299')

    /*====================================================================================================
    # 规则代码: AM00171
    # 目标接口: J1017-交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {主体类别}的值与{投资者类型}的值必须匹配
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00171'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (
        -- 主体类别代码: 01-个人
        -- 投资者类型:
        --    个人: 001-个人
        (t1.ztlb = '01' AND t1.tzzlx NOT IN ('001')) OR
            -- 主体类别代码: 02-机构
            -- 投资者类型:
            --    机构: 101-银行、102-银行子公司、103-保险公司、104-保险公司子公司、105-信托公司、106-财务公司、107-证券公司、108-证券公司子公司、109-基金管理公司、110-基金管理公司子公司、111-期货公司、112-期货公司子公司、113-私募基金管理人、114-其他金融机构、118-地方政府、119-国有企业、115-境内非金融机构、116-境外金融机构、117-境外非金融机构
        (t1.ztlb = '02' AND
         t1.tzzlx NOT IN ('101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113',
                          '114', '118', '119', '115', '116', '117')) OR
            -- 主体类别代码: 03-产品
            -- 投资者类型:
            --    产品: 201-银行理财产品、202-银行子公司公募理财、203-银行子公司理财产品、204-信托计划、205-保险产品、206-保险公司资产管理计划、229-保险公司子公司公募基金、207-保险公司子公司资产管理计划、208-证券公司公募基金、209-证券公司资产管理计划、210-证券公司子公司公募基金、211-证券公司子公司资产管理计划、212-基金管理公司公募基金、213-基金管理公司资产管理计划、214-基金管理公司子公司资产管理计划、215-期货公司资产管理计划、216-期货公司子公司资产管理计划、217-私募投资基金、218-政府引导基金、219-全国社保基金、220-地方社保基金、221-基本养老保险基金、222-养老金产品、223-企业年金、224-中央职业年金、225-省属职业年金、226-社会公益基金（慈善基金、捐赠基金等）、227-境外资金（QFII）、228-境外资金（RQFII）、299-其他产品
        (t1.ztlb = '03' AND
         t1.tzzlx NOT IN ('201', '202', '203', '204', '205', '206', '229', '207', '208', '209', '210', '211', '212',
                          '213', '214', '215', '216', '217', '218', '219', '220', '221', '222', '223', '224', '225',
                          '226', '227', '228', '299'))
        )

    /*====================================================================================================
    # 规则代码: AM00172
    # 目标接口: J1017-交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {确认业务代码}的值在<确认业务代码表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00172'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 确认业务代码: 120-认购、122-申购、124-赎回、143-红利发放、199-其他因非认申购赎回导致的份额变动
      AND t1.qrywdm NOT IN ('120', '122', '124', '143', '199')

    /*====================================================================================================
    # 规则代码: AM00173
    # 目标接口: J1017-交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {转入转出标志}的值在<转入转出标志表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00173'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 转入转出标志: 1-转出、2-转入
      AND t1.qrywdm NOT IN ('1', '2')

    /*====================================================================================================
    # 规则代码: AM00174
    # 目标接口: J1017-交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {确认份数}和{确认金额}不能同时为0，{参与交易账户数量}>=0，{费用合计}>=0，{过户费}>=0，{手续费}>=0，{手续费（归销售机构）}>=0，{手续费（归产品资产）}>=0，{后收费}>=0，{后收费（归管理人）}>=0，{后收费（归销售机构）}>=0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00174'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND ((t1.qrfs = 0 AND t1.qrje = 0)
        OR t1.cyjyzhsl < 0 OR t1.fyhj < 0 OR t1.ghf < 0
        OR t1.sxf < 0 OR t1.sxfgglr < 0 OR t1.sxfgxsjg < 0 OR t1.sxfgcpzc < 0
        OR t1.hsf < 0 OR t1.hsfgglr < 0 OR t1.hsfgxsjg < 0)

    /*====================================================================================================
    # 规则代码: AM00175
    # 目标接口: J1017-交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {场内场外标志}为“0-场内”时，{手续费}、{手续费（归管理人）}、{手续费（归销售机构）}、{手续费（归产品资产）}、{后收费}、{后收费（归管理人）}、{后收费（归销售机构）}的值必须为0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00175'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cncwbz = '0'
      AND (t1.sxf <> 0 OR t1.sxfgglr <> 0 OR t1.sxfgxsjg <> 0 OR t1.sxfgcpzc <> 0
        OR t1.hsf <> 0 OR t1.hsfgglr <> 0 OR t1.hsfgxsjg <> 0)

    /*====================================================================================================
    # 规则代码: AM00176
    # 目标接口: J1018-QDII及FOF交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {产品代码}的值在[产品基本信息].{产品代码}中必须存在，且[产品基本信息].{估值完成天数}>=1
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00176'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_tran_sum t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t2.cpdm IS NULL OR t2.gzwcts < 1)

    /*====================================================================================================
    # 规则代码: AM00177
    # 目标接口: J1018-QDII及FOF交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {场内场外标志}的值必须为“0”或“1”
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00177'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cncwbz NOT IN ('1', '0')

    /*====================================================================================================
    # 规则代码: AM00178
    # 目标接口: J1018-QDII及FOF交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {主体类别}的值在<主体类别代码表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00178'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 主体类别代码: 01-个人、02-机构、03-产品
      AND t1.ztlb NOT IN ('01', '02', '03')

    /*====================================================================================================
    # 规则代码: AM00179
    # 目标接口: J1018-QDII及FOF交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {投资者类型}的值在<投资者类型表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00179'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 投资者类型:
      --    个人: 001-个人
      --    机构: 101-银行、102-银行子公司、103-保险公司、104-保险公司子公司、105-信托公司、106-财务公司、107-证券公司、108-证券公司子公司、109-基金管理公司、110-基金管理公司子公司、111-期货公司、112-期货公司子公司、113-私募基金管理人、114-其他金融机构、118-地方政府、119-国有企业、115-境内非金融机构、116-境外金融机构、117-境外非金融机构
      --    产品: 201-银行理财产品、202-银行子公司公募理财、203-银行子公司理财产品、204-信托计划、205-保险产品、206-保险公司资产管理计划、229-保险公司子公司公募基金、207-保险公司子公司资产管理计划、208-证券公司公募基金、209-证券公司资产管理计划、210-证券公司子公司公募基金、211-证券公司子公司资产管理计划、212-基金管理公司公募基金、213-基金管理公司资产管理计划、214-基金管理公司子公司资产管理计划、215-期货公司资产管理计划、216-期货公司子公司资产管理计划、217-私募投资基金、218-政府引导基金、219-全国社保基金、220-地方社保基金、221-基本养老保险基金、222-养老金产品、223-企业年金、224-中央职业年金、225-省属职业年金、226-社会公益基金（慈善基金、捐赠基金等）、227-境外资金（QFII）、228-境外资金（RQFII）、299-其他产品
      AND t1.tzzlx NOT IN ('001',
                           '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113',
                           '114', '118', '119', '115', '116', '117',
                           '201', '202', '203', '204', '205', '206', '229', '207', '208', '209', '210', '211', '212',
                           '213', '214',
                           '215', '216', '217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227',
                           '228', '299')

    /*====================================================================================================
    # 规则代码: AM00180
    # 目标接口: J1018-QDII及FOF交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {主体类别}的值与{投资者类型}的值必须匹配
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00180'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (
        -- 主体类别代码: 01-个人
        -- 投资者类型:
        --    个人: 001-个人
        (t1.ztlb = '01' AND t1.tzzlx NOT IN ('001')) OR
            -- 主体类别代码: 02-机构
            -- 投资者类型:
            --    机构: 101-银行、102-银行子公司、103-保险公司、104-保险公司子公司、105-信托公司、106-财务公司、107-证券公司、108-证券公司子公司、109-基金管理公司、110-基金管理公司子公司、111-期货公司、112-期货公司子公司、113-私募基金管理人、114-其他金融机构、118-地方政府、119-国有企业、115-境内非金融机构、116-境外金融机构、117-境外非金融机构
        (t1.ztlb = '02' AND
         t1.tzzlx NOT IN ('101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113',
                          '114', '118', '119', '115', '116', '117')) OR
            -- 主体类别代码: 03-产品
            -- 投资者类型:
            --    产品: 201-银行理财产品、202-银行子公司公募理财、203-银行子公司理财产品、204-信托计划、205-保险产品、206-保险公司资产管理计划、229-保险公司子公司公募基金、207-保险公司子公司资产管理计划、208-证券公司公募基金、209-证券公司资产管理计划、210-证券公司子公司公募基金、211-证券公司子公司资产管理计划、212-基金管理公司公募基金、213-基金管理公司资产管理计划、214-基金管理公司子公司资产管理计划、215-期货公司资产管理计划、216-期货公司子公司资产管理计划、217-私募投资基金、218-政府引导基金、219-全国社保基金、220-地方社保基金、221-基本养老保险基金、222-养老金产品、223-企业年金、224-中央职业年金、225-省属职业年金、226-社会公益基金（慈善基金、捐赠基金等）、227-境外资金（QFII）、228-境外资金（RQFII）、299-其他产品
        (t1.ztlb = '03' AND
         t1.tzzlx NOT IN ('201', '202', '203', '204', '205', '206', '229', '207', '208', '209', '210', '211', '212',
                          '213', '214', '215', '216', '217', '218', '219', '220', '221', '222', '223', '224', '225',
                          '226', '227', '228', '299'))
        )

    /*====================================================================================================
    # 规则代码: AM00181
    # 目标接口: J1018-QDII及FOF交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {确认业务代码}的值在<确认业务代码表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00181'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 确认业务代码: 120-认购、122-申购、124-赎回、143-红利发放、199-其他因非认申购赎回导致的份额变动
      AND t1.qrywdm NOT IN ('120', '122', '124', '143', '199')

    /*====================================================================================================
    # 规则代码: AM00182
    # 目标接口: J1018-QDII及FOF交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {转入转出标志}的值在<转入转出标志表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00182'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 转入转出标志: 1-转出、2-转入
      AND t1.qrywdm NOT IN ('1', '2')

    /*====================================================================================================
    # 规则代码: AM00183
    # 目标接口: J1018-QDII及FOF交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {确认份数}和{确认金额}不能同时为0，{参与交易账户数量}>=0，{费用合计}>=0，{过户费}>=0，{手续费}>=0，{手续费（归销售机构）}>=0，{手续费（归产品资产）}>=0，{后收费}>=0，{后收费（归管理人）}>=0，{后收费（归销售机构）}>=0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00183'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND ((t1.qrfs = 0 AND t1.qrje = 0)
        OR t1.cyjyzhsl < 0 OR t1.fyhj < 0 OR t1.ghf < 0
        OR t1.sxf < 0 OR t1.sxfgglr < 0 OR t1.sxfgxsjg < 0 OR t1.sxfgcpzc < 0
        OR t1.hsf < 0 OR t1.hsfgglr < 0 OR t1.hsfgxsjg < 0)

    /*====================================================================================================
    # 规则代码: AM00184
    # 目标接口: J1018-QDII及FOF交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF交易汇总]中存在的产品，不能同时在[交易汇总]中报送
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口: J1017-交易汇总
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+2日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00184'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_tran_sum t1
             LEFT JOIN report_cisp.wdb_amp_sl_tran_sum t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.cpdm IS NOT NULL

    /*====================================================================================================
    # 规则代码: AM00185
    # 目标接口: J1018-QDII及FOF交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {场内场外标志}为“0-场内”时，{手续费}、{手续费（归管理人）}、{手续费（归销售机构）}、{手续费（归产品资产）}、{后收费}、{后收费（归管理人）}、{后收费（归销售机构）}的值必须为0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00185'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cncwbz = '0'
      AND (t1.sxf <> 0 OR t1.sxfgglr <> 0 OR t1.sxfgxsjg <> 0 OR t1.sxfgcpzc <> 0
        OR t1.hsf <> 0 OR t1.hsfgglr <> 0 OR t1.hsfgxsjg <> 0)

    /*====================================================================================================
    # 规则代码: AM00186
    # 目标接口: J1019-净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {产品代码}的值在[产品基本信息].{产品代码}中必须存在，且[产品基本信息].{估值完成天数}<=1
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00186'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_bignetrdm t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t2.cpdm IS NULL OR t2.gzwcts > 1)

    /*====================================================================================================
    # 规则代码: AM00187
    # 目标接口: J1019-净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {投资者类型}的值在<投资者类型表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00187'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_bignetrdm t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 投资者类型:
      --    个人: 001-个人
      --    机构: 101-银行、102-银行子公司、103-保险公司、104-保险公司子公司、105-信托公司、106-财务公司、107-证券公司、108-证券公司子公司、109-基金管理公司、110-基金管理公司子公司、111-期货公司、112-期货公司子公司、113-私募基金管理人、114-其他金融机构、118-地方政府、119-国有企业、115-境内非金融机构、116-境外金融机构、117-境外非金融机构
      --    产品: 201-银行理财产品、202-银行子公司公募理财、203-银行子公司理财产品、204-信托计划、205-保险产品、206-保险公司资产管理计划、229-保险公司子公司公募基金、207-保险公司子公司资产管理计划、208-证券公司公募基金、209-证券公司资产管理计划、210-证券公司子公司公募基金、211-证券公司子公司资产管理计划、212-基金管理公司公募基金、213-基金管理公司资产管理计划、214-基金管理公司子公司资产管理计划、215-期货公司资产管理计划、216-期货公司子公司资产管理计划、217-私募投资基金、218-政府引导基金、219-全国社保基金、220-地方社保基金、221-基本养老保险基金、222-养老金产品、223-企业年金、224-中央职业年金、225-省属职业年金、226-社会公益基金（慈善基金、捐赠基金等）、227-境外资金（QFII）、228-境外资金（RQFII）、299-其他产品
      AND t1.tzzlx NOT IN ('001',
                           '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113',
                           '114', '118', '119', '115', '116', '117',
                           '201', '202', '203', '204', '205', '206', '229', '207', '208', '209', '210', '211', '212',
                           '213', '214',
                           '215', '216', '217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227',
                           '228', '299')

    /*====================================================================================================
    # 规则代码: AM00188
    # 目标接口: J1019-净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {申购金额}>=0，{申购份数}>=0
             {赎回金额}>=0，{赎回份数}>=0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00188'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_bignetrdm t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND ((t1.sgje < 0 OR t1.sgfs < 0)
        OR (t1.shje < 0 OR t1.shfs < 0))

    /*====================================================================================================
    # 规则代码: AM00189
    # 目标接口: J1019-净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {净申赎金额}>=0时，{净申赎份数}>=0，{净申赎金额占基金资产净值比例}>=0
             {净申赎金额}<0时，{净申赎份数}<0，{净申赎金额占基金资产净值比例}<=0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00189'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_bignetrdm t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND ((t1.jssje >= 0 AND (t1.jssfs < 0 OR t1.jssjezjjzcjzbl < 0))
        OR (t1.jssje < 0 AND (t1.jssfs >= 0 OR t1.jssjezjjzcjzbl > 0)))

    /*====================================================================================================
    # 规则代码: AM00190
    # 目标接口: J1019-净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: |{净申赎金额}|>=1亿 OR |{净申赎金额占基金资产净值比例}|>=10%
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00190'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_bignetrdm t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (abs(t1.jssje) < 100000000 AND abs(t1.jssjezjjzcjzbl) < 0.1)

    /*====================================================================================================
    # 规则代码: AM00191
    # 目标接口: J1019-净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {主体类别}的值在<主体类别代码表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00191'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_bignetrdm t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 主体类别代码: 01-个人、02-机构、03-产品
      AND t1.ztlb NOT IN ('01', '02', '03')

    /*====================================================================================================
    # 规则代码: AM00192
    # 目标接口: J1020-QDII及FOF净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {产品代码}的值在[产品基本信息].{产品代码}中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00192'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_bignetrdm t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00193
    # 目标接口: J1020-QDII及FOF净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {主体类别}的值在<主体类别代码表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00193'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_bignetrdm t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 主体类别代码: 01-个人、02-机构、03-产品
      AND t1.ztlb NOT IN ('01', '02', '03')

    /*====================================================================================================
    # 规则代码: AM00194
    # 目标接口: J1020-QDII及FOF净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {投资者类型}的值在<投资者类型表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00194'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_bignetrdm t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 投资者类型:
      --    个人: 001-个人
      --    机构: 101-银行、102-银行子公司、103-保险公司、104-保险公司子公司、105-信托公司、106-财务公司、107-证券公司、108-证券公司子公司、109-基金管理公司、110-基金管理公司子公司、111-期货公司、112-期货公司子公司、113-私募基金管理人、114-其他金融机构、118-地方政府、119-国有企业、115-境内非金融机构、116-境外金融机构、117-境外非金融机构
      --    产品: 201-银行理财产品、202-银行子公司公募理财、203-银行子公司理财产品、204-信托计划、205-保险产品、206-保险公司资产管理计划、229-保险公司子公司公募基金、207-保险公司子公司资产管理计划、208-证券公司公募基金、209-证券公司资产管理计划、210-证券公司子公司公募基金、211-证券公司子公司资产管理计划、212-基金管理公司公募基金、213-基金管理公司资产管理计划、214-基金管理公司子公司资产管理计划、215-期货公司资产管理计划、216-期货公司子公司资产管理计划、217-私募投资基金、218-政府引导基金、219-全国社保基金、220-地方社保基金、221-基本养老保险基金、222-养老金产品、223-企业年金、224-中央职业年金、225-省属职业年金、226-社会公益基金（慈善基金、捐赠基金等）、227-境外资金（QFII）、228-境外资金（RQFII）、299-其他产品
      AND t1.tzzlx NOT IN ('001',
                           '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113',
                           '114', '118', '119', '115', '116', '117',
                           '201', '202', '203', '204', '205', '206', '229', '207', '208', '209', '210', '211', '212',
                           '213', '214',
                           '215', '216', '217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227',
                           '228', '299')

    /*====================================================================================================
    # 规则代码: AM00195
    # 目标接口: J1020-QDII及FOF净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {净申赎金额占基金资产净值比例}<=1
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00195'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_bignetrdm t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.jssjezjjzcjzbl > 1

    /*====================================================================================================
    # 规则代码: AM00196
    # 目标接口: J1020-QDII及FOF净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {申购金额}>=0，{申购份数}>=0
             {赎回金额}>=0，{赎回份数}>=0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00196'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_bignetrdm t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND ((t1.sgje < 0 OR t1.sgfs < 0)
        OR (t1.shje < 0 OR t1.shfs < 0))

    /*====================================================================================================
    # 规则代码: AM00197
    # 目标接口: J1020-QDII及FOF净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {净申赎金额}>=0时，{净申赎份数}>=0，{净申赎金额占基金资产净值比例}>=0
             {净申赎金额}<0时，{净申赎份数}<0，{净申赎金额占基金资产净值比例}<=0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00197'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_bignetrdm t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND ((t1.jssje >= 0 AND (t1.jssfs < 0 OR t1.jssjezjjzcjzbl < 0))
        OR (t1.jssje < 0 AND (t1.jssfs >= 0 OR t1.jssjezjjzcjzbl > 0)))

    /*====================================================================================================
    # 规则代码: AM00198
    # 目标接口: J1020-QDII及FOF净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: |{净申赎金额}|>=1亿 OR |{净申赎金额占基金资产净值比例}|>=10%
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00198'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_bignetrdm t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (abs(t1.jssje) < 100000000 AND abs(t1.jssjezjjzcjzbl) < 0.1)

    /*====================================================================================================
    # 规则代码: AM00199
    # 目标接口: J1020-QDII及FOF净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF净申赎超1亿或超基金资产净值10%客户]中存在的产品，不能同时在[净申赎超1亿或超基金资产净值10%客户]中报送
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1019-净申赎超1亿或超基金资产净值10%客户
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+2日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00199'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_bignetrdm t1
             LEFT JOIN report_cisp.wdb_amp_sl_bignetrdm t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.cpdm IS NOT NULL

    /*====================================================================================================
    # 规则代码: AM00200
    # 目标接口: J1005-产品投资比例限制
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {产品代码}的值在[产品基本信息].{产品代码}中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 0
    # 备注: 样例
    ====================================================================================================*/
    /*UNION ALL
    SELECT DISTINCT 'AM00200'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_invlmt t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t2.cpdm IS NULL*/

    /*====================================================================================================
    # 规则代码: AM00201
    # 目标接口: J1026-资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: 债券型基金债券的投资比例应大于等于80%
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1002-产品基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 0
    # 备注: 样例
    ====================================================================================================*/
    /*UNION ALL
    SELECT DISTINCT 'AM00201'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , tt1.zclb
               , tt2.cplb
               , sum(tt1.qmsz) OVER (PARTITION BY tt1.sjrq, tt1.cpdm, tt1.zclb) AS qmsz1
               , sum(tt1.qmsz) OVER (PARTITION BY tt1.sjrq, tt1.cpdm)           AS qmsz2
          FROM report_cisp.wdb_amp_inv_assets tt1
                   LEFT JOIN report_cisp.wdb_amp_prod_baseinfo tt2 ON tt1.jgdm = tt2.jgdm
              AND tt1.status = tt2.status
              AND tt1.cpdm = tt2.cpdm
              AND tt1.sjrq = tt2.sjrq) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 103-债券
      AND t1.zclb = '103'
      -- 131-债券基金、139-债券型QDII基金、231-债券型、239-债券型QDII
      AND t1.cplb IN ('131', '139', '231', '239')
      AND round(t1.qmsz1 * 100 / t1.qmsz2, 2) < 0.8*/

    ;
    COMMIT;
END;
/