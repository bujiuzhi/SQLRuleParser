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
    # 目标接口: J1004-产品侧袋基本信息
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
    # 其他接口传输时间: T+1日24:00前
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
    # 其他接口数量: 1
    # 其他接口: J1008-下属产品运行信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00114'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
             LEFT JOIN (SELECT tt1.jgdm
                             , tt1.status
                             , tt1.sjrq
                             , tt1.cpzdm
                             , tt1.jyzt
                             , count(DISTINCT tt1.jyzt) AS jyzt_count
                        FROM report_cisp.wdb_amp_subprod_oprt tt1
                        GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpzdm, tt1.jyzt
                        HAVING count(DISTINCT tt1.jyzt) = 1 -- 确保所有交易状态相同
    ) t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpzdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.jyzt IS NULL OR t1.jyzt = t2.jyzt)

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
                  , t1.ztlb     AS cpdm        -- 产品代码
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
                  , t1.ztlb     AS cpdm        -- 产品代码
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
    # 目标接口: J1021-份额汇总
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
    SELECT DISTINCT 'AM00200'        AS gzdm        -- 规则代码
                  , v_pi_end_date_t1 AS sjrq        -- 数据日期
                  , 'NoDataFound'    AS cpdm        -- 产品代码
                  , pi_end_date      AS insert_time -- 插入时间，若测试，请注释本行
                  , 0                AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_shr_sum t1
    WHERE NOT exists(SELECT 1
                     FROM report_cisp.wdb_amp_sl_shr_sum tt1
                     WHERE tt1.jgdm = '70610000'
                       AND tt1.status NOT IN ('3', '5')
                       AND tt1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
    )

    /*====================================================================================================
    # 规则代码: AM00201
    # 目标接口: J1021-份额汇总
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
    SELECT DISTINCT 'AM00201'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00202
    # 目标接口: J1021-份额汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
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
    SELECT DISTINCT 'AM00202'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_shr_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cncwbz NOT IN ('1', '0')

    /*====================================================================================================
    # 规则代码: AM00203
    # 目标接口: J1021-份额汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
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
    SELECT DISTINCT 'AM00203'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_shr_sum t1
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
    # 规则代码: AM00204
    # 目标接口: J1021-份额汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
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
    SELECT DISTINCT 'AM00204'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_shr_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 主体类别代码: 01-个人、02-机构、03-产品
      AND t1.ztlb NOT IN ('01', '02', '03')

    /*====================================================================================================
    # 规则代码: AM00205
    # 目标接口: J1021-份额汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
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
    SELECT DISTINCT 'AM00205'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_shr_sum t1
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
    # 规则代码: AM00206
    # 目标接口: J1021-份额汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {持有投资者数量}>0
             {持有份额}>0
             {持有市值}>=0
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
    SELECT DISTINCT 'AM00206'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_shr_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND (t1.cytzzsl <= 0 OR t1.cyfe <= 0 OR t1.cysz < 0)

    /*====================================================================================================
    # 规则代码: AM00207
    # 目标接口: J1022-QDII及FOF份额汇总
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
    SELECT DISTINCT 'AM00207'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_shr_sum t1
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
    # 规则代码: AM00208
    # 目标接口: J1022-QDII及FOF份额汇总
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
    SELECT DISTINCT 'AM00208'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_shr_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cncwbz NOT IN ('1', '0')

    /*====================================================================================================
    # 规则代码: AM00209
    # 目标接口: J1022-QDII及FOF份额汇总
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
    SELECT DISTINCT 'AM00209'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_shr_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 主体类别代码: 01-个人、02-机构、03-产品
      AND t1.ztlb NOT IN ('01', '02', '03')

    /*====================================================================================================
    # 规则代码: AM00210
    # 目标接口: J1022-QDII及FOF份额汇总
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
    SELECT DISTINCT 'AM00210'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_shr_sum t1
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
    # 规则代码: AM00211
    # 目标接口: J1022-QDII及FOF份额汇总
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
    SELECT DISTINCT 'AM00211'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_shr_sum t1
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
    # 规则代码: AM00212
    # 目标接口: J1022-QDII及FOF份额汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {持有投资者数量}>0
             {持有份额}>0
             {持有市值}>=0
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
    SELECT DISTINCT 'AM00212'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_shr_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.cytzzsl <= 0 OR t1.cyfe <= 0 OR t1.cysz < 0)

    /*====================================================================================================
    # 规则代码: AM00213
    # 目标接口: J1022-QDII及FOF份额汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ([产品基本信息].{估值完成天数}>=1，[产品基本信息].{子资产单元标志}不为“是”，[产品运行信息].{合同已终止标志}为“否”，[产品运行信息].{总份数}>0)时，在本表中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1002-产品基本信息 J1007-产品运行信息
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前 T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00213'   AS gzdm        -- 规则代码
                  , t2.sjrq     AS sjrq        -- 数据日期
                  , t2.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_shr_sum t1
             RIGHT JOIN report_cisp.wdb_amp_prod_baseinfo tt1
                        ON t1.jgdm = t2.jgdm
                            AND t1.status = t2.status
                            AND t1.sjrq = t2.sjrq
                            AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_prod_oprt tt2
                       ON t2.jgdm = t3.jgdm
                           AND t2.status = t3.status
                           AND t2.sjrq = t3.sjrq
                           AND t2.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.gzwcts >= 1
      AND t2.zzcdybz = '0'
      AND t3.htyzzbz = '0'
      AND t3.zfs > 0
      AND t1.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00214
    # 目标接口: J1022-QDII及FOF份额汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF份额汇总]中存在的产品，不能同时在[份额汇总]中报送
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1021-份额汇总
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00214'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_shr_sum t1
             LEFT JOIN report_cisp.wdb_amp_sl_shr_sum t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.cpdm IS NOT NULL

    /*====================================================================================================
    # 规则代码: AM00215
    # 目标接口: J1023-产品关系人购买情况
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
    SELECT DISTINCT 'AM00215'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_stkhldr_hold t1
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
    # 规则代码: AM00216
    # 目标接口: J1023-产品关系人购买情况
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {投资者证件类型}的值在<证件类型表>中必须存在
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
    SELECT DISTINCT 'AM00216'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_stkhldr_hold t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 证件类型:
      --    个人证件类型: 0101-身份证、0102-护照、0103-港澳居民来往内地通行证、0104-台湾居民来往大陆通行证、0105-军官证、0106-士兵证、010601-解放军士兵证、010602-武警士兵证、0107-户口本、0108-文职证、010801-解放军文职干部证、010802-武警文职干部证、0109-警官证、0110-社会保障号、0111-外国人永久居留证、0112-外国护照、0113-临时身份证、0114-港澳：回乡证、0115-台：台胞证、0116-港澳台居民居住证、0199-其他人员证件
      --    机构证件类型: 0201-组织机构代码、0202-工商营业执照、0203-社团法人注册登记证书、0204-机关事业法人成立批文、0205-批文、0206-军队凭证、0207-武警凭证、0208-基金会凭证、0209-特殊法人注册号、0210-统一社会信用代码、0211-行政机关、0212-社会团体、0213-下属机构（具有主管单位批文号）、0299-其他机构证件号
      --    产品证件类型: 0301-营业执照、0302-登记证书、0303-批文、0304-产品正式代码、0399-其它
      AND t1.tzzzjlx NOT IN
          ('0101', '0102', '0103', '0104', '0105', '0106', '010601', '010602', '0107', '0108', '010801', '010802',
           '0109', '0110', '0111', '0112', '0113', '0114', '0115', '0116', '0199',
           '0201', '0202', '0203', '0204', '0205', '0206', '0207', '0208', '0209', '0210', '0211', '0212', '0213',
           '0299', '0301', '0302', '0303', '0304', '0399')

    /*====================================================================================================
    # 规则代码: AM00217
    # 目标接口: J1023-产品关系人购买情况
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {持有份额}>=0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00217'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_stkhldr_hold t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cyfe < 0

    /*====================================================================================================
    # 规则代码: AM00218
    # 目标接口: J1023-产品关系人购买情况
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {投资者与本产品关系}的值在<投资者与本产品关系表>中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00218'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_stkhldr_hold t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 投资者与本产品关系: 1-本产品的管理人、2-本产品的管理人管理的其他公募基金、3-本产品的管理人管理的其他资产管理计划、4-本产品的管理人的母公司、5-本产品的管理人的母公司管理的其他公募基金、6-本产品的管理人的母公司管理的其他资产管理计划、7-本产品的管理人的子公司、8-本产品的管理人的子公司管理的其他公募基金、9-本产品的管理人的子公司管理的其他资产管理计划
      AND t1.tzzybcpgx NOT IN ('1', '2', '3', '4', '5', '6', '7', '8', '9')

    /*====================================================================================================
    # 规则代码: AM00219
    # 目标接口: J1023-产品关系人购买情况
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 上期[产品关系人购买情况].{持有份额}>0时，{持有份额}>=0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00219'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_stkhldr_hold t1
             LEFT JOIN report_cisp.wdb_amp_sl_stkhldr_hold t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = (SELECT max(tt1.sjrq)
                                          FROM report_cisp.wdb_amp_sl_stkhldr_hold tt1
                                          WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.cyfe > 0
      AND t1.cyfe < 0

    /*====================================================================================================
    # 规则代码: AM00220
    # 目标接口: J1023-产品关系人购买情况
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {投资者与本产品关系}不为空时，{持有市值}>0
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00220'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_stkhldr_hold t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.tzzybcpgx IS NOT NULL
      AND t1.cysz < 0

    /*====================================================================================================
    # 规则代码: AM00221
    # 目标接口: J1024-前200大投资者
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
    SELECT DISTINCT 'AM00221'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_top200hldr t1
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
    # 规则代码: AM00222
    # 目标接口: J1024-前200大投资者
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {投资者证件类型}的值在<证件类型表>中必须存在
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
    SELECT DISTINCT 'AM00222'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_top200hldr t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 证件类型:
      --    个人证件类型: 0101-身份证、0102-护照、0103-港澳居民来往内地通行证、0104-台湾居民来往大陆通行证、0105-军官证、0106-士兵证、010601-解放军士兵证、010602-武警士兵证、0107-户口本、0108-文职证、010801-解放军文职干部证、010802-武警文职干部证、0109-警官证、0110-社会保障号、0111-外国人永久居留证、0112-外国护照、0113-临时身份证、0114-港澳：回乡证、0115-台：台胞证、0116-港澳台居民居住证、0199-其他人员证件
      --    机构证件类型: 0201-组织机构代码、0202-工商营业执照、0203-社团法人注册登记证书、0204-机关事业法人成立批文、0205-批文、0206-军队凭证、0207-武警凭证、0208-基金会凭证、0209-特殊法人注册号、0210-统一社会信用代码、0211-行政机关、0212-社会团体、0213-下属机构（具有主管单位批文号）、0299-其他机构证件号
      --    产品证件类型: 0301-营业执照、0302-登记证书、0303-批文、0304-产品正式代码、0399-其它
      AND t1.tzzzjlx NOT IN
          ('0101', '0102', '0103', '0104', '0105', '0106', '010601', '010602', '0107', '0108', '010801', '010802',
           '0109', '0110', '0111', '0112', '0113', '0114', '0115', '0116', '0199',
           '0201', '0202', '0203', '0204', '0205', '0206', '0207', '0208', '0209', '0210', '0211', '0212', '0213',
           '0299', '0301', '0302', '0303', '0304', '0399')

    /*====================================================================================================
    # 规则代码: AM00223
    # 目标接口: J1024-前200大投资者
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
    SELECT DISTINCT 'AM00223'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_top200hldr t1
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
    # 规则代码: AM00224
    # 目标接口: J1024-前200大投资者
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {序号}>=1，{序号}<=200
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
    SELECT DISTINCT 'AM00224'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_top200hldr t1
    WHERE t1.jgdm = '70610000'
        AND t1.status NOT IN ('3', '5')
        AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
        AND t1.xh < 1
       OR t1.xh > 200

    /*====================================================================================================
    # 规则代码: AM00225
    # 目标接口: J1024-前200大投资者
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {持有份额}>0
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
    SELECT DISTINCT 'AM00225'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_top200hldr t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cyfe <= 0

    /*====================================================================================================
    # 规则代码: AM00226
    # 目标接口: J1024-前200大投资者
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
    SELECT DISTINCT 'AM00226'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_top200hldr t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 主体类别代码: 01-个人、02-机构、03-产品
      AND t1.ztlb NOT IN ('01', '02', '03')

    /*====================================================================================================
    # 规则代码: AM00227
    # 目标接口: J1024-前200大投资者
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
    SELECT DISTINCT 'AM00227'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_top200hldr t1
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
    # 规则代码: AM00228
    # 目标接口: J1024-前200大投资者
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [产品运行信息].{产品暂停运作标志}为“否”时，[产品基本信息].{产品代码}的值在本表中必须存在
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
    SELECT DISTINCT 'AM00228'   AS gzdm        -- 规则代码
                  , t2.sjrq     AS sjrq        -- 数据日期
                  , t2.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_top200hldr t1
             RIGHT JOIN report_cisp.wdb_amp_prod_oprt t2
                        ON t1.jgdm = t2.jgdm
                            AND t1.status = t2.status
                            AND t1.sjrq = t2.sjrq
                            AND t1.cpdm = t2.cpdm
    WHERE t2.jgdm = '70610000'
      AND t2.status NOT IN ('3', '5')
      AND t2.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.cpztyzbz = '0'
      AND t1.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00229
    # 目标接口: J1024-前200大投资者
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 同一{产品代码}的{序号}不能重复，且{序号}较小者的{持有市值}>={序号}较大者的{持有市值}
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
    SELECT DISTINCT 'AM00229'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.sjrq                                                                    AS sjrq
               , tt1.cpdm                                                                    AS cpdm
               , tt1.xh
               , count(1) OVER (PARTITION BY tt1.sjrq,tt2.cpdm,tt1.xh )                      AS cnt_xh
               , lead(tt1.cysz, 1, 0) OVER (PARTITION BY tt1.sjrq,tt1.cpdm ORDER BY tt1.xh ) AS lag_cysz
          FROM report_cisp.wdb_amp_sl_top200hldr tt1) t1
    WHERE t1.jgdm = '70610000'
        AND t1.status NOT IN ('3', '5')
        AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
        AND t1.cnt_xh > 1
       OR t1.cysz < t1.lag_cysz

    /*====================================================================================================
    # 规则代码: AM00230
    # 目标接口: J1025-客户情况
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
    SELECT DISTINCT 'AM00230'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_cust_hold t1
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
    # 规则代码: AM00231
    # 目标接口: J1025-客户情况
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ([产品运行信息].{合同已终止标志}为“否”,[产品运行信息].{总份数}>0)时，在[客户情况]中必须存在记录
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
    SELECT DISTINCT 'AM00231'   AS gzdm        -- 规则代码
                  , t2.sjrq     AS sjrq        -- 数据日期
                  , t2.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_cust_hold t1
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
    # 规则代码: AM00232
    # 目标接口: J1025-客户情况
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {持有投资者数量}>=0，{个人投资者数量}>=0，{机构投资者数量}>=0，{产品投资者数量}>=0
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
    SELECT DISTINCT 'AM00232'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_cust_hold t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.cytzzsl < 0 OR t1.grtzzsl < 0 OR t1.jgtzzsl < 0 OR t1.cptzzsl < 0)

    /*====================================================================================================
    # 规则代码: AM00233
    # 目标接口: J1025-客户情况
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ([产品基本信息].{产品类别}首字符为“1”，{持有投资者数量}>=200)时，[前200大投资者]的记录数==200
             ([产品基本信息].{产品类别}首字符为“1”，{持有投资者数量}<200)时，{持有投资者数量} ==[前200大投资者]的记录数
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1002-产品基本信息 J1024-前200大投资者
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前 T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00233'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_cust_hold t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN (SELECT tt1.jgdm
                             , tt1.status
                             , tt1.sjrq
                             , tt1.cpdm
                             , count(1) AS top200_count
                        FROM report_cisp.wdb_amp_top200_investors tt1
                        GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND substr(t2.cplb, 1, 1) = '1'
      AND ((t1.cytzzsl >= 200 AND (t3.top200_count IS NULL OR t3.top200_count <> 200))
        OR (t1.cytzzsl < 200 AND t1.cytzzsl <> coalesce(t3.top200_count, 0)))

    /*====================================================================================================
    # 规则代码: AM00234
    # 目标接口: J1025-客户情况
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {境外持有投资者数量}>0时，[产品基本信息].{内地互认基金标志}必须为“是”
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
    SELECT DISTINCT 'AM00234'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_cust_hold t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.jwcytzzsl > 0
      AND t2.ndhrjjbz <> '1'

    /*====================================================================================================
    # 规则代码: AM00235
    # 目标接口: J1025-客户情况
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {质押客户数量}不为空时，{持有投资者数量}>={质押客户数量}
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
    SELECT DISTINCT 'AM00235'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_cust_hold t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.zykhsl IS NOT NULL
      AND t1.cytzzsl < t1.zykhsl

    /*====================================================================================================
    # 规则代码: AM00236
    # 目标接口: J1026-资产组合
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
    SELECT DISTINCT 'AM00236'        AS gzdm        -- 规则代码
                  , v_pi_end_date_t1 AS sjrq        -- 数据日期
                  , 'NoDataFound'    AS cpdm        -- 产品代码
                  , pi_end_date      AS insert_time -- 插入时间，若测试，请注释本行
                  , 0                AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_assets t1
    WHERE NOT exists(SELECT 1
                     FROM report_cisp.wdb_amp_inv_assets tt1
                     WHERE tt1.jgdm = '70610000'
                       AND tt1.status NOT IN ('3', '5')
                       AND tt1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
    )

    /*====================================================================================================
    # 规则代码: AM00237
    # 目标接口: J1026-资产组合
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
    SELECT DISTINCT 'AM00237'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_assets t1
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
    # 规则代码: AM00238
    # 目标接口: J1026-资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
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
    SELECT DISTINCT 'AM00238'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_assets t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                            '107', '108', '111', '112', '113', '121', '122', '123', '124',
                            '125', '126', '131', '132', '133', '134', '135', '136', '137',
                            '138', '138', '199', '200', '210', '220', '230', '240', '250',
                            '299')

    /*====================================================================================================
    # 规则代码: AM00239
    # 目标接口: J1026-资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {资产类别}的值在<资产类别表>中必须存在
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
    SELECT DISTINCT 'AM00239'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_assets t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 资产类别: 101-股票、102-优先股、103-债券、104-标准化资管产品、105-现金和活期存款、106-银行定期存款、107-同业存单、108-买入返售资产、109-商品、110-商品及金融衍生品（场内集中交易清算）、111-融入资金、199-其他标准化资产、205-资产支持证券（未在交易所挂牌）、209-转融券、299-其他非标准化资产
      AND t1.zclb NOT IN
          ('101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '199', '205', '209', '299')

    /*====================================================================================================
    # 规则代码: AM00240
    # 目标接口: J1026-资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: [产品净值信息]中存在[产品净值信息].{资产总值}>0时，在本表中必须存在
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
    SELECT DISTINCT 'AM00240'   AS gzdm        -- 规则代码
                  , t2.sjrq     AS sjrq        -- 数据日期
                  , t2.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_assets t1
             RIGHT JOIN report_cisp.wdb_amp_prod_nav t2
                        ON t1.jgdm = t2.jgdm
                            AND t1.status = t2.status
                            AND t1.sjrq = t2.sjrq
                            AND t1.cpdm = t2.cpdm
    WHERE t2.jgdm = '70610000'
      AND t2.status NOT IN ('3', '5')
      AND t2.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t2.zczz > 0
      AND t1.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00241
    # 目标接口: J1026-资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {资产类别}为“其他标准化资产”、“其他非标准化资产”时，{备注}不能为空
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
    SELECT DISTINCT 'AM00241'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_assets t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 资产类别: 199-其他标准化资产、299-其他非标准化资产
      AND t1.zclb IN ('199', '299')
      AND t1.bz IS NULL

    /*====================================================================================================
    # 规则代码: AM00242
    # 目标接口: J1026-资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {期末市值}不能为0
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
    SELECT DISTINCT 'AM00242'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_assets t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.qmsz = 0

    /*====================================================================================================
    # 规则代码: AM00243
    # 目标接口: J1027-QDII及FOF资产组合
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
    SELECT DISTINCT 'AM00243'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_qd_assets t1
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
    # 规则代码: AM00244
    # 目标接口: J1027-QDII及FOF资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
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
    SELECT DISTINCT 'AM00244'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_qd_assets t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                            '107', '108', '111', '112', '113', '121', '122', '123', '124',
                            '125', '126', '131', '132', '133', '134', '135', '136', '137',
                            '138', '138', '199', '200', '210', '220', '230', '240', '250',
                            '299')

    /*====================================================================================================
    # 规则代码: AM00245
    # 目标接口: J1027-QDII及FOF资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF产品净值信息]中存在[QDII及FOF产品净值信息].{资产总值}>0时，在本表中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1010-QDII及FOF产品净值信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00245'   AS gzdm        -- 规则代码
                  , t2.sjrq     AS sjrq        -- 数据日期
                  , t2.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_qd_assets t1
             RIGHT JOIN report_cisp.wdb_amp_prod_qd_nav t2
                        ON t1.jgdm = t2.jgdm
                            AND t1.status = t2.status
                            AND t1.sjrq = t2.sjrq
                            AND t1.cpdm = t2.cpdm
    WHERE t2.jgdm = '70610000'
      AND t2.status NOT IN ('3', '5')
      AND t2.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.zczz > 0
      AND t1.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00246
    # 目标接口: J1027-QDII及FOF资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {资产类别}的值在<资产类别表>中必须存在
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
    SELECT DISTINCT 'AM00246'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_qd_assets t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 101-股票、102-优先股、103-债券、104-标准化资管产品、105-现金和活期存款、106-银行定期存款、107-同业存单、108-买入返售资产、109-商品、110-商品及金融衍生品（场内集中交易清算）、111-融入资金、199-其他标准化资产、205-资产支持证券（未在交易所挂牌）、209-转融券、299-其他非标准化资产
      AND t1.zclb NOT IN
          ('101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '199', '205', '209', '299')

    /*====================================================================================================
    # 规则代码: AM00247
    # 目标接口: J1027-QDII及FOF资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {资产类别}为“其他标准化资产”、“其他非标准化资产”时，{备注}不能为空
             {备注}为空时，{资产类别}不能为“其他非标准化资产”
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
    SELECT DISTINCT 'AM00247'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_qd_assets t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 199-其他标准化资产、299-其他非标准化资产
      AND ((t1.zclb IN ('199', '299') AND t1.bz IS NULL)
        OR (t1.bz IS NULL AND t1.zclb = '299'))

    /*====================================================================================================
    # 规则代码: AM00248
    # 目标接口: J1027-QDII及FOF资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF资产组合]中存在的产品，不能同时在[资产组合]中报送
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
    SELECT DISTINCT 'AM00248'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_qd_assets t1
             LEFT JOIN report_cisp.wdb_amp_inv_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.cpdm IS NOT NULL

    /*====================================================================================================
    # 规则代码: AM00249
    # 目标接口: J1027-QDII及FOF资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末市值}不能为0
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
    SELECT DISTINCT 'AM00249'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_qd_assets t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsz = 0

    /*====================================================================================================
    # 规则代码: AM00250
    # 目标接口: J1028-资产信息（侧袋）
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {产品主代码}+{产品侧袋代码}的值在[产品侧袋基本信息]中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1004-产品侧袋基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00250'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_pocket t1
             LEFT JOIN report_cisp.wdb_amp_prod_pocket t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpcddm = t2.cpcddm
                           AND t1.cpzdm = t2.cpzdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t2.cpcddm IS NULL OR t2.cpzdm IS NULL)

    /*====================================================================================================
    # 规则代码: AM00251
    # 目标接口: J1028-资产信息（侧袋）
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [产品侧袋基本信息].{当期状态}为“运行中"时，[产品侧袋基本信息].{产品主代码}+[产品侧袋基本信息].{产品侧袋代码}的值在本表中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1004-产品侧袋基本信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00251'   AS gzdm        -- 规则代码
                  , t2.sjrq     AS sjrq        -- 数据日期
                  , t2.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_pocket t1
             RIGHT JOIN report_cisp.wdb_amp_prod_pocket t2
                        ON t1.jgdm = t2.jgdm
                            AND t1.status = t2.status
                            AND t1.sjrq = t2.sjrq
                            AND t1.cpcddm = t2.cpcddm
                            AND t1.cpzdm = t2.cpzdm
    WHERE t2.jgdm = '70610000'
      AND t2.status NOT IN ('3', '5')
      AND t2.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.dqzt = '0'
      AND (t1.cpcddm IS NULL OR t1.cpzdm IS NULL)

    /*====================================================================================================
    # 规则代码: AM00252
    # 目标接口: J1028-资产信息（侧袋）
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
             ({交易场所代码}为“上海证券交易所”、“深圳证券交易所”、“北京证券交易所”)时，{证券代码}的长度等于6位
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
    SELECT DISTINCT 'AM00252'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_pocket t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND ((t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                              '107', '108', '111', '112', '113', '121', '122', '123', '124',
                              '125', '126', '131', '132', '133', '134', '135', '136', '137',
                              '138', '138', '199', '200', '210', '220', '230', '240', '250',
                              '299')
        OR (t1.jycsdm IN ('102', '103', '108') AND length(t1.zjdm) <> 6)))

    /*====================================================================================================
    # 规则代码: AM00253
    # 目标接口: J1028-资产信息（侧袋）
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易方向}的值在<交易方向表>中必须存在
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
    SELECT DISTINCT 'AM00253'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_pocket t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易方向: 1-买入、2-卖出、3-融资买入、4-融券卖出
      AND t1.jyfx NOT IN ('1', '2', '3', '4')

    /*====================================================================================================
    # 规则代码: AM00254
    # 目标接口: J1029-股票投资明细
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
    SELECT DISTINCT 'AM00254'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stock t1
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
    # 规则代码: AM00255
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
             ({交易场所代码}为“上海证券交易所”、“深圳证券交易所”、“北京证券交易所”)时，{证券代码}的长度等于6位
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
    SELECT DISTINCT 'AM00255'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stock t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND ((t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                              '107', '108', '111', '112', '113', '121', '122', '123', '124',
                              '125', '126', '131', '132', '133', '134', '135', '136', '137',
                              '138', '138', '199', '200', '210', '220', '230', '240', '250',
                              '299')
        OR (t1.jycsdm IN ('102', '103', '108') AND length(t1.zjdm) <> 6)))

    /*====================================================================================================
    # 规则代码: AM00256
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}>=0，{非流通股份数量}>=0，{流通受限股份数量}>=0，{其他流通受限股份数量}>=0，{新发行股份数量}>=0，{增发股份数量}>=0
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
    SELECT DISTINCT 'AM00256'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stock t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.qmsl < 0 OR t1.fltgfsl < 0 OR t1.ltsxgfsl < 0 OR t1.qtltsxgfsl < 0 OR t1.xfxgfsl < 0 OR t1.zfgfsl < 0)

    /*====================================================================================================
    # 规则代码: AM00257
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 当{非流通股份市值}不为空时，{非流通股份市值}>=0
             当{流通受限股份市值}不为空时，{流通受限股份市值}>=0
             当{其他流通受限股份市值}不为空时，{其他流通受限股份市值}>=0
             当{新发行股份市值}不为空时，{新发行股份市值}>=0
             当{增发股份市值}不为空时，{增发股份市值}>=0
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
    SELECT DISTINCT 'AM00257'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stock t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (
        (t1.fltgfsz IS NOT NULL AND t1.fltgfsz < 0)
            OR (t1.fltgfsz IS NOT NULL AND t1.fltgfsz < 0)
            OR (t1.ltsxgfsz IS NOT NULL AND t1.ltsxgfsz < 0)
            OR (t1.qtltsxgfsz IS NOT NULL AND t1.qtltsxgfsz < 0)
            OR (t1.xfxgfsz IS NOT NULL AND t1.xfxgfsz < 0)
            OR (t1.zfgfsz IS NOT NULL AND t1.zfgfsz < 0)
        )

    /*====================================================================================================
    # 规则代码: AM00258
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 上期[股票投资明细].{期末数量}>0，且本期[产品运行信息].{合同已终止标志}=“否”时，{期末数量}>=0
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
    SELECT DISTINCT 'AM00258'   AS gzdm        -- 规则代码
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
             LEFT JOIN report_cisp.wdb_amp_prod_oprt t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.qmsl > 0
      AND t3.htyzzbz = '0'
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00259
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {股票类别}的值在<股票类别表>中必须存在
             ({股票类别}为“普通A股”、“普通B股”)时，{交易场所代码}必须为“上海证券交易所”、“深圳证券交易所”、“北京证券交易所”，{发行人代码}不为空
             {股票类别}为“港股通”时，{交易场所代码}必须为“沪港通下港股通”或“深港通下港股通”
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
    SELECT DISTINCT 'AM00259'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stock t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 股票类别: 01-普通A股、02-普通B股、03-港股通、04-境外股票（除港股通外）、05-存托凭证、99-其他
      AND ((t1.gplb NOT IN ('01', '02', '03', '04', '05', '99'))
        -- 交易场所代码:
        --    境内: 102-上海证券交易所、103-深圳证券交易所、108-北京证券交易所、104-沪港通下港股通、105-深港通下港股通
        OR (t1.gplb IN ('01', '02') AND (t1.jycsdm NOT IN ('102', '103', '108') OR t1.fxrdm IS NULL))
        OR (t1.gplb = '03' AND t1.jycsdm NOT IN ('104', '105'))
        )

    /*====================================================================================================
    # 规则代码: AM00260
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ({交易场所代码}为“上海证券交易所”、“深圳证券交易所”、“北京证券交易所”，{行业分类}不为空)时，{行业分类}的值在<上市公司行业分类表>中必须存在
             ({交易场所代码}为“沪港通下港股通”、“深港通下港股通”和所有境外市场，{行业分类}不为空)时，{行业分类}的值在<全球行业分类系统表>中必须存在
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
    SELECT DISTINCT 'AM00260'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stock t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 102-上海证券交易所、103-深圳证券交易所、108-北京证券交易所、104-沪港通下港股通、105-深港通下港股通
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND (
        -- 上市公司行业分类: A-农、林、牧、渔业、B-采矿业、C-制造业、D-电力、热力、燃气及水生产和供应业、E-建筑业、F-批发和零售业、G-交通运输、仓储和邮政业、H-住宿和餐饮业、I-信息传输、软件和信息技术服务业、J-金融业、K-房地产业、L-租赁和商务服务业、M-科学研究和技术服务业、N-水利、环境和公共设施管理业、O-居民服务、修理和其他服务业、P-教育、Q-卫生和社会工作、R-文化、体育和娱乐业、S-综合
        (t1.jycsdm IN ('102', '103', '108') AND t1.hyfl IS NOT NULL AND
         t1.hyfl NOT IN ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S'))
            -- 全球行业分类系统（GICS）: 10-能源、15-原材料、20-工业、25-消费者非必需品、30-消费者常用品、35-医疗保健、40-金融、45-信息技术、50-电信业务、55-公用事业、60-房地产
            OR (t1.jycsdm IN ('104', '105', '200', '210', '220', '230', '240', '250', '299') AND t1.hyfl IS NOT NULL AND
                t1.hyfl NOT IN ('10', '15', '20', '25', '30', '35', '40', '45', '50', '55', '60'))
        )

    /*====================================================================================================
    # 规则代码: AM00261
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {证券代码}不能以“SH”、“SZ”开头
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
    SELECT DISTINCT 'AM00261'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stock t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.zqdm IN ('SH%', 'SZ%')

    /*====================================================================================================
    # 规则代码: AM00262
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}不能为101-银行间市场”、“106-全国中小企业股份转让系统”、“107-区域股权市场”
             {交易场所代码}只能为“102-上海证券交易所”、“103-深圳证券交易所”、“104-沪港通下港股通”、“105-深港通下港股通”、“108-北京证券交易所”、“111-沪港通(北向)”、“112-深港通(北向)”、“113-沪伦通(东向)”、“200-北美”、“210-南美”、“220-欧洲”、“230-日本”、“240-亚太（除日本和香港外）”、“250-香港”、“299-其他（境外）”
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
    SELECT DISTINCT 'AM00262'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stock t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND ((t1.jycsdm IN ('101', '106', '107'))
        OR (t1.jycsdm NOT IN
            ('102', '103', '104', '105', '108', '111', '112', '113', '200', '210', '220', '230', '240', '250', '299')))

    /*====================================================================================================
    # 规则代码: AM00263
    # 目标接口: J1030-优先股投资明细
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
    SELECT DISTINCT 'AM00263'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_prestock t1
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
    # 规则代码: AM00264
    # 目标接口: J1030-优先股投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
             ({交易场所代码}为“上海证券交易所”、“深圳证券交易所”、“北京证券交易所”)时，{证券代码}的长度等于6位
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
    SELECT DISTINCT 'AM00264'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_prestock t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND ((t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                              '107', '108', '111', '112', '113', '121', '122', '123', '124',
                              '125', '126', '131', '132', '133', '134', '135', '136', '137',
                              '138', '138', '199', '200', '210', '220', '230', '240', '250',
                              '299')
        OR (t1.jycsdm IN ('102', '103', '108') AND length(t1.zjdm) <> 6)))

    /*====================================================================================================
    # 规则代码: AM00265
    # 目标接口: J1030-优先股投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}>=0
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
    SELECT DISTINCT 'AM00265'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_prestock t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00266
    # 目标接口: J1030-优先股投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 上期[优先股投资明细].{期末数量}>0，且本期[产品运行信息].{合同已终止标志}=“否”时，{期末数量}>=0
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
    SELECT DISTINCT 'AM00266'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_prestock t1
             LEFT JOIN report_cisp.wdb_amp_inv_prestock t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_prestock tt1 WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_prod_oprt t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.qmsl > 0
      AND t3.htyzzbz = '0'
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00267
    # 目标接口: J1030-优先股投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ({交易场所代码}为“上海证券交易所”、“深圳证券交易所”、“北京证券交易所”)时，{发行人代码}不为空
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
    SELECT DISTINCT 'AM00267'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_prestock t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 102-上海证券交易所、103-深圳证券交易所、108-北京证券交易所
      AND t1.jycsdm IN ('102', '103', '108')
      AND t1.fxrdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00268
    # 目标接口: J1030-优先股投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {可转换为普通股标志}的值必须为“0”或“1”
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
    SELECT DISTINCT 'AM00268'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_prestock t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.kzhwptgbz NOT IN ('1', '0')

    /*====================================================================================================
    # 规则代码: AM00269
    # 目标接口: J1030-优先股投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}不能为101-银行间市场”、“107-区域股权市场”
             {交易场所代码}只能为“102-上海证券交易所”、“103-深圳证券交易所”、“104-沪港通下港股通”、“105-深港通下港股通”、“106-全国中小企业股份转让系统”、“108-北京证券交易所”、“111-沪港通(北向)”、“112-深港通(北向)”、“113-沪伦通(东向)”、“200-北美”、“210-南美”、“220-欧洲”、“230-日本”、“240-亚太（除日本和香港外）”、“250-香港”、“299-其他（境外）”
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
    SELECT DISTINCT 'AM00269'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stock t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND ((t1.jycsdm IN ('101', '107'))
        OR (t1.jycsdm NOT IN
            ('102', '103', '104', '105', '106', '108', '111', '112', '113', '200', '210', '220', '230', '240', '250',
             '299')))

    /*====================================================================================================
    # 规则代码: AM00270
    # 目标接口: J1031-债券投资明细
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
    SELECT DISTINCT 'AM00270'   AS gzdm        -- 规则代码
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
      AND t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00271
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
             ({交易场所代码}为“上海证券交易所”、“深圳证券交易所”、“北京证券交易所”)时，{证券代码}的长度等于6位
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
    SELECT DISTINCT 'AM00271'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND (t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                             '107', '108', '111', '112', '113', '121', '122', '123', '124',
                             '125', '126', '131', '132', '133', '134', '135', '136', '137',
                             '138', '138', '199', '200', '210', '220', '230', '240', '250',
                             '299')
        OR (t1.jycsdm IN ('102', '103', '108') AND length(t1.zjdm) <> 6))

    /*====================================================================================================
    # 规则代码: AM00272
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}>=0
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
    SELECT DISTINCT 'AM00272'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00273
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 上期[债券投资明细].{期末数量}>0，且本期[产品运行信息].{合同已终止标志}=“否”时，{期末数量}>=0
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
    SELECT DISTINCT 'AM00273'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
             LEFT JOIN report_cisp.wdb_amp_inv_bond t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_bond tt1 WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_prod_oprt t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.qmsl > 0
      AND t3.htyzzbz = '0'
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00274
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {债券类别}的值在<债券类别表>中必须存在
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
    SELECT DISTINCT 'AM00274'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 债券类别: 01-国债、02-地方政府债、03-央行票据、04-政策性银行债、05-商业银行债、06-非银行金融机构债、07-企业债、08-公司债、09-中期票据、10-短期融资券（含超短融）、11-定向工具、12-政府支持机构债（铁道债、中央汇金债券，不含城投债）、13-可转债、14-可交换债、15-可分离转债纯债、16-资产支持证券（在交易所挂牌）、17-私募债、18-境外债券、19-国际机构债、20-次级债、21-永续债、22-永续次级债、23-集合票据、99-其他
      AND t1.zqlb NOT IN
          ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18',
           '19', '20', '21', '22', '23', '99')

    /*====================================================================================================
    # 规则代码: AM00275
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ([产品基本信息].{产品类别}为“货币基金”、“货币型”，{债券类别}为“商业银行债”)时，{发行人代码}不能为空，{发行人净资产}不能为空且不能为0
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
    SELECT DISTINCT 'AM00275'   AS gzdm        -- 规则代码
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
      -- 产品类别:
      --    公募基金: 141-货币基金
      --    大集合: 241-货币型
      AND t2.cplb IN ('141', '241')
      -- 债券类别: 05-商业银行债
      AND t1.zqlb = '05'
      AND (t1.fxrdm IS NULL OR t1.fxrjzc IS NULL OR t1.fxrjzc = 0)

    /*====================================================================================================
    # 规则代码: AM00276
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ({交易场所代码}为“银行间市场”、“上海证券交易所”、“深圳证券交易所”、”北京证券交易所”，{行业分类}不为空)时，{行业分类}的值在<上市公司行业分类表>中必须存在
             {行业分类}不为空时，{行业分类}的值在<上市公司行业分类表>或<全球行业分类系统表>中必须存在
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
    SELECT DISTINCT 'AM00276'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (
        -- 交易场所代码:
        --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、108-北京证券交易所
        -- 上市公司行业分类: A-农、林、牧、渔业、B-采矿业、C-制造业、D-电力、热力、燃气及水生产和供应业、E-建筑业、F-批发和零售业、G-交通运输、仓储和邮政业、H-住宿和餐饮业、I-信息传输、软件和信息技术服务业、J-金融业、K-房地产业、L-租赁和商务服务业、M-科学研究和技术服务业、N-水利、环境和公共设施管理业、O-居民服务、修理和其他服务业、P-教育、Q-卫生和社会工作、R-文化、体育和娱乐业、S-综合
        (t1.jycsdm IN ('101', '102', '103', '108') AND t1.hyfl IS NOT NULL AND
         t1.hyfl NOT IN ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S'))
            -- 全球行业分类系统（GICS）: 10-能源、15-原材料、20-工业、25-消费者非必需品、30-消费者常用品、35-医疗保健、40-金融、45-信息技术、50-电信业务、55-公用事业、60-房地产
            OR (t1.hyfl IS NOT NULL AND
                t1.hyfl NOT IN
                ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', '10',
                 '15', '20', '25', '30', '35', '40', '45', '50', '55', '60'))
        )

    /*====================================================================================================
    # 规则代码: AM00277
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {主体评级}的值在<主体评级表>中必须存在
             {债项评级}的值在<债项评级表>中必须存在
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
    SELECT DISTINCT 'AM00277'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (
        -- 主体评级: 201-AAA+、202-AAA、203-AAA-、204-AA+、205-AA、206-AA-、207-A+、208-A、209-A-、210-BBB+、211-BBB、212-BBB-、213-BB+、214-BB、215-BB-、216-B+、217-B、218-B-、219-CCC、220-CC、221-C、222-D
        --    特殊情况: 301-无评级（信用债）、302-无评级（国债、政策性金融债、地方政府债、央行票据、政府支持机构债等利率债，不含城投债）、399-不适用
        (t1.ztypj NOT IN
         ('201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215',
          '216', '217', '218', '219', '220', '221', '222', '301', '302', '399'))
            -- 债项评级:
            --    短期债券: 101-A-1、102-A-2、103-A-3、104-B、105-C、106-D
            --    中长期债券: 201-AAA+、202-AAA、203-AAA-、204-AA+、205-AA、206-AA-、207-A+、208-A、209-A-、210-BBB+、211-BBB、212-BBB-、213-BB+、214-BB、215-BB-、216-B+、217-B、218-B-、219-CCC、220-CC、221-C、222-D
            --    特殊情况: 301-无评级（信用债）、302-无评级（国债、政策性金融债、地方政府债、央行票据、政府支持机构债等利率债，不含城投债）、399-不适用
            OR (t1.zxpj NOT IN
                ('101', '102', '103', '104', '105', '106',
                 '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214',
                 '215', '216', '217', '218', '219', '220', '221', '222',
                 '301', '302', '399'))
        )

    /*====================================================================================================
    # 规则代码: AM00278
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {发行日期}<={到期日期}
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
    SELECT DISTINCT 'AM00278'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.fxrq > t1.dqrq

    /*====================================================================================================
    # 规则代码: AM00279
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {面值}>0
             {票面利率}>=0
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
    SELECT DISTINCT 'AM00279'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stock t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.mz <= 0
        OR t1.pmll < 0)

    /*====================================================================================================
    # 规则代码: AM00280
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ({债券类别}不为“可转债”、“可交换债”、“境外债券”，{数据日期}<{到期日期})时，{修正久期(年)}不能为空
             {债券类别}为“其他”时，{备注}不能为空
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
    SELECT DISTINCT 'AM00280'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 债券类别: 13-可转债、14-可交换债、18-境外债券、99-其他
      AND ((t1.zqlb NOT IN ('13', '14', '18') AND t1.sjrq < t1.dqrq AND (xzjq IS NULL OR xzjq = 0))
        OR (t1.zqlb = '99' AND bz IS NULL))

    /*====================================================================================================
    # 规则代码: AM00281
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {资产支持证券类别}不为空时，{资产支持证券类别}的值在<资产支持证券类别表>中必须存在
             {付息方式}的值在<付息方式表>中必须存在
             {债券违约类型}的值在<债券违约类型表>中必须存在
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
    SELECT DISTINCT 'AM00281'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (
        -- 资产支持证券类别: 1-资产支持证券、2-资产支持票据
        (t1.zczczqlb IS NOT NULL AND t1.zczczqlb NOT IN ('1', '2'))
            -- 付息方式: 1-零息、2-贴现、3-附息
            OR (t1.fxfs NOT IN ('1', '2', '3'))
            -- 债券违约类型: 1-未违约、2-债券未违约但发行人发行的其他债券已违约、3-已触发交叉违约、4-已实质违约
            OR (t1.zqwylx NOT IN ('1', '2', '3', '4'))
        )

    /*====================================================================================================
    # 规则代码: AM00282
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {债券类别}为“01”、“02”、“03”、“04”、“12”时，{主体评级}、{债项评级}不能为“301”
             {主体评级}或{债项评级}为“302”时，{债券类别}只能为“01”、“02”、“03”、“04”、“12”其中之一
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
    SELECT DISTINCT 'AM00282'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 债券类别: 01-国债、02-地方政府债、03-央行票据、04-政策性银行债、12-政府支持机构债（铁道债、中央汇金债券，不含城投债）
      -- 主体评级:
      --    特殊情况: 301-无评级（信用债）、302-无评级（国债、政策性金融债、地方政府债、央行票据、政府支持机构债等利率债，不含城投债）
      -- 债项评级:
      --    特殊情况: 301-无评级（信用债）、302-无评级（国债、政策性金融债、地方政府债、央行票据、政府支持机构债等利率债，不含城投债）
      AND (
        (t1.zqlb IN ('01', '02', '03', '04', '12') AND (t1.ztpj = '301' OR t1.zxpj = '301'))
            OR ((t1.ztpj = '302' OR t1.zxpj = '302') AND (t1.zqlb NOT IN ('01', '02', '03', '04', '12')))
        )

    /*====================================================================================================
    # 规则代码: AM00283
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {债券代码}不能以“SH”、“SZ”开头
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
    SELECT DISTINCT 'AM00283'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.zqdm IN ('SH%', 'SZ%')

    /*====================================================================================================
    # 规则代码: AM00284
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}只能为“101-银行间市场”、“102-上海证券交易所”、“103-深圳证券交易所”、“106-全国中小企业股份转让系统”、“108-北京证券交易所”、“111-沪港通(北向)”、“112-深港通(北向)”、“113-沪伦通(东向)”、“200-北美”、“210-南美”、“220-欧洲”、“230-日本”、“240-亚太（除日本和香港外）”、“250-香港”、“299-其他（境外）”
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
    SELECT DISTINCT 'AM00284'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、106-全国中小企业股份转让系统、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND t1.jycsdm NOT IN
          ('101', '102', '103', '106', '108', '111', '112', '113', '200', '210', '220', '230', '240', '250', '299')

    /*====================================================================================================
    # 规则代码: AM00285
    # 目标接口: J1032-标准化资管产品投资明细
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
    SELECT DISTINCT 'AM00285'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stdampp t1
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
    # 规则代码: AM00286
    # 目标接口: J1032-标准化资管产品投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
             ({交易场所代码}为“上海证券交易所”、“深圳证券交易所”、“北京证券交易所”)时，{证券代码}的长度等于6位
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
    SELECT DISTINCT 'AM00286'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stdampp t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND (t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                             '107', '108', '111', '112', '113', '121', '122', '123', '124',
                             '125', '126', '131', '132', '133', '134', '135', '136', '137',
                             '138', '138', '199', '200', '210', '220', '230', '240', '250',
                             '299')
        OR (t1.jycsdm IN ('102', '103', '108') AND length(t1.zjdm) <> 6))

    /*====================================================================================================
    # 规则代码: AM00287
    # 目标接口: J1032-标准化资管产品投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}>=0
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
    SELECT DISTINCT 'AM00287'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stdampp t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00288
    # 目标接口: J1032-标准化资管产品投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 上期[债券投资明细].{期末数量}>0，且本期[产品运行信息].{合同已终止标志}=“否”时，{期末数量}>=0
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
    SELECT DISTINCT 'AM00288'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stdampp t1
             LEFT JOIN report_cisp.wdb_amp_inv_stdampp t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_stdampp tt1 WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_prod_oprt t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.qmsl > 0
      AND t3.htyzzbz = '0'
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00289
    # 目标接口: J1032-标准化资管产品投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {产品类别}的值在<产品类别表>中必须存在
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
    SELECT DISTINCT 'AM00289'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stdampp t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 产品类别:
      --    公募基金: 111-股票型基金、119-股票型QDII基金、121-偏股混合型基金、122-偏债混合型基金、123-混合型基金（灵活配置或其他）、129-混合型QDII基金、131-债券基金、139-债券型QDII基金、141-货币基金、151-FOF基金、152-MOM基金、153-ETF联接、158-FOF型QDII基金、159-MOM型QDII基金、161-同业存单基金、171-商品基金（黄金）、172-商品基金（其他商品）、179-其他另类基金、180-REITS基金、198-其他QDII基金、199-以上范围外的公募基金
      --    大集合: 211-股票型、219-股票型QDII、221-偏股混合型、222-偏债混合型、223-混合型（灵活配置或其他）、229-混合型QDII、231-债券型、239-债券型QDII、241-货币型、251-FOF、252-MOM、253-ETF联接、258-FOF型QDII、259-MOM型QDII、261-同业存单型、271-商品（黄金）、272-商品（其他商品）、279-其他另类、280-REITS、298-其他QDII、299-以上范围外的大集合
      AND t1.cplb NOT IN
          ('111', '119', '121', '122', '123', '129', '131', '139', '141', '151', '152', '153', '158', '159', '161',
           '171', '172', '179', '180', '198', '199',
           '211', '219', '221', '222', '223', '229', '231', '239', '241', '251', '252', '253', '258', '259', '261',
           '271', '272', '279', '280', '298', '299')

    /*====================================================================================================
    # 规则代码: AM00290
    # 目标接口: J1032-标准化资管产品投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [产品基本信息].{产品类别}为“151-FOF基金”、“158-FOF型QDII基金”、“251-FOF”、“258-FOF型QDII”时，产品在本表必须存在记录
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
    SELECT DISTINCT 'AM00290'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stdampp t1
             RIGHT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                        ON t1.jgdm = t2.jgdm
                            AND t1.status = t2.status
                            AND t1.sjrq = t2.sjrq
                            AND t1.cpdm = t2.cpdm
    WHERE t2.jgdm = '70610000'
      AND t2.status NOT IN ('3', '5')
      AND t2.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 产品类别:
      --    公募基金:151-FOF基金、158-FOF型QDII基金
      --    大集合:251-FOF、258-FOF型QDII
      AND t2.cplb IN ('151', '158', '251', '258', )
      AND t1.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00291
    # 目标接口: J1033-定期存款投资明细
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
    SELECT DISTINCT 'AM00291'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_fixdepo t1
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
    # 规则代码: AM00292
    # 目标接口: J1033-定期存款投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {存款类别}的值在<定期存款类别表>中必须存在
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
    SELECT DISTINCT 'AM00292'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_fixdepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 存款类别: 1-定期存款、2-协议存款、3-通知存款
      AND t1.cklb NOT IN ('1', '2', '3')

    /*====================================================================================================
    # 规则代码: AM00293
    # 目标接口: J1033-定期存款投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末金额}>=0
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
    SELECT DISTINCT 'AM00293'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_fixdepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmje < 0

    /*====================================================================================================
    # 规则代码: AM00294
    # 目标接口: J1033-定期存款投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {起息日期}<={到期日期}
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
    SELECT DISTINCT 'AM00294'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_fixdepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qxrq > t1.dqrq

    /*====================================================================================================
    # 规则代码: AM00295
    # 目标接口: J1033-定期存款投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {存款天数}>0
             {存款利率}>0
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
    SELECT DISTINCT 'AM00295'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_fixdepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.ckts <= 0
        OR t1.ckll <= 0)

    /*====================================================================================================
    # 规则代码: AM00296
    # 目标接口: J1033-定期存款投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ([产品基本信息].{产品类别}为“货币基金”、“货币型”)时，{银行净资产}不能为空
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
    SELECT DISTINCT 'AM00296'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_fixdepo t1
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
      AND t1.yhjzc IS NULL

    /*====================================================================================================
    # 规则代码: AM00297
    # 目标接口: J1034-活期存款投资明细
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
    SELECT DISTINCT 'AM00297'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_curdepo t1
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
    # 规则代码: AM00298
    # 目标接口: J1035-同业存单投资明细
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
    SELECT DISTINCT 'AM00298'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_ncd t1
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
    # 规则代码: AM00299
    # 目标接口: J1035-同业存单投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
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
    SELECT DISTINCT 'AM00299'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_ncd t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                            '107', '108', '111', '112', '113', '121', '122', '123', '124',
                            '125', '126', '131', '132', '133', '134', '135', '136', '137',
                            '138', '138', '199', '200', '210', '220', '230', '240', '250',
                            '299')

    /*====================================================================================================
    # 规则代码: AM00300
    # 目标接口: J1035-同业存单投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}>=0
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
    SELECT DISTINCT 'AM00300'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_ncd t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00301
    # 目标接口: J1035-同业存单投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 上期[同业存单投资明细].{期末数量}>0，且本期[产品运行信息].{合同已终止标志}=“否”时，{期末数量}>=0
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
    SELECT DISTINCT 'AM00301'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_ncd t1
             LEFT JOIN report_cisp.wdb_amp_inv_ncd t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_ncd tt1 WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_prod_oprt t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.qmsl > 0
      AND t3.htyzzbz = '0'
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00302
    # 目标接口: J1035-同业存单投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {发行日期}<={数据日期}
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
    SELECT DISTINCT 'AM00302'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_ncd t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.fxrq > t1.sjrq

    /*====================================================================================================
    # 规则代码: AM00303
    # 目标接口: J1035-同业存单投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {面值}>0
             {存单利率}不为空时，{存单利率}>0
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
    SELECT DISTINCT 'AM00303'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_ncd t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.mz <= 0
        OR (t1.cdll IS NOT NULL AND t1.cdlv <= 0))

    /*====================================================================================================
    # 规则代码: AM00304
    # 目标接口: J1035-同业存单投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {主体评级}的值在<主体评级表>中必须存在
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
    SELECT DISTINCT 'AM00304'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_ncd t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 主体评级: 201-AAA+、202-AAA、203-AAA-、204-AA+、205-AA、206-AA-、207-A+、208-A、209-A-、210-BBB+、211-BBB、212-BBB-、213-BB+、214-BB、215-BB-、216-B+、217-B、218-B-、219-CCC、220-CC、221-C、222-D
      --    特殊情况: 301-无评级（信用债）、302-无评级（国债、政策性金融债、地方政府债、央行票据、政府支持机构债等利率债，不含城投债）、399-不适用
      AND t1.ztpj NOT IN
          ('201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215',
           '216', '217', '218', '219', '220', '221', '222', '301', '302', '399')

    /*====================================================================================================
    # 规则代码: AM00305
    # 目标接口: J1035-同业存单投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ([产品基本信息].{产品类别}为“货币基金”、“货币型”)时，{银行净资产}不能为空
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
    SELECT DISTINCT 'AM00305'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_ncd t1
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
      AND t1.yhjzc IS NULL

    /*====================================================================================================
    # 规则代码: AM00306
    # 目标接口: J1035-同业存单投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {银行所在省份}的值在国标GB/T 2260-2007中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口:
    # 其他接口传输频度:
    # 其他接口传输时间:
    # 工作状态: 1
    # 备注: 不明确，暂简化
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00306'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_ncd t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND length(t1.yhszsf) <> 6

    /*====================================================================================================
    # 规则代码: AM00307
    # 目标接口: J1036-债券回购投资明细
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
    SELECT DISTINCT 'AM00307'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bdrepo t1
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
    # 规则代码: AM00308
    # 目标接口: J1036-债券回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
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
    SELECT DISTINCT 'AM00308'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bdrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                            '107', '108', '111', '112', '113', '121', '122', '123', '124',
                            '125', '126', '131', '132', '133', '134', '135', '136', '137',
                            '138', '138', '199', '200', '210', '220', '230', '240', '250',
                            '299')

    /*====================================================================================================
    # 规则代码: AM00309
    # 目标接口: J1036-债券回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {回购方向}的值在<回购方向表>中必须存在
             {回购类型}的值在<回购类型表>中必须存在
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
    SELECT DISTINCT 'AM00309'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bdrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 回购方向: 1-正回购、2-逆回购
      AND ((t1.hgfx NOT IN ('1', '2'))
        -- 回购类型: 01-债券买断式回购、02-债券质押式协议回购、03-债券质押式三方回购、04-质押式报价回购、05-约定购回、06-股票质押式回购（场内）、07-股票质押式回购（场外）、08-股权质押式回购、99-其他
        OR (t1.hglx NOT IN ('01', '02', '03', '04', '05', '06', '07', '08', '99'))
        )

    /*====================================================================================================
    # 规则代码: AM00310
    # 目标接口: J1036-债券回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {成交日期}<={数据日期}
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
    SELECT DISTINCT 'AM00310'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bdrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cjrq > t1.sjrq

    /*====================================================================================================
    # 规则代码: AM00311
    # 目标接口: J1036-债券回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {回购期限(天)}>0
             {回购利率}不为空时，{回购利率}>=0
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
    SELECT DISTINCT 'AM00311'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bdrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.hgqx <= 0
        OR (t1.hgll IS NOT NULL AND t1.hgll < 0)
        )

    /*====================================================================================================
    # 规则代码: AM00312
    # 目标接口: J1037-协议回购投资明细
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
    SELECT DISTINCT 'AM00312'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_agrmrepo t1
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
    # 规则代码: AM00313
    # 目标接口: J1037-协议回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
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
    SELECT DISTINCT 'AM00313'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_agrmrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                            '107', '108', '111', '112', '113', '121', '122', '123', '124',
                            '125', '126', '131', '132', '133', '134', '135', '136', '137',
                            '138', '138', '199', '200', '210', '220', '230', '240', '250',
                            '299')

    /*====================================================================================================
    # 规则代码: AM00314
    # 目标接口: J1037-协议回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {回购方向}的值在<回购方向表>中必须存在
             {回购类型}的值在<回购类型表>中必须存在
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
    SELECT DISTINCT 'AM00314'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_agrmrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 回购方向: 1-正回购、2-逆回购
      AND ((t1.hgfx NOT IN ('1', '2'))
        -- 回购类型: 01-债券买断式回购、02-债券质押式协议回购、03-债券质押式三方回购、04-质押式报价回购、05-约定购回、06-股票质押式回购（场内）、07-股票质押式回购（场外）、08-股权质押式回购、99-其他
        OR (t1.hglx NOT IN ('01', '02', '03', '04', '05', '06', '07', '08', '99'))
        )

    /*====================================================================================================
    # 规则代码: AM00315
    # 目标接口: J1037-协议回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ({交易场所代码}为“上海证券交易所”、“深圳证券交易所”、“北京证券交易所”)时，{回购类型}不能为“其他”
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
    SELECT DISTINCT 'AM00315'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_agrmrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 102-上海证券交易所、103-深圳证券交易所、108-北京证券交易所
      -- 回购类型: 99-其他
      AND t1.jycsdm IN ('102', '103', '108')
      AND t1.hglx = '99'

    /*====================================================================================================
    # 规则代码: AM00316
    # 目标接口: J1037-协议回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {成交日期}<={数据日期}
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
    SELECT DISTINCT 'AM00316'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_agrmrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cjrq > t1.sjrq

    /*====================================================================================================
    # 规则代码: AM00317
    # 目标接口: J1037-协议回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {回购期限(天)}>0
             {回购利率}>0
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
    SELECT DISTINCT 'AM00317'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_agrmrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.hgqx <= 0 OR t1.hgll <= 0)

    /*====================================================================================================
    # 规则代码: AM00318
    # 目标接口: J1037-协议回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易对手方类型}的值在<交易对手方类型表>中必须存在
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
    SELECT DISTINCT 'AM00318'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_agrmrepo t1
    WHERE t1.jgdm = 70610000
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易对手方类型:
      --    个人: 001-个人
      --    机构: 101-银行、102-银行子公司、103-保险公司、104-保险公司子公司、105-信托公司、106-财务公司、107-证券公司、108-证券公司子公司、109-基金管理公司、110-基金管理公司子公司、111-期货公司、112-期货公司子公司、113-私募基金管理人、114-其他金融机构、115-境内非金融机构、116-境外金融机构、117-境外非金融机构
      --    产品: 201-银行理财产品、202-银行子公司公募理财、203-银行子公司理财产品、204-信托计划、205-保险产品、206-保险公司资产管理计划、229-保险公司子公司公募基金、207-保险公司子公司资产管理计划、208-证券公司公募基金、209-证券公司资产管理计划、210-证券公司子公司公募基金、211-证券公司子公司资产管理计划、212-基金管理公司公募基金、213-基金管理公司资产管理计划、214-基金管理公司子公司资产管理计划、215-期货公司资产管理计划、216-期货公司子公司资产管理计划、217-私募投资基金、218-政府引导基金、219-全国社保基金、220-地方社保基金、221-基本养老保险、222-养老金产品、223-企业年金、224-中央职业年金、225-省属职业年金、226-社会公益基金（慈善基金、捐赠基金等）、227-境外资金（QFII）、228-境外资金（RQFII）、299-其他产品
      AND t1.jydsf NOT IN
          ('001',
           '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115',
           '116', '117',
           '201', '202', '203', '204', '205', '206', '229', '207', '208', '209', '210', '211', '212', '213', '214',
           '215', '216', '217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227', '228', '299')

    /*====================================================================================================
    # 规则代码: AM00319
    # 目标接口: J1037-协议回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ({回购方向}为“逆回购”，{回购类型}不为“债券买断式回购”、“债券质押式三方回购”和”质押式报价回购”)时，{交易编号}+{成交日期}在[回购标的券明细]中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口: J1038-回购标的券明细
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00319'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_agrmrepo t1
             LEFT JOIN report_cisp.wdb_amp_inv_repomortag t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
                           AND t1.jybh = t2.jybh
                           AND t1.cjrq = t2.cjrq
    WHERE t1.jgdm = 70610000
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 回购方向: 2-逆回购
      -- 回购类型: 01-债券买断式回购、03-债券质押式三方回购、04-质押式报价回购
      AND t1.hgfx = '2'
      AND t1.hglx NOT IN ('01', '03', '04')
      AND t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00320
    # 目标接口: J1037-协议回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {对手方违约标志}的值必须为“0”或“1”
             {对手方违约标志}为”是”时，{违约日期}不能为空，{已收回金额}不能为空，{违约情况描述}不能为空
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
    SELECT DISTINCT 'AM00320'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_agrmrepo t1
    WHERE t1.jgdm = 70610000
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.dsfwybz NOT IN ('0', '1')
        OR (t1.dsfwybz = '1' AND (t1.wyrq IS NULL OR t1.yshje IS NULL OR t1.wyqkms IS NULL)))

    /*====================================================================================================
    # 规则代码: AM00321
    # 目标接口: J1037-协议回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易对手方产品代码}第7位为“1”时，{交易对手方类型}必须为“201-银行理财产品”、“202-银行子公司公募理财”、“203-银行子公司理财产品”
             {交易对手方产品代码}第7位为“2”时，{交易对手方类型}必须为“204-信托计划”
             {交易对手方产品代码}第7位为“3”时，{交易对手方类型}必须为“208-证券公司公募基金”、“209-证券公司资产管理计划“、“210-证券公司子公司公募基金”、“211-证券公司子公司资产管理计划”
             {交易对手方产品代码}第7位为“4”时，{交易对手方类型}必须为“212-基金管理公司公募基金”、“213-基金管理公司资产管理计划”、“214-基金管理公司子公司资产管理计划”
             {交易对手方产品代码}第7位为“5”时，{交易对手方类型}必须为“215-期货公司资产管理计划”、“216-期货公司子公司资产管理计划”
             {交易对手方产品代码}第7位为“6”时，{交易对手方类型}必须为“205-保险产品”、“206-保险公司资产管理计划”、“229-保险公司子公司公募基金”、“207-保险公司子公司资产管理计划”
             {交易对手方产品代码}第7位为“8”时，{交易对手方类型}必须为“208-证券公司公募基金”、“210-证券公司子公司公募基金”、“212-基金管理公司公募基金”、“229-保险公司子公司公募基金”
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
    SELECT DISTINCT 'AM00321'   AS gzdm        -- 规则代码
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
            -- 交易对手方类型: 212-基金管理公司公募基金、213-基金管理公司资产管理计划、214-基金管理公司子公司资产管理计划
            OR (substr(t1.jydsfcpdm, 7, 1) = '4' AND t1.jydsflx NOT IN ('212', '213', '214'))
            -- 交易对手方类型: 215-期货公司资产管理计划、216-期货公司子公司资产管理计划
            OR (substr(t1.jydsfcpdm, 7, 1) = '5' AND t1.jydsflx NOT IN ('215', '216'))
            -- 交易对手方类型: 205-保险产品、206-保险公司资产管理计划、229-保险公司子公司公募基金、207-保险公司子公司资产管理计划
            OR (substr(t1.jydsfcpdm, 7, 1) = '6' AND t1.jydsflx NOT IN ('205', '206', '229', '207'))
            -- 交易对手方类型: 208-证券公司公募基金、210-证券公司子公司公募基金、212-基金管理公司公募基金、229-保险公司子公司公募基金
            OR (substr(t1.jydsfcpdm, 7, 1) = '8' AND t1.jydsflx NOT IN ('208', '210', '212', '229'))
        )

    /*====================================================================================================
    # 规则代码: AM00322
    # 目标接口: J1038-回购标的券明细
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
    SELECT DISTINCT 'AM00322'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_repomortag t1
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
    # 规则代码: AM00323
    # 目标接口: J1038-回购标的券明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易编号}+{成交日期}在[债券回购投资明细]或[协议回购投资明细]中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1036-债券回购投资明细 J1037-协议回购投资明细
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+4日24:00前 T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00323'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_repomortag t1
             LEFT JOIN report_cisp.wdb_amp_inv_bdrepo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
                           AND t1.jybh = t2.jybh
                           AND t1.cjrq = t2.cjrq
             LEFT JOIN report_cisp.wdb_amp_inv_agrmrepo t3
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
                           AND t1.jybh = t3.jybh
                           AND t1.cjrq = t3.cjrq
    WHERE t1.jgdm = 70610000
        AND t1.status NOT IN ('3', '5')
        AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
        AND t2.cpdm IS NULL
       OR t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00324
    # 目标接口: J1038-回购标的券明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
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
    SELECT DISTINCT 'AM00324'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_repomortag t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                            '107', '108', '111', '112', '113', '121', '122', '123', '124',
                            '125', '126', '131', '132', '133', '134', '135', '136', '137',
                            '138', '138', '199', '200', '210', '220', '230', '240', '250',
                            '299')

    /*====================================================================================================
    # 规则代码: AM00325
    # 目标接口: J1038-回购标的券明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {回购类型}的值在<回购类型表>中必须存在
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
    SELECT DISTINCT 'AM00325'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bdrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 回购类型: 01-债券买断式回购、02-债券质押式协议回购、03-债券质押式三方回购、04-质押式报价回购、05-约定购回、06-股票质押式回购（场内）、07-股票质押式回购（场外）、08-股权质押式回购、99-其他
      AND t1.hglx NOT IN ('01', '02', '03', '04', '05', '06', '07', '08', '99')

    /*====================================================================================================
    # 规则代码: AM00326
    # 目标接口: J1038-回购标的券明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {借入标志}的值必须为“0”或“1”
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
    SELECT DISTINCT 'AM00326'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bdrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.jrbz NOT IN ('0', '1')

    /*====================================================================================================
    # 规则代码: AM00327
    # 目标接口: J1038-回购标的券明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {标的券类别}的值在<标的券类别表>中必须存在
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
    SELECT DISTINCT 'AM00327'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bdrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 标的券类别: 01-国债、02-地方政府债、03-央行票据、04-金融债、05-企业债、06-政府支持机构债、07-中期票据、08-短期融资券、09-定向工具、10-国际机构债、11-大额存单、12-资产支持证券、13-同业存单、14-公司债、15-私募债、16-IRS保证券、17-股票、18-股权、19-基金、99-其他
      AND t1.bdqlb NOT IN
          ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18',
           '19', '99')

    /*====================================================================================================
    # 规则代码: AM00328
    # 目标接口: J1038-回购标的券明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {标的券主体评级}的值不为空时，{标的券主体评级}的值在<主体评级表>中必须存在
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
    SELECT DISTINCT 'AM00328'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bdrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 主体评级: 201-AAA+、202-AAA、203-AAA-、204-AA+、205-AA、206-AA-、207-A+、208-A、209-A-、210-BBB+、211-BBB、212-BBB-、213-BB+、214-BB、215-BB-、216-B+、217-B、218-B-、219-CCC、220-CC、221-C、222-D
      --    特殊情况: 301-无评级（信用债）、302-无评级（国债、政策性金融债、地方政府债、央行票据、政府支持机构债等利率债，不含城投债）、399-不适用
      AND t1.bdqztpj NOT IN
          ('201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215',
           '216', '217', '218', '219', '220', '221', '222', '301', '302', '399')

    /*====================================================================================================
    # 规则代码: AM00329
    # 目标接口: J1038-回购标的券明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ({交易场所代码}为境内场所，{行业分类}不为空)时，{行业分类}的值在<上市公司行业分类表>中必须存在
             ({交易场所代码}为境外场所，{行业分类}不为空)时，{行业分类}的值在<上市公司行业分类表>或<全球行业分类系统表>中必须存在
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
    SELECT DISTINCT 'AM00329'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bdrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND (
        -- 上市公司行业分类: A-农、林、牧、渔业、B-采矿业、C-制造业、D-电力、热力、燃气及水生产和供应业、E-建筑业、F-批发和零售业、G-交通运输、仓储和邮政业、H-住宿和餐饮业、I-信息传输、软件和信息技术服务业、J-金融业、K-房地产业、L-租赁和商务服务业、M-科学研究和技术服务业、N-水利、环境和公共设施管理业、O-居民服务、修理和其他服务业、P-教育、Q-卫生和社会工作、R-文化、体育和娱乐业、S-综合
        (t1.jycsdm IN ('101', '102', '103', '104', '105', '106',
                       '107', '108', '111', '112', '113', '121', '122', '123', '124',
                       '125', '126', '131', '132', '133', '134', '135', '136', '137',
                       '138', '138', '199') AND t1.hyfl IS NOT NULL AND
         t1.hyfl NOT IN ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S'))
            -- 全球行业分类系统（GICS）: 10-能源、15-原材料、20-工业、25-消费者非必需品、30-消费者常用品、35-医疗保健、40-金融、45-信息技术、50-电信业务、55-公用事业、60-房地产
            OR (t1.jycsdm IN ('200', '210', '220', '230', '240', '250', '299') AND t1.hyfl IS NOT NULL AND
                t1.hyfl NOT IN
                ('10', '15', '20', '25', '30', '35', '40', '45', '50', '55', '60', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
                 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S'))
        )

    /*====================================================================================================
    # 规则代码: AM00330
    # 目标接口: J1038-回购标的券明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {标的券评级}的值在<债项评级表>中必须存在
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
    SELECT DISTINCT 'AM00330'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bdrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 债项评级:
      --    短期债券: 101-A-1、102-A-2、103-A-3、104-B、105-C、106-D
      --    中长期债券: 201-AAA+、202-AAA、203-AAA-、204-AA+、205-AA、206-AA-、207-A+、208-A、209-A-、210-BBB+、211-BBB、212-BBB-、213-BB+、214-BB、215-BB-、216-B+、217-B、218-B-、219-CCC、220-CC、221-C、222-D
      --    特殊情况: 301-无评级（信用债）、302-无评级（国债、政策性金融债、地方政府债、央行票据、政府支持机构债等利率债，不含城投债）、399-不适用
      AND t1.bdqpj NOT IN
          ('101', '102', '103', '104', '105', '106',
           '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214',
           '215', '216', '217', '218', '219', '220', '221', '222',
           '301', '302', '399')

    /*====================================================================================================
    # 规则代码: AM00331
    # 目标接口: J1038-回购标的券明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {股东证件类型}不为空时，{股东证件类型}的值在<证件类型表>中必须存在
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
    SELECT DISTINCT 'AM00331'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bdrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 证件类型:
      --    个人证件类型: 0101-身份证、0102-护照、0103-港澳居民来往内地通行证、0104-台湾居民来往大陆通行证、0105-军官证、0106-士兵证、010601-解放军士兵证、010602-武警士兵证、0107-户口本、0108-文职证、010801-解放军文职干部证、010802-武警文职干部证、0109-警官证、0110-社会保障号、0111-外国人永久居留证、0112-外国护照、0113-临时身份证、0114-港澳：回乡证、0115-台：台胞证、0116-港澳台居民居住证、0199-其他人员证件
      --    机构证件类型: 0201-组织机构代码、0202-工商营业执照、0203-社团法人注册登记证书、0204-机关事业法人成立批文、0205-批文、0206-军队凭证、0207-武警凭证、0208-基金会凭证、0209-特殊法人注册号、0210-统一社会信用代码、0211-行政机关、0212-社会团体、0213-下属机构（具有主管单位批文号）、0299-其他机构证件号
      --    产品证件类型: 0301-营业执照、0302-登记证书、0303-批文、0304-产品正式代码、0399-其它
      AND t1.gdzjlx IS NOT NULL
      AND t1.gdzjlx NOT IN
          ('0101', '0102', '0103', '0104', '0105', '0106', '010601', '010602', '0107', '0108', '010801', '010802',
           '0109', '0110', '0111', '0112', '0113', '0114', '0115', '0116', '0199',
           '0201', '0202', '0203', '0204', '0205', '0206', '0207', '0208', '0209', '0210', '0211', '0212', '0213',
           '0299', '0301', '0302', '0303', '0304', '0399')

    /*====================================================================================================
    # 规则代码: AM00332
    # 目标接口: J1038-回购标的券明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {标的券数量}>0
             {标的券最新市值}＞0
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
    SELECT DISTINCT 'AM00332'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bdrepo t1
    WHERE t1.jgdm = '70610000'
        AND t1.status NOT IN ('3', '5')
        AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
        AND t1.bdqsl <= 0
       OR t1.bdqzxsz <= 0

    /*====================================================================================================
    # 规则代码: AM00333
    # 目标接口: J1038-回购标的券明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {为无限售条件的流通股标志}不为空时，{为无限售条件的流通股标志}的值必须为“0”或“1”
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
    SELECT DISTINCT 'AM00333'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bdrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.wwxstjdltgbz IS NOT NULL
      AND t1.wwxstjdltgbz NOT IN ('0', '1')

    /*====================================================================================================
    # 规则代码: AM00334
    # 目标接口: J1039-商品现货投资明细
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
    SELECT DISTINCT 'AM00334'   AS gzdm        -- 规则代码
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
      AND t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00335
    # 目标接口: J1039-商品现货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
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
    SELECT DISTINCT 'AM00335'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comspt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                            '107', '108', '111', '112', '113', '121', '122', '123', '124',
                            '125', '126', '131', '132', '133', '134', '135', '136', '137',
                            '138', '138', '199', '200', '210', '220', '230', '240', '250',
                            '299')

    /*====================================================================================================
    # 规则代码: AM00336
    # 目标接口: J1039-商品现货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}>=0
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
    SELECT DISTINCT 'AM00336'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comspt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00337
    # 目标接口: J1039-商品现货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 上期[商品现货投资明细].{期末数量}>0，且本期[产品运行信息].{合同已终止标志}=“否”时，{期末数量}>=0
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
    SELECT DISTINCT 'AM00337'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comspt t1
             LEFT JOIN report_cisp.wdb_amp_inv_comspt t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_comspt tt1 WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_prod_oprt t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.qmsl > 0
      AND t3.htyzzbz = '0'
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00338
    # 目标接口: J1040-商品现货延期交收投资明细
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
    SELECT DISTINCT 'AM00338'   AS gzdm        -- 规则代码
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
      AND t2.cpdm IS NULL

    /*====================================================================================================
    # 规则代码: AM00339
    # 目标接口: J1040-商品现货延期交收投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
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
    SELECT DISTINCT 'AM00339'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comsptdly t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                            '107', '108', '111', '112', '113', '121', '122', '123', '124',
                            '125', '126', '131', '132', '133', '134', '135', '136', '137',
                            '138', '138', '199', '200', '210', '220', '230', '240', '250',
                            '299')

    /*====================================================================================================
    # 规则代码: AM00340
    # 目标接口: J1040-商品现货延期交收投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {买卖方向}的值在<买卖方向表>中必须存在
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
    SELECT DISTINCT 'AM00340'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comsptdly t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 买卖方向: 1-买入、2-卖出
      AND t1.mmfx NOT IN ('1', '2')

    /*====================================================================================================
    # 规则代码: AM00341
    # 目标接口: J1040-商品现货延期交收投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {开仓数量}>=0，{开仓保证金}>=0或为空
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
    SELECT DISTINCT 'AM00341'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comsptdly t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.kcsl < 0 OR t1.kcbzj < 0 OR t1.kcbzj IS NOT NULL)

    /*====================================================================================================
    # 规则代码: AM00342
    # 目标接口: J1040-商品现货延期交收投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}>=0
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
    SELECT DISTINCT 'AM00342'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comsptdly t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00343
    # 目标接口: J1040-商品现货延期交收投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 上期[商品现货投资明细].{期末数量}>0，且本期[产品运行信息].{合同已终止标志}=“否”时，{期末数量}>=0
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
    SELECT DISTINCT 'AM00343'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comsptdly t1
             LEFT JOIN report_cisp.wdb_amp_inv_comsptdly t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq)
                                FROM report_cisp.wdb_amp_inv_comsptdly tt1
                                WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_prod_oprt t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.qmsl > 0
      AND t3.htyzzbz = '0'
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00344
    # 目标接口: J1041-金融期货投资明细
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
    SELECT DISTINCT 'AM00344'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_finftr t1
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
    # 规则代码: AM00345
    # 目标接口: J1041-金融期货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
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
    SELECT DISTINCT 'AM00345'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_finftr t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                            '107', '108', '111', '112', '113', '121', '122', '123', '124',
                            '125', '126', '131', '132', '133', '134', '135', '136', '137',
                            '138', '138', '199', '200', '210', '220', '230', '240', '250',
                            '299')

    /*====================================================================================================
    # 规则代码: AM00346
    # 目标接口: J1041-金融期货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {买卖方向}的值在<买卖方向表>中必须存在
             {交易目的}的值在<交易目的表>中必须存在
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
    SELECT DISTINCT 'AM00346'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_finftr t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 买卖方向: 1-买入、2-卖出
      AND (t1.mmfx NOT IN ('1', '2')
        -- 交易目的: 1-套保、2-投机
        OR t1.jymd NOT IN ('1', '2'))

    /*====================================================================================================
    # 规则代码: AM00347
    # 目标接口: J1041-金融期货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {开仓数量}>=0，{开仓保证金}>=0或为空
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
    SELECT DISTINCT 'AM00347'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_finftr t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.kcsl < 0 OR t1.kcbzj < 0 OR t1.kcbzj IS NOT NULL)

    /*====================================================================================================
    # 规则代码: AM00348
    # 目标接口: J1041-金融期货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {金融期货类别}的值在<金融期货类别表>中必须存在
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
    SELECT DISTINCT 'AM00348'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_finftr t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 金融期货类别 1-股指期货、2-利率期货、3-外汇期货
      AND t1.jrqhlb NOT IN ('1', '2', '3')

    /*====================================================================================================
    # 规则代码: AM00349
    # 目标接口: J1041-金融期货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ({交易场所代码}为“中国金融期货交易所”，{金融期货类别}为“股指期货”)时，{交易代码}的值必须为“IF”或“IC”或“IH”或“IM”
			 ({交易场所代码}为“中国金融期货交易所”，{金融期货类别}为“利率期货”)时，{交易代码}的值必须为“TS”或“TF”或“T”
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
    SELECT DISTINCT 'AM00349'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_finftr t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码: 121-中国金融期货交易所
      AND t1.jycsdm = '121'
      AND ((
               -- 金融期货类别: 1-股指期货、2-利率期货、3-外汇期货
               t1.jrqhlb = '1' AND t1.jydm NOT IN ('IF', 'IC', 'IH', 'IM'))
        OR (t1.jrqhlb = '2' AND t1.jydm NOT IN ('IF', 'IC', 'IH', 'IM')))

    /*====================================================================================================
    # 规则代码: AM00350
    # 目标接口: J1041-金融期货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}>=0
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
    SELECT DISTINCT 'AM00350'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_finftr t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00351
    # 目标接口: J1041-金融期货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 上期[金融期货投资明细].{期末数量}>0，且本期[产品运行信息].{合同已终止标志}=“否”时，{期末数量}>=0
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
    SELECT DISTINCT 'AM00351'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_finftr t1
             LEFT JOIN report_cisp.wdb_amp_inv_finftr t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_finftr tt1 WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_prod_oprt t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.qmsl > 0
      AND t3.htyzzbz = '0'
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00352
    # 目标接口: J1042-商品期货投资明细
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
    SELECT DISTINCT 'AM00352'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comftr t1
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
    # 规则代码: AM00353
    # 目标接口: J1042-商品期货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
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
    SELECT DISTINCT 'AM00353'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comftr t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                            '107', '108', '111', '112', '113', '121', '122', '123', '124',
                            '125', '126', '131', '132', '133', '134', '135', '136', '137',
                            '138', '138', '199', '200', '210', '220', '230', '240', '250',
                            '299')

    /*====================================================================================================
    # 规则代码: AM00354
    # 目标接口: J1042-商品期货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {买卖方向}的值在<买卖方向表>中必须存在
             {交易目的}的值在<交易目的表>中必须存在
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
    SELECT DISTINCT 'AM00354'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comftr t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 买卖方向: 1-买入、2-卖出
      AND (t1.mmfx NOT IN ('1', '2')
        -- 交易目的: 1-套保、2-投机
        OR t1.jymd NOT IN ('1', '2'))

    /*====================================================================================================
    # 规则代码: AM00355
    # 目标接口: J1042-商品期货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {开仓数量}>=0，{开仓保证金}>=0或为空
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
    SELECT DISTINCT 'AM00355'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comftr t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.kcsl < 0 OR t1.kcbzj < 0 OR t1.kcbzj IS NOT NULL)

    /*====================================================================================================
    # 规则代码: AM00356
    # 目标接口: J1042-商品期货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}>=0
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
    SELECT DISTINCT 'AM00356'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comftr t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00357
    # 目标接口: J1042-商品期货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 上期[商品期货投资明细].{期末数量}>0，且本期[产品运行信息].{合同已终止标志}=“否”时，{期末数量}>=0
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
    SELECT DISTINCT 'AM00357'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comftr t1
             LEFT JOIN report_cisp.wdb_amp_inv_comftr t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_comftr tt1 WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_prod_oprt t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.qmsl > 0
      AND t3.htyzzbz = '0'
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00358
    # 目标接口: J1043-场内期权投资明细
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
    SELECT DISTINCT 'AM00358'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_opt t1
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
    # 规则代码: AM00359
    # 目标接口: J1043-场内期权投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
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
    SELECT DISTINCT 'AM00359'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_opt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                            '107', '108', '111', '112', '113', '121', '122', '123', '124',
                            '125', '126', '131', '132', '133', '134', '135', '136', '137',
                            '138', '138', '199', '200', '210', '220', '230', '240', '250',
                            '299')

    /*====================================================================================================
    # 规则代码: AM00360
    # 目标接口: J1043-场内期权投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {开仓数量}>0时，{开仓权利金}>0
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
    SELECT DISTINCT 'AM00360'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_opt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.kcsl > 0
      AND t1.kcqlj <= 0

    /*====================================================================================================
    # 规则代码: AM00361
    # 目标接口: J1043-场内期权投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ({买卖方向}为“卖出”，{开仓数量}>0)时，{开仓保证金}>=0或为空
			 {买卖方向}为“卖出”时，{期末保证金}>=0或为空
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
    SELECT DISTINCT 'AM00361'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_opt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 买卖方向: 1-买入、2-卖出
      AND t1.mmfx = '2'
      AND ((t1.kcsl > 0 AND t1.ckbzj < 0 AND t1.ckbzj IS NOT NULL)
        OR (t1.qmbzj < 0 AND t1.qmbzj IS NOT NULL))

    /*====================================================================================================
    # 规则代码: AM00362
    # 目标接口: J1043-场内期权投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}>=0
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
    SELECT DISTINCT 'AM00362'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_opt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00363
    # 目标接口: J1043-场内期权投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 上期[场内期权投资明细].{期末数量}>0，且本期[产品运行信息].{合同已终止标志}=“否”时，{期末数量}>=0
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
    SELECT DISTINCT 'AM00363'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_opt t1
             LEFT JOIN report_cisp.wdb_amp_inv_opt t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_opt tt1 WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_prod_oprt t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.qmsl > 0
      AND t3.htyzzbz = '0'
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00364
    # 目标接口: J1043-场内期权投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {合约类型}的值在<期权合约类型表>中必须存在
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
    SELECT DISTINCT 'AM00364'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_opt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 期权合约类型: 1-认购、2-认沽
      AND t1.hylx NOT IN ('1', '2')

    /*====================================================================================================
    # 规则代码: AM00365
    # 目标接口: J1043-场内期权投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {行权价格}>0
			 {行权方式}的值在<行权方式表>中必须存在
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
    SELECT DISTINCT 'AM00365'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_opt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 行权方式: 1-欧式、2-美式、3-百慕大、4-亚式
      AND (t1.xqjg <= 0
        OR t1.xqfs NOT IN ('1', '2', '3', '4'))

    /*====================================================================================================
    # 规则代码: AM00366
    # 目标接口: J1044-其他各项资产明细
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
    SELECT DISTINCT 'AM00366'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_othassets t1
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
    # 规则代码: AM00367
    # 目标接口: J1044-其他各项资产明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {其他各项资产类别}的值在<其他各项资产类别表>中必须存在
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
    SELECT DISTINCT 'AM00367'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_othassets t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 其他各项资产类别: 1-结算备付金、2-应收证券清算款、3-应收股利、4-应收利息、5-应收申购款、6-待摊费用、7-存出保证金、9-其他
      AND t1.qtgxzclb NOT IN ('1', '2', '3', '4', '5', '6', '7', '9')

    /*====================================================================================================
    # 规则代码: AM00368
    # 目标接口: J1044-其他各项资产明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末金额}<>0
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
    SELECT DISTINCT 'AM00368'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_othassets t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmje = 0

    /*====================================================================================================
    # 规则代码: AM00369
    # 目标接口: J1045-未挂牌资产支持证券投资明细
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
    SELECT DISTINCT 'AM00369'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_abs t1
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
      # 规则代码: AM00370
      # 目标接口: J1045-未挂牌资产支持证券投资明细
      # 目标接口传输频度: 日
      # 目标接口传输时间: T+4日24:00前
      # 规则说明: {期末数量}>=0
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
    SELECT DISTINCT 'AM00370'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_abs t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00371
    # 目标接口: J1045-未挂牌资产支持证券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {资产支持证券类别}的值在<资产支持证券类别表>中必须存在
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
    SELECT DISTINCT 'AM00371'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_abs t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产支持证券类别: 1-资产支持证券、2-资产支持票据
      AND t1.zczczqlb NOT IN ('1', '2')

    /*====================================================================================================
    # 规则代码: AM00372
    # 目标接口: J1045-未挂牌资产支持证券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {主体评级}的值在<主体评级表>中必须存在
             {债项评级}的值在<债项评级表>中必须存在
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
    SELECT DISTINCT 'AM00372'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_abs t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (
        -- 主体评级: 201-AAA+、202-AAA、203-AAA-、204-AA+、205-AA、206-AA-、207-A+、208-A、209-A-、210-BBB+、211-BBB、212-BBB-、213-BB+、214-BB、215-BB-、216-B+、217-B、218-B-、219-CCC、220-CC、221-C、222-D
        --    特殊情况: 301-无评级（信用债）、302-无评级（国债、政策性金融债、地方政府债、央行票据、政府支持机构债等利率债，不含城投债）、399-不适用
        (t1.ztypj NOT IN
         ('201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215',
          '216', '217', '218', '219', '220', '221', '222', '301', '302', '399'))
            -- 债项评级:
            --    短期债券: 101-A-1、102-A-2、103-A-3、104-B、105-C、106-D
            --    中长期债券: 201-AAA+、202-AAA、203-AAA-、204-AA+、205-AA、206-AA-、207-A+、208-A、209-A-、210-BBB+、211-BBB、212-BBB-、213-BB+、214-BB、215-BB-、216-B+、217-B、218-B-、219-CCC、220-CC、221-C、222-D
            --    特殊情况: 301-无评级（信用债）、302-无评级（国债、政策性金融债、地方政府债、央行票据、政府支持机构债等利率债，不含城投债）、399-不适用
            OR (t1.zxpj NOT IN
                ('101', '102', '103', '104', '105', '106',
                 '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214',
                 '215', '216', '217', '218', '219', '220', '221', '222',
                 '301', '302', '399'))
        )

    /*====================================================================================================
    # 规则代码: AM00373
    # 目标接口: J1045-未挂牌资产支持证券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {发行日期}<={数据日期}
			 {发行日期}<{到期日期}
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
    SELECT DISTINCT 'AM00373'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_abs t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.fxrq > t1.sjrq
        OR t1.fxrq >= t1.dqrq)

    /*====================================================================================================
    # 规则代码: AM00374
    # 目标接口: J1045-未挂牌资产支持证券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {付息方式}的值在<付息方式表>中必须存在
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
    SELECT DISTINCT 'AM00374'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_abs t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 付息方式: 1-零息、2-贴现、3-附息
      AND t1.fxfs NOT IN ('1', '2', '3')

    /*====================================================================================================
    # 规则代码: AM00375
    # 目标接口: J1045-未挂牌资产支持证券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {面值}>0
             {票面利率}>不为空时，{票面利率}>0
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
    SELECT DISTINCT 'AM00375'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_abs t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.mz <= 0
        OR (t1.pmll IS NOT NULL AND t1.pmll <= 0))

    /*====================================================================================================
    # 规则代码: AM00376
    # 目标接口: J1045-未挂牌资产支持证券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 上期[未挂牌资产支持证券投资明细].{期末数量}>0，且本期[产品运行信息].{合同已终止标志}=“否”时，{期末数量}>=0
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
    SELECT DISTINCT 'AM00376'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_abs t1
             LEFT JOIN report_cisp.wdb_amp_inv_abs t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_abs tt1 WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_prod_oprt t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.qmsl > 0
      AND t3.htyzzbz = '0'
      AND t1.qmsl < 0

    /*====================================================================================================
    # 规则代码: AM00377
    # 目标接口: J1046-转融券投资明细
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
    SELECT DISTINCT 'AM00377'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_refinance t1
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
    # 规则代码: AM00378
    # 目标接口: J1046-转融券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
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
    SELECT DISTINCT 'AM00378'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_refinance t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                            '107', '108', '111', '112', '113', '121', '122', '123', '124',
                            '125', '126', '131', '132', '133', '134', '135', '136', '137',
                            '138', '138', '199', '200', '210', '220', '230', '240', '250',
                            '299')

    /*====================================================================================================
    # 规则代码: AM00379
    # 目标接口: J1046-转融券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {出借数量}>=0
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
    SELECT DISTINCT 'AM00379'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_refinance t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cjsl < 0

    /*====================================================================================================
    # 规则代码: AM00380
    # 目标接口: J1046-转融券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {证券类别}的值在<融资融券证券类别表>中必须存在
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
    SELECT DISTINCT 'AM00380'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_refinance t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 融资融券证券类别: 1-普通A股、2-普通B股、3-新三板股票、4-场外公募基金、5-债券、9-其他
      AND t1.zqlb NOT IN ('1', '2', '3', '4', '5', '9')

    /*====================================================================================================
    # 规则代码: AM00381
    # 目标接口: J1046-转融券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {出借期限}的值在<转融券出借期限表>中必须存在
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
    SELECT DISTINCT 'AM00381'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_refinance t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 转融券出借期限: 1-3天、2-7天、3-14天、4-28天、5-182天、9-其他
      AND t1.cjqx NOT IN ('1', '2', '3', '4', '5', '9')

    /*====================================================================================================
    # 规则代码: AM00382
    # 目标接口: J1046-转融券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {出借费率}>=0，{收盘价}>=0，{借券费用}>=0
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
    SELECT DISTINCT 'AM00382'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_refinance t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.cjfl < 0 OR t1.spj < 0 OR t1.sqfy < 0)

    /*====================================================================================================
    # 规则代码: AM00383
    # 目标接口: J1046-转融券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: 上期[转融券投资明细].{出借数量}>0时，{出借数量}>=0
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
    SELECT DISTINCT 'AM00383'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_refinance t1
             LEFT JOIN report_cisp.wdb_amp_inv_refinance t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = (SELECT max(tt1.sjrq)
                                          FROM report_cisp.wdb_amp_inv_refinance tt1
                                          WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.cjsl > 0
      AND t1.cjsl < 0

    /*====================================================================================================
    # 规则代码: AM00384
    # 目标接口: J1047-债券借贷投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {成交编号}+{成交日期}的值在[债券借贷投资质押券明细]中必须存在
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 0
    # 其他接口: J1048-债券借贷投资质押券明细
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00384'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_loan t1
             LEFT JOIN report_cisp.wdb_amp_inv_loan_bond t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
                           AND t1.cjbh = t2.cjbh
                           AND t1.cjrq = t2.cjrq
    WHERE t1.jgdm = 70610000
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t2.cjbh IS NULL OR t2.cjrq IS NULL)

    /*====================================================================================================
    # 规则代码: AM00385
    # 目标接口: J1047-债券借贷投资明细
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
    SELECT DISTINCT 'AM00385'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_loan t1
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
    # 规则代码: AM00386
    # 目标接口: J1047-债券借贷投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
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
    SELECT DISTINCT 'AM00386'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_loan t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                            '107', '108', '111', '112', '113', '121', '122', '123', '124',
                            '125', '126', '131', '132', '133', '134', '135', '136', '137',
                            '138', '138', '199', '200', '210', '220', '230', '240', '250',
                            '299')

    /*====================================================================================================
    # 规则代码: AM00387
    # 目标接口: J1047-债券借贷投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {借贷方向}的值必须只包含”1”或”2”
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
    SELECT DISTINCT 'AM00387'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_loan t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.jdfx NOT IN ('1', '2')

    /*====================================================================================================
    # 规则代码: AM00388
    # 目标接口: J1047-债券借贷投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {质押券转换安排标志}的值必须只包含”0”或”1”
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
    SELECT DISTINCT 'AM00388'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_loan t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.zyqzhapbz NOT IN ('0', '1')

    /*====================================================================================================
    # 规则代码: AM00389
    # 目标接口: J1047-债券借贷投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易对手方类型}的值在<交易对手方类型表>中必须存在
			 {交易对手方类型}为“个人”或“机构”时，{交易对手方证件号码}不能为空
			 {交易对手方类型}为“机构”时，{交易对手方主体评级}的值在<主体评级表>中必须存在
			 {交易对手方类型}为“产品”时，{交易对手方产品代码}不能为空
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
    SELECT DISTINCT 'AM00389'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_loan t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易对手方类型:
      --    个人: 001-个人
      --    机构: 101-银行、102-银行子公司、103-保险公司、104-保险公司子公司、105-信托公司、106-财务公司、107-证券公司、108-证券公司子公司、109-基金管理公司、110-基金管理公司子公司、111-期货公司、112-期货公司子公司、113-私募基金管理人、114-其他金融机构、115-境内非金融机构、116-境外金融机构、117-境外非金融机构
      --    产品: 201-银行理财产品、202-银行子公司公募理财、203-银行子公司理财产品、204-信托计划、205-保险产品、206-保险公司资产管理计划、229-保险公司子公司公募基金、207-保险公司子公司资产管理计划、208-证券公司公募基金、209-证券公司资产管理计划、210-证券公司子公司公募基金、211-证券公司子公司资产管理计划、212-基金管理公司公募基金、213-基金管理公司资产管理计划、214-基金管理公司子公司资产管理计划、215-期货公司资产管理计划、216-期货公司子公司资产管理计划、217-私募投资基金、218-政府引导基金、219-全国社保基金、220-地方社保基金、221-基本养老保险、222-养老金产品、223-企业年金、224-中央职业年金、225-省属职业年金、226-社会公益基金（慈善基金、捐赠基金等）、227-境外资金（QFII）、228-境外资金（RQFII）、299-其他产品
      AND (t1.jydsflx NOT IN ('001',
                              '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113',
                              '114', '115',
                              '116', '117',
                              '201', '202', '203', '204', '205', '206', '229', '207', '208', '209', '210', '211', '212',
                              '213', '214',
                              '215', '216', '217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227',
                              '228', '299')
        OR (t1.jydsflx IN
            ('001', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114',
             '115', '116', '117') AND t1.jydsfzjhm IS NULL)
        OR
        -- 主体评级: 201-AAA+、202-AAA、203-AAA-、204-AA+、205-AA、206-AA-、207-A+、208-A、209-A-、210-BBB+、211-BBB、212-BBB-、213-BB+、214-BB、215-BB-、216-B+、217-B、218-B-、219-CCC、220-CC、221-C、222-D
        --    特殊情况: 301-无评级（信用债）、302-无评级（国债、政策性金融债、地方政府债、央行票据、政府支持机构债等利率债，不含城投债）、399-不适用
           (t1.jydsflx IN ('101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113',
                           '114', '115', '116', '117') AND t1.jydsfztpj NOT IN
                                                           ('201', '202', '203', '204', '205', '206', '207', '208',
                                                            '209', '210', '211', '212', '213', '214',
                                                            '215',
                                                            '216', '217', '218', '219', '220', '221', '222', '301',
                                                            '302', '399'))
        OR (t1.jydsflx IN
            ('201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214',
             '215', '216', '217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227', '228',
             '299') AND t1.jydsfcpdm IS NULL)
        )

    /*====================================================================================================
    # 规则代码: AM00390
    # 目标接口: J1047-债券借贷投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易对手方产品代码}第7位为“1”时，{交易对手方类型}必须为“201-银行理财产品”、“202-银行子公司公募理财”、“203-银行子公司理财产品”
             {交易对手方产品代码}第7位为“2”时，{交易对手方类型}必须为“204-信托计划”
             {交易对手方产品代码}第7位为“3”时，{交易对手方类型}必须为“208-证券公司公募基金”、“209-证券公司资产管理计划“、“210-证券公司子公司公募基金”、“211-证券公司子公司资产管理计划”
             {交易对手方产品代码}第7位为“4”时，{交易对手方类型}必须为“212-基金管理公司公募基金”、“213-基金管理公司资产管理计划”、“214-基金管理公司子公司资产管理计划”
             {交易对手方产品代码}第7位为“5”时，{交易对手方类型}必须为“215-期货公司资产管理计划”、“216-期货公司子公司资产管理计划”
             {交易对手方产品代码}第7位为“6”时，{交易对手方类型}必须为“205-保险产品”、“206-保险公司资产管理计划”、“229-保险公司子公司公募基金”、“207-保险公司子公司资产管理计划”
             {交易对手方产品代码}第7位为“8”时，{交易对手方类型}必须为“208-证券公司公募基金”、“210-证券公司子公司公募基金”、“212-基金管理公司公募基金”、“229-保险公司子公司公募基金”
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
    SELECT DISTINCT 'AM00390'   AS gzdm        -- 规则代码
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
            -- 交易对手方类型: 212-基金管理公司公募基金、213-基金管理公司资产管理计划、214-基金管理公司子公司资产管理计划
            OR (substr(t1.jydsfcpdm, 7, 1) = '4' AND t1.jydsflx NOT IN ('212', '213', '214'))
            -- 交易对手方类型: 215-期货公司资产管理计划、216-期货公司子公司资产管理计划
            OR (substr(t1.jydsfcpdm, 7, 1) = '5' AND t1.jydsflx NOT IN ('215', '216'))
            -- 交易对手方类型: 205-保险产品、206-保险公司资产管理计划、229-保险公司子公司公募基金、207-保险公司子公司资产管理计划
            OR (substr(t1.jydsfcpdm, 7, 1) = '6' AND t1.jydsflx NOT IN ('205', '206', '229', '207'))
            -- 交易对手方类型: 208-证券公司公募基金、210-证券公司子公司公募基金、212-基金管理公司公募基金、229-保险公司子公司公募基金
            OR (substr(t1.jydsfcpdm, 7, 1) = '8' AND t1.jydsflx NOT IN ('208', '210', '212', '229'))
        )

    /*====================================================================================================
    # 规则代码: AM00391
    # 目标接口: J1048-债券借贷投资质押券明细
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
    SELECT DISTINCT 'AM00391'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_loan_bond t1
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
    # 规则代码: AM00392
    # 目标接口: J1048-债券借贷投资质押券明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {交易场所代码}的值在<交易场所代码表>中必须存在
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
    SELECT DISTINCT 'AM00392'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_loan_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码:
      --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、104-沪港通下港股通、105-深港通下港股通、106-全国中小企业股份转让系统、107-区域股权市场、108-北京证券交易所、111-沪港通(北向)、112-深港通(北向)、113-沪伦通(东向)、121-中国金融期货交易所、122-上海期货交易所、123-上海国际能源交易中心、124-上海黄金交易所、125-大连商品交易所、126-郑州商品交易所、127-广州期货交易所、131-机构间私募产品报价与服务系统、132-证券公司柜台市场、133-银行业信贷资产登记流转中心、134-上海票据交易所、135-北京金融资产交易所、136-上海保险交易所、137-商业银行柜台市场、138-地方金融资产交易所（除北京外）、199-其他（境内）
      --    境外: 200-北美、210-南美、220-欧洲、230-日本、240-亚太（除日本和香港外）、250-香港、299-其他（境外）
      AND t1.jycsdm NOT IN ('101', '102', '103', '104', '105', '106',
                            '107', '108', '111', '112', '113', '121', '122', '123', '124',
                            '125', '126', '131', '132', '133', '134', '135', '136', '137',
                            '138', '138', '199', '200', '210', '220', '230', '240', '250',
                            '299')

    /*====================================================================================================
    # 规则代码: AM00393
    # 目标接口: J1048-债券借贷投资质押券明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {债券类别}的值在<债券类别表>中必须存在
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
    SELECT DISTINCT 'AM00393'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_loan_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 债券类别: 01-国债、02-地方政府债、03-央行票据、04-政策性银行债、05-商业银行债、06-非银行金融机构债、07-企业债、08-公司债、09-中期票据、10-短期融资券（含超短融）、11-定向工具、12-政府支持机构债（铁道债、中央汇金债券，不含城投债）、13-可转债、14-可交换债、15-可分离转债纯债、16-资产支持证券（在交易所挂牌）、17-私募债、18-境外债券、19-国际机构债、20-次级债、21-永续债、22-永续次级债、23-集合票据、99-其他
      AND t1.zqlb NOT IN
          ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18',
           '19', '20', '21', '22', '23', '99')

    /*====================================================================================================
    # 规则代码: AM00394
    # 目标接口: J1048-债券借贷投资质押券明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {主体评级}的值在<主体评级表>中必须存在
             {债项评级}的值在<债项评级表>中必须存在
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
    SELECT DISTINCT 'AM00394'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_loan_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (
        -- 主体评级: 201-AAA+、202-AAA、203-AAA-、204-AA+、205-AA、206-AA-、207-A+、208-A、209-A-、210-BBB+、211-BBB、212-BBB-、213-BB+、214-BB、215-BB-、216-B+、217-B、218-B-、219-CCC、220-CC、221-C、222-D
        --    特殊情况: 301-无评级（信用债）、302-无评级（国债、政策性金融债、地方政府债、央行票据、政府支持机构债等利率债，不含城投债）、399-不适用
        (t1.ztypj NOT IN
         ('201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215',
          '216', '217', '218', '219', '220', '221', '222', '301', '302', '399'))
            -- 债项评级:
            --    短期债券: 101-A-1、102-A-2、103-A-3、104-B、105-C、106-D
            --    中长期债券: 201-AAA+、202-AAA、203-AAA-、204-AA+、205-AA、206-AA-、207-A+、208-A、209-A-、210-BBB+、211-BBB、212-BBB-、213-BB+、214-BB、215-BB-、216-B+、217-B、218-B-、219-CCC、220-CC、221-C、222-D
            --    特殊情况: 301-无评级（信用债）、302-无评级（国债、政策性金融债、地方政府债、央行票据、政府支持机构债等利率债，不含城投债）、399-不适用
            OR (t1.zxpj NOT IN
                ('101', '102', '103', '104', '105', '106',
                 '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214',
                 '215', '216', '217', '218', '219', '220', '221', '222',
                 '301', '302', '399'))
        )

    /*====================================================================================================
    # 规则代码: AM00395
    # 目标接口: J1048-债券借贷投资质押券明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {付息方式}的值在<付息方式表>中必须存在
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
    SELECT DISTINCT 'AM00395'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_loan_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 付息方式	: 1-零息、2-贴现、3-附息
      AND t1.fxfs NOT IN ('1', '2', '3')

    /*====================================================================================================
    # 规则代码: AM00396
    # 目标接口: J1048-债券借贷投资质押券明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {行业分类}不为空时，{行业分类}的值在<上市公司行业分类>或<全球行业分类系统（GICS）>中必须存在
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
    SELECT DISTINCT 'AM00396'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_loan_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (
        -- 交易场所代码:
        --    境内: 101-银行间市场、102-上海证券交易所、103-深圳证券交易所、108-北京证券交易所
        -- 上市公司行业分类: A-农、林、牧、渔业、B-采矿业、C-制造业、D-电力、热力、燃气及水生产和供应业、E-建筑业、F-批发和零售业、G-交通运输、仓储和邮政业、H-住宿和餐饮业、I-信息传输、软件和信息技术服务业、J-金融业、K-房地产业、L-租赁和商务服务业、M-科学研究和技术服务业、N-水利、环境和公共设施管理业、O-居民服务、修理和其他服务业、P-教育、Q-卫生和社会工作、R-文化、体育和娱乐业、S-综合
        (t1.jycsdm IN ('101', '102', '103', '108') AND t1.hyfl IS NOT NULL AND
         t1.hyfl NOT IN ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S'))
            -- 全球行业分类系统（GICS）: 10-能源、15-原材料、20-工业、25-消费者非必需品、30-消费者常用品、35-医疗保健、40-金融、45-信息技术、50-电信业务、55-公用事业、60-房地产
            OR (t1.hyfl IS NOT NULL AND
                t1.hyfl NOT IN
                ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', '10',
                 '15', '20', '25', '30', '35', '40', '45', '50', '55', '60'))
        )

    /*====================================================================================================
    # 规则代码: AM00397
    # 目标接口: J1048-债券借贷投资质押券明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {债券类别}为“商业银行债”时，{发行人净资产}不能为空
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
    SELECT DISTINCT 'AM00397'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_loan_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 债券类别: 05-商业银行债
      AND t1.zqlb = '05'
      AND t1.fxrjzc IS NULL

    /*====================================================================================================
    # 规则代码: AM00398
    # 目标接口: J1049-货币市场基金监控
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {影子定价确定的资产净值}不为空时，{影子定价确定的资产净值}>0
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
    SELECT DISTINCT 'AM00398'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_rsk_monitor t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.yzdjqddzcjz IS NOT NULL
      AND t1.yzdjqddzcjz <= 0

    /*====================================================================================================
    # 规则代码: AM00399
    # 目标接口: J1049-货币市场基金监控
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {产品投资组合平均剩余期限}>=0
             {产品投资组合平均剩余存续期}>=0
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
    SELECT DISTINCT 'AM00399'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_rsk_monitor t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND (t1.cptzzhpjsyqx < 0
        OR t1.cptzzhpjsyxq < 0)

    /*====================================================================================================
    # 规则代码: AM00400
    # 目标接口: J3050-涉诉情况
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
    SELECT DISTINCT 'AM00400'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_rsk_appeal t1
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
    # 规则代码: AM00401
    # 目标接口: J3050-涉诉情况
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {涉诉金额}>=0
			 {赔付金额}>=0
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
    SELECT DISTINCT 'AM00401'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_rsk_appeal t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND (t1.ssje < 0
        OR t1.pfje < 0)

    /*====================================================================================================
    # 规则代码: AM00402
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {总份数}=={场内总份数}+{场外总份数}
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
    SELECT DISTINCT 'AM00402'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.zfs <> t1.cnzfs + t1.cwzfs

    /*====================================================================================================
    # 规则代码: AM00403
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {产品代码}在[下属产品运行信息]中有多个下属产品时，{总份数}==SUM[下属产品运行信息].{总份数}
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1008-下属产品运行信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00403'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
             LEFT JOIN (SELECT tt1.jgdm
                             , tt1.status
                             , tt1.sjrq
                             , tt1.cpzdm
                             , sum(tt1.zfs) AS subprod_zfs_sum
                        FROM report_cisp.wdb_amp_subprod_oprt tt1
                        GROUP BY jgdm, status, sjrq, cpzdm) t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpzdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.zfs <> t2.subprod_zfs_sum

    /*====================================================================================================
    # 规则代码: AM00404
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {本年累计费用总和}={本年累计管理费}+{本年累计托管费}+{本年累计销售服务费}+{本年累计业绩报酬}
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
    SELECT DISTINCT 'AM00404'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_oprt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.bnljfyzh <> t1.bnljglf + t1.bnljxsfwf + t1.bnljyjbc

    /*====================================================================================================
    # 规则代码: AM00405
    # 目标接口: J1008-下属产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {总份数}=={场内总份数}+{场外总份数}
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
    SELECT DISTINCT 'AM00405'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_subprod_oprt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.zfs <> t1.cnzfs + t1.cwzfs

    /*====================================================================================================
    # 规则代码: AM00406
    # 目标接口: J1009-产品净值信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {总份额}不为0时，{单位净值}==ROUND({资产净值}/{总份额},8)
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
    SELECT DISTINCT 'AM00406'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_nav t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.zfs <> 0
      AND t1.dwjz - round(t1.zcjz / t1.zfe, 8) > 0.001

    /*====================================================================================================
    # 规则代码: AM00407
    # 目标接口: J1010-QDII及FOF产品净值信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {总份额}不为0时，{单位净值}==ROUND({资产净值}/{总份额},8)
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
    SELECT DISTINCT 'AM00407'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_qd_nav t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.zfs <> 0
      AND t1.dwjz - round(t1.zcjz / t1.zfe, 8) > 0.001

    /*====================================================================================================
    # 规则代码: AM00408
    # 目标接口: J1011-产品收益分配信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [产品基本信息].{净值型产品标志}为“是”时，{实际分配收益}={现金分配金额}+{再投资金额}
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
    SELECT DISTINCT 'AM00408'   AS gzdm        -- 规则代码
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
      AND t1.sjfpsy <> t1.xjfpsy + t1.ztzje

    /*====================================================================================================
    # 规则代码: AM00409
    # 目标接口: J1011-产品收益分配信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {应分配收益}={分配基数}*{单位产品分配收益}
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
    SELECT DISTINCT 'AM00409'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_prod_divid t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.jzxcpbz = '1'
      AND t1.yfpsy <> t1.fpjs + t1.dwcpfpsy

    /*====================================================================================================
    # 规则代码: AM00410
    # 目标接口: J3012-资产负债表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {月末余额}[1000]={月末余额}[1001+1002+1003+1004+1005+1006+1007+1008+1009+1010+1011+1012+1013+1014+1015+1099]
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
    SELECT DISTINCT 'AM00410'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 资产负债项目:
               --    1000-资产合计、1001-银行存款、1002-结算备付金、1003-存出保证金、1004-衍生金融资产、1005-应收清算款、1006-应收利息、1007-应收股利、1008-应收申购款、1009-买入返售金融资产、1010-发放贷款和垫款、1011-交易性金融资产、1012-债权投资、1013-其他债权投资、1014-其他权益工具投资、1015-长期股权投资、1099-其他资产
               , sum(CASE WHEN tt1.xmbh = '1000' THEN tt1.ymye ELSE 0 END) AS ymye_1000
               , sum(CASE
                         WHEN tt1.xmbh IN
                              ('1001', '1002', '1003', '1004', '1005', '1006', '1007', '1008', '1009', '1010', '1011',
                               '1012', '1013', '1014', '1015', '1099')
                             THEN tt1.ymye
                         ELSE 0 END)                                       AS ymye_total
          FROM report_cisp.wdb_amp_fin_balsht tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND t1.ymye_1000 <> t1.ymye_total

    /*====================================================================================================
    # 规则代码: AM00411
    # 目标接口: J3012-资产负债表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {月末余额}[1011]>={月末余额}[101101+101102+101103]
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
    SELECT DISTINCT 'AM00411'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 资产负债项目:
               --    1011-交易性金融资产、101101-股票投资、101102-基金投资、101103-贵金属投资
               , sum(CASE WHEN tt1.xmbh = '1011' THEN tt1.ymye ELSE 0 END) AS ymye_1011
               , sum(CASE
                         WHEN tt1.xmbh IN ('101101', '101102', '101103') THEN tt1.ymye
                         ELSE 0 END)                                       AS ymye_total
          FROM report_cisp.wdb_amp_fin_balsht tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND t1.ymye_1011 < t1.ymye_total

    /*====================================================================================================
    # 规则代码: AM00412
    # 目标接口: J3012-资产负债表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {月末余额}[1012]>={月末余额}[101201+101202]
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
    SELECT DISTINCT 'AM00412'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 资产负债项目:
               --    1012-债权投资、101201-债券投资、101202-未挂牌资产支持证券投资
               , sum(CASE WHEN tt1.xmbh = '1012' THEN tt1.ymye ELSE 0 END) AS ymye_1012
               , sum(CASE
                         WHEN tt1.xmbh IN ('101201', '101202') THEN tt1.ymye
                         ELSE 0 END)                                       AS ymye_total
          FROM report_cisp.wdb_amp_fin_balsht tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND t1.ymye_1012 < t1.ymye_total

    /*====================================================================================================
    # 规则代码: AM00413
    # 目标接口: J3012-资产负债表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {月末余额}[2000]={月末余额}[2001+2002+2003+2004+2005+2006+2007+2008+2009+2010+2011+2012+2013+2099]
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
    SELECT DISTINCT 'AM00413'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 资产负债项目:
               --    2000-负债合计、2001-短期借款、2002-交易性金融负债、2003-衍生金融负债、2004-卖出回购金融资产款、2005-应付管理人报酬、2006-应付托管费、2007-应付销售服务费、2008-应付投资顾问费、2009-应交税费、2010-应付清算款、2011-应付赎回款、2012-应付利息、2013-应付利润、2099-其他负债
               , sum(CASE WHEN tt1.xmbh = '2000' THEN tt1.ymye ELSE 0 END) AS ymye_2000
               , sum(CASE
                         WHEN tt1.xmbh IN
                              ('2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011',
                               '2012', '2013', '2099')
                             THEN tt1.ymye
                         ELSE 0 END)                                       AS ymye_total
          FROM report_cisp.wdb_amp_fin_balsht tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND t1.ymye_2000 <> t1.ymye_total

    /*====================================================================================================
    # 规则代码: AM00414
    # 目标接口: J3012-资产负债表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {月末余额}[4000]={月末余额}[2000+3000]
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
    SELECT DISTINCT 'AM00414'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 资产负债项目:
               --    4000-负债和所有者权益合计、2000-负债合计、3000-所有者权益合计
               , sum(CASE WHEN tt1.xmbh = '4000' THEN tt1.ymye ELSE 0 END) AS ymye_4000
               , sum(CASE
                         WHEN tt1.xmbh IN ('2000', '3000') THEN tt1.ymye
                         ELSE 0 END)                                       AS ymye_total
          FROM report_cisp.wdb_amp_fin_balsht tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND t1.ymye_4000 <> t1.ymye_total

    /*====================================================================================================
    # 规则代码: AM00415
    # 目标接口: J3012-资产负债表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {月末余额}[1000]={月末余额}[4000]
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
    SELECT DISTINCT 'AM00415'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 资产负债项目:
               --    1000-资产合计、4000-负债和所有者权益合计
               , sum(CASE WHEN tt1.xmbh = '1000' THEN tt1.ymye ELSE 0 END) AS ymye_1000
               , sum(CASE WHEN tt1.xmbh = '4000' THEN tt1.ymye ELSE 0 END) AS ymye_4000
          FROM report_cisp.wdb_amp_fin_balsht tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND t1.ymye_1000 <> t1.ymye_4000

    /*====================================================================================================
    # 规则代码: AM00416
    # 目标接口: J3012-资产负债表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {月末余额}[1000-资产总计]=[产品净值信息].{资产总值}
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
    SELECT DISTINCT 'AM00416'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 资产负债项目:
               --    1000-资产合计
               , sum(CASE WHEN tt1.xmbh = '1000' THEN tt1.ymye ELSE 0 END) AS ymye_1000
          FROM report_cisp.wdb_amp_fin_balsht tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_prod_nav t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND t1.ymye_1000 <> t2.zczz

    /*====================================================================================================
    # 规则代码: AM00417
    # 目标接口: J3012-资产负债表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {月末余额}(1000-资产总计)=[QDII及FOF产品净值信息].{资产总值}
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1010-QDII及FOF产品净值信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00417'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 资产负债项目:
               --    1000-资产合计
               , sum(CASE WHEN tt1.xmbh = '1000' THEN tt1.ymye ELSE 0 END) AS ymye_1000
          FROM report_cisp.wdb_amp_fin_balsht tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_prod_qd_nav t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND t1.ymye_1000 <> t2.zczz

    /*====================================================================================================
    # 规则代码: AM00418
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {本月金额}[1000]={本月金额}[1001+1002+1003+1004+1099]
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
    SELECT DISTINCT 'AM00418'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 利润项目:
               --    1000-营业总收入、1001-利息收入、1002-投资收益、1003-公允价值变动收益、1004-汇兑收益、1099-其他业务收入
               , sum(CASE WHEN tt1.xmbh = '1000' THEN tt1.byje ELSE 0 END) AS byje_1000
               , sum(CASE
                         WHEN tt1.xmbh IN ('1001', '1002', '1003', '1004', '1099') THEN tt1.byje
                         ELSE 0 END)                                       AS byje_total
          FROM report_cisp.wdb_amp_fin_profit tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND t1.byje_1000 <> t1.byje_total

    /*====================================================================================================
    # 规则代码: AM00419
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {本月金额}[1002]={本月金额}[100211+100212+100213+100214+100215+100216+100217+100218+100219]
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
    SELECT DISTINCT 'AM00419'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 利润项目:
               --    1002-投资收益、100211-股票投资收益、100212-基金投资收益、100213-债券投资收益、100214-资产支持证券投资收益、100215-贵金属投资收益、100216-衍生工具收益、100217-股利收益、100218-差价收入增值税抵减、100219-其他投资收益
               , sum(CASE WHEN tt1.xmbh = '1002' THEN tt1.byje ELSE 0 END) AS byje_1002
               , sum(CASE
                         WHEN tt1.xmbh IN
                              ('100211', '100212', '100213', '100214', '100215', '100216', '100217', '100218', '100219')
                             THEN tt1.byje
                         ELSE 0 END)                                       AS byje_total
          FROM report_cisp.wdb_amp_fin_profit tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND t1.byje_1002 <> t1.byje_total

    /*====================================================================================================
    # 规则代码: AM00420
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {本月金额}[2000]={本月金额}[2001+2002+2003+2004+2005+2006+2007+2099]
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
    SELECT DISTINCT 'AM00420'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 利润项目:
               --    2000-营业总支出、2001-管理人报酬、2002-托管费、2003-销售服务费、2004-投资顾问费、2005-利息支出、2006-信用减值损失、2007-税金及附加、2099-其他费用
               , sum(CASE WHEN tt1.xmbh = '2000' THEN tt1.byje ELSE 0 END) AS byje_2000
               , sum(CASE
                         WHEN tt1.xmbh IN ('2001', '2002', '2003', '2004', '2005', '2006', '2007', '2099') THEN tt1.byje
                         ELSE 0 END)                                       AS byje_total
          FROM report_cisp.wdb_amp_fin_profit tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND t1.byje_2000 <> t1.byje_total

    /*====================================================================================================
    # 规则代码: AM00421
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {本月金额}[2005]>={本月金额}[200501]
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
    SELECT DISTINCT 'AM00421'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 利润项目:
               --    2005-利息支出、200501-卖出回购金融资产利息支出
               , sum(CASE WHEN tt1.xmbh = '2005' THEN tt1.byje ELSE 0 END)   AS byje_2005
               , sum(CASE WHEN tt1.xmbh = '200501' THEN tt1.byje ELSE 0 END) AS byje_200501
          FROM report_cisp.wdb_amp_fin_profit tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND t1.byje_2005 < t1.byje_200501

    /*====================================================================================================
    # 规则代码: AM00422
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {本月金额}[1000-2000]={本月金额}[3000]
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
    SELECT DISTINCT 'AM00422'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 利润项目:
               --    1000-营业总收入、2000-营业总支出、3000-利润总额
               , sum(CASE WHEN tt1.xmbh = '1000' THEN tt1.byje ELSE 0 END) AS byje_1000
               , sum(CASE WHEN tt1.xmbh = '2000' THEN tt1.byje ELSE 0 END) AS byje_2000
               , sum(CASE WHEN tt1.xmbh = '3000' THEN tt1.byje ELSE 0 END) AS byje_3000
          FROM report_cisp.wdb_amp_fin_profit tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND (t1.byje_1000 - t1.byje_2000) <> t1.byje_3000

    /*====================================================================================================
    # 规则代码: AM00423
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {本月金额}[3000-300001]={本月金额}[4000]
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
    SELECT DISTINCT 'AM00423'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 利润项目:
               --    3000-利润总额、300001-所得税费用、4000-净利润
               , sum(CASE WHEN tt1.xmbh = '3000' THEN tt1.byje ELSE 0 END)   AS byje_3000
               , sum(CASE WHEN tt1.xmbh = '300001' THEN tt1.byje ELSE 0 END) AS byje_300001
               , sum(CASE WHEN tt1.xmbh = '4000' THEN tt1.byje ELSE 0 END)   AS byje_4000
          FROM report_cisp.wdb_amp_fin_profit tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND (t1.byje_3000 - t1.byje_300001) <> t1.byje_4000

    /*====================================================================================================
    # 规则代码: AM00424
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {本月金额}[6000]={本月金额}[4000+5000]
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
    SELECT DISTINCT 'AM00424'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 利润项目:
               --    4000-净利润、5000-其他综合收益的税后净额、6000-综合收益总额
               , sum(CASE WHEN tt1.xmbh = '4000' THEN tt1.byje ELSE 0 END) AS byje_4000
               , sum(CASE WHEN tt1.xmbh = '5000' THEN tt1.byje ELSE 0 END) AS byje_5000
               , sum(CASE WHEN tt1.xmbh = '6000' THEN tt1.byje ELSE 0 END) AS byje_6000
          FROM report_cisp.wdb_amp_fin_profit tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND t1.byje_6000 <> (t1.byje_4000 + t1.byje_5000)

    /*====================================================================================================
    # 规则代码: AM00425
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {本年累计金额}[1000]={本年累计金额}[1001+1002+1003+1004+1099]
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
    SELECT DISTINCT 'AM00425'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 利润项目:
               --    1000-营业总收入、1001-利息收入、1002-投资收益、1003-公允价值变动收益、1004-汇兑收益、1099-其他业务收入
               , sum(CASE WHEN tt1.xmbh = '1000' THEN tt1.bnljje ELSE 0 END) AS bnljje_1000
               , sum(CASE
                         WHEN tt1.xmbh IN ('1001', '1002', '1003', '1004', '1099') THEN tt1.bnljje
                         ELSE 0 END)                                         AS bnljje_total
          FROM report_cisp.wdb_amp_fin_profit tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND t1.bnljje_1000 <> t1.bnljje_total

    /*====================================================================================================
    # 规则代码: AM00426
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {本年累计金额}[1002]={本年累计金额}[100211+100212+100213+100214+100215+100216+100217+100218+100219]
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
    SELECT DISTINCT 'AM00426'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 利润项目:
               --    1002-投资收益、100211-股票投资收益、100212-基金投资收益、100213-债券投资收益、100214-资产支持证券投资收益、100215-贵金属投资收益、100216-衍生工具收益、100217-股利收益、100218-差价收入增值税抵减、100219-其他投资收益
               , sum(CASE WHEN tt1.xmbh = '1002' THEN tt1.bnljje ELSE 0 END) AS bnljje_1002
               , sum(CASE
                         WHEN tt1.xmbh IN
                              ('100211', '100212', '100213', '100214', '100215', '100216', '100217', '100218', '100219')
                             THEN tt1.bnljje
                         ELSE 0 END)                                         AS bnljje_total
          FROM report_cisp.wdb_amp_fin_profit tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND t1.bnljje_1002 <> t1.bnljje_total

    /*====================================================================================================
    # 规则代码: AM00427
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {本年累计金额}[2000]={本年累计金额}[2001+2002+2003+2004+2005+2006+2007+2099]
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
    SELECT DISTINCT 'AM00427'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 利润项目:
               --    2000-营业总支出、2001-管理人报酬、2002-托管费、2003-销售服务费、2004-投资顾问费、2005-利息支出、2006-信用减值损失、2007-税金及附加、2099-其他费用
               , sum(CASE WHEN tt1.xmbh = '2000' THEN tt1.bnljje ELSE 0 END) AS bnljje_2000
               , sum(CASE
                         WHEN tt1.xmbh IN ('2001', '2002', '2003', '2004', '2005', '2006', '2007', '2099')
                             THEN tt1.bnljje
                         ELSE 0 END)                                         AS bnljje_total
          FROM report_cisp.wdb_amp_fin_profit tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND t1.bnljje_2000 <> t1.bnljje_total

    /*====================================================================================================
    # 规则代码: AM00428
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {本年累计金额}[2005]>={本年累计金额}[200501]
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
    SELECT DISTINCT 'AM00428'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 利润项目:
               --    2005-利息支出、200501-卖出回购金融资产利息支出
               , sum(CASE WHEN tt1.xmbh = '2005' THEN tt1.bnljje ELSE 0 END)   AS bnljje_2005
               , sum(CASE WHEN tt1.xmbh = '200501' THEN tt1.bnljje ELSE 0 END) AS bnljje_200501
          FROM report_cisp.wdb_amp_fin_profit tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND t1.bnljje_2005 < t1.bnljje_200501

    /*====================================================================================================
    # 规则代码: AM00429
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {本年累计金额}[1000-2000]={本年累计金额}[3000]
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
    SELECT DISTINCT 'AM00429'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 利润项目:
               --    1000-营业总收入、2000-营业总支出、3000-利润总额
               , sum(CASE WHEN tt1.xmbh = '1000' THEN tt1.bnljje ELSE 0 END) AS bnljje_1000
               , sum(CASE WHEN tt1.xmbh = '2000' THEN tt1.bnljje ELSE 0 END) AS bnljje_2000
               , sum(CASE WHEN tt1.xmbh = '3000' THEN tt1.bnljje ELSE 0 END) AS bnljje_3000
          FROM report_cisp.wdb_amp_fin_profit tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND (t1.bnljje_1000 - t1.bnljje_2000) <> t1.bnljje_3000

    /*====================================================================================================
    # 规则代码: AM00430
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {本年累计金额}[3000-300001]={本年累计金额}[4000]
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
    SELECT DISTINCT 'AM00430'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 利润项目:
               --    3000-利润总额、300001-所得税费用、4000-净利润
               , sum(CASE WHEN tt1.xmbh = '3000' THEN tt1.bnljje ELSE 0 END)   AS bnljje_3000
               , sum(CASE WHEN tt1.xmbh = '300001' THEN tt1.bnljje ELSE 0 END) AS bnljje_300001
               , sum(CASE WHEN tt1.xmbh = '4000' THEN tt1.bnljje ELSE 0 END)   AS bnljje_4000
          FROM report_cisp.wdb_amp_fin_profit tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND (t1.bnljje_3000 - t1.bnljje_300001) <> t1.bnljje_4000

    /*====================================================================================================
    # 规则代码: AM00431
    # 目标接口: J3013-利润表
    # 目标接口传输频度: 月
    # 目标接口传输时间: T+7日24:00前
    # 规则说明: {本年累计金额}[6000]={本年累计金额}[4000+5000]
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
    SELECT DISTINCT 'AM00431'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 利润项目:
               --    4000-净利润、5000-其他综合收益的税后净额、6000-综合收益总额
               , sum(CASE WHEN tt1.xmbh = '4000' THEN tt1.bnljje ELSE 0 END) AS bnljje_4000
               , sum(CASE WHEN tt1.xmbh = '5000' THEN tt1.bnljje ELSE 0 END) AS bnljje_5000
               , sum(CASE WHEN tt1.xmbh = '6000' THEN tt1.bnljje ELSE 0 END) AS bnljje_6000
          FROM report_cisp.wdb_amp_fin_profit tt1
          GROUP BY jgdm, status, sjrq, cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 -- 若测试，请注释本行
      AND t1.bnljje_6000 <> (t1.bnljje_4000 + t1.bnljje_5000)

    /*====================================================================================================
    # 规则代码: AM00432
	# 目标接口: J3013-利润表
	# 目标接口传输频度: 月
	# 目标接口传输时间: T+7日24:00前
    # 规则说明: {数据日期}的后四位为“0131”时，{本年累计金额}=={本月金额}
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
    SELECT DISTINCT 'AM00432'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND substr(t1.sjrq, 5, 4) = '0131'
      AND t1.bnljje <> t1.byje

    /*====================================================================================================
    # 规则代码: AM00433
	# 目标接口: J3013-利润表
	# 目标接口传输频度: 月
	# 目标接口传输时间: T+7日24:00前
    # 规则说明: {数据日期}的后四位不为“0131”时，{本年累计金额}==上期[利润表].{本年累计金额}+{本月金额}
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
    SELECT DISTINCT 'AM00433'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
             LEFT JOIN report_cisp.wdb_amp_inv_bond t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_bond tt1 WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND substr(t1.sjrq, 5, 4) <> '0131'
      AND t1.bnljje <> t2.bnljje + t1.byje

    /*====================================================================================================
    # 规则代码: AM00434
    # 目标接口: J1014-账户汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末账户数}={期末有持仓的户数}+{期末无持仓的户数}
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
    SELECT DISTINCT 'AM00434'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.ztlb     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.ztlb
               , tt1.tzzlx
               , sum(tt1.qmwccdhs)     AS qmwccdhs
               , sum(tt1.jzqmcwyjydhs) AS jzqmcwyjydhs
               , sum(tt1.jzqmcjyjydhs) AS jzqmcjyjydhs
          FROM report_cisp.wdb_amp_sl_acctnum_sum tt1
          GROUP BY tt1.ztlb, tt1.tzzlx) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmwccdhs <> t1.jzqmcwyjydhs + t1.jzqmcjyjydhs

    /*====================================================================================================
    # 规则代码: AM00435
    # 目标接口: J1014-账户汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末账户数}={期末有持仓的户数}+{期末无持仓的户数}
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
    SELECT DISTINCT 'AM00435'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.ztlb     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_acctnum_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmzhs <> t1.qmyccdhs + t1.qmwccdhs

    /*====================================================================================================
    # 规则代码: AM00436
    # 目标接口: J1014-账户汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末无持仓的户数}={截至期末从未有交易的户数}+{截至期末曾经有交易的户数}
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
    SELECT DISTINCT 'AM00436'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.ztlb     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_acctnum_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmwccdhs <> t1.jzqmcwyjydhs + t1.jzqmcjyjydhs

    /*====================================================================================================
    # 规则代码: AM00437
    # 目标接口: J1014-账户汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末账户数}=上期{期末账户数}+{本期开户数}-{本期销户数}
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
    SELECT DISTINCT 'AM00437'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.ztlb     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_acctnum_sum t1
             LEFT JOIN report_cisp.wdb_amp_sl_acctnum_sum t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = (SELECT max(tt1.sjrq)
                                          FROM report_cisp.wdb_amp_sl_acctnum_sum tt1
                                          WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmzhs <> t2.qmzhs + t1.bqkhs - t1.bqxhs

    /*====================================================================================================
    # 规则代码: AM00438
    # 目标接口: J1014-账户汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末账户数}={截止期末从未有交易的户数}+{截止期末曾经有交易的户数}
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
    SELECT DISTINCT 'AM00438'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.ztlb     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_acctnum_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmzhs <> t1.jzqmcwyjydhs + t1.jzqmcjyjydhs

    /*====================================================================================================
    # 规则代码: AM00439
    # 目标接口: J1017-交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {手续费}=={手续费(归管理人)}+{手续费(归销售机构)}+{手续费(归产品资产)}
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
    SELECT DISTINCT 'AM00439'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.sxf <> t1.sxfgglr + t1.sxfgxsjg + t1.sxfgcpzc

    /*====================================================================================================
    # 规则代码: AM00440
    # 目标接口: J1017-交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {后收费}=={后收费(归管理人)}+{后收费(归销售机构)}
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
    SELECT DISTINCT 'AM00440'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.hsf <> t1.hsfgglr + t1.hsfgxsjg

    /*====================================================================================================
    # 规则代码: AM00441
    # 目标接口: J1018-QDII及FOF交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {手续费}=={手续费(归管理人)}+{手续费(归销售机构)}+{手续费(归产品资产)}
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
    SELECT DISTINCT 'AM00441'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cncwbz = '0'
      AND t1.sxf <> t1.sxfgglr + t1.sxfgxsjg + t1.sxfgcpzc

    /*====================================================================================================
    # 规则代码: AM00442
    # 目标接口: J1018-QDII及FOF交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {后收费}=={后收费(归管理人)}+{后收费(归销售机构)}
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
    SELECT DISTINCT 'AM00442'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cncwbz = '0'
      AND t1.hsf <> t1.hsfgglr + t1.hsfgxsjg

    /*====================================================================================================
    # 规则代码: AM00443
    # 目标接口: J1019-净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {净申赎金额}={申购金额}-{赎回金额}
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
    SELECT DISTINCT 'AM00443'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_bignetrdm t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.jssje <> t1.sgje - t1.shje

    /*====================================================================================================
    # 规则代码: AM00444
    # 目标接口: J1019-净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+2日24:00前
    # 规则说明: {净申赎份数}={申购份数}-{赎回份数}
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
    SELECT DISTINCT 'AM00444'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_bignetrdm t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.jssfs <> t1.sgfs - t1.shfs

    /*====================================================================================================
    # 规则代码: AM00445
    # 目标接口: J1020-QDII及FOF净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {净申赎金额}={申购金额}-{赎回金额}
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
    SELECT DISTINCT 'AM00445'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_bignetrdm t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.jssje <> t1.sgje - t1.shje

    /*====================================================================================================
    # 规则代码: AM00446
    # 目标接口: J1020-QDII及FOF净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {净申赎份数}={申购份数}-{赎回份数}
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
    SELECT DISTINCT 'AM00446'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_bignetrdm t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.jssfs <> t1.sgfs - t1.shfs

    /*====================================================================================================
    # 规则代码: AM00447
    # 目标接口: J1021-份额汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: SUM{持有份额}=[产品净值信息].{总份额}
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
    SELECT DISTINCT 'AM00447'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.cyfe) AS sum_cyfe
          FROM report_cisp.wdb_amp_sl_shr_sum
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_prod_nav t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.sum_cyfe <> t2.zfe

    /*====================================================================================================
    # 规则代码: AM00448
    # 目标接口: J1022-QDII及FOF份额汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: SUM{持有份额}=[QDII及FOF产品净值信息].{总份额}
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1010-QDII及FOF产品净值信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00448'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.cyfe) AS sum_cyfe
          FROM report_cisp.wdb_amp_sl_qd_shr_sum
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_prod_qd_nav t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.sum_cyfe <> t2.zfe

    /*====================================================================================================
    # 规则代码: AM00449
    # 目标接口: J1024-前200大投资者
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [客户情况].{持有投资者数量}<=200时，{主体类别}为“个人”的记录数=[客户情况].{个人投资者数量}，{主体类别}为“机构”的记录数=[客户情况].{机构投资者数量}，{主体类别}为“产品”的记录数=[客户情况].{产品投资者数量}
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1025-客户情况
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00449'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 主体类别代码: 01-个人、02-机构、03-产品
               , sum(CASE WHEN tt1.ztlb = '01' THEN 1 ELSE 0 END) AS cnt_gr
               , sum(CASE WHEN tt1.ztlb = '02' THEN 1 ELSE 0 END) AS cnt_jg
               , sum(CASE WHEN tt1.ztlb = '03' THEN 1 ELSE 0 END) AS cnt_cp
          FROM report_cisp.wdb_amp_sl_top200hldr tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_sl_cust_hold t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cytzzsl <= 200
      AND (t1.cnt_gr <> t2.grtzzsl OR t1.cnt_jg <> t2.jgtzzsl OR t1.cnt_cp <> t2.cptzzsl)

    /*====================================================================================================
    # 规则代码: AM00450
    # 目标接口: J1024-前200大投资者
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [客户情况].{持有投资者数量}>200时，本表记录数为200
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1025-客户情况
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+4日24:00前
    # 工作状态: 0
    # 备注: 有歧义，暂搁置
    ====================================================================================================*/
    /*UNION ALL
    SELECT DISTINCT 'AM00450'   AS gzdm        -- 规则代码
                  , t2.sjrq     AS sjrq        -- 数据日期
                  , t2.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_top200hldr t1
             RIGHT JOIN report_cisp.wdb_amp_sl_cust_hold t2
                        ON t1.jgdm = t2.jgdm
                            AND t1.status = t2.status
                            AND t1.sjrq = t2.sjrq
                            AND t1.cpdm = t2.cpdm
    WHERE t2.jgdm = '70610000'
      AND t2.status NOT IN ('3', '5')
      AND t2.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      and t2.cytzzsl>200
      and t1.cpdm is null*/

    /*====================================================================================================
    # 规则代码: AM00451
    # 目标接口: J1025-客户情况
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {持有投资者数量}=={个人投资者数量}+{机构投资者数量}+{产品投资者数量}
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
    SELECT DISTINCT 'AM00451'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_cust_hold t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cytzzsl <> t1.grtzzsl + t1.jgtzzsl + t1.cptzzsl

    /*====================================================================================================
    # 规则代码: AM00452
    # 目标接口: J1025-客户情况
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {持有投资者数量}=={单笔委托300万(含)以上的投资者数量}+{单笔委托300万以下的投资者数量}
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
    SELECT DISTINCT 'AM00452'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_cust_hold t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.cytzzsl <> t1.dbwt300wysdtzzsl + t1.dbwt300wyxdtzzsl

    /*====================================================================================================
    # 规则代码: AM00453
    # 目标接口: J1026-资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: [产品净值信息].{资产总值}==资产类别为“101-股票”的{期末市值}+资产类别为“102-优先股”的{期末市值}+资产类别为“103-债券”的{期末市值}+资产类别为“104-标准化资管产品”的{期末市值}+资产类别为“105-现金和活期存款”的{期末市值}+资产类别为“106-银行定期存款”的{期末市值}+资产类别为“107-同业存单”的{期末市值}+资产类别为“108-买入返售资产”的{期末市值}+资产类别为“109-商品”的{期末市值}+资产类别为“110-商品及金融衍生品（场内集中交易清算）”的{期末市值}+资产类别为“199-其他标准化资产”的{期末市值}+资产类别为“205-资产支持证券（未在交易所挂牌）”的{期末市值}+资产类别为“299-其他非标准化资产”的{期末市值}
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
    SELECT DISTINCT 'AM00453'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt2.jgdm
               , tt2.status
               , tt2.sjrq
               , tt2.cpdm
               , tt2.zczz
               , tt1.zclb
               , tt1.qmsz
               , sum(CASE
                         -- 资产类别: 101-股票、102-优先股、103-债券、104-标准化资管产品、105-现金和活期存款、106-银行定期存款、107-同业存单、108-买入返售资产、109-商品、110-商品及金融衍生品（场内集中交易清算）、199-其他标准化资产、205-资产支持证券（未在交易所挂牌）、299-其他非标准化资产
                         WHEN tt1.zclb IN
                              ('101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '199', '205',
                               '299')
                             THEN tt1.qmsz
                         ELSE 0
                         END) OVER (PARTITION BY tt2.jgdm,tt2.status, tt2.sjrq, tt2.cpdm) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_assets tt1
                   RIGHT JOIN report_cisp.wdb_amp_prod_nav tt2
                              ON tt1.jgdm = tt2.jgdm
                                  AND tt1.status = tt2.status
                                  AND tt1.sjrq = tt2.sjrq
                                  AND tt1.cpdm = tt2.cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.zczz <> t1.sum_qmsz

    /*====================================================================================================
    # 规则代码: AM00454
    # 目标接口: J1027-QDII及FOF资产组合
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF产品净值信息].{资产总值}==资产类别为“101-股票”的{期末市值}+资产类别为“102-优先股”的{期末市值}+资产类别为“103-债券”的{期末市值}+资产类别为“104-标准化资管产品”的{期末市值}+资产类别为“105-现金和活期存款”的{期末市值}+资产类别为“106-银行定期存款”的{期末市值}+资产类别为“107-同业存单”的{期末市值}+资产类别为“108-买入返售资产”的{期末市值}+资产类别为“109-商品”的{期末市值}+资产类别为“110-商品及金融衍生品（场内集中交易清算）”的{期末市值}+资产类别为“199-其他标准化资产”的{期末市值}+资产类别为“205-资产支持证券（未在交易所挂牌）”的{期末市值}+资产类别为“299-其他非标准化资产”的{期末市值}
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1010-QDII及FOF产品净值信息
    # 其他接口传输频度: 日
    # 其他接口传输时间: T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00454'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt2.jgdm
               , tt2.status
               , tt2.sjrq
               , tt2.cpdm
               , tt2.zczz
               , tt1.zclb
               , tt1.qmsz
               , sum(CASE
                         -- 资产类别: 101-股票、102-优先股、103-债券、104-标准化资管产品、105-现金和活期存款、106-银行定期存款、107-同业存单、108-买入返售资产、109-商品、110-商品及金融衍生品（场内集中交易清算）、199-其他标准化资产、205-资产支持证券（未在交易所挂牌）、299-其他非标准化资产
                         WHEN tt1.zclb IN
                              ('101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '199', '205',
                               '299')
                             THEN tt1.qmsz
                         ELSE 0
                         END) OVER (PARTITION BY tt2.jgdm,tt2.status, tt2.sjrq, tt2.cpdm) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_qd_assets tt1
                   RIGHT JOIN report_cisp.wdb_amp_prod_qd_nav tt2
                              ON tt1.jgdm = tt2.jgdm
                                  AND tt1.status = tt2.status
                                  AND tt1.sjrq = tt2.sjrq
                                  AND tt1.cpdm = tt2.cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.zczz <> t1.sum_qmsz

    /*====================================================================================================
    # 规则代码: AM00455
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}==上期[股票投资明细].{期末数量}+{买入数量}-{卖出数量}
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
    SELECT DISTINCT 'AM00455'   AS gzdm        -- 规则代码
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
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl <> t2.qmsl + t1.mrsl - ti.mcsl

    /*====================================================================================================
    # 规则代码: AM00456
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [资产组合].{资产类别}为“101-股票”的[资产组合].{期末市值}==SUM{期末市值}
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
    SELECT DISTINCT 'AM00456'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_stock tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_inv_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 101-股票
      AND t2.zclb = '101'
      AND t1.sum_qmsz <> t2.qmsz

    /*====================================================================================================
    # 规则代码: AM00457
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF资产组合].{资产类别}为“101-股票”的[QDII及FOF资产组合].{期末市值}==SUM{期末市值}
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
    SELECT DISTINCT 'AM00457'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_stock tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_inv_qd_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 101-股票
      AND t2.zclb = '101'
      AND t1.sum_qmsz <> t2.qmsz

    /*====================================================================================================
    # 规则代码: AM00458
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [产品基本信息].{产品类别}为“111-股票型基金”、“211-股票型”时，SUM（{期末市值}-{非流通股份市值}-{流通受限股份市值}-{其他流通受限股份市值}-{新发行股份市值}-{增发股份市值}）/SUM[资产组合].{期末市值}>=80%
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1002-产品基本信息 J1026-资产组合
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前 T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00458'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz - tt1.fltgfsz - tt1.ltsxgfsz - tt1.qtltsxgfsz - tt1.xfxgfsz - tt1.zfgfsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_stock tt1
                   LEFT JOIN report_cisp.wdb_amp_prod_baseinfo tt2
                             ON tt1.jgdm = tt2.jgdm
                                 AND tt1.status = tt2.status
                                 AND tt1.sjrq = tt2.sjrq
                                 AND tt1.cpdm = tt2.cpdm
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm
          -- 产品类别:
          --    公募基金: 111-股票型基金
          --    大集合: 211-股票型
          HAVING tt2.cplb IN ('111', '211')) t1
             LEFT JOIN (SELECT tt3.jgdm
                             , tt3.status
                             , tt1.sjrq
                             , tt1.cpdm
                             , sum(tt1.qmsz) AS sum_qmsz
                        FROM report_cisp.wdb_amp_inv_assets tt3
                        GROUP BY tt3.jgdm, tt3.status, tt3.sjrq, tt3.cpdm) t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND round(t1.sum_qmsz / t2.sum_qmsz, 4) < 0.8

    /*====================================================================================================
    # 规则代码: AM00459
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [产品基本信息].{产品类别}为“119-股票型QDII基金”、“219-股票型QDII”时，SUM（{期末市值}-{非流通股份市值}-{流通受限股份市值}-{其他流通受限股份市值}-{新发行股份市值}-{增发股份市值}）/SUM[QDII及FOF资产组合].{期末市值}>=80%
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1002-产品基本信息 J1027-QDII及FOF资产组合
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前 T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00459'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz - tt1.fltgfsz - tt1.ltsxgfsz - tt1.qtltsxgfsz - tt1.xfxgfsz - tt1.zfgfsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_stock tt1
                   LEFT JOIN report_cisp.wdb_amp_prod_baseinfo tt2
                             ON tt1.jgdm = tt2.jgdm
                                 AND tt1.status = tt2.status
                                 AND tt1.sjrq = tt2.sjrq
                                 AND tt1.cpdm = tt2.cpdm
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm
          -- 产品类别:
          --    公募基金: 119-股票型QDII基金
          --    大集合: 219-股票型QDII
          HAVING tt2.cplb IN ('111', '211')) t1
             LEFT JOIN (SELECT tt3.jgdm
                             , tt3.status
                             , tt1.sjrq
                             , tt1.cpdm
                             , sum(tt1.qmsz) AS sum_qmsz
                        FROM report_cisp.wdb_amp_inv_qd_assets tt3
                        GROUP BY tt3.jgdm, tt3.status, tt3.sjrq, tt3.cpdm) t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND round(t1.sum_qmsz / t2.sum_qmsz, 4) < 0.8

    /*====================================================================================================
    # 规则代码: AM00460
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {非流通股份数量}={流通受限股份数量}+{其他流通受限股份数量}
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
    SELECT DISTINCT 'AM00460'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stock t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.fltgfsl <> t1.ltsxgfsl + t1.qtltsxgfsl

    /*====================================================================================================
    # 规则代码: AM00461
    # 目标接口: J1029-股票投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {非流通股份市值}={流通受限股份市值}+{其他流通受限股份市值}
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
    SELECT DISTINCT 'AM00461'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stock t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.fltgfsz <> t1.ltsxgfsz + t1.qtltsxgfsz

    /*====================================================================================================
    # 规则代码: AM00462
    # 目标接口: J1030-优先股投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}==上期[优先股投资明细].{期末数量}+{买入数量}-{卖出数量}
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
    SELECT DISTINCT 'AM00462'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00463
    # 目标接口: J1030-优先股投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [资产组合].{资产类别}为“102-优先股”的[资产组合].{期末市值}==SUM{期末市值}
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
    SELECT DISTINCT 'AM00463'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_prestock tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_inv_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 102-优先股
      AND t2.zclb = '102'
      AND t1.sum_qmsz <> t2.qmsz

    /*====================================================================================================
    # 规则代码: AM00464
    # 目标接口: J1030-优先股投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF资产组合].{资产类别}为“102-优先股”的[QDII及FOF资产组合].{期末市值}==SUM{期末市值}
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
    SELECT DISTINCT 'AM00464'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_prestock tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_inv_qd_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 102-优先股
      AND t2.zclb = '102'
      AND t1.sum_qmsz <> t2.qmsz

    /*====================================================================================================
    # 规则代码: AM00465
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}==上期[债券投资明细].{期末数量}+{买入数量}-{卖出数量}
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
    SELECT DISTINCT 'AM00465'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00466
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [资产组合].{资产类别}为“103-债券”的[资产组合].{期末市值}==SUM{期末市值}
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
    SELECT DISTINCT 'AM00466'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_bond tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_inv_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 103-债券
      AND t2.zclb = '103'
      AND t1.sum_qmsz <> t2.qmsz

    /*====================================================================================================
    # 规则代码: AM00467
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF资产组合].{资产类别}为“103-债券”的[QDII及FOF资产组合].{期末市值}==SUM{期末市值}
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
    SELECT DISTINCT 'AM00467'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_bond tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_inv_qd_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 103-债券
      AND t2.zclb = '103'
      AND t1.sum_qmsz <> t2.qmsz

    /*====================================================================================================
    # 规则代码: AM00468
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [产品基本信息].{产品类别}为“131-债券基金”、“231-债券型”时，SUM{期末市值}/SUM[资产组合].{期末市值}>=80%
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1002-产品基本信息 J1026-资产组合
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前 T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00468'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_bond tt1
                   LEFT JOIN report_cisp.wdb_amp_prod_baseinfo tt2
                             ON tt1.jgdm = tt2.jgdm
                                 AND tt1.status = tt2.status
                                 AND tt1.sjrq = tt2.sjrq
                                 AND tt1.cpdm = tt2.cpdm
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm
          -- 产品类别:
          --    公募基金: 131-债券基金
          --    大集合: 231-债券型
          HAVING tt2.cplb IN ('131', '231')) t1
             LEFT JOIN (SELECT tt3.jgdm
                             , tt3.status
                             , tt1.sjrq
                             , tt1.cpdm
                             , sum(tt1.qmsz) AS sum_qmsz
                        FROM report_cisp.wdb_amp_inv_assets tt3
                        GROUP BY tt3.jgdm, tt3.status, tt3.sjrq, tt3.cpdm) t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND round(t1.sum_qmsz / t2.sum_qmsz, 4) < 0.8

    /*====================================================================================================
    # 规则代码: AM00469
    # 目标接口: J1031-债券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [产品基本信息].{产品类别}为“139-债券型QDII基金”、“239-债券型QDII” 时，SUM{期末市值}/SUM[QDII及FOF资产组合].{期末市值}>=80%
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1002-产品基本信息 J1027-QDII及FOF资产组合
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前 T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00469'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_bond tt1
                   LEFT JOIN report_cisp.wdb_amp_prod_baseinfo tt2
                             ON tt1.jgdm = tt2.jgdm
                                 AND tt1.status = tt2.status
                                 AND tt1.sjrq = tt2.sjrq
                                 AND tt1.cpdm = tt2.cpdm
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm
          -- 产品类别:
          --    公募基金: 139-债券型QDII基金
          --    大集合: 239-债券型QDII
          HAVING tt2.cplb IN ('139', '239')) t1
             LEFT JOIN (SELECT tt3.jgdm
                             , tt3.status
                             , tt1.sjrq
                             , tt1.cpdm
                             , sum(tt1.qmsz) AS sum_qmsz
                        FROM report_cisp.wdb_amp_inv_qd_assets tt3
                        GROUP BY tt3.jgdm, tt3.status, tt3.sjrq, tt3.cpdm) t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND round(t1.sum_qmsz / t2.sum_qmsz, 4) < 0.8

    /*====================================================================================================
    # 规则代码: AM00470
    # 目标接口: J1032-标准化资管产品投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}==上期[标准化资管产品投资明细].{期末数量}+{买入数量}-{卖出数量}
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
    SELECT DISTINCT 'AM00470'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_stdampp t1
             LEFT JOIN report_cisp.wdb_amp_inv_stdampp t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_stdampp tt1 WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl = t2.qmsl + t1.mrsl - t2.mcsl

    /*====================================================================================================
    # 规则代码: AM00471
    # 目标接口: J1032-标准化资管产品投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [资产组合].{资产类别}为“104-标准化资管产品”的[资产组合].{期末市值}==SUM{期末市值}
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
    SELECT DISTINCT 'AM00471'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_stdampp tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_inv_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 104-标准化资管产品
      AND t2.zclb = '104'
      AND t1.sum_qmsz <> t2.qmsz

    /*====================================================================================================
    # 规则代码: AM00472
    # 目标接口: J1032-标准化资管产品投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF资产组合].{资产类别}为“104-标准化资管产品”的[QDII及FOF资产组合].{期末市值}==SUM{期末市值}
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
    SELECT DISTINCT 'AM00472'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_stdampp tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_inv_qd_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 104-标准化资管产品
      AND t2.zclb = '104'
      AND t1.sum_qmsz <> t2.qmsz

    /*====================================================================================================
    # 规则代码: AM00473
    # 目标接口: J1033-定期存款投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [资产组合].{资产类别}为“106-银行定期存款”的[资产组合].{期末市值}==SUM{期末金额}
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
    SELECT DISTINCT 'AM00473'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_fixdepo tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_inv_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 106-银行定期存款
      AND t2.zclb = '106'
      AND t1.sum_qmsz <> t2.qmsz

    /*====================================================================================================
    # 规则代码: AM00474
    # 目标接口: J1033-定期存款投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF资产组合].{资产类别}为“106-银行定期存款”的[QDII及FOF资产组合].{期末市值}==SUM{期末金额}
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
    SELECT DISTINCT 'AM00474'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_fixdepo tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_inv_qd_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 106-银行定期存款
      AND t2.zclb = '106'
      AND t1.sum_qmsz <> t2.qmsz

    /*====================================================================================================
    # 规则代码: AM00475
    # 目标接口: J1035-同业存单投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}==上期[同业存单投资明细].{期末数量}+{买入数量}-{卖出数量}
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
    SELECT DISTINCT 'AM00475'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_ncd t1
             LEFT JOIN report_cisp.wdb_amp_inv_ncd t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_ncd tt1 WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl = t2.qmsl + t1.mrsl - t2.mcsl

    /*====================================================================================================
    # 规则代码: AM00476
    # 目标接口: J1035-同业存单投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [资产组合].{资产类别}为“107-同业存单”的[资产组合].{期末市值}==SUM{期末市值}
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
    SELECT DISTINCT 'AM00476'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_ncd tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_inv_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 107-同业存单
      AND t2.zclb = '107'
      AND t1.sum_qmsz <> t2.qmsz

    /*====================================================================================================
    # 规则代码: AM00477
    # 目标接口: J1035-同业存单投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF资产组合].{资产类别}为“107-同业存单”的[QDII及FOF资产组合].{期末市值}==SUM{期末市值}
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
    SELECT DISTINCT 'AM00477'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_ncd tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_inv_qd_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 107-同业存单
      AND t2.zclb = '107'
      AND t1.sum_qmsz <> t2.qmsz

    /*====================================================================================================
    # 规则代码: AM00478
    # 目标接口: J1036-债券回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [资产组合].{资产类别}为“108-买入返售资产”的[资产组合].{期末市值}==(SUM{期末金额} WHERE {回购方向}为“逆回购”)+(SUM[协议回购投资明细].{期末金额} WHERE {回购方向}为“逆回购” AND {回购类型}不为“股票质押式回购（场内）” AND {回购类型}不为“股票质押式回购（场外）” AND {回购类型}不为“股权质押式回购”)
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1026-资产组合 J1037-协议回购投资明细
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前 T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00478'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 回购方向: 2-逆回购
               , sum(CASE WHEN tt1.hgfx = '2' THEN tt1.qmje ELSE 0 END)
                     OVER (PARTITION BY tt1.jgdm, tt1.status,tt1.sjrq, tt1.cpdm) AS sum_qmje
          FROM report_cisp.wdb_amp_inv_bdrepo tt1) t1
             LEFT JOIN (SELECT tt2.jgdm
                             , tt2.status
                             , tt2.sjrq
                             , tt2.cpdm
                             -- 回购方向: 2-逆回购
                             -- 回购类型: 06-股票质押式回购（场内）、07-股票质押式回购（场外）、08-股权质押式回购
                             , sum(
                CASE WHEN tt2.hgfx = '2' AND tt2.hglx NOT IN ('06', '07', '08') THEN tt2.qmje ELSE 0 END)
                OVER (PARTITION BY tt2.jgdm, tt2.status,tt2.sjrq, tt2.cpdm) AS sum_qmje
                        FROM report_cisp.wdb_amp_inv_agrmrepo tt2) t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_inv_assets t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 108-买入返售资产
      AND t3.zclb = '108'
      AND t3.qmsz <> t1.sum_qmje + t2.sum_qmje

    /*====================================================================================================
    # 规则代码: AM00479
    # 目标接口: J1036-债券回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [资产组合].{资产类别}为“111-融入资金”的[资产组合].{期末市值}==(SUM{期末金额} WHERE {回购方向}为“正回购”)+(SUM[协议回购投资明细].{期末金额} WHERE {回购方向}为“正回购” AND {回购类型}不为“股票质押式回购（场内）” AND {回购类型}不为“股票质押式回购（场外）” AND {回购类型}不为“股权质押式回购”)
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1026-资产组合 J1037-协议回购投资明细
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前 T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00479'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 回购方向: 1-正回购
               , sum(CASE WHEN tt1.hgfx = '1' THEN tt1.qmje ELSE 0 END)
                     OVER (PARTITION BY tt1.jgdm, tt1.status,tt1.sjrq, tt1.cpdm) AS sum_qmje
          FROM report_cisp.wdb_amp_inv_bdrepo tt1) t1
             LEFT JOIN (SELECT tt2.jgdm
                             , tt2.status
                             , tt2.sjrq
                             , tt2.cpdm
                             -- 回购方向: 1-正回购
                             -- 回购类型: 06-股票质押式回购（场内）、07-股票质押式回购（场外）、08-股权质押式回购
                             , sum(
                CASE WHEN tt2.hgfx = '1' AND tt2.hglx NOT IN ('06', '07', '08') THEN tt2.qmje ELSE 0 END)
                OVER (PARTITION BY tt2.jgdm, tt2.status,tt2.sjrq, tt2.cpdm) AS sum_qmje
                        FROM report_cisp.wdb_amp_inv_agrmrepo tt2) t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_inv_assets t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 111-融入资金
      AND t3.zclb = '111'
      AND t3.qmsz <> t1.sum_qmje + t2.sum_qmje

    /*====================================================================================================
    # 规则代码: AM00480
    # 目标接口: J1036-债券回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF资产组合].{资产类别}为“108-买入返售资产”的[QDII及FOF资产组合].{期末市值}==(SUM{期末金额} WHERE {回购方向}为“逆回购”)+(SUM[协议回购投资明细].{期末金额} WHERE {回购方向}为“逆回购” AND {回购类型}不为“股票质押式回购（场内）” AND {回购类型}不为“股票质押式回购（场外）” AND {回购类型}不为“股权质押式回购”)
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1027-QDII及FOF资产组合 J1037-协议回购投资明细
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+4日24:00前 T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00480'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 回购方向: 2-逆回购
               , sum(CASE WHEN tt1.hgfx = '2' THEN tt1.qmje ELSE 0 END)
                     OVER (PARTITION BY tt1.jgdm, tt1.status,tt1.sjrq, tt1.cpdm) AS sum_qmje
          FROM report_cisp.wdb_amp_inv_bdrepo tt1) t1
             LEFT JOIN (SELECT tt2.jgdm
                             , tt2.status
                             , tt2.sjrq
                             , tt2.cpdm
                             -- 回购方向: 2-逆回购
                             -- 回购类型: 06-股票质押式回购（场内）、07-股票质押式回购（场外）、08-股权质押式回购
                             , sum(
                CASE WHEN tt2.hgfx = '2' AND tt2.hglx NOT IN ('06', '07', '08') THEN tt2.qmje ELSE 0 END)
                OVER (PARTITION BY tt2.jgdm, tt2.status,tt2.sjrq, tt2.cpdm) AS sum_qmje
                        FROM report_cisp.wdb_amp_inv_agrmrepo tt2) t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_inv_qd_assets t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 108-买入返售资产
      AND t3.zclb = '108'
      AND t3.qmsz <> t1.sum_qmje + t2.sum_qmje

    /*====================================================================================================
    # 规则代码: AM00481
    # 目标接口: J1036-债券回购投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF资产组合].{资产类别}为“111-融入资金”的[QDII及FOF资产组合].{期末市值}==(SUM{期末金额} WHERE {回购方向}为“正回购”)+(SUM[协议回购投资明细].{期末金额} WHERE {回购方向}为“正回购” AND {回购类型}不为“股票质押式回购（场内）” AND {回购类型}不为“股票质押式回购（场外）” AND {回购类型}不为“股权质押式回购”)
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1027-QDII及FOF资产组合 J1037-协议回购投资明细
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+4日24:00前 T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00481'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               -- 回购方向: 1-正回购
               , sum(CASE WHEN tt1.hgfx = '1' THEN tt1.qmje ELSE 0 END)
                     OVER (PARTITION BY tt1.jgdm, tt1.status,tt1.sjrq, tt1.cpdm) AS sum_qmje
          FROM report_cisp.wdb_amp_inv_bdrepo tt1) t1
             LEFT JOIN (SELECT tt2.jgdm
                             , tt2.status
                             , tt2.sjrq
                             , tt2.cpdm
                             -- 回购方向: 1-正回购
                             -- 回购类型: 06-股票质押式回购（场内）、07-股票质押式回购（场外）、08-股权质押式回购
                             , sum(
                CASE WHEN tt2.hgfx = '1' AND tt2.hglx NOT IN ('06', '07', '08') THEN tt2.qmje ELSE 0 END)
                OVER (PARTITION BY tt2.jgdm, tt2.status,tt2.sjrq, tt2.cpdm) AS sum_qmje
                        FROM report_cisp.wdb_amp_inv_agrmrepo tt2) t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_inv_qd_assets t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 111-融入资金
      AND t3.zclb = '111'
      AND t3.qmsz <> t1.sum_qmje + t2.sum_qmje

    /*====================================================================================================
    # 规则代码: AM00482
    # 目标接口: J1039-商品现货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}=上期[商品现货投资明细].{期末数量}+{买入数量}-{卖出数量}
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
    SELECT DISTINCT 'AM00482'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comspt t1
             LEFT JOIN report_cisp.wdb_amp_inv_comspt t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_comspt tt1 WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl <> t2.qmsl + t1.mrsl - t1.mcsl

    /*====================================================================================================
    # 规则代码: AM00483
    # 目标接口: J1039-商品现货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [资产组合].{资产类别}为“109-商品”的[资产组合].{期末市值}==SUM{期末市值}
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
    SELECT DISTINCT 'AM00483'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_comspt tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_inv_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 109-商品
      AND t2.zclb = '109'
      AND t2.qmsz <> t1.sum_qmsz

    /*====================================================================================================
    # 规则代码: AM00484
    # 目标接口: J1039-商品现货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF资产组合].{资产类别}为“109-商品”的[QDII及FOF资产组合].{期末市值}==SUM{期末市值}
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
    SELECT DISTINCT 'AM00484'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_comspt tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_prod_qd_nav t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 109-商品
      AND t2.zclb = '109'
      AND t2.qmsz <> t1.sum_qmsz

    /*====================================================================================================
    # 规则代码: AM00485
    # 目标接口: J1040-商品现货延期交收投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}==上期[商品现货延期交收投资明细].{期末数量}+{开仓数量}-{平仓数量}
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
    SELECT DISTINCT 'AM00485'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comsptdly t1
             LEFT JOIN report_cisp.wdb_amp_inv_comsptdly t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_comspt tt1 WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl <> t2.qmsl + t1.kcsl - t1.pcsl

    /*====================================================================================================
    # 规则代码: AM00486
    # 目标接口: J1040-商品现货延期交收投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [资产组合].{资产类别}为“110-商品及金融衍生品（场内集中交易清算）”的[资产组合].{期末市值}==SUM{期末合约价值}+SUM[金融期货投资明细].{期末合约价值}+SUM[商品期货投资明细].{期末合约价值}+SUM[场内期权投资明细].{期末合约价值}
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 4
    # 其他接口: J1026-资产组合 J1041-金融期货投资明细 J1042-商品期货投资明细 J1043-场内期权投资明细
    # 其他接口传输频度: 日 日 日 日
    # 其他接口传输时间: T+1日24:00前 T+4日24:00前 T+4日24:00前 T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00486'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmhyjz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_comsptdly tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN (SELECT tt2.jgdm
                             , tt2.status
                             , tt2.sjrq
                             , tt2.cpdm
                             , sum(tt2.qmhyjz) AS sum_qmsz
                        FROM report_cisp.wdb_amp_inv_finftr tt2
                        GROUP BY tt2.jgdm, tt2.status, tt2.sjrq, tt2.cpdm) t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN (SELECT tt3.jgdm
                             , tt3.status
                             , tt3.sjrq
                             , tt3.cpdm
                             , sum(tt3.qmhyjz) AS sum_qmsz
                        FROM report_cisp.wdb_amp_inv_comftr tt3
                        GROUP BY tt3.jgdm, tt3.status, tt3.sjrq, tt3.cpdm) t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
             LEFT JOIN (SELECT tt4.jgdm
                             , tt4.status
                             , tt4.sjrq
                             , tt4.cpdm
                             , sum(tt4.qmhyjz) AS sum_qmsz
                        FROM report_cisp.wdb_amp_inv_opt tt4
                        GROUP BY tt4.jgdm, tt4.status, tt4.sjrq, tt4.cpdm) t4
                       ON t1.jgdm = t4.jgdm
                           AND t1.status = t4.status
                           AND t1.sjrq = t4.sjrq
                           AND t1.cpdm = t4.cpdm
             LEFT JOIN report_cisp.wdb_amp_inv_assets t5
                       ON t1.jgdm = t5.jgdm
                           AND t1.status = t5.status
                           AND t1.sjrq = t5.sjrq
                           AND t1.cpdm = t5.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 110-商品及金融衍生品（场内集中交易清算）
      AND t3.zclb = '110'
      AND t5.qmsz <> t1.sum_qmsz + t2.sum_qmsz + t3.sum_qmsz + t4.sum_qmsz

    /*====================================================================================================
    # 规则代码: AM00487
    # 目标接口: J1040-商品现货延期交收投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF资产组合].{资产类别}为“110-商品及金融衍生品（场内集中交易清算）”的[QDII及FOF资产组合].{期末市值}==SUM{期末合约价值}+SUM[金融期货投资明细].{期末合约价值}+SUM[商品期货投资明细].{期末合约价值}+SUM[场内期权投资明细].{期末合约价值}
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 4
    # 其他接口: J1027-QDII及FOF资产组合 J1041-金融期货投资明细 J1042-商品期货投资明细 J1043-场内期权投资明细
    # 其他接口传输频度: 日 日 日 日
    # 其他接口传输时间: T+4日24:00前 T+4日24:00前 T+4日24:00前 T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00487'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmhyjz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_comsptdly tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN (SELECT tt2.jgdm
                             , tt2.status
                             , tt2.sjrq
                             , tt2.cpdm
                             , sum(tt2.qmhyjz) AS sum_qmsz
                        FROM report_cisp.wdb_amp_inv_finftr tt2
                        GROUP BY tt2.jgdm, tt2.status, tt2.sjrq, tt2.cpdm) t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN (SELECT tt3.jgdm
                             , tt3.status
                             , tt3.sjrq
                             , tt3.cpdm
                             , sum(tt3.qmhyjz) AS sum_qmsz
                        FROM report_cisp.wdb_amp_inv_comftr tt3
                        GROUP BY tt3.jgdm, tt3.status, tt3.sjrq, tt3.cpdm) t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
             LEFT JOIN (SELECT tt4.jgdm
                             , tt4.status
                             , tt4.sjrq
                             , tt4.cpdm
                             , sum(tt4.qmhyjz) AS sum_qmsz
                        FROM report_cisp.wdb_amp_inv_opt tt4
                        GROUP BY tt4.jgdm, tt4.status, tt4.sjrq, tt4.cpdm) t4
                       ON t1.jgdm = t4.jgdm
                           AND t1.status = t4.status
                           AND t1.sjrq = t4.sjrq
                           AND t1.cpdm = t4.cpdm
             LEFT JOIN report_cisp.wdb_amp_prod_qd_nav t5
                       ON t1.jgdm = t5.jgdm
                           AND t1.status = t5.status
                           AND t1.sjrq = t5.sjrq
                           AND t1.cpdm = t5.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 110-商品及金融衍生品（场内集中交易清算）
      AND t3.zclb = '110'
      AND t5.qmsz <> t1.sum_qmsz + t2.sum_qmsz + t3.sum_qmsz + t4.sum_qmsz

    /*====================================================================================================
    # 规则代码: AM00488
    # 目标接口: J1041-金融期货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}==上期[金融期货投资明细].{期末数量}+{开仓数量}-{平仓数量}
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
    SELECT DISTINCT 'AM00488'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_finftr t1
             LEFT JOIN report_cisp.wdb_amp_inv_finftr t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_finftr tt1 WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl <> t2.qmsl + t1.kcsl - t1.pcsl

    /*====================================================================================================
    # 规则代码: AM00489
    # 目标接口: J1042-商品期货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}==上期[商品期货投资明细].{期末数量}+{开仓数量}-{平仓数量}
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
    SELECT DISTINCT 'AM00489'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_comftr t1
             LEFT JOIN report_cisp.wdb_amp_inv_comftr t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_comftr tt1 WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl <> t2.qmsl + t1.kcsl - t1.pcsl

    /*====================================================================================================
    # 规则代码: AM00490
    # 目标接口: J1043-场内期权投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {期末数量}==上期[场内期权投资明细].{期末数量}+{开仓数量}-{平仓数量}
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
    SELECT DISTINCT 'AM00490'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_opt t1
             LEFT JOIN report_cisp.wdb_amp_inv_opt t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_opt tt1 WHERE tt1.sjrq < t1.sjrq)
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl <> t2.qmsl + t1.kcsl - t1.pcsl

    /*====================================================================================================
    # 规则代码: AM00491
    # 目标接口: J1044-其他各项资产明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [资产组合].{资产类别}为“105-现金和活期存款”、“199-其他标准化资产”的[资产组合].{期末市值}==SUM[活期存款投资明细]{期末金额}+SUM{期末金额}
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1026-资产组合 J1034-活期存款投资明细
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前 T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00491'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmje) AS sum_qmje
          FROM report_cisp.wdb_amp_inv_othassets tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN (SELECT tt2.jgdm
                             , tt2.status
                             , tt2.sjrq
                             , tt2.cpdm
                             , sum(tt2.qmje) AS sum_qmje
                        FROM report_cisp.wdb_amp_inv_curdepo tt2
                        GROUP BY tt2.jgdm, tt2.status, tt2.sjrq, tt2.cpdm) t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_inv_assets t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 105-现金和活期存款、199-其他标准化资产
      AND t2.zclb IN ('105', '199')
      AND t3.qmje <> t1.sum_qmje + t2.sum_qmje

    /*====================================================================================================
    # 规则代码: AM00492
    # 目标接口: J1044-其他各项资产明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF资产组合].{资产类别}为“105-现金和活期存款”、“199-其他标准化资产”的[QDII及FOF资产组合].{期末市值}==SUM[活期存款投资明细]{期末金额}+SUM{期末金额}
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1027-QDII及FOF资产组合 J1034-活期存款投资明细
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+4日24:00前 T+4日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00492'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmje) AS sum_qmje
          FROM report_cisp.wdb_amp_inv_othassets tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN (SELECT tt2.jgdm
                             , tt2.status
                             , tt2.sjrq
                             , tt2.cpdm
                             , sum(tt2.qmje) AS sum_qmje
                        FROM report_cisp.wdb_amp_inv_curdepo tt2
                        GROUP BY tt2.jgdm, tt2.status, tt2.sjrq, tt2.cpdm) t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_inv_qd_assets t3
                       ON t1.jgdm = t3.jgdm
                           AND t1.status = t3.status
                           AND t1.sjrq = t3.sjrq
                           AND t1.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 105-现金和活期存款、199-其他标准化资产
      AND t2.zclb IN ('105', '199')
      AND t3.qmje <> t1.sum_qmje + t2.sum_qmje

    /*====================================================================================================
    # 规则代码: AM00493
    # 目标接口: J1045-未挂牌资产支持证券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [资产组合].{资产类别}为“205-资产支持证券（未在交易所挂牌）”的[资产组合].{期末市值}==SUM{期末市值}
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1026-资产组合
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00493'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_abs tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_inv_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 205-资产支持证券（未在交易所挂牌）
      AND t2.zclb = '205'
      AND t2.qmsz <> t1.sum_qmsz

    /*====================================================================================================
    # 规则代码: AM00494
    # 目标接口: J1045-未挂牌资产支持证券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF资产组合].{资产类别}为“205-资产支持证券（未在交易所挂牌）”的[QDII及FOF资产组合].{期末市值}==SUM{期末市值}
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
    SELECT DISTINCT 'AM00494'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_abs tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_inv_qd_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 205-资产支持证券（未在交易所挂牌）
      AND t2.zclb = '205'
      AND t2.qmsz <> t1.sum_qmsz

    /*====================================================================================================
    # 规则代码: AM00495
    # 目标接口: J1046-转融券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [资产组合].{资产类别}为“209-转融券”的[资产组合].{期末市值}==SUM{期末市值}
    # 规则来源: 证监会-报送接口规范检查
    # 风险等级: 0
    # 其他接口数量: 1
    # 其他接口: J1026-资产组合
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前
    # 工作状态: 1
    # 备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00495'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_refinance tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_inv_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 209-转融券
      AND t2.zclb = '209'
      AND t2.qmsz <> t1.sum_qmsz

    /*====================================================================================================
    # 规则代码: AM00496
    # 目标接口: J1046-转融券投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: [QDII及FOF资产组合].{资产类别}为“209-转融券”的[QDII及FOF资产组合].{期末市值}==SUM{期末市值}
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
    SELECT DISTINCT 'AM00496'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM (SELECT tt1.jgdm
               , tt1.status
               , tt1.sjrq
               , tt1.cpdm
               , sum(tt1.qmsz) AS sum_qmsz
          FROM report_cisp.wdb_amp_inv_refinance tt1
          GROUP BY tt1.jgdm, tt1.status, tt1.sjrq, tt1.cpdm) t1
             LEFT JOIN report_cisp.wdb_amp_inv_qd_assets t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 资产类别: 209-转融券
      AND t2.zclb = '209'
      AND t2.qmsz <> t1.sum_qmsz

    /*====================================================================================================
    # 规则代码: AM00497
    # 目标接口: J1049-货币市场基金监控
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {影子定价确定的资产净值与摊余成本法计算的资产净值的偏离金额}=={影子定价确定的资产净值}-{摊余成本法计算的资产净值}
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
    SELECT DISTINCT 'AM00497'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_rsk_monitor t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.yzdjqddzcjzytycbfjsdzcjzdplje <> t1.yzdjdzcjz - t1.tycbfjsdzcjz

    /*====================================================================================================
    # 规则代码: AM00498
    # 目标接口: J1049-货币市场基金监控
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {影子定价确定的资产净值与摊余成本法计算的资产净值的偏离度}==ROUND({影子定价确定的资产净值与摊余成本法计算的资产净值的偏离金额}/{摊余成本法计算的资产净值},6)
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
    SELECT DISTINCT 'AM00498'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_rsk_monitor t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.yzdjqddzcjzytycbfjsdzcjzdpld <> round(t1.yzdjqddzcjzytycbfjsdzcjzdplje / t1.tycbfjsdzcjz, 6)

    /*====================================================================================================
    # 规则代码: AM00499
    # 目标接口: J1049-货币市场基金监控
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
    # 规则说明: {基金净值}=[产品净值信息].{资产净值},{七日年化收益率}=[产品净值信息].{七日年化收益率}
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
    SELECT DISTINCT 'AM00499'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_rsk_monitor t1
             LEFT JOIN report_cisp.wdb_amp_prod_nav t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND (t1.jjjz <> t2.zcjz OR t1.qrnhsyl <> t2.qrnhsyl)

    /*====================================================================================================
    # 规则代码: AM00500
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
    SELECT DISTINCT 'AM00500'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00501
    # 目标接口: J1022-QDII及FOF份额汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: ([产品基本信息].{估值完成天数}>=1，[产品基本信息].{子资产单元标志}不为“是”，[产品运行信息].{合同已终止标志}为“否”，[产品运行信息].{总份数}>0)时，在本表中必须存在
    # 规则来源: AMTEC
    # 风险等级: 0
    # 其他接口数量: 2
    # 其他接口: J1002-产品基本信息 J1007-产品运行信息
    # 其他接口传输频度: 日 日
    # 其他接口传输时间: T+1日24:00前 T+4日24:00前
    # 工作状态: 0
    # 备注: 样例
    ====================================================================================================*/
    /*UNION ALL
    SELECT DISTINCT 'AM00501'   AS gzdm        -- 规则代码
                  , t2.sjrq     AS sjrq        -- 数据日期
                  , t2.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_qd_shr_sum t1
             RIGHT JOIN report_cisp.wdb_amp_prod_baseinfo tt1
                        ON t1.jgdm = t2.jgdm
                            AND t1.status = t2.status
                            AND t1.sjrq = t2.sjrq
                            AND t1.cpdm = t2.cpdm
             LEFT JOIN report_cisp.wdb_amp_prod_oprt tt2
                       ON t2.jgdm = t3.jgdm
                           AND t2.status = t3.status
                           AND t2.sjrq = t3.sjrq
                           AND t2.cpdm = t3.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t2.gzwcts >= 1
      AND t2.zzcdybz = '0'
      AND t3.htyzzbz = '0'
      AND t3.zfs > 0
      AND t1.cpdm IS NULL*/

    ;
    COMMIT;
END;
/