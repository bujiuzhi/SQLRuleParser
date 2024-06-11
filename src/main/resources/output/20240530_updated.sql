CREATE OR REPLACE PROCEDURE p_report_cisp_wdb_am0p_bsjyb_1(
    pi_end_date IN NUMBER --加载日期
)
    IS
    v_pi_end_date_t1 NUMBER(8);
    v_pi_end_date_t2 NUMBER(8);
BEGIN

    --删除重复数据
    DELETE FROM rdm.wdb_am0p_bsjyb_1 WHERE insert_time = pi_end_date;
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
    INSERT INTO rdm.wdb_am0p_bsjyb_1
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
    FROM report_cisp.wdb_am0p_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 产品类别: 119-股票型QDII基金、129-混合型QDII基金、139-债券型QDII基金、158-FOF型QDII基金、159-MOM型QDII基金、198-其他QDII基金
      --          219-股票型QDII、229-混合型QDII、239-债券型QDII、258-FOF型QDII、259-MOM型QDII、298-其他QDII
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
          FROM report_cisp.wdb_am0p_inv_assets tt1
                   LEFT JOIN report_cisp.wdb_am0p_prod_nav tt2 ON tt1.jgdm = tt2.jgdm
              AND tt1.status = tt2.status
              AND tt1.sjrq = tt2.sjrq
              AND tt1.cpdm = tt2.cpdm
                   LEFT JOIN report_cisp.wdb_am0p_prod_baseinfo tt3 ON tt2.jgdm = tt3.jgdm
              AND tt2.status = tt3.status
              AND tt2.sjrq = tt3.sjrq
              AND tt2.cpdm = tt3.cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 资产类别: 101-股票
      AND t1.zclb = '101'
      -- 产品类别: 111-股票型基金、119-股票型QDII基金、211-股票型、219-股票型QDII
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
          FROM report_cisp.wdb_am0p_inv_assets tt1
                   LEFT JOIN report_cisp.wdb_am0p_prod_nav tt2 ON tt1.jgdm = tt2.jgdm
              AND tt1.status = tt2.status
              AND tt1.sjrq = tt2.sjrq
              AND tt1.cpdm = tt2.cpdm
                   LEFT JOIN report_cisp.wdb_am0p_prod_baseinfo tt3 ON tt2.jgdm = tt3.jgdm
              AND tt2.status = tt3.status
              AND tt2.sjrq = tt3.sjrq
              AND tt2.cpdm = tt3.cpdm) t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 资产类别: 103-债券
      AND t1.zclb = '103'
      -- 产品类别: 131-债券基金、139-债券型QDII基金、231-债券型、239-债券型QDII
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
    FROM report_cisp.wdb_am0p_prod_baseinfo t1
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
    FROM report_cisp.wdb_am0p_prod_baseinfo t1
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
    FROM report_cisp.wdb_am0p_prod_baseinfo t1
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
    FROM report_cisp.wdb_am0p_prod_baseinfo t1
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
    FROM report_cisp.wdb_am0p_prod_baseinfo t1
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
    # 目标接口传输时间: T+1日24:00前
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
    FROM report_cisp.wdb_am0p_prod_divid t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
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
    FROM report_cisp.wdb_am0p_inv_stock t1
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
    FROM report_cisp.wdb_am0p_inv_bond t1
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
    FROM report_cisp.wdb_am0p_inv_agrmrepo t1
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
    FROM report_cisp.wdb_am0p_inv_loan t1
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
          FROM report_cisp.wdb_am0p_prod_fundmngr tt1
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
    # 目标接口传输时间: T+1日24:00前
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
    FROM report_cisp.wdb_am0p_prod_oprt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
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
    FROM report_cisp.wdb_am0p_prod_pocket t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.sjtzzsl <> 0
      AND t1.dqzt = '1'

    /*====================================================================================================
    # 规则代码: AM00017
    # 目标接口: J1007-产品运行信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
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
    FROM report_cisp.wdb_am0p_prod_oprt t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
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
    FROM report_cisp.wdb_am0p_prod_nav t1
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
    FROM report_cisp.wdb_am0p_prod_nav t1
             LEFT JOIN report_cisp.wdb_am0p_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 产品类别: 141-货币基金、241-货币型
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
    FROM report_cisp.wdb_am0p_prod_qd_nav t1
             LEFT JOIN report_cisp.wdb_am0p_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 产品类别: 141-货币基金、241-货币型
      AND t2.cplb IN ('141', '241')
      AND t1.mwfsy IS NULL

    /*====================================================================================================
    # 规则代码: AM00021
    # 目标接口: J1011-产品收益分配信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
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
    FROM report_cisp.wdb_am0p_prod_divid t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t1.qydjrq < t1.fprq

    /*====================================================================================================
    # 规则代码: AM00022
    # 目标接口: J1011-产品收益分配信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
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
    FROM report_cisp.wdb_am0p_prod_divid t1
             LEFT JOIN report_cisp.wdb_am0p_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t2.jzxcpbz = '1'
      AND (t1.sjfpsy < 0 OR t1.xjfpje < 0 OR t1.ztzje < 0)

    /*====================================================================================================
    # 规则代码: AM00023
    # 目标接口: J1011-产品收益分配信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
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
    FROM report_cisp.wdb_am0p_prod_divid t1
             LEFT JOIN report_cisp.wdb_am0p_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND t2.jzxcpbz = '0'
      AND t1.yfpbj < 0

    /*====================================================================================================
    # 规则代码: AM00024
    # 目标接口: J1011-产品收益分配信息
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
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
    FROM report_cisp.wdb_am0p_prod_divid t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
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
             {有效投资者数量}>0
             {持有市值}>0
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
      AND (t1.nlfd IS NULL OR t1.yxtzzsl <= 0 OR t1.cysz <= 0)

    /*====================================================================================================
    # 规则代码: AM00029
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
    SELECT DISTINCT 'AM00029'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00030
    # 目标接口: J1017-交易汇总
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
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
    SELECT DISTINCT 'AM00030'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_tran_sum t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND (t1.sxf <> '0' OR t1.sxfgglr <> '0' OR t1.sxfgxsjg <> '0' OR t1.sxfgcpzc <> '0' OR t1.hsf <> '0' OR
           t1.hsfgglr <> '0' OR t1.hsfgxsjg <> '0')
      AND t1.cpdm NOT IN ('501059', '502000')

    /*====================================================================================================
    # 规则代码: AM00031
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
    SELECT DISTINCT 'AM00031'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00032
    # 目标接口: J1019-净申赎超1亿或超基金资产净值10%客户
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+1日24:00前
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
    SELECT DISTINCT 'AM00032'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_sl_bignetrdm t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      AND (t1.jssje <> t1.sgje - t1.shje OR t1.jssfs <> t1.sgfs - t1.shfs)

    /*====================================================================================================
    # 规则代码: AM00033
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
    SELECT DISTINCT 'AM00033'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00034
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
    SELECT DISTINCT 'AM00034'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00035
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
    SELECT DISTINCT 'AM00035'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00036
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
    SELECT DISTINCT 'AM00036'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00037
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
    SELECT DISTINCT 'AM00037'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00038
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
    SELECT DISTINCT 'AM00038'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00039
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
    SELECT DISTINCT 'AM00039'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00040
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
    SELECT DISTINCT 'AM00040'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00041
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
    SELECT DISTINCT 'AM00041'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00042
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
    SELECT DISTINCT 'AM00042'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00043
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
    SELECT DISTINCT 'AM00043'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00044
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
    SELECT DISTINCT 'AM00044'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00045
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
    SELECT DISTINCT 'AM00045'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_prestock t1
             LEFT JOIN report_cisp.wdb_amp_inv_prestock t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_prestock WHERE tt1.sjrq > t1.sjrq)
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl <> t2.qmsl + t1.mrsl - t1.mcsl

    /*====================================================================================================
    # 规则代码: AM00046
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
    SELECT DISTINCT 'AM00046'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
             LEFT JOIN report_cisp.wdb_amp_inv_bond t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_bond WHERE tt1.sjrq > t1.sjrq)
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl <> t2.qmsl + t1.mrsl - t1.mcsl

    /*====================================================================================================
    # 规则代码: AM00047
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
    # 备注: 规则有误，先搁置
    ====================================================================================================*/
    /*UNION ALL
    SELECT DISTINCT 'AM00047'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00048
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
    SELECT DISTINCT 'AM00048'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00049
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
    SELECT DISTINCT 'AM00049'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00050
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
    # 工作状态: 0
    # 备注: 规则有误，先搁置
    ====================================================================================================*/
    /*UNION ALL
    SELECT DISTINCT 'AM00050'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bdrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码: 101-银行间市场
      AND t1.jycsdm <> '101'*/

    /*====================================================================================================
    # 规则代码: AM00051
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
    # 工作状态: 0
    # 备注: 规则有误，先搁置
    ====================================================================================================*/
    /*UNION ALL
    SELECT DISTINCT 'AM00051'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_agrmrepo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 交易场所代码: 101-银行间市场
      AND t1.jycsdm <> '101'*/

    /*====================================================================================================
    # 规则代码: AM00052
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
    SELECT DISTINCT 'AM00052'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00053
    # 目标接口: J1039-商品现货投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {产品类别}应该为"15-商品及金融衍生品类" or "16-商品及金融衍生品类QDII"
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
    SELECT DISTINCT 'AM00053'   AS gzdm        -- 规则代码
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
      -- 产品类别: 15-商品及金融衍生品类、16-商品及金融衍生品类QDII
      AND t2.cplb NOT IN ('15', '16')

    /*====================================================================================================
    # 规则代码: AM00054
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
    SELECT DISTINCT 'AM00054'   AS gzdm        -- 规则代码
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
    # 规则代码: AM00055
    # 目标接口: J1040-商品现货延期交收投资明细
    # 目标接口传输频度: 日
    # 目标接口传输时间: T+4日24:00前
    # 规则说明: {产品类别}应该为"15-商品及金融衍生品类" or "16-商品及金融衍生品类QDII"
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
    FROM report_cisp.wdb_amp_inv_comsptdly t1
             LEFT JOIN report_cisp.wdb_amp_prod_baseinfo t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq = t2.sjrq
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      -- 产品类别: 15-商品及金融衍生品类、16-商品及金融衍生品类QDII
      AND t2.cplb NOT IN ('15', '16')

    /*====================================================================================================
    # 规则代码: AM00056
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
    # 工作状态: 0
    # 备注: 样例
    ====================================================================================================*/
    /*UNION ALL
    SELECT DISTINCT 'AM00056'   AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_amp_inv_bond t1
             LEFT JOIN report_cisp.wdb_amp_inv_bond t2
                       ON t1.jgdm = t2.jgdm
                           AND t1.status = t2.status
                           AND t1.sjrq =
                               (SELECT max(tt1.sjrq) FROM report_cisp.wdb_amp_inv_bond WHERE tt1.sjrq > t1.sjrq)
                           AND t1.cpdm = t2.cpdm
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t2 --若测试，请注释本行
      AND t1.qmsl <> t2.qmsl + t1.mrsl - t1.mcsl*/

    /*====================================================================================================
    # 规则代码: AM00057
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
    SELECT DISTINCT 'AM00057'   AS gzdm        -- 规则代码
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
          FROM report_cisp.wdb_am0p_inv_assets tt1
                   LEFT JOIN report_cisp.wdb_am0p_prod_baseinfo tt2 ON tt1.jgdm = tt2.jgdm
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