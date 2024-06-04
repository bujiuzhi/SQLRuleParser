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
    规则代码: AM00001
    目标接口: J1002-产品基本信息
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: 公司无QDII产品的情况下，不应当有境外托管人
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    SELECT DISTINCT 'AM00001' AS gzdm        -- 规则代码
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
      AND t1.jwtgrzwmc IS NOT NULL

    /*====================================================================================================
    规则代码: AM00002
    目标接口: J1002-产品基本信息
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: 公司无QDII产品的情况下，不应当有境外投资顾问
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00002' AS gzdm        -- 规则代码
                  , t1.sjrq     AS sjrq        -- 数据日期
                  , t1.cpdm     AS cpdm        -- 产品代码
                  , pi_end_date AS insert_time -- 插入时间，若测试，请注释本行
                  , 0           AS fxdj        -- 风险等级 0-严重 1-警告
    FROM report_cisp.wdb_am0p_prod_baseinfo t1
    WHERE t1.jgdm = '70610000'
      AND t1.status NOT IN ('3', '5')
      AND t1.sjrq = v_pi_end_date_t1 --若测试，请注释本行
      -- 产品类别: 119-股票型QDII基金、129-混合型QDII基金、139-债券型QDII基金、158-FOF型QDII基金、159-MOM型QDII基金、198-其他QDII基金
      --          219-股票型QDII、229-混合型QDII、239-债券型QDII、258-FOF型QDII、259-MOM型QDII、298-其他QDIII
      AND t1.cplb NOT IN ('119', '129', '139', '158', '159', '198', '219', '229', '239', '258', '259', '298')
      AND (t1.jwtzgwzwmc IS NOT NULL OR t1.jwtzgwywmc IS NOT NULL)

    /*====================================================================================================
    规则代码: AM00003
    目标接口: J1026-资产组合
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: 股票型基金股票的投资比例应大于等于80%
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 1
    其他接口: J1002-产品基本信息 J1009-产品净值信息
    其他接口传输频度: 日 日
    其他接口传输时间: T+1日24:00前 T+1日24:00前
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00003' AS gzdm        -- 规则代码
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
              AND tt1.cpdm = tt2.cpdm
              AND tt1.sjrq = tt2.sjrq
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
    规则代码: AM00004
    目标接口: J1026-资产组合
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: 债券型基金债券的投资比例应大于等于80%
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 1
    其他接口: J1002-产品基本信息 J1009-产品净值信息
    其他接口传输频度: 日 日
    其他接口传输时间: T+1日24:00前 T+1日24:00前
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00004' AS gzdm        -- 规则代码
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
    规则代码: AM00005
    目标接口: J1002-产品基本信息
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: 产品起始日期应小于或等于当前系统日期
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00005' AS gzdm        -- 规则代码
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
    规则代码: AM00006
    目标接口: J1002-产品基本信息
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: {注册批文变更日期}不为空时，{核准日期}<={注册批文变更日期},且{注册批文变更日期}<={数据日期}
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00006' AS gzdm        -- 规则代码
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
    规则代码: AM00007
    目标接口: J1002-产品基本信息
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: {正式转型日期}不为空时，{正式转型日期}<={数据日期}
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00007' AS gzdm        -- 规则代码
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
    规则代码: AM00008
    目标接口: J1002-产品基本信息
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: {转型前产品正式代码}和{正式转型日期}有绑定关系
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00008' AS gzdm        -- 规则代码
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
    规则代码: AM00009
    目标接口: J1002-产品基本信息
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: 指数基金受托责任为被动投资,其中指数增强受托责任为两者兼具
    规则来源: 证监会-FAQ
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00009' AS gzdm        -- 规则代码
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
    规则代码: AM00010
    目标接口: J1011-产品收益分配信息
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: 分配收益为负值时，需要在{备注}中写明原因
    规则来源: 证监会-FAQ
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00010' AS gzdm        -- 规则代码
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
    规则代码: AM00011
    目标接口: J1029-股票投资明细
    目标接口传输频度: 日
    目标接口传输时间: T+4日24:00前
    规则说明: {证券代码}无需包含“SH”“SZ”
    规则来源: 证监会-FAQ
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00011' AS gzdm        -- 规则代码
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
    规则代码: AM00012
    目标接口: J1031-债券投资明细
    目标接口传输频度: 日
    目标接口传输时间: T+4日24:00前
    规则说明: {债券代码}不能包含字符“SH”、“SZ”、“IB”
    规则来源: 证监会-FAQ
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00012' AS gzdm        -- 规则代码
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
    规则代码: AM00013
    目标接口: J1031-债券投资明细
    目标接口传输频度: 日
    目标接口传输时间: T+4日24:00前
    规则说明: {债券代码}不能包含字符“SH”、“SZ”、“IB”
    规则来源: 证监会-FAQ
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00013' AS gzdm        -- 规则代码
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
    规则代码: AM00014
    目标接口: J1037-协议回购投资明细
    目标接口传输频度: 日
    目标接口传输时间: T+4日24:00前
    规则说明: {交易对手方产品代码}第7位为"1"的，{交易对手方类型}应为“银行非保本理财";
             {交易对手方产品代码}第7位为"2"的，{交易对手方类型}应为“信托公司资管产品";
             {交易对手方产品代码}第7位为"3"的，{交易对手方类型}应为“证券公司及其子公司资管产品";
             {交易对手方产品代码}第7位为"4"的，{交易对手方类型}应为“基金管理公司及其子公司专户";
             {交易对手方产品代码}第7位为"5"的，{交易对手方类型}应为“期货公司及其子公司资管产品";
             {交易对手方产品代码}第7位为"6"的，{交易对手方类型}应为“保险资管产品";
             {交易对手方产品代码}第7位为"8"的，{交易对手方类型}应为“公募基金";
    规则来源: 人行
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00014' AS gzdm        -- 规则代码
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
            OR (substr(t1.jydsfcpdm, 7, 1) = '6' AND t1.jydsflx NOT IN ('212', '214'))
        )

    /*====================================================================================================
    规则代码: AM00015
    目标接口: J1047-债券借贷投资明细
    目标接口传输频度: 日
    目标接口传输时间: T+4日24:00前
    规则说明: {交易对手方产品代码}第7位为"1"的，{交易对手方类型}应为“银行非保本理财";
             {交易对手方产品代码}第7位为"2"的，{交易对手方类型}应为“信托公司资管产品";
             {交易对手方产品代码}第7位为"3"的，{交易对手方类型}应为“证券公司及其子公司资管产品";
             {交易对手方产品代码}第7位为"4"的，{交易对手方类型}应为“基金管理公司及其子公司专户";
             {交易对手方产品代码}第7位为"5"的，{交易对手方类型}应为“期货公司及其子公司资管产品";
             {交易对手方产品代码}第7位为"6"的，{交易对手方类型}应为“保险资管产品";
             {交易对手方产品代码}第7位为"8"的，{交易对手方类型}应为“公募基金";
    规则来源: 人行
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00015' AS gzdm        -- 规则代码
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
            OR (substr(t1.jydsfcpdm, 7, 1) = '6' AND t1.jydsflx NOT IN ('212', '214'))
        )

    /*====================================================================================================
    规则代码: AM00016
    目标接口: J3006-基金经理信息
    目标接口传输频度: 月
    目标接口传输时间: T+7日24:00前
    规则说明: 基金经理的{任职日期}与{公告日期}应该相同，剔除已经离职的基金经理
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00016' AS gzdm        -- 规则代码
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
    规则代码: AM00017
    目标接口: J1007-产品运行信息
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: 基金报告期间+数据报送日期，与基金已清盘标志强关联。在该基金已清盘的当月，不需要报送该产品
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00017' AS gzdm        -- 规则代码
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
    规则代码: AM00018
    目标接口: J1004-产品侧袋信息
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: 产品侧袋基本信息中的{当前状态}与{涉及投资者数量}，呈强相关。例如，{涉及投资者数量}为0的情况下，{当前状态}才可以是已终止。
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00018' AS gzdm        -- 规则代码
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
    规则代码: AM00019
    目标接口: J1007-产品运行信息
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: {本年累计费用总和}={本年累计业绩报酬}+{本年累计销售服务费}+{本年累计托管费}+{本年累计管理费}
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00019' AS gzdm        -- 规则代码
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
    规则代码: AM00020
    目标接口: J1009-产品净值信息
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: {资产净值}={总份额}*{单位净值}
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00020' AS gzdm        -- 规则代码
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
    规则代码: AM00021
    目标接口: J1009-产品净值信息
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: ([产品基本信息].{产品类别}为“货币基金”、“货币型”)时，{每万份收益}不能为空
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 1
    其他接口: J1002-产品基本信息
    其他接口传输频度: 日
    其他接口传输时间: T+1日24:00前
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00021' AS gzdm        -- 规则代码
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
    规则代码: AM00022
    目标接口: J1010-QDII及FOF产品净值信息
    目标接口传输频度: 日
    目标接口传输时间: T+4日24:00前
    规则说明: ([产品基本信息].{产品类别}为“货币基金”、“货币型”)时，{每万份收益}不能为空
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 1
    其他接口: J1002-产品基本信息
    其他接口传输频度: 日
    其他接口传输时间: T+1日24:00前
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00022' AS gzdm        -- 规则代码
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
    规则代码: AM00023
    目标接口: J1011-产品收益分配信息
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: 权益登记日期不能在分配日期之前。
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00023' AS gzdm        -- 规则代码
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
    规则代码: AM00024
    目标接口: J1011-产品收益分配信息
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: [产品基本信息].{净值型产品标志}为“是”时，[产品收益分配信息].{实际分配收益}>=0，[产品收益分配信息].{现金分配金额}>=0，[产品收益分配信息].{再投资金额}>=0
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 1
    其他接口: J1002-产品基本信息
    其他接口传输频度: 日
    其他接口传输时间: T+1日24:00前
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00024' AS gzdm        -- 规则代码
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
    规则代码: AM00025
    目标接口: J1011-产品收益分配信息
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: [产品基本信息].{净值型产品标志}为“否”时，[产品收益分配信息].{应分配本金}>=0
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 1
    其他接口: J1002-产品基本信息
    其他接口传输频度: 日
    其他接口传输时间: T+1日24:00前
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00025' AS gzdm        -- 规则代码
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
    规则代码: AM00026
    目标接口: J1011-产品收益分配信息
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: {应分配收益}={分配基数}*{单位产品分配收益}
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 0
    其他接口:
    其他接口传输频度:
    其他接口传输时间:
    工作状态: 1
    备注:
    ====================================================================================================*/
    UNION ALL
    SELECT DISTINCT 'AM00026' AS gzdm        -- 规则代码
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
    规则代码: AM00027
    目标接口: J1011-产品收益分配信息
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: [产品基本信息].{净值型产品标志}为“是”时，[产品收益分配信息].{实际分配收益}>=0，[产品收益分配信息].{现金分配金额}>=0，[产品收益分配信息].{再投资金额}>=0
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 1
    其他接口: J1002-产品基本信息
    其他接口传输频度: 日
    其他接口传输时间: T+1日24:00前
    工作状态: 0
    备注: 样例
    ====================================================================================================*/
    /*UNION ALL
    SELECT DISTINCT 'AM00027' AS gzdm        -- 规则代码
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
      AND (t1.sjfpsy < 0 OR t1.xjfpje < 0 OR t1.ztzje < 0)*/

    /*====================================================================================================
    规则代码: AM00028
    目标接口: J1026-资产组合
    目标接口传输频度: 日
    目标接口传输时间: T+1日24:00前
    规则说明: 债券型基金债券的投资比例应大于等于80%
    规则来源: AM0TEC
    风险等级: 0
    其他接口数量: 1
    其他接口: J1002-产品基本信息
    其他接口传输频度: 日
    其他接口传输时间: T+1日24:00前
    工作状态: 0
    备注: 样例
    ====================================================================================================*/
    /*UNION ALL
    SELECT DISTINCT 'AM00028' AS gzdm        -- 规则代码
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


