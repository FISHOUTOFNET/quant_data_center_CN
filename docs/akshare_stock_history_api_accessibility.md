# AkShare 股票历史 API 可访问性报告

- 生成时间: `2026-05-24T19:37:29`
- AkShare 版本: `1.18.57`
- 文档来源: https://akshare.akfamily.xyz/data/stock/stock.html
- 候选接口数: `233`
- 可访问: `23`
- 空数据: `2`
- 不可访问: `208`

本文件由 `scripts/audit_akshare_stock_history_apis.py` 生成。再次运行会覆盖并更新为最新探测状态。

| 接口 | 标题 | 状态 | 最早观测时间 | 行数 | 轮次 | 参数 | 最后错误 |
| --- | --- | --- | --- | ---: | ---: | --- | --- |
| `stock_szse_area_summary` | 地区交易排序 | accessible |  | 34 | 1 | date='202203' |  |
| `stock_szse_sector_summary` | 股票行业成交 | inaccessible |  | 0 | 3 | date='202501', symbol='当月' | TimeoutError: stock_szse_sector_summary timed out after 10s |
| `stock_zh_a_hist` | 历史行情数据-东财 | inaccessible |  | 0 | 3 | end_date='20260524', period='daily', start_date='19900101', symbol='603777' | ProxyError: HTTPSConnectionPool(host='push2his.eastmoney.com', port=443): Max retries exceeded with url: /api/qt/stock/kline/get?fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57 |
| `stock_zh_a_daily` | 历史行情数据-新浪 | inaccessible |  | 0 | 3 | end_date='20260524', start_date='19900101', symbol='sh600000' | TimeoutError: stock_zh_a_daily timed out after 10s |
| `stock_zh_a_hist_tx` | 历史行情数据-腾讯 | inaccessible |  | 0 | 3 | end_date='20260524', start_date='19900101', symbol='sz000001' | TimeoutError: stock_zh_a_hist_tx timed out after 10s |
| `stock_zh_a_minute` | 分时数据-新浪 | inaccessible |  | 0 | 3 | adjust='', period='1', symbol='sh000300' | TimeoutError: stock_zh_a_minute timed out after 10s |
| `stock_zh_a_hist_min_em` | 分时数据-东财 | inaccessible |  | 0 | 3 | adjust='', end_date='20260524', period='5', start_date='19900101', symbol='000300' | ProxyError: HTTPSConnectionPool(host='push2his.eastmoney.com', port=443): Max retries exceeded with url: /api/qt/stock/kline/get?fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57 |
| `stock_intraday_em` | 日内分时数据-东财 | inaccessible |  | 0 | 3 | symbol='000001' | ProxyError: HTTPSConnectionPool(host='70.push2.eastmoney.com', port=443): Max retries exceeded with url: /api/qt/stock/details/sse?fields1=f1%2Cf2%2Cf3%2Cf4&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55&mpi=2000&ut=bd1d9dd |
| `stock_intraday_sina` | 日内分时数据-新浪 | inaccessible |  | 0 | 3 | date='20240321', symbol='sz000001' | KeyError: 'ticktime' |
| `stock_zh_a_hist_pre_min_em` | 盘前数据 | inaccessible |  | 0 | 3 | end_time='15:40:00', start_time='09:00:00', symbol='000001' | JSONDecodeError: Expecting value: line 1 column 1 (char 0) |
| `stock_zh_valuation_comparison_em` | 估值比较 | accessible |  | 8 | 2 | symbol='SZ000895' |  |
| `stock_zh_a_cdr_daily` | 历史行情数据 | inaccessible |  | 0 | 3 | end_date='20260524', start_date='19900101', symbol='sh689009' | TimeoutError: stock_zh_a_cdr_daily timed out after 10s |
| `stock_zh_b_daily` | 历史行情数据 | inaccessible |  | 0 | 3 | end_date='20260524', start_date='19900101', symbol='sh900901' | TimeoutError: stock_zh_b_daily timed out after 10s |
| `stock_zh_b_minute` | 分时数据 | inaccessible |  | 0 | 3 | adjust='', period='1', symbol='sh900901' | TimeoutError: stock_zh_b_minute timed out after 10s |
| `stock_gsrl_gsdt_em` | 公司动态 | accessible | 2023-08-08 | 72 | 1 | date='20230808' |  |
| `stock_zh_kcb_daily` | 历史行情数据 | inaccessible |  | 0 | 3 | symbol='sh688008' | TimeoutError: stock_zh_kcb_daily timed out after 10s |
| `stock_zh_kcb_report_em` | 科创板公告 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_zh_kcb_report_em timed out after 10s |
| `stock_zh_ah_daily` | 历史行情数据 | inaccessible |  | 0 | 3 | adjust='', end_year='2019', start_year='1990', symbol='02318' | TimeoutError: stock_zh_ah_daily timed out after 10s |
| `stock_us_hist` | 历史行情数据-东财 | inaccessible |  | 0 | 3 | adjust='', end_date='20260524', period='daily', start_date='19900101' | TimeoutError: stock_us_hist timed out after 10s |
| `stock_us_hist_min_em` | 分时数据-东财 | inaccessible |  | 0 | 3 | end_date='20260524', start_date='19900101', symbol='105.ATER' | ProxyError: HTTPSConnectionPool(host='push2his.eastmoney.com', port=443): Max retries exceeded with url: /api/qt/stock/trends2/get?fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6%2Cf7%2Cf8%2Cf9%2Cf10%2Cf11%2Cf12%2Cf13&fields |
| `stock_us_daily` | 历史行情数据-新浪 | inaccessible |  | 0 | 3 | adjust='qfq' | TimeoutError: stock_us_daily timed out after 10s |
| `stock_hk_hist_min_em` | 分时数据-东财 | inaccessible |  | 0 | 3 | adjust='', end_date='20260524', period='5', start_date='19900101', symbol='01611' | TimeoutError: stock_hk_hist_min_em timed out after 10s |
| `stock_hk_hist` | 历史行情数据-东财 | inaccessible |  | 0 | 3 | adjust='', end_date='20260524', period='daily', start_date='19900101', symbol='00593' | TimeoutError: stock_hk_hist timed out after 10s |
| `stock_hk_daily` | 历史行情数据-新浪 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_hk_daily timed out after 10s |
| `stock_hk_financial_indicator_em` | 财务指标 | accessible |  | 1 | 1 | symbol='03900' |  |
| `stock_hk_dividend_payout_em` | 分红派息 | accessible | 2007-05-03 | 19 | 1 | symbol='03900' |  |
| `stock_hk_valuation_comparison_em` | 估值对比 | accessible |  | 1 | 2 | symbol='03900' |  |
| `stock_zygc_em` | 主营构成-东财 | accessible | 2018-12-31 | 79 | 2 | symbol='SH688041' |  |
| `stock_gpzy_profile_em` | 股权质押市场概况 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_gpzy_profile_em timed out after 10s |
| `stock_gpzy_pledge_ratio_detail_em` | 重要股东股权质押明细 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_gpzy_pledge_ratio_detail_em timed out after 10s |
| `stock_gpzy_individual_pledge_ratio_detail_em` | 个股重要股东股权质押明细 | inaccessible |  | 0 | 3 | symbol='603132' | TimeoutError: stock_gpzy_individual_pledge_ratio_detail_em timed out after 10s |
| `stock_gpzy_industry_data_em` | 上市公司质押比例 | accessible | 2026-02-13 | 127 | 2 |  |  |
| `stock_sy_profile_em` | A股商誉市场概况 | accessible | 2010-12-31 | 17 | 1 |  |  |
| `stock_sy_yq_em` | 商誉减值预期明细 | inaccessible |  | 0 | 3 | date='20221231' | TimeoutError: stock_sy_yq_em timed out after 10s |
| `stock_sy_jz_em` | 个股商誉减值明细 | inaccessible |  | 0 | 3 | date='20230331' | TypeError: 'NoneType' object is not subscriptable |
| `stock_sy_em` | 个股商誉明细 | inaccessible |  | 0 | 3 | date='20240630' | TypeError: 'NoneType' object is not subscriptable |
| `stock_sy_hy_em` | 行业商誉 | inaccessible |  | 0 | 3 | date='20240930' | TypeError: 'NoneType' object is not subscriptable |
| `stock_account_statistics_em` | 股票账户统计月度 | accessible |  | 101 | 2 |  |  |
| `stock_analyst_rank_em` | 分析师指数排行 | inaccessible |  | 0 | 3 | year='2024' | TimeoutError: stock_analyst_rank_em timed out after 10s |
| `stock_comment_em` | 千股千评 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_comment_em timed out after 10s |
| `stock_comment_detail_zhpj_lspf_em` | 历史评分 | accessible | 2026-04-08 | 30 | 2 | symbol='600000' |  |
| `stock_hsgt_fund_min_em` | 沪深港通分时数据 | inaccessible |  | 0 | 3 | symbol='北向资金' | TimeoutError: stock_hsgt_fund_min_em timed out after 10s |
| `stock_hsgt_institution_statistics_em` | 机构排行 | inaccessible |  | 0 | 3 | end_date='20260524', market='北向持股', start_date='19900101' | TimeoutError: stock_hsgt_institution_statistics_em timed out after 10s |
| `stock_hsgt_hist_em` | 沪深港通历史数据 | inaccessible |  | 0 | 3 | symbol='北向资金' | TimeoutError: stock_hsgt_hist_em timed out after 10s |
| `stock_tfp_em` | 停复牌信息 | inaccessible |  | 0 | 3 | date='20240426' | TimeoutError: stock_tfp_em timed out after 10s |
| `stock_ipo_ths` | 新股申购与中签-同花顺 | inaccessible |  | 0 | 3 | symbol='全部A股' | TimeoutError: stock_ipo_ths timed out after 10s |
| `stock_yjbb_em` | 业绩报表 | inaccessible |  | 0 | 3 | date='20200331' | TimeoutError: stock_yjbb_em timed out after 10s |
| `stock_yjkb_em` | 业绩快报 | inaccessible |  | 0 | 3 | date='20200331' | TimeoutError: stock_yjkb_em timed out after 10s |
| `stock_yjyg_em` | 业绩预告 | inaccessible |  | 0 | 3 | date='20200331' | TimeoutError: stock_yjyg_em timed out after 10s |
| `stock_yysj_em` | 预约披露时间-东方财富 | inaccessible |  | 0 | 3 | date='20200331', symbol='沪深A股' | TimeoutError: stock_yysj_em timed out after 10s |
| `stock_report_disclosure` | 预约披露时间-巨潮资讯 | inaccessible |  | 0 | 3 | market='沪深京', period='2021年报' | TimeoutError: stock_report_disclosure timed out after 10s |
| `stock_zh_a_disclosure_report_cninfo` | 信息披露公告-巨潮资讯 | inaccessible |  | 0 | 3 | category='', end_date='20260524', keyword='', market='沪深京', start_date='19900101', symbol='000001' | TimeoutError: stock_zh_a_disclosure_report_cninfo timed out after 10s |
| `stock_industry_change_cninfo` | 上市公司行业归属的变动情况-巨潮资讯 | inaccessible |  | 0 | 3 | end_date='20260524', start_date='19900101', symbol='002594' | TimeoutError: stock_industry_change_cninfo timed out after 10s |
| `stock_share_change_cninfo` | 公司股本变动-巨潮资讯 | inaccessible |  | 0 | 3 | end_date='20260524', start_date='19900101', symbol='002594' | TimeoutError: stock_share_change_cninfo timed out after 10s |
| `stock_allotment_cninfo` | 配股实施方案-巨潮资讯 | inaccessible |  | 0 | 3 | end_date='20260524', start_date='19900101', symbol='600030' | TimeoutError: stock_allotment_cninfo timed out after 10s |
| `stock_ipo_summary_cninfo` | 上市相关-巨潮资讯 | inaccessible |  | 0 | 3 | symbol='600030' | TimeoutError: stock_ipo_summary_cninfo timed out after 10s |
| `stock_zcfz_em` | 资产负债表-沪深 | inaccessible |  | 0 | 3 | date='20240331' | TimeoutError: stock_zcfz_em timed out after 10s |
| `stock_zcfz_bj_em` | 资产负债表-北交所 | inaccessible |  | 0 | 3 | date='20240331' | ConnectionError: ('Connection aborted.', ConnectionResetError(10054, '远程主机强迫关闭了一个现有的连接。', None, 10054, None)) |
| `stock_lrb_em` | 利润表 | inaccessible |  | 0 | 3 | date='20240331' | TimeoutError: stock_lrb_em timed out after 10s |
| `stock_xjll_em` | 现金流量表 | inaccessible |  | 0 | 3 | date='20200331' | TimeoutError: stock_xjll_em timed out after 10s |
| `stock_ggcg_em` | 股东增减持 | inaccessible |  | 0 | 3 | symbol='全部' | TimeoutError: stock_ggcg_em timed out after 10s |
| `stock_fhps_em` | 分红配送-东财 | inaccessible |  | 0 | 3 | date='20231231' | TimeoutError: stock_fhps_em timed out after 10s |
| `stock_fhps_detail_em` | 分红配送详情-东财 | inaccessible |  | 0 | 3 | symbol='300073' | TimeoutError: stock_fhps_detail_em timed out after 10s |
| `stock_fhps_detail_ths` | 分红情况-同花顺 | inaccessible |  | 0 | 3 | symbol='603444' | ProxyError: HTTPSConnectionPool(host='basic.10jqka.com.cn', port=443): Max retries exceeded with url: /new/603444/bonus.html (Caused by ProxyError('Unable to connect to proxy', RemoteDisconnected('Remote end clos |
| `stock_hk_fhpx_detail_ths` | 分红配送详情-港股-同花顺 | inaccessible |  | 0 | 3 | symbol='0700' | SSLError: HTTPSConnectionPool(host='basic.10jqka.com.cn', port=443): Max retries exceeded with url: /176/HK0700/bonus.html (Caused by SSLError(SSLEOFError(8, '[SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in |
| `stock_fund_flow_individual` | 个股资金流 | inaccessible |  | 0 | 3 | symbol='即时' | TimeoutError: stock_fund_flow_individual timed out after 10s |
| `stock_individual_fund_flow` | 个股资金流 | inaccessible |  | 0 | 3 | market='sh', stock='000425' | TimeoutError: stock_individual_fund_flow timed out after 10s |
| `stock_individual_fund_flow_rank` | 个股资金流排名 | inaccessible |  | 0 | 3 | indicator='今日' | TimeoutError: stock_individual_fund_flow_rank timed out after 10s |
| `stock_market_fund_flow` | 大盘资金流 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_market_fund_flow timed out after 10s |
| `stock_sector_fund_flow_rank` | 板块资金流排名 | inaccessible |  | 0 | 3 | indicator='今日', sector_type='行业资金流' | JSONDecodeError: Expecting value: line 1 column 1 (char 0) |
| `stock_sector_fund_flow_summary` | 行业个股资金流 | inaccessible |  | 0 | 3 | indicator='今日', symbol='电源设备' | TimeoutError: stock_sector_fund_flow_summary timed out after 10s |
| `stock_sector_fund_flow_hist` | 行业历史资金流 | inaccessible |  | 0 | 3 | symbol='汽车服务' | TimeoutError: stock_sector_fund_flow_hist timed out after 10s |
| `stock_concept_fund_flow_hist` | 概念历史资金流 | inaccessible |  | 0 | 3 | symbol='数据要素' | TimeoutError: stock_concept_fund_flow_hist timed out after 10s |
| `stock_gddh_em` | 股东大会 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_gddh_em timed out after 10s |
| `stock_zdhtmx_em` | 重大合同 | inaccessible |  | 0 | 3 | end_date='20260524', start_date='19900101' | TimeoutError: stock_zdhtmx_em timed out after 10s |
| `stock_research_report_em` | 个股研报 | inaccessible |  | 0 | 3 | symbol='000001' | TimeoutError: stock_research_report_em timed out after 10s |
| `stock_notice_report` | 沪深京 A 股公告 | inaccessible |  | 0 | 3 | date='20220511', symbol='财务报告' | TimeoutError: stock_notice_report timed out after 10s |
| `stock_individual_notice_report` | 沪深京 A 股个股公告 | inaccessible |  | 0 | 3 | security='300237', symbol='财务报告' | TimeoutError: stock_individual_notice_report timed out after 10s |
| `stock_financial_report_sina` | 财务报表-新浪 | inaccessible |  | 0 | 3 | stock='sh600600', symbol='现金流量表' | TimeoutError: stock_financial_report_sina timed out after 10s |
| `stock_balance_sheet_by_report_em` | 资产负债表-按报告期 | inaccessible |  | 0 | 3 | symbol='SH600519' | TimeoutError: stock_balance_sheet_by_report_em timed out after 10s |
| `stock_balance_sheet_by_yearly_em` | 资产负债表-按年度 | inaccessible |  | 0 | 3 | symbol='SH600519' | TimeoutError: stock_balance_sheet_by_yearly_em timed out after 10s |
| `stock_profit_sheet_by_report_em` | 利润表-按报告期 | inaccessible |  | 0 | 3 | symbol='SH600519' | TimeoutError: stock_profit_sheet_by_report_em timed out after 10s |
| `stock_profit_sheet_by_yearly_em` | 利润表-按年度 | inaccessible |  | 0 | 3 | symbol='SH600519' | TimeoutError: stock_profit_sheet_by_yearly_em timed out after 10s |
| `stock_profit_sheet_by_quarterly_em` | 利润表-按单季度 | inaccessible |  | 0 | 3 | symbol='SH600519' | TimeoutError: stock_profit_sheet_by_quarterly_em timed out after 10s |
| `stock_cash_flow_sheet_by_report_em` | 现金流量表-按报告期 | inaccessible |  | 0 | 3 | symbol='SH600519' | TimeoutError: stock_cash_flow_sheet_by_report_em timed out after 10s |
| `stock_cash_flow_sheet_by_yearly_em` | 现金流量表-按年度 | inaccessible |  | 0 | 3 | symbol='SH600519' | TimeoutError: stock_cash_flow_sheet_by_yearly_em timed out after 10s |
| `stock_cash_flow_sheet_by_quarterly_em` | 现金流量表-按单季度 | inaccessible |  | 0 | 3 | symbol='SH600519' | TimeoutError: stock_cash_flow_sheet_by_quarterly_em timed out after 10s |
| `stock_financial_debt_new_ths` | 资产负债表 | inaccessible |  | 0 | 3 | indicator='按报告期', symbol='000063' | TimeoutError: stock_financial_debt_new_ths timed out after 10s |
| `stock_financial_benefit_new_ths` | 利润表 | inaccessible |  | 0 | 3 | indicator='按报告期', symbol='000063' | ConnectionError: ('Connection aborted.', ConnectionResetError(10054, '远程主机强迫关闭了一个现有的连接。', None, 10054, None)) |
| `stock_financial_cash_new_ths` | 现金流量表 | inaccessible |  | 0 | 3 | indicator='按报告期', symbol='000063' | TimeoutError: stock_financial_cash_new_ths timed out after 10s |
| `stock_balance_sheet_by_report_delisted_em` | 资产负债表-按报告期 | inaccessible |  | 0 | 3 | symbol='SZ000013' | TimeoutError: stock_balance_sheet_by_report_delisted_em timed out after 10s |
| `stock_profit_sheet_by_report_delisted_em` | 利润表-按报告期 | inaccessible |  | 0 | 3 | symbol='SZ000013' | TimeoutError: stock_profit_sheet_by_report_delisted_em timed out after 10s |
| `stock_cash_flow_sheet_by_report_delisted_em` | 现金流量表-按报告期 | inaccessible |  | 0 | 3 | symbol='SZ000013' | TimeoutError: stock_cash_flow_sheet_by_report_delisted_em timed out after 10s |
| `stock_financial_hk_report_em` | 港股财务报表 | inaccessible |  | 0 | 3 | indicator='年度', stock='00700', symbol='现金流量表' | TimeoutError: stock_financial_hk_report_em timed out after 10s |
| `stock_financial_us_report_em` | 美股财务报表 | inaccessible |  | 0 | 3 | indicator='年报', stock='TSLA', symbol='资产负债表' | TimeoutError: stock_financial_us_report_em timed out after 10s |
| `stock_financial_abstract` | 关键指标-新浪 | inaccessible |  | 0 | 3 | symbol='600004' | TimeoutError: stock_financial_abstract timed out after 10s |
| `stock_financial_abstract_new_ths` | 关键指标-同花顺 | inaccessible |  | 0 | 3 | indicator='按报告期', symbol='000063' | TimeoutError: stock_financial_abstract_new_ths timed out after 10s |
| `stock_financial_analysis_indicator_em` | 主要指标-东方财富 | inaccessible |  | 0 | 3 | indicator='按报告期', symbol='301389.SZ' | TimeoutError: stock_financial_analysis_indicator_em timed out after 10s |
| `stock_financial_analysis_indicator` | 财务指标 | inaccessible |  | 0 | 3 | start_year='1990', symbol='600004' | TimeoutError: stock_financial_analysis_indicator timed out after 10s |
| `stock_financial_hk_analysis_indicator_em` | 港股财务指标 | inaccessible |  | 0 | 3 | indicator='年度', symbol='00700' | TimeoutError: stock_financial_hk_analysis_indicator_em timed out after 10s |
| `stock_financial_us_analysis_indicator_em` | 美股财务指标 | inaccessible |  | 0 | 3 | indicator='年报', symbol='TSLA' | TimeoutError: stock_financial_us_analysis_indicator_em timed out after 10s |
| `stock_history_dividend` | 历史分红 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_history_dividend timed out after 10s |
| `stock_gdfx_free_top_10_em` | 十大流通股东(个股) | inaccessible |  | 0 | 3 | date='20240930', symbol='sh688686' | TimeoutError: stock_gdfx_free_top_10_em timed out after 10s |
| `stock_gdfx_top_10_em` | 十大股东(个股) | inaccessible |  | 0 | 3 | date='20210630', symbol='sh688686' | TimeoutError: stock_gdfx_top_10_em timed out after 10s |
| `stock_gdfx_free_holding_change_em` | 股东持股变动统计-十大流通股东 | inaccessible |  | 0 | 3 | date='20210930' | TimeoutError: stock_gdfx_free_holding_change_em timed out after 10s |
| `stock_gdfx_holding_change_em` | 股东持股变动统计-十大股东 | inaccessible |  | 0 | 3 | date='20210930' | TimeoutError: stock_gdfx_holding_change_em timed out after 10s |
| `stock_management_change_ths` | 高管持股变动统计 | inaccessible |  | 0 | 3 | symbol='688981' | TimeoutError: stock_management_change_ths timed out after 10s |
| `stock_shareholder_change_ths` | 股东持股变动统计 | inaccessible |  | 0 | 3 | symbol='688981' | TimeoutError: stock_shareholder_change_ths timed out after 10s |
| `stock_gdfx_free_holding_analyse_em` | 股东持股分析-十大流通股东 | inaccessible |  | 0 | 3 | date='20230930' | TimeoutError: stock_gdfx_free_holding_analyse_em timed out after 10s |
| `stock_gdfx_holding_analyse_em` | 股东持股分析-十大股东 | inaccessible |  | 0 | 3 | date='20210930' | ConnectionError: ('Connection aborted.', ConnectionAbortedError(10053, '你的主机中的软件中止了一个已建立的连接。', None, 10053, None)) |
| `stock_gdfx_free_holding_detail_em` | 股东持股明细-十大流通股东 | inaccessible |  | 0 | 3 | date='20210930' | ConnectionError: ('Connection aborted.', ConnectionAbortedError(10053, '你的主机中的软件中止了一个已建立的连接。', None, 10053, None)) |
| `stock_gdfx_holding_detail_em` | 股东持股明细-十大股东 | inaccessible |  | 0 | 3 | date='20230331', indicator='个人', symbol='新进' | TimeoutError: stock_gdfx_holding_detail_em timed out after 10s |
| `stock_gdfx_free_holding_statistics_em` | 股东持股统计-十大流通股东 | inaccessible |  | 0 | 3 | date='20210930' | TimeoutError: stock_gdfx_free_holding_statistics_em timed out after 10s |
| `stock_gdfx_holding_statistics_em` | 股东持股统计-十大股东 | inaccessible |  | 0 | 3 | date='20210930' | TimeoutError: stock_gdfx_holding_statistics_em timed out after 10s |
| `stock_gdfx_free_holding_teamwork_em` | 股东协同-十大流通股东 | inaccessible |  | 0 | 3 | symbol='社保' | TimeoutError: stock_gdfx_free_holding_teamwork_em timed out after 10s |
| `stock_gdfx_holding_teamwork_em` | 股东协同-十大股东 | inaccessible |  | 0 | 3 | symbol='社保' | TimeoutError: stock_gdfx_holding_teamwork_em timed out after 10s |
| `stock_zh_a_gdhs` | 股东户数 | inaccessible |  | 0 | 3 | symbol='20230930' | TimeoutError: stock_zh_a_gdhs timed out after 10s |
| `stock_zh_a_gdhs_detail_em` | 股东户数详情 | inaccessible |  | 0 | 3 | symbol='000001' | TimeoutError: stock_zh_a_gdhs_detail_em timed out after 10s |
| `stock_history_dividend_detail` | 分红配股 | inaccessible |  | 0 | 3 | date='1994-12-24', indicator='配股', symbol='600012' | TimeoutError: stock_history_dividend_detail timed out after 10s |
| `stock_dividend_cninfo` | 历史分红 | inaccessible |  | 0 | 3 | symbol='600009' | TimeoutError: stock_dividend_cninfo timed out after 10s |
| `stock_ipo_info` | 新股发行 | inaccessible |  | 0 | 3 | stock='600004' | TimeoutError: stock_ipo_info timed out after 10s |
| `stock_ipo_review_em` | 新股上会信息 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_ipo_review_em timed out after 10s |
| `stock_add_stock` | 股票增发 | inaccessible |  | 0 | 3 | symbol='600004' | TimeoutError: stock_add_stock timed out after 10s |
| `stock_restricted_release_queue_sina` | 个股限售解禁-新浪 | accessible | 2006-05-09 | 8 | 3 | symbol='600000' |  |
| `stock_restricted_release_detail_em` | 限售股解禁详情 | inaccessible |  | 0 | 3 | end_date='20260524', start_date='19900101' | TimeoutError: stock_restricted_release_detail_em timed out after 10s |
| `stock_restricted_release_queue_em` | 解禁批次 | inaccessible |  | 0 | 3 | symbol='600000' | TimeoutError: stock_restricted_release_queue_em timed out after 10s |
| `stock_restricted_release_stockholder_em` | 解禁股东 | inaccessible |  | 0 | 3 | date='20200904', symbol='600000' | TimeoutError: stock_restricted_release_stockholder_em timed out after 10s |
| `stock_circulate_stock_holder` | 流通股东 | inaccessible |  | 0 | 3 | symbol='600000' | TimeoutError: stock_circulate_stock_holder timed out after 10s |
| `stock_sector_detail` | 板块详情 | accessible |  | 14 | 3 | sector='hangye_ZL01' |  |
| `stock_info_change_name` | 股票更名 | inaccessible |  | 0 | 3 | symbol='000503' | TimeoutError: stock_info_change_name timed out after 10s |
| `stock_info_sz_change_name` | 名称变更-深证 | inaccessible |  | 0 | 3 | symbol='全称变更' | TimeoutError: stock_info_sz_change_name timed out after 10s |
| `stock_fund_stock_holder` | 基金持股 | inaccessible |  | 0 | 3 | symbol='600004' | TimeoutError: stock_fund_stock_holder timed out after 10s |
| `stock_main_stock_holder` | 主要股东 | inaccessible |  | 0 | 3 | stock='600004' | TimeoutError: stock_main_stock_holder timed out after 10s |
| `stock_institute_hold` | 机构持股一览表 | inaccessible |  | 0 | 3 | symbol='20051' | TimeoutError: stock_institute_hold timed out after 10s |
| `stock_institute_hold_detail` | 机构持股详情 | inaccessible |  | 0 | 3 | quarter='20201', stock='300003' | TimeoutError: stock_institute_hold_detail timed out after 10s |
| `stock_rank_forecast_cninfo` | 投资评级 | accessible | 2021-09-10 | 54 | 1 | date='20210910' |  |
| `stock_industry_clf_hist_sw` | 申万个股行业分类变动历史 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_industry_clf_hist_sw timed out after 10s |
| `stock_industry_pe_ratio_cninfo` | 行业市盈率 | inaccessible |  | 0 | 3 | date='20210910', symbol='证监会行业分类' | TimeoutError: stock_industry_pe_ratio_cninfo timed out after 10s |
| `stock_new_gh_cninfo` | 新股过会 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_new_gh_cninfo timed out after 10s |
| `stock_new_ipo_cninfo` | 新股发行 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_new_ipo_cninfo timed out after 10s |
| `stock_hold_num_cninfo` | 股东人数及持股集中度 | inaccessible |  | 0 | 3 | date='20210630' | TimeoutError: stock_hold_num_cninfo timed out after 10s |
| `stock_hold_change_cninfo` | 股本变动 | inaccessible |  | 0 | 3 | symbol='全部' | TimeoutError: stock_hold_change_cninfo timed out after 10s |
| `stock_hold_control_cninfo` | 实际控制人持股变动 | inaccessible |  | 0 | 3 | symbol='全部' | TimeoutError: stock_hold_control_cninfo timed out after 10s |
| `stock_hold_management_detail_cninfo` | 高管持股变动明细 | inaccessible |  | 0 | 3 | symbol='增持' | TimeoutError: stock_hold_management_detail_cninfo timed out after 10s |
| `stock_hold_management_person_em` | 人员增减持股变动明细 | accessible | 2022-06-17 | 4 | 1 | name='吴远', symbol='001308' |  |
| `stock_cg_guarantee_cninfo` | 对外担保 | inaccessible |  | 0 | 3 | end_date='20260524', start_date='19900101', symbol='全部' | TimeoutError: stock_cg_guarantee_cninfo timed out after 10s |
| `stock_cg_lawsuit_cninfo` | 公司诉讼 | inaccessible |  | 0 | 3 | end_date='20260524', start_date='19900101', symbol='全部' | TimeoutError: stock_cg_lawsuit_cninfo timed out after 10s |
| `stock_cg_equity_mortgage_cninfo` | 股权质押 | accessible | 2021-09-30 | 103 | 2 | date='20210930' |  |
| `stock_a_gxl_lg` | A 股股息率 | inaccessible |  | 0 | 3 | symbol='上证A股' | TimeoutError: stock_a_gxl_lg timed out after 10s |
| `stock_hk_gxl_lg` | 恒生指数股息率 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_hk_gxl_lg timed out after 10s |
| `stock_a_congestion_lg` | 大盘拥挤度 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_a_congestion_lg timed out after 10s |
| `stock_ebs_lg` | 股债利差 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_ebs_lg timed out after 10s |
| `stock_market_pe_lg` | 主板市盈率 | inaccessible |  | 0 | 3 | symbol='上证' | TimeoutError: stock_market_pe_lg timed out after 10s |
| `stock_index_pe_lg` | 指数市盈率 | inaccessible |  | 0 | 3 | symbol='上证50' | TimeoutError: stock_index_pe_lg timed out after 10s |
| `stock_market_pb_lg` | 主板市净率 | inaccessible |  | 0 | 3 | symbol='上证' | TimeoutError: stock_market_pb_lg timed out after 10s |
| `stock_index_pb_lg` | 指数市净率 | inaccessible |  | 0 | 3 | symbol='上证50' | TimeoutError: stock_index_pb_lg timed out after 10s |
| `stock_zh_valuation_baidu` | A 股估值指标 | inaccessible |  | 0 | 3 | indicator='总市值', period='近一年', symbol='002044' | TimeoutError: stock_zh_valuation_baidu timed out after 10s |
| `stock_hk_indicator_eniu` | 港股个股指标 | inaccessible |  | 0 | 3 | indicator='港股', symbol='hk01093' | TimeoutError: stock_hk_indicator_eniu timed out after 10s |
| `stock_hk_valuation_baidu` | 港股估值指标 | inaccessible |  | 0 | 3 | indicator='总市值', period='近一年', symbol='02358' | TimeoutError: stock_hk_valuation_baidu timed out after 10s |
| `stock_us_valuation_baidu` | 美股估值指标 | inaccessible |  | 0 | 3 | indicator='总市值', period='近一年', symbol='NVDA' | TimeoutError: stock_us_valuation_baidu timed out after 10s |
| `stock_a_high_low_statistics` | 创新高和新低的股票数量 | inaccessible |  | 0 | 3 | symbol='all' | TimeoutError: stock_a_high_low_statistics timed out after 10s |
| `stock_a_below_net_asset_statistics` | 破净股统计 | inaccessible |  | 0 | 3 | symbol='全部A股' | TimeoutError: stock_a_below_net_asset_statistics timed out after 10s |
| `stock_report_fund_hold` | 基金持股 | inaccessible |  | 0 | 3 | date='20200630', symbol='基金持仓' | TimeoutError: stock_report_fund_hold timed out after 10s |
| `stock_report_fund_hold_detail` | 基金持股明细 | accessible |  | 37 | 1 | date='20200630', symbol='005827' |  |
| `stock_lhb_detail_em` | 龙虎榜详情 | inaccessible |  | 0 | 3 | end_date='20260524', start_date='19900101' | TimeoutError: stock_lhb_detail_em timed out after 10s |
| `stock_lhb_stock_statistic_em` | 个股上榜统计 | inaccessible |  | 0 | 3 | symbol='近一月' | TimeoutError: stock_lhb_stock_statistic_em timed out after 10s |
| `stock_lhb_jgmmtj_em` | 机构买卖每日统计 | inaccessible |  | 0 | 3 | end_date='20260524', start_date='19900101' | TimeoutError: stock_lhb_jgmmtj_em timed out after 10s |
| `stock_lhb_jgstatistic_em` | 机构席位追踪 | inaccessible |  | 0 | 3 | symbol='近一月' | TimeoutError: stock_lhb_jgstatistic_em timed out after 10s |
| `stock_lhb_hyyyb_em` | 每日活跃营业部 | inaccessible |  | 0 | 3 | end_date='20260524', start_date='19900101' | TimeoutError: stock_lhb_hyyyb_em timed out after 10s |
| `stock_lhb_yyb_detail_em` | 营业部详情数据-东财 | inaccessible |  | 0 | 3 | symbol='10026729' | TimeoutError: stock_lhb_yyb_detail_em timed out after 10s |
| `stock_lhb_yybph_em` | 营业部排行 | inaccessible |  | 0 | 3 | symbol='近一月' | TimeoutError: stock_lhb_yybph_em timed out after 10s |
| `stock_lhb_traderstatistic_em` | 营业部统计 | inaccessible |  | 0 | 3 | symbol='近一月' | TimeoutError: stock_lhb_traderstatistic_em timed out after 10s |
| `stock_lhb_stock_detail_em` | 个股龙虎榜详情 | inaccessible |  | 0 | 3 | date='20220310', flag='卖出', symbol='600077' | TimeoutError: stock_lhb_stock_detail_em timed out after 10s |
| `stock_lh_yyb_most` | 龙虎榜-营业部排行-上榜次数最多 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_lh_yyb_most timed out after 10s |
| `stock_lh_yyb_capital` | 龙虎榜-营业部排行-资金实力最强 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_lh_yyb_capital timed out after 10s |
| `stock_lh_yyb_control` | 龙虎榜-营业部排行-抱团操作实力 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_lh_yyb_control timed out after 10s |
| `stock_lhb_detail_daily_sina` | 龙虎榜-每日详情 | inaccessible |  | 0 | 3 | date='20240222' | TimeoutError: stock_lhb_detail_daily_sina timed out after 10s |
| `stock_lhb_ggtj_sina` | 龙虎榜-个股上榜统计 | inaccessible |  | 0 | 3 | symbol='5' | TimeoutError: stock_lhb_ggtj_sina timed out after 10s |
| `stock_lhb_yytj_sina` | 龙虎榜-营业上榜统计 | inaccessible |  | 0 | 3 | symbol='5' | TimeoutError: stock_lhb_yytj_sina timed out after 10s |
| `stock_lhb_jgzz_sina` | 龙虎榜-机构席位追踪 | inaccessible |  | 0 | 3 | symbol='5' | TimeoutError: stock_lhb_jgzz_sina timed out after 10s |
| `stock_lhb_jgmx_sina` | 龙虎榜-机构席位成交明细 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_lhb_jgmx_sina timed out after 10s |
| `stock_ipo_declare_em` | 首发申报信息 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_ipo_declare_em timed out after 10s |
| `stock_register_all_em` | 全部 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_register_all_em timed out after 10s |
| `stock_register_kcb` | 科创板 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_register_kcb timed out after 10s |
| `stock_register_cyb` | 创业板 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_register_cyb timed out after 10s |
| `stock_register_sh` | 上海主板 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_register_sh timed out after 10s |
| `stock_register_sz` | 深圳主板 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_register_sz timed out after 10s |
| `stock_register_bj` | 北交所 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_register_bj timed out after 10s |
| `stock_register_db` | 达标企业 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_register_db timed out after 10s |
| `stock_qbzf_em` | 增发 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_qbzf_em timed out after 10s |
| `stock_pg_em` | 配股 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_pg_em timed out after 10s |
| `stock_repurchase_em` | 股票回购数据 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_repurchase_em timed out after 10s |
| `stock_zh_a_gbjg_em` | 股本结构 | accessible | 2016-06-24 | 13 | 1 | symbol='603392.SH' |  |
| `stock_dzjy_sctj` | 市场统计 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_dzjy_sctj timed out after 10s |
| `stock_dzjy_mrmx` | 每日明细 | inaccessible |  | 0 | 3 | start_date='19900101', symbol='债券' | TimeoutError: stock_dzjy_mrmx timed out after 10s |
| `stock_dzjy_mrtj` | 每日统计 | inaccessible |  | 0 | 3 | start_date='19900101' | TimeoutError: stock_dzjy_mrtj timed out after 10s |
| `stock_dzjy_hygtj` | 活跃 A 股统计 | inaccessible |  | 0 | 3 | symbol='近三月' | TimeoutError: stock_dzjy_hygtj timed out after 10s |
| `stock_dzjy_yybph` | 营业部排行 | inaccessible |  | 0 | 3 | symbol='近三月' | TimeoutError: stock_dzjy_yybph timed out after 10s |
| `stock_yzxdr_em` | 一致行动人 | inaccessible |  | 0 | 3 | date='20200930' | TimeoutError: stock_yzxdr_em timed out after 10s |
| `stock_margin_account_info` | 两融账户信息 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_margin_account_info timed out after 10s |
| `stock_margin_sse` | 融资融券汇总 | inaccessible |  | 0 | 3 | end_date='20260524', start_date='19900101' | TimeoutError: stock_margin_sse timed out after 10s |
| `stock_margin_detail_sse` | 融资融券明细 | inaccessible |  | 0 | 3 | date='20210205' | TimeoutError: stock_margin_detail_sse timed out after 10s |
| `stock_margin_szse` | 融资融券汇总 | inaccessible |  | 0 | 3 | date='20240411' | TimeoutError: stock_margin_szse timed out after 10s |
| `stock_margin_detail_szse` | 融资融券明细 | inaccessible |  | 0 | 3 | date='20220118' | TimeoutError: stock_margin_detail_szse timed out after 10s |
| `stock_hk_profit_forecast_et` | 港股盈利预测-经济通 | inaccessible |  | 0 | 3 | indicator='盈利预测概览', symbol='09999' | TimeoutError: stock_hk_profit_forecast_et timed out after 10s |
| `stock_profit_forecast_ths` | 盈利预测-同花顺 | inaccessible |  | 0 | 3 | indicator='预测年报每股收益', symbol='600519' | TimeoutError: stock_profit_forecast_ths timed out after 10s |
| `stock_board_concept_index_ths` | 同花顺-概念板块指数 | inaccessible |  | 0 | 3 | end_date='20260524', start_date='19900101', symbol='阿里巴巴概念' | TimeoutError: stock_board_concept_index_ths timed out after 10s |
| `stock_board_concept_hist_em` | 东方财富-指数 | inaccessible |  | 0 | 3 | adjust='', end_date='20260524', period='daily', start_date='19900101', symbol='绿色电力' | TimeoutError: stock_board_concept_hist_em timed out after 10s |
| `stock_board_concept_hist_min_em` | 东方财富-指数-分时 | inaccessible |  | 0 | 3 | period='5', symbol='长寿药' | TimeoutError: stock_board_concept_hist_min_em timed out after 10s |
| `stock_board_industry_index_ths` | 同花顺-指数 | inaccessible |  | 0 | 3 | end_date='20260524', start_date='19900101', symbol='元件' | TimeoutError: stock_board_industry_index_ths timed out after 10s |
| `stock_board_industry_cons_em` | 东方财富-成份股 | inaccessible |  | 0 | 3 | symbol='小金属' | TimeoutError: stock_board_industry_cons_em timed out after 10s |
| `stock_board_industry_hist_em` | 东方财富-指数-日频 | inaccessible |  | 0 | 3 | adjust='', end_date='20260524', period='日k', start_date='19900101', symbol='小金属' | TimeoutError: stock_board_industry_hist_em timed out after 10s |
| `stock_board_industry_hist_min_em` | 东方财富-指数-分时 | inaccessible |  | 0 | 3 | period='', symbol='小金属' | TimeoutError: stock_board_industry_hist_min_em timed out after 10s |
| `stock_hot_rank_detail_em` | A股 | inaccessible |  | 0 | 3 | symbol='SZ000665' | TimeoutError: stock_hot_rank_detail_em timed out after 10s |
| `stock_hk_hot_rank_detail_em` | 港股 | accessible | 2026-01-25 | 120 | 2 | symbol='00700' |  |
| `stock_inner_trade_xq` | 内部交易 | inaccessible |  | 0 | 3 |  | SSLError: HTTPSConnectionPool(host='xueqiu.com', port=443): Max retries exceeded with url: /service/v5/stock/f10/cn/skholderchg?size=100000&page=1&extend=true (Caused by SSLError(SSLEOFError(8, '[SSL: UNEXPECTE |
| `stock_hot_rank_latest_em` | A股 | inaccessible |  | 0 | 3 | symbol='SZ000665' | TimeoutError: stock_hot_rank_latest_em timed out after 10s |
| `stock_hk_hot_rank_latest_em` | 港股 | inaccessible |  | 0 | 3 | symbol='00700' | TimeoutError: stock_hk_hot_rank_latest_em timed out after 10s |
| `stock_hot_search_baidu` | 热搜股票 | accessible |  | 12 | 2 | date='20250616', symbol='A股', time='今日' |  |
| `stock_hot_rank_relate_em` | 相关股票 | inaccessible |  | 0 | 3 | symbol='SZ000665' | TimeoutError: stock_hot_rank_relate_em timed out after 10s |
| `stock_changes_em` | 盘口异动 | inaccessible |  | 0 | 3 | symbol='大笔买入' | TimeoutError: stock_changes_em timed out after 10s |
| `stock_zt_pool_em` | 涨停股池 | empty |  | 0 | 3 | date='20241008' | returned empty data |
| `stock_zt_pool_previous_em` | 昨日涨停股池 | inaccessible |  | 0 | 3 | date='20240415' | TimeoutError: stock_zt_pool_previous_em timed out after 10s |
| `stock_zt_pool_strong_em` | 强势股池 | empty |  | 0 | 3 | date='20241009' | returned empty data |
| `stock_zt_pool_sub_new_em` | 次新股池 | inaccessible |  | 0 | 3 | date='20241231' | TimeoutError: stock_zt_pool_sub_new_em timed out after 10s |
| `stock_zt_pool_zbgc_em` | 炸板股池 | inaccessible |  | 0 | 3 | date='20241011' | ValueError: 炸板股池只能获取最近 30 个交易日的数据 |
| `stock_zt_pool_dtgc_em` | 跌停股池 | inaccessible |  | 0 | 3 | date='20241011' | ValueError: 跌停股池只能获取最近 30 个交易日的数据 |
| `stock_info_cjzc_em` | 财经早餐-东财财富 | inaccessible |  | 0 | 3 |  | TimeoutError: stock_info_cjzc_em timed out after 10s |
| `stock_info_global_futu` | 快讯-富途牛牛 | accessible | 2026-05-24 | 50 | 1 |  |  |
| `stock_info_global_ths` | 全球财经直播-同花顺财经 | accessible | 2026-05-24 | 20 | 1 |  |  |
| `stock_rank_cxg_ths` | 创新高 | inaccessible |  | 0 | 3 | symbol='创月新高' | TimeoutError: stock_rank_cxg_ths timed out after 10s |
| `stock_rank_cxd_ths` | 创新低 | inaccessible |  | 0 | 3 | symbol='创月新低' | TimeoutError: stock_rank_cxd_ths timed out after 10s |
| `stock_rank_xzjp_ths` | 险资举牌 | accessible | 2025-12-11 | 16 | 1 |  |  |
