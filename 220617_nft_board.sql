-- BAYC: 0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d
-- lower({{token_address}})



/*****************************************************************/
/*********************   part0 nft summary   *********************/
/*****************************************************************/

-- dw.drpt_nft_general_ranking_di (WIP)
select a.token_address
	,c.opensea_name
	,a.total_value
	,a.total_value * COALESCE(b.low_eth_price,c.floor_price) as market_cap
	,COALESCE(b.low_eth_price,c.floor_price) as floor_price
	,a.holders
	,coalesce(d.nft_start_ts,c.first_mint_time) as start_ts
	,concat(DATEDIFF(NOW(),d.nft_start_ts),'days ago') as deloyed
	,e.trade_nft_num_7d
	,e.trade_eth_vol_7d
	,e.avg_trade_eth_amt_7d
	,e.trade_nft_num_24h
	,e.trade_eth_vol_24h
	,e.avg_trade_eth_amt_24h
from
(
	SELECT token_address
		,sum(`value`) as total_value
		,count(distinct token_id) as token_cnt
		,count(distinct account_address) as holders
	FROM dw.dws_nft_balance_eth
	WHERE `value` > 0
	GROUP BY 1
) AS a
left join dw.dwb_nft_price_eth_byday_hi as b
on a.token_address = b.token_address
and b.dt = DATE_SUB(DATE(now()),1)
left join dw.dwb_nft_opensea_detail_di as c
on a.token_address = c.token_address
left join
(
	select token_address
		,min(ts) as nft_start_ts
	from dw.dwb_nft_transfer_detail_eth_hi
	group by 1
) as d
on a.token_address = d.token_address
left join
(
	SELECT token_address
		,sum(token_num) as trade_nft_num_7d
		,sum(buyer_pay_amount) as trade_eth_vol_7d
		,1.0*sum(buyer_pay_amount)/sum(token_num) as avg_trade_eth_amt_7d
		,sum(case when trade_time >= DATE_SUB(date(now()),1) then token_num else 0 end) as trade_nft_num_24h
		,sum(case when trade_time >= DATE_SUB(date(now()),1) then buyer_pay_amount else 0 end) as trade_eth_vol_24h
		,1.0*sum(case when trade_time >= DATE_SUB(date(now()),1) then token_num else 0 end)/sum(case when trade_time >= DATE_SUB(date(now()),1) then buyer_pay_amount else 0 end) as avg_trade_eth_amt_24h
	FROM dw.dwb_nft_trade_eth_detail_hi
	WHERE trade_time >= DATE_SUB(date(now()),7)
	GROUP BY 1
) as e
on a.token_address = e.token_address




/*****************************************************************/
/*******************   part1 nft basic info   ********************/
/*****************************************************************/






/*****************************************************************/
/**********************   part2 nft trade   **********************/
/*****************************************************************/





/*****************************************************************/
/*********************   part3 nft holding   *********************/
/*****************************************************************/


