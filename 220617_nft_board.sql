-- BAYC: 0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d
-- lower({{token_address}})



/*****************************************************************/
/*********************   part0 nft summary   *********************/
/*****************************************************************/

-- insert into dw.dwrpt_nft_general_metrics_di
select a.token_address
	,c.opensea_name
    ,c.seven_day_volume_rk
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
left join dw.dwm_nft_detail_ha as c
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

-- opensea name
select opensea_name
from dw.dwb_nft_opensea_detail_di
where token_address = lower({{nft_address}})


-- realtime floor price
SELECT low_eth_price,dt
FROM (
    SELECT dt, token_address
		,COUNT(*) as trading_count
		,MIN(nft_avg_eth_price) as low_eth_price
        , SUM(if(eth_price_rn IN (floor(n / 2) + 1, if(mod(n, 2) = 0, floor(n / 2), floor(n / 2) + 1)), nft_avg_eth_price, 0)) AS lc
        , SUM(if(eth_price_rn IN (floor(n / 2) + 1, if(mod(n, 2) = 0, floor(n / 2), floor(n / 2) + 1)), 1, 0)) AS ln
    FROM 
    (
        SELECT token_address, DATE(trade_time) AS dt, nft_avg_eth_price
            , row_number() OVER (PARTITION BY DATE(trade_time), token_address ORDER BY nft_avg_eth_price) AS eth_price_rn
            , COUNT() OVER (PARTITION BY DATE(trade_time), token_address ) AS n
        FROM 
		(
			select block_number,log_index,transaction_hash,trade_time
				,from_address,to_address,token_address,token_ids,token_num
				,currency_address
				,buyer_pay_amount,nft_avg_eth_price
			from dw.dwb_nft_trade_eth_detail_hi
			where currency_address IN ('0xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxeth', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
			and trade_time>= DATE_ADD(DAY, -3, now())
			and token_address = lower({{nft_address}})

			union all 
			SELECT o.block_number,o.log_index,o.transaction_hash,FROM_UNIXTIME(b.`timestamp`) AS trade_time
				, seller AS from_address, buyer AS to_address, o.nft_contract_address AS token_address
				, nft_token_id AS token_ids, nft_token_num as token_num
				,currency_contract as currency_address
				,buyer_pay_amount / POWER(10, t.decimals) as buyer_pay_amount
				,buyer_pay_eth_amount/nft_token_num as nft_avg_eth_price
			FROM
			(
				SELECT block_number,log_index,transaction_hash
					,seller,buyer,nft_contract_address,nft_token_id,nft_token_num
					,currency_contract,buyer_pay_amount
					,case when currency_contract in ('0xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxeth','0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
						then buyer_pay_amount/POWER(10,18) ELSE NULL END AS buyer_pay_eth_amount
				FROM prod.nft_order_eth
				WHERE trade_type != 'Empty Trade'
					AND nft_contract_address != ''
					AND buyer_pay_amount !=0
					AND buyer != ''
					AND seller != ''
					AND block_number > (SELECT MAX(block_number) from dw.dwb_nft_trade_eth_detail_hi)
					and nft_contract_address = lower({{nft_address}})
			) as o
			LEFT JOIN prod.blocks as b
			ON o.block_number = b.block_number
			AND b.chain = 'eth'
			LEFT JOIN prod.token t
			ON t.chain = 'eth'
			AND o.currency_contract = t.address
		) as drv
        WHERE currency_address IN (
            '0xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxeth', 
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
        AND trade_time>= DATE_ADD(DAY, -3, now())
     ) t
     WHERE (eth_price_rn/n > 0.01 AND eth_price_rn/n < 0.99) OR (n<100)
     GROUP BY dt, token_address
) r
ORDER BY dt DESC


-- holders
select count(distinct account_address) as account_cnt
from dw.dws_nft_balance_eth
where token_address = lower({{nft_adddress}})
and `value` > 0


-- supplies
select sum(`value`) as nft_supplies
from dw.dws_nft_balance_eth
where token_address = lower({{nft_adddress}})
and `value` > 0


-- deployed
select create_time
from dw.dwm_nft_detail_ha
where token_address = lower({{nft_address}})


/*****************************************************************/
/**********************   part2 nft trade   **********************/
/*****************************************************************/





/*****************************************************************/
/*********************   part3 nft holding   *********************/
/*****************************************************************/


