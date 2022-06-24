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
from dw.dim_nft_tokens
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

-- daily floor price and trade volume
select dt,low_eth_price,trading_eth_volume
from dw.dwb_nft_price_eth_byday_hi
where dt >= DATE_SUB(now(),30)
and token_address = lower({{nft_address}})


-- Top NFT Holder Balance Coverage
select rnk
    ,sum(cum_bal)/max(total_bal) as cum_bal
from
(
    select account_address,hold_value,rnk,cum_bal
    	,sum(hold_value) over() as total_bal
    from
    (
    	select account_address
    		,cast(sum(cast(`value` as LARGEINT)) as bigint) as hold_value
    		,row_number() over(order by sum(cast(`value` as LARGEINT)) desc) as rnk
    	    ,cast(sum(sum(cast(`value` as LARGEINT))) over(
        	        order by sum(cast(`value` as LARGEINT)) desc 
        	        Rows Between Unbounded Preceding and Current Row)
    	     as bigint) as cum_bal
    	from dw.dws_nft_balance_eth
    	where token_address = lower({{nft_address}})
    	and `value` > 0
    	and account_address not in (
        	'0x0000000000000000000000000000000000000000',
        	'0x000000000000000000000000000000000000dead')
    	group by 1
    ) as a
) as t
group by 1
order by 1 asc


-- solder address vs buyin address
select DATE(trade_time) as trade_dt
	,count(distinct transaction_hash) as txn_cnt
	,count(distinct from_address) as solders
	,count(distinct to_address) as buyers
from dw.dwb_nft_trade_eth_detail_hi
where trade_time >= DATE_SUB(now(),30)
and token_address = lower({{nft_address}})
group by 1
order by 1 asc


-- NFT holding Pieces and Days
select avg_holding_days,holding_pieces
    ,count(*) as account_cnt
from
(
    select account_address
        ,round(avg(value*datediff(now(),last_transfer_in))) as avg_holding_days
        ,sum(value) as holding_pieces
    from dw.dws_nft_balance_eth
    where token_address = lower({{token_address}})
    and account_address not in (
    	'0x0000000000000000000000000000000000000000',
    	'0x000000000000000000000000000000000000dead')
    and value > 0
    group by 1
) as n
group by 1,2


-- trade profit log
with nft_sell_log as (
    select from_address as account_address
        ,token_address
        ,token_ids
        ,token_num
        ,trade_eth_value
        ,buyer_pay_amount,seller_receive_amount
        ,currency_symbol
        ,null as cost, null as gas_value
        ,trade_time
        ,transaction_hash
        ,ROW_NUMBER() OVER (PARTITION BY token_address, token_ids ORDER BY trade_time asc) AS sell_rn
    from dw.dwb_nft_trade_eth_detail_hi
    where token_address = lower({{account_address}})
)
,nft_buy_log as (
    select to_address as account_address
        ,token_address
        ,token_ids
        ,token_num
        ,trade_eth_value
        ,buyer_pay_amount,seller_receive_amount
        ,currency_symbol
        ,null as cost, null as gas_value
        ,trade_time
        ,transaction_hash
        ,ROW_NUMBER() OVER (PARTITION BY token_address, token_ids ORDER BY trade_time asc) AS buy_rn
    from dw.dwb_nft_trade_eth_detail_hi
    where token_address = lower({{account_address}})
)
SELECT s.token_address
    ,s.token_ids
    ,s.token_num
    ,ifnull(coalesce(b.buyer_pay_amount,m.cost+m.gas_value), 0) as bought_price
    ,s.buyer_pay_amount as sale_price
    ,s.buyer_pay_amount - ifnull(coalesce(b.buyer_pay_amount,m.cost+m.gas_value), 0) as profit
    ,coalesce(b.trade_time,m.ts) as bought_time
    ,coalesce(b.transaction_hash,m.transaction_hash) as bought_txn
    ,coalesce(b.account_address,m.account_address) as bought_account_address
    ,s.trade_time as sale_time
    ,s.transaction_hash as sale_txn
    ,s.account_address as sale_account_address
    ,case when b.transaction_hash is not null and b.account_address = s.account_address then 'buy'
            when b.transaction_hash is not null and b.account_address <> s.account_address then 'OTC buy'
            when m.transaction_hash is not null and m.account_address = s.account_address then 'mint'
            when m.transaction_hash is not null and m.account_address <> s.account_address then 'master mint'
            else 'transfer' end as buy_type
from nft_sell_log as s
left join nft_buy_log as b
ON b.token_address = s.token_address
AND b.token_ids = s.token_ids
AND b.buy_rn = s.sell_rn - 1
left join 
(
    select account_address,token_address,token_id
        ,mint_time as ts,block_number,transaction_hash
        ,mint_amount as `value`,gas_cost as gas_value,mint_cost as cost
    from dw.dwb_nft_mint_detail_eth_hi
    where token_address = lower({{account_address}})
)as m
on s.token_address = m.token_address
and s.token_ids = m.token_id
[[
where s.account_address = lower({{acct}})
or b.account_address = lower({{acct}})
or m.account_address = lower({{acct}})
]]
order by sale_time desc


/*****************************************************************/
/*********************   part3 nft holding   *********************/
/*****************************************************************/

-- NFT holder 7d Activities
select b.token_address
	,max(opensea_name) as opensea_name
	,max(create_time) as nft_creat_time
	,count(distinct a.account_address) as txn_acct_cnt
	,sum(bought_value) as bought_value_7d
	,sum(sold_value) as sold_value_7d
	,sum(mint_value) as mint_value_7d
from
(
	select account_address
		,sum(`value`) as hold_value
	from dw.dws_nft_balance_eth
	where token_address = lower({{nft_address}})
	and value > 0
	and account_address not in (
	'0x0000000000000000000000000000000000000000',
	'0x000000000000000000000000000000000000dead')
	group by 1
) as a
inner join
(
	select account_address,aa.token_address
	    ,max(bb.opensea_name) as opensea_name
	    ,max(bb.create_time) as create_time
	    ,sum(abs(value)) as trade_vol
	    ,sum(value) as net_value
		,sum(case when value>0 and is_mint = 0 then value else 0 end) as bought_value
		,sum(case when value<0 then value else 0 end) as sold_value
		,sum(case when is_mint >= 1 then value else 0 end) as mint_value
	from dw.dwb_nft_transfer_detail_eth_hi as aa
	left join dw.dim_nft_tokens as bb
    on aa.token_address = bb.token_address
    where ts > DATE_SUB(now(),7)
	and aa.token_address <> lower({{nft_address}})
	group by 1,2
) as b
on a.account_address = b.account_address
group by 1
order by txn_acct_cnt desc


-- nft holder high concentration projects
select a.token_address
	,max(opensea_name) as opensea_name
	,max(create_time) as create_time
	,max(seven_day_volume_rk) as seven_day_volume_rk
	,count(a.account_address) as holder_cnt
	,count(b.account_address) as intersection_holder_cnt
	,1.0*count(b.account_address)/count(a.account_address) as holder_concentration
	,cast(sum(a.balance_value) as bigint) as supply_cnt
	,cast(sum(case when b.account_address is not null then balance_value else 0 end) as bigint) as intersection_supply_cnt
	,1.0*sum(case when b.account_address is not null then balance_value else 0 end)/sum(a.balance_value) as supply_concentration
from
(
    select aa.account_address
        ,aa.token_address
		,max(bb.opensea_name) as opensea_name
		,max(bb.create_time) as create_time
		,max(bb.seven_day_volume_rk) as seven_day_volume_rk
        ,sum(cast(aa.`value` as largeint)) as balance_value
    from dw.dws_nft_balance_eth as aa
    left join dw.dwm_nft_detail_ha as bb
    on aa.token_address = bb.token_address
    where aa.`value` > 0
		and aa.token_address != lower({{nft_address}})
		and account_address not in (
    	'0x0000000000000000000000000000000000000000',
    	'0x000000000000000000000000000000000000dead')
    group by 1,2
) as a
left join
(
	select account_address
		,sum(cast(`value` as largeint)) as hold_value
	from dw.dws_nft_balance_eth
	where token_address = lower({{nft_address}})
	and value > 0
	and account_address not in (
    	'0x0000000000000000000000000000000000000000',
    	'0x000000000000000000000000000000000000dead')
	group by 1
) as b
on a.account_address = b.account_address
group by 1
having max(seven_day_volume_rk) < 5000
and count(a.account_address) > 100
order by holder_concentration+supply_concentration desc