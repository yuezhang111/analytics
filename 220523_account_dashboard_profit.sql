-- sample account  '0x00000000034b55ebd82cde9b38a85ab0978b7a47'
-- dashboard setting  lower({{account_address}})


/*****************************************************************/
/*********   part1 account token balance calculation   ***********/
/*****************************************************************/

-- token balance trend
with drv as (
	-- partition date driver table
	select dt as partition_date
	from dim_date_di
	where dt between DATE_SUB(date(now()),30) and date(now())
)
,account_bal_drv as (
	-- account balance history, account + start_at level
	select *
	from dw.dws_token_balance_history_eth_dil
	where account_address = '0x00000000034b55ebd82cde9b38a85ab0978b7a47'
	and end_at > DATE_SUB(date(now()),30)
)
,account_token_bal as (
	-- account balance snapshot
	select a.partition_date
			,b.account_address
			,b.token_address
			,b.symbol
			,b.balance
			,b.start_at
			,b.end_at
	from drv as a
	,account_bal_drv as b
	where a.partition_date >= b.start_at
	and a.partition_date < b.end_at
	and b.balance > 0
)
,account_bal_res as (
	select a.partition_date
			,a.account_address
			,sum(balance * price) as usd_balance
	from account_token_bal as a
	left join dw.dwb_token_price_eth_byday_di as b
	on a.token_address = b.token_address
	and a.partition_date = b.dt
	and b.dt >= DATE_SUB(date(now()),30)
	group by 1,2
)
,account_cost_drv as (
	select account_address
			,partition_date
			,cost
			,sum(cost) over(partition by account_address order by partition_date ASC) as total_cost
	FROM
	(
		select account_address
				,DATE(ts) as partition_date
				,sum(`value`*price) as cost
		from dw.dwb_token_transfer_detail_eth_hi
		where (id like 'transfer__%' or id like 'trace__%')
		and account_address = lower({{account_address}})
		group by 1,2
	) as t
	order by partition_date DESC
	limit 30
)
,account_cost_res as (
	select account_address,partition_date,total_cost as usd_cost
	from
	(
	select b.account_address
				,a.partition_date
				,b.total_cost
				,row_number() over(partition by a.partition_date order by b.partition_date desc) as rnk
	from drv as a
	,account_cost_drv as b
	where a.partition_date >= b.partition_date
	) as t
	where rnk = 1
)
select a.account_address
		,a.partition_date
		,a.usd_balance
		,b.usd_cost
		,a.usd_balance-b.usd_cost as usd_profit
		,c.price as eth_price
from account_bal_res as a
left join account_cost_res as b
on a.partition_date = b.partition_date
left join 
(
	select token_address,dt,symbol,price
	from dw.dwb_token_price_eth_byday_di
	where token_address = '0xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxeth'
	and dt >= DATE_SUB(date(now()),30)
) as c
on a.partition_date = c.dt
;













/**************************************************************************/
/*********   part1 account nft balance calculation (only sold)  ***********/
/**************************************************************************/

with drv as (
	select dt as partition_date
	from dim_date_di
	where dt between DATE_SUB(date(now()),30) and date(now())
)
,nft_profit_log as (
	SELECT b.account_address
			, b.token_address
			, b.trade_time AS buy_time
			, s.trade_time AS sell_time
			, b.buyer_pay_amount AS buy_price
			, s.buyer_pay_amount AS sell_price
			, s.buyer_pay_amount - b.buyer_pay_amount AS profit
	FROM
	(
			-- 所有买入
			SELECT to_address AS account_address, buyer_pay_amount, token_address, token_ids, trade_time
					 , ROW_NUMBER() OVER (PARTITION BY to_address, token_address, token_ids ORDER BY trade_time) AS buy_rn
			FROM dw.dwb_nft_trade_eth_detail_hi
			where to_address = '0x00000000034b55ebd82cde9b38a85ab0978b7a47'
	) b
	JOIN
	(
			 -- 所有卖出
			 SELECT from_address AS account_address, buyer_pay_amount, token_address, token_ids, trade_time
						, ROW_NUMBER() OVER (PARTITION BY to_address, token_address, token_ids ORDER BY trade_time) AS sell_rn
			 FROM dw.dwb_nft_trade_eth_detail_hi
			 where from_address = '0x00000000034b55ebd82cde9b38a85ab0978b7a47'
	) s
	ON b.account_address = s.account_address
	AND b.token_address = s.token_address
	AND b.token_ids = s.token_ids
	AND b.buy_rn = s.sell_rn
)
,account_nft_profit as (
	select account_address,partition_date,profit
				,sum(profit) over(partition by account_address order by partition_date asc) as cum_profit
	from
	(
		select account_address,date(sell_time) as partition_date
					,sum(profit) as profit
		from nft_profit_log
		group by 1,2
	) as a
)
select drv.partition_date
			,a.account_address
			,max(a.cum_profit) as cum_profit
from drv,
( 
	select account_address,partition_date,profit,cum_profit
	from account_nft_profit
	order by partition_date DESC
	limit 30
) as a
where drv.partition_date >= a.partition_date
group by 1,2
order by 1




/**************************************************************************/
/************   part1 account nft balance calculation (hold)  *************/
/**************************************************************************/

with drv as (
	-- partition date driver table
	select dt as partition_date
	from dim_date_di
	where dt between DATE_SUB(date(now()),30) and date(now())
)
,account_nft_bal_drv as (
	-- account balance history, account + start_at level
	select *
	from dw.dws_nft_balance_history_eth_dil
	where account_address = '0x00000000034b55ebd82cde9b38a85ab0978b7a47'
	and end_at > DATE_SUB(date(now()),30)
)
,account_nft_bal as (
	-- account balance snapshot
	select a.partition_date
			,b.account_address
			,b.token_address
			,b.`value`
			,b.start_at
			,b.end_at
	from drv as a
	,account_nft_bal_drv as b
	where a.partition_date >= b.start_at
	and a.partition_date < b.end_at
    and `value` > 0
)
select a.account_address,a.partition_date
			,sum(`value`*low_eth_price) as nft_eth_bal
			,sum(`value`*low_usd_price) as nft_usd_bal
from account_nft_bal as a
join dw.dwb_nft_price_eth_byday_hi as b
on a.token_address = b.token_address
and a.partition_date = b.dt
group by 1,2