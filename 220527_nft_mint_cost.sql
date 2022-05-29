-- about nft mint txn
-- https://etherscan.io/tx/0x1f2e4f1bd08de2abab5e41bb59c18a8f5365c557ff1648f71b97e86ea1b2b99b
-- 1. mint可能和普通划转合并一起打包进一个transaction HASH
-- 2. 一次转账会变成两条记录存入dw.dwb_nft_transfer_detail_eth_hi

/*****************************************************************/
/**********   part10 account nft mint log   ***********/
/*****************************************************************/
with mint_txn as 
(
	select id,account_address,token_address,token_id,value,gas_value,cost,is_mint,ts
	from dw.dwb_nft_transfer_detail_eth_hi
	where id in (
		select id
		from dw.dwb_nft_transfer_detail_eth_hi
		where account_address = lower({{account_address}})
		and is_mint = 1
		group by 1
	)
)
select a.id as transaction_hash
        ,a.token_address
        ,a.token_id
        ,a.`value`
        ,a.ts
        ,b.cost
        ,b.gas_value
        ,a.cost as total_txn_cost
        ,a.gas_value as total_txn_gas_value
        ,b.n/2 as sub_txn_cnt
from mint_txn as a
left join
(
	select id
		,count(*) as n
		,2*max(cost)/count(*) as cost
		,2*max(gas_value)/count(*) as gas_value
	from mint_txn
	group by 1
) as b
on a.id = b.id
where a.account_address = lower({{account_address}})
and a.is_mint = 1
order by ts desc




/*****************************************************************/
/**********   part1 account nft trade log sell & buy   ***********/
/*****************************************************************/
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
                ,ROW_NUMBER() OVER (PARTITION BY from_address, token_address, token_ids ORDER BY trade_time) AS sell_rn
        from dw.dwb_nft_trade_eth_detail_hi
        where from_address = lower({{account_address}})
)
,nft_buy_log as (
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
                ,ROW_NUMBER() OVER (PARTITION BY from_address, token_address, token_ids ORDER BY trade_time) AS buy_rn
        from dw.dwb_nft_trade_eth_detail_hi
        where from_address = lower({{account_address}})
)
SELECT s.token_address
        ,s.token_ids
        ,s.token_num
        ,s.transaction_hash as sell_txn
        ,b.transaction_hash as buy_txn
        ,s.trade_time as sell_time
        ,b.trade_time as buy_time
        ,s.buyer_pay_amount as sell_price
        ,b.buyer_pay_amount as buy_price
from nft_sell_log as s
left join nft_buy_log as b
ON b.account_address = s.account_address
AND b.token_address = s.token_address
AND b.token_ids = s.token_ids
AND b.buy_rn = s.sell_rn






/*****************************************************************/
/***************   part2 account nft trade log   *****************/
/*****************************************************************/
select a.account_address
        ,a.token_address,a.token_ids,a.token_num
        ,b.contract_name
        ,a.trade_type
        ,a.trade_eth_value,buyer_pay_amount,seller_receive_amount,a.currency_symbol
        ,a.cost,a.gas_value
        ,a.trade_time,a.transaction_hash
from
(
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
            ,'sold' as trade_type
    from dw.dwb_nft_trade_eth_detail_hi
    where from_address = lower({{account_address}})

    union all
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
            ,'buy' as trade_type
    from dw.dwb_nft_trade_eth_detail_hi
    where to_address = lower({{account_address}})
    
    union all
    select account_address
        ,token_address
        ,token_id as token_ids
        ,`value` as token_num
        ,null as trade_eth_value
        ,null as buyer_pay_amount
        ,null as seller_receive_amount
        ,'ETH' as currency_symbol
        ,cost,gas_value
        ,ts as trade_time
        ,id as transaction_hash
        ,'mint' as trade_type
    from dw.dwb_nft_transfer_detail_eth_hi
	where is_mint = 1
	and account_address = lower({{account_address}})
) as a
left join dwb_nft_opensea_detail_di as b
on a.token_address = b.token_address
order by trade_time desc




/*****************************************************************/
/***********   part3 account nft profit calculation   ************/
/*****************************************************************/

with drv as (
	-- partition date driver table
	select dt as partition_date
	from dim_date_di
	where dt between DATE_SUB(date(now()),30) and date(now())
)
,account_nft_trade_log as (
	select from_address as account_address
					,token_address
					,token_ids
					,token_num
					,-1.0*trade_eth_value as trade_eth_value
					,currency_symbol
					,trade_time
					,transaction_hash
					,'sold' as trade_type
	from dw.dwb_nft_trade_eth_detail_hi
	where from_address = lower({{account_address}})

	union all
	select to_address as account_address
					,token_address
					,token_ids
					,token_num
					,trade_eth_value as trade_eth_value
					,currency_symbol
					,trade_time
					,transaction_hash
					,'buy' as trade_type
	from dw.dwb_nft_trade_eth_detail_hi
	where to_address = lower({{account_address}})
)
,account_nft_trade_sum as (
	select account_address
					,partition_date
					,total_traded_eth
					,sum(total_traded_eth) over(order by partition_date asc) as cum_nft_cost 
	from
	(
		SELECT account_address
					,date(trade_time) as partition_date
					,sum(trade_eth_value) as total_traded_eth
		FROM account_nft_trade_log
		group by 1,2
	) as a
)
,account_nft_cost_res as (
	select account_address,partition_date,cum_nft_cost
	from
	(
		SELECT a.partition_date
					,b.account_address
					,b.cum_nft_cost
					,row_number() over(partition by a.partition_date order by b.partition_date desc) as rnk
		FROM drv as a
		,account_nft_trade_sum as b
		where a.partition_date >= b.partition_date
	) as t
	where rnk = 1
)
,account_nft_mint_log as (
        select 
        from 



)




,account_nft_bal_drv as (
	-- account balance history, account + start_at level
	select *
	from dw.dws_nft_balance_history_eth_dil
	where account_address = lower({{account_address}})
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
,account_nft_bal_res as (
	select a.account_address,a.partition_date
				,sum(`value`*low_eth_price) as nft_eth_bal
				,sum(`value`*low_usd_price) as nft_usd_bal
	from account_nft_bal as a
	join dw.dwb_nft_price_eth_byday_hi as b
	on a.token_address = b.token_address
	and a.partition_date = b.dt
	group by 1,2
)
select a.account_address
		,a.partition_date
		,a.nft_eth_bal
-- 		,a.nft_usd_bal
-- 		,b.cum_nft_cost as nft_eth_cost
		,a.nft_eth_bal - coalesce(b.cum_nft_cost,0) as nft_eth_profit
from account_nft_bal_res as a
left join account_nft_cost_res as b
on a.partition_date = b.partition_date






-- sold log

select a.token_address,a.token_ids,a.token_num
        ,b.contract_name
        ,a.trade_type
        ,a.trade_eth_value,buyer_pay_amount,seller_receive_amount,a.currency_symbol
        ,a.trade_time,a.transaction_hash
from
(
    select from_address as account_address
            ,token_address
            ,token_ids
            ,token_num
            ,trade_eth_value
            ,buyer_pay_amount,seller_receive_amount
            ,currency_symbol
            ,trade_time
            ,transaction_hash
            ,'sold' as trade_type
    from dw.dwb_nft_trade_eth_detail_hi
    where from_address = lower({{account_address}})
    and trade_type ='Single Trade'
) as a
left join
(
    select to_address as account_address
            ,token_address
            ,token_ids
            ,token_num
            ,trade_eth_value
            ,buyer_pay_amount,seller_receive_amount
            ,currency_symbol
            ,trade_time
            ,transaction_hash
            ,'buy' as trade_type
            , ROW_NUMBER() OVER (PARTITION BY to_address, d.token_address, token_ids ORDER BY trade_time) AS sell_rn
    from dw.dwb_nft_trade_eth_detail_hi
    where to_address = lower({{account_address}})
    and trade_type ='Single Trade'
) as b
ON b.account_address = s.account_address
AND b.token_address = s.token_address
AND b.token_ids = s.token_ids
AND b.buy_rn = s.sell_rn

left join dwb_nft_opensea_detail_di as b
on a.token_address = b.token_address
order by trade_time desc


