-- APE Coin: 0x4d224452801aced8b2f0aebe155379bb5d594381
-- lower({{token_address}})



/*****************************************************************/
/**********************   part0 token rank   *********************/
/*****************************************************************/

with token_bal_drv as (
	select token_address
		,sum(`value`) as total_token_bal
		,sum(usd_value) as total_token_bal_usd
	from dw.dws_token_balance_eth_usd_di
	where account_address not in (
	'0x0000000000000000000000000000000000000000',
	'0x000000000000000000000000000000000000dead'
	)
	group by 1
)
,token_trade_vol_drv as (
	select token_address
		,sum(abs(value))/2 as trade_vol_24h
		,sum(abs(value)*price)/2 as trade_usd_vol_24h
	from dw.dwb_token_transfer_detail_eth_hi
	where ts >= date_sub(now(),1)
	and account_address not in (
	'0x0000000000000000000000000000000000000000',
	'0x000000000000000000000000000000000000dead'
	)
	group by 1
)
select a.token_address
	,b.`name`
	,b.symbol
	,b.current_price
	,c.trade_vol_24h
	,c.trade_usd_vol_24h
	,b.last_updated_ts
	,a.total_token_bal
	,a.total_token_bal_usd
	,b.change_rate_24h
	,b.change_rate_7d
from token_bal_drv as a
inner join dw.dm_token_metrics_eth_di as b
on a.token_address = b.token_address
inner join token_trade_vol_drv as c
on a.token_address = c.token_address
where date(last_updated_ts) >= DATE_SUB(now(),1)
order by trade_usd_vol_24h desc




/*****************************************************************/
/*************************   part1 info   ************************/
/*****************************************************************/

-- symbol
SELECT symbol
FROM prod.token
where token_address = lower({{token_address}})
;

-- usd total balance
select sum(usd_value) as usd_value
from dw.dws_token_balance_eth_usd_di
where token_address = lower({{token_address}})
and account_address not in (
	'0x0000000000000000000000000000000000000000',
	'0x000000000000000000000000000000000000dead')

-- token holder cnt
select count(distinct account_address) as holder_cnt
from
(
    select account_address,start_at,end_at,balance
    from dw.dws_token_balance_history_eth_dil
    where token_address = lower({{token_address}})
    and end_blo = 999999999
    and balance > power(10,-10)
) as a
left join prod.contracts c
on a.account_address = c.address
and c.chain='eth'
where c.address is null

-- token current price
SELECT price
FROM dw.dwb_token_price_eth_hil
WHERE end_at = 999999999
and address = lower({{token_address}})

-- token current price update ts
SELECT start_ts
FROM dw.dwb_token_price_eth_hil
WHERE end_at = 999999999
and address = lower({{token_address}})

-- token historic max price
SELECT max(price) as max_price
FROM dw.dwb_token_price_eth_hil
WHERE address = lower({{token_address}})


/*****************************************************************/
/*************************   part1 price   ***********************/
/*****************************************************************/

-- trend & volume
select a.dt,a.price,b.trade_vol
from
(
    select dt,price
    from dw.dwb_token_price_eth_byday_di
    where token_address = '0x40e0a6ef9dbadfc83c5e0d15262feb4638588d77'
    and dt >= date_sub(date(now()),90)
) as a
left join 
(
	select date(ts) as dt
		,sum(abs(value))/2 as trade_vol
		,sum(abs(value)*price)/2 as trade_usd_vol
	from dw.dwb_token_transfer_detail_eth_hi
	where ts >= date_sub(now(),90)
	and account_address not in (
	'0x0000000000000000000000000000000000000000',
	'0x000000000000000000000000000000000000dead'
	)
    and token_address = '0x4d224452801aced8b2f0aebe155379bb5d594381'
	group by 1
) as b
on a.dt = b.dt


-- Balance Distribution
select account_address
    ,`value`
    -- ,usd_value
    -- ,sum(`value`) over() as total_value
    -- ,`value` / sum(`value`) over() as bal_pct
from dw.dws_token_balance_eth_usd_di
where token_address = lower({{token_address}})
order by `value` desc


-- Holder Cnt Trend
with dt_drv as (
	select dt as partition_date
	from dim_date_di
	where dt between DATE_SUB(date(now()),90) and date(now())
)
,token_bal_drv as (
    select a.account_address,a.start_at,a.end_at,a.balance
        ,if(c.address is null,0,1) as is_contract
    from
    (
        select account_address,start_at,end_at,balance
    	from dw.dws_token_balance_history_eth_dil
    	where token_address = lower({{token_address}})
    		and end_at > DATE_SUB(date(now()),90)
    		and balance > power(10,-10)
    ) as a
    left join prod.contracts c
    on a.account_address = c.address
    and c.chain='eth'
)
select a.partition_date
    ,account_address
    ,balance
from dt_drv as a
,token_bal_drv as b
where a.partition_date >= b.start_at
and a.partition_date < b.end_at
and b.is_contract = 0
group by 1,2,3


-- Token top holder Trade Log
select a.account_address
    ,a.`value`
    ,a.value_pct
    ,a.rnk
    ,b.trade_vol_24h
    ,b.trade_usd_vol_24h
    ,b.trade_vol_7d
    ,b.trade_usd_vol_7d
from
(
	select account_address,value,usd_value
				,value/sum(value) over() as value_pct
				,rank() over(order by value desc) as rnk
	from dw.dws_token_balance_eth_usd_di
	where token_address = lower({{token_address}})
) as a
inner join
(
	select account_address
		,sum(case when ts > date_sub(now(),7) then value else 0 end) as trade_vol_24h
		,sum(case when ts > date_sub(now(),7) then value*price else 0 end) as trade_usd_vol_24h
		,sum(value) as trade_vol_7d
		,sum(value*price) as trade_usd_vol_7d
	from dw.dwb_token_transfer_detail_eth_hi
	where ts >= date_sub(now(),7)
	and token_address = lower({{token_address}})
	group by 1
) as b
on a.account_address = b.account_address
order by value_pct desc
limit 2000


-- token balance bar chart distribute by value bucket
select case when usd_value < 10 then 'G1: $10-'
			when usd_value >= 10 and usd_value < 100 then 'G2: $10 ~ $100'
			when usd_value >= 100 and usd_value < 1000 then 'G3: $100 ~ $1k'
			when usd_value >= 1000 and usd_value < 10000 then 'G4: $1k ~ $10k'
			when usd_value >= 10000 and usd_value < 100000 then 'G5: $10k ~ $100k'
			when usd_value >= 100000 and usd_value < 1000000 then 'G6: $100k ~ $1M'
	else 'G7: $1M+' end as balance_bucket
    ,sum(usd_value) as usd_value
    ,count(distinct account_address) as account_cnt
from dw.dws_token_balance_eth_usd_di
where token_address = lower({{token_address}})
and value > 0
group by 1
order by 1 asc


-- Token balance distributed by account first trade time
select case when DATEDIFF(now(),b.first_trade_ts) < 7 then 'G1: 7d-'
			when DATEDIFF(now(),b.first_trade_ts) >= 7 and DATEDIFF(now(),b.first_trade_ts) < 30 then 'G2: 7d~30d'
			when DATEDIFF(now(),b.first_trade_ts) >= 30 and DATEDIFF(now(),b.first_trade_ts) < 60 then 'G3: 30d~60d'
			when DATEDIFF(now(),b.first_trade_ts) >= 60 and DATEDIFF(now(),b.first_trade_ts) < 90 then 'G4: 60d~90d'
			when DATEDIFF(now(),b.first_trade_ts) >= 90 and DATEDIFF(now(),b.first_trade_ts) < 365 then 'G5: 90d~365d'
			else 'G6: 365d+' end as time_bucket
	,count(distinct a.account_address) as account_cnt
	,sum(a.usd_value) as token_usd_balance
from
(
	select account_address,`value`,usd_value
		,value/sum(`value`) over() as value_pct
		,rank() over(order by `value` desc) as rnk
	from dw.dws_token_balance_eth_usd_di
	where token_address = lower({{token_address}})
) as a
left join 
(
	select account_address
		,min(ts) as first_trade_ts
	from dw.dwb_token_transfer_detail_eth_hi
	where token_address = lower({{token_address}})
	group by 1
) as b
on a.account_address = b.account_address
group by 1
order by 1




/*****************************************************************/
/************************   part3 twitter   **********************/
/*****************************************************************/

-- token's official Twitter address
select concat('@',username) as displayname
    ,username
from dw.dws_twitter_metrics_ha
where twitter_id = (
    select twitter_id
    from dw.dwb_token_coingecko_detail_hi
    where token_address = lower({{token_address}})
)


-- token's official Twitter followers
select followers_count
    ,date(metrics_updated_at) as metrics_updated_dt
from dw.dwb_twitter_user_info_snapshot_di
where twitter_id = (
	select twitter_id
	from dw.dwb_token_coingecko_detail_hi
	where token_address = lower({{token_address}})
)
and metrics_updated_at >= DATE_SUB(date(now()),2)
order by metrics_updated_at asc


-- Twitter kol Interations
-- daily twitter kol interations
select drv.dt as partition_date
    ,coalesce(interact_user_cnt,0) as interact_user_cnt
from dw.dim_date_di as drv
left join
(
    select date(a.interact_ts) as interact_date
        ,count(distinct a.interact_twitter_id) as interact_user_cnt
    from dw.dm_twitter_interaction_log_di as a
    left join dw.dwb_twitter_user_info_ha as b
    on a.interact_twitter_id = b.twitter_id
    where a.twitter_id = (
        select twitter_id
        from dw.dwb_token_coingecko_detail_hi
        where token_address = lower({{token_address}})
    )
    and date(a.interact_ts) > date_sub(now(),30)
    and b.followers_count > 1000
    group by 1
) as t
on drv.dt = t.interact_date
where drv.dt > date_sub(now(),30)
and drv.dt <= now()


-- token's daily activities
select b.username
    ,b.followers_count
	,date(a.interact_ts) as interact_dt
	,a.interaction_type
	,a.tweet_id
	,concat(substring(a.text,1,30),'...') as text
	,a.created_at
from dw.dm_twitter_interaction_log_di as a
left join dw.dwb_twitter_user_info_ha as b
on a.interact_twitter_id = b.twitter_id
where a.twitter_id = (
		select twitter_id
		from dw.dwb_token_coingecko_detail_hi
		where token_address = lower({{token_address}})
)
and date(a.interact_ts) > date_sub(now(),30)
and b.followers_count > 1000
and interaction_type = 'like'
order by followers_count+datediff(created_at,now())*2000 desc
