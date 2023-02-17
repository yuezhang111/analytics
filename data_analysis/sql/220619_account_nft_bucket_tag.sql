insert into dw.dm_nft_eth_cost_di
select t1.account_address
	,t1.token_address
	,t1.token_id
	,t1.is_mint
	,t1.nft_create_time
	,t1.opensea_name
	,t1.icon_url
	,t1.account_first_transfer
	
	,t2.token_num
	,coalesce(t2.nft_eth_cost,0) as nft_eth_cost
	
	,t3.`value` as balance_value
	,t3.first_transfer
	
	,now() as etl_time
from (
	select a.account_address
		,a.token_address
		,a.token_id
		,b.create_time as nft_create_time
		,b.opensea_name
		,b.icon_url
		,a.`value` as `value`
		,a.ts as account_first_transfer
		,a.is_mint
	from
	(
		select account_address
			,token_address
			,token_id
			,`value`
			,ts
			,is_mint
			,cost
			,gas_value
			,row_number()over(partition by account_address,token_address,token_id order by ts asc) as rnk
		from dw.dwb_nft_transfer_detail_eth_hi
		where `value` > 0
	) as a
	left join dw.dim_nft_tokens as b
	on a.token_address = b.token_address
	where a.rnk = 1
) as t1

left join (
	select account_address
		,token_address,token_ids as token_id
		,sum(token_num) as token_num
		,sum(trade_eth_value) as nft_eth_cost
	from
	(
		select from_address as account_address
			,token_address
			,token_ids
			,-1.0*token_num as token_num
			,-1.0*trade_eth_value as trade_eth_value
			,trade_time as ts
			,transaction_hash
			,'sold' as trade_type
		from dw.dwb_nft_trade_eth_detail_hi
	
		union all
		select to_address as account_address
			,token_address
			,token_ids
			,token_num
			,trade_eth_value as trade_eth_value
			,trade_time as ts
			,transaction_hash
			,'buy' as trade_type
		from dw.dwb_nft_trade_eth_detail_hi
	 
		union all
		select account_address
			,token_address
			,token_id as token_ids
			,mint_amount as token_num
			,(gas_cost+mint_cost) as trade_eth_value
			,mint_time as ts
			,transaction_hash
			,'mint' as trade_type
		from dw.dwb_nft_mint_detail_eth_hi
	) as t
	group by 1,2,3
) as t2
on t1.account_address = t2.account_address
and t1.token_address = t2.token_address
and t1.token_id = t2.token_id

left join 
(
	select account_address, token_address, token_id,`value`,first_transfer
	from dws_nft_balance_eth
	where `value` > 0
) as t3
on t1.account_address = t3.account_address
and t1.token_address = t3.token_address
and t1.token_id = t3.token_id