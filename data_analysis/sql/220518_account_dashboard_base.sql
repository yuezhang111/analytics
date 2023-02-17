-- part1 numbers

-- default address v神  0x1db3439a222c519ab44bb1144fc28167b4fa6ee6 
-- testing address 路人 0x9999ba66609f11adcb5d3b71ea153ca3987a25a3

select sum(usd_value) as total_usd_value
from dw.dws_token_balance_eth_usd_di
where account_address = lower({{account_address}})
;

select count(distinct token_address) as token_cnt
from dw.dwb_token_transfer_detail_eth_hi
where account_address = lower({{account_address}})
;


-- NFT USD value v1
select sum(low_usd_price) as total_nft_usd_value
(
	select *
	from dw.dws_nft_balance_eth
	where account_address = lower({{account_address}})
) as drv
left join 
(
	select b.*
	from 
	(
		select token_address
					,max(dt) as max_dt
		from dw.dwb_nft_price_eth_byday_hi
		group by 1
	) as a
	left join dw.dwb_nft_price_eth_byday_hi as b
	on a.token_address = b.token_address
	and a.max_dt = b.dt
) as prc
on drv.token_address = prc.token_address
;



-- NFT USD value v2
select sum(low_usd_price) as total_nft_usd_value
(
	select *
	from dw.dws_nft_balance_eth
	where account_address = lower({{account_address}})
) as drv
left join dw.dwb_nft_opensea_detail_di as b
on a.token_address = b.token_address

;



select count(distinct token_address) as nft_project_cnt
from dw.dws_nft_balance_eth
where account_address = lower({{account_address}})
;



select sum(cost) as nft_mint_cost
from dw.dwb_nft_transfer_detail_eth_hi
where account_address = lower({{account_address}})
and is_mint = 1
;




select sum(`value`) as total_gas_eth
			,count(*) as txn_cnt
from dw.dwb_token_transfer_detail_eth_hi
where account_address = lower({{account_address}})
and id like 'transactions_%'
;



select symbol, usd_value
from dw.dws_token_balance_eth_usd_di
where account_address = lower({{account_address}})
;



select a.token_address,b.contract_name,sum(a.`value`) as nft_cnt
from dw.dws_nft_balance_eth as a
left join dw.dwb_nft_opensea_detail_di as b
on a.token_address = b.token_address
where a.account_address = lower({{account_address}})
group by 1,2


-- get latest date's price







-- default address v神  0x1db3439a222c519ab44bb1144fc28167b4fa6ee6 
-- testing address 路人 0x9999ba66609f11adcb5d3b71ea153ca3987a25a3

-- default value 0xb33fc590944170de216fa982eefe7584ace0789c

