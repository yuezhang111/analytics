SELECT r.address as token_address, r.chain, r.slug, r.contract_name, r.opensea_name
    , r.floor_price, r.floor_price * p.price AS usd_price
    ,rank() OVER (ORDER BY total_volume DESC) AS rk
    , interaction_hundredth_time AS first_mint_time, r.icon_url, is_infinite,r.one_day_volume,r.one_day_change,r.one_day_sales,r.one_day_average_price,r.seven_day_volume,r.seven_day_change,r.seven_day_sales,r.seven_day_average_price,r.thirty_day_volume,r.thirty_day_change,r.thirty_day_sales,r.thirty_day_average_price,r.total_volume,r.total_sales,r.total_supply,r.`count`,r.num_owners,r.average_price,r.num_reports,r.market_cap
    , r.telegram_url,r.twitter_username,r.instagram_username,r.wiki_url
    , now() AS etl_time
FROM 
(
SELECT *
    FROM prod.ods_mysql_prod_opensea_nft_info
    where address NOT IN (
        SELECT token_address
        FROM dw.ods_tag_nft_blacklist
    )
) r
JOIN dw.dwb_token_price_eth_hil p
ON p.address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    AND p.end_at = 999999999
left join
(
    select a.token_address,interaction_hundredth_time,
        case when last_mint_time is not null and last_mint_time < DATE_ADD(month, 3, interaction_hundredth_time)
        then 1 else 0 end as is_infinite
    from
        (
            SELECT token_address,ts as interaction_hundredth_time
            FROM
            (
                SELECT token_address,
                        row_number() OVER (PARTITION BY token_address ORDER BY block_number) AS rk,
                        n.ts
                from dw.dwb_nft_transfer_detail_eth_hi n
            ) ni
            WHERE rk = 200
        ) a
    left join
        (
            SELECT token_address,max(ts) as last_mint_time
            FROM dw.dwb_nft_transfer_detail_eth_hi where is_mint =1
            group by token_address
        ) b
    on a.token_address = b.token_address
) nf
on  r.address = nf.token_address
;



with top_drv as (
	select *
	from
	(
		select token_address
					,sum(trade_usd_value) as trade_usd_value
					,sum(trade_eth_value) as trade_eth_value
					,count(*) as n
					,rank()over( order by sum(trade_eth_value) desc) as rnk
		from dw.dwb_nft_trade_eth_detail_hi
		where DATE(trade_time) >= DATE_SUB(date(now()),7) 
		and DATE(trade_time) <= date(now())
		and platform like 'Opensea%'
		group by 1
	) as a
	where rnk <= 100
)
select a.*,b.contract_name,b.opensea_name,c.low_eth_price
from top_drv as a
left join dw.dwb_nft_opensea_detail_di as b
on a.token_address = b.token_address
left join 
(
	select *
	from
	(
		select token_address, dt, low_eth_price
					,rank()over(partition by token_address order by dt desc) as rnk
		from dw.dwb_nft_price_eth_byday_hi
		where dt > DATE_SUB(dt,5)
	) as a
	where rnk = 1
) as c
on a.token_address = c.token_address
order by rnk asc
;
;