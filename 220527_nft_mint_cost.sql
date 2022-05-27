select a.account_address
        ,a.token_address,a.token_ids,a.token_num
        ,b.contract_name
        ,a.trade_eth_value,a.currency_symbol
        ,a.trade_time,a.transaction_hash
        ,a.trade_type
from
(
select from_address as account_address
        ,token_address
        ,token_ids
        ,token_num
        ,trade_eth_value
        ,currency_symbol
        ,trade_time
        ,transaction_hash
        ,'sold' as trade_type
from dw.dwb_nft_trade_eth_detail_hi
where from_address = '0x00000000034b55ebd82cde9b38a85ab0978b7a47'

union all
select to_address as account_address
        ,token_address
        ,token_ids
        ,token_num
        ,trade_eth_value
        ,currency_symbol
        ,trade_time
        ,transaction_hash
        ,'buy' as trade_type
from dw.dwb_nft_trade_eth_detail_hi
where to_address = '0x00000000034b55ebd82cde9b38a85ab0978b7a47'

union all
select 
    ,'mint' as trade_type
from dw.dwb_nft_trade_eth_detail_hi


) as a
left join dwb_nft_opensea_detail_di as b
on a.token_address = b.token_address
order by trade_time desc



