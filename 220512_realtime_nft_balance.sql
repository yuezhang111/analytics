
select a.account_address
      ,a.token_address
      ,a.token_id
      ,a.`value` as snapshot_balance
      ,b.`value` as recent_txn_value
      ,a.`value` + coalesce(b.`value`,0) as realtime_balance
from dw.dws_nft_balance_eth as a
left join
(
   select 
    token_address,token_id,from_address as account_address,0 as is_mint,block_number,token_num *-1 as `value`,transaction_hash
    from prod.nft_transfer_eth
    WHERE block_number > (select max(block_number) from dw.dws_nft_balance_eth)
    union all
    select
    token_address,token_id,to_address as account_address,if(from_address='0x0000000000000000000000000000000000000000',1,0) as is_mint,
    block_number,token_num as `value`,transaction_hash
    from prod.nft_transfer_eth
    WHERE block_number > (select max(block_number) from dw.dws_nft_balance_eth)
) as b
on a.token_address = b.token_address
and a.token_id = b.token_id
and a.account_address = b.account_address