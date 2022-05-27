SELECT t1.account_address
    ,t1.token_address
    ,t1.`value` as snapshot_balance
    ,coalesce(t2.recent_txn_value,0) as recent_txn_value
    ,t1.`value` + coalesce(t2.recent_txn_value,0) as realtime_balance
FROM dw.dws_token_balance_eth as t1

left join
(
    select token_address,account_address,sum(recent_txn_value) as recent_txn_value
    from
    (
        -- part1 other token
        select token_address
            ,account_address
            ,sum(`value`) as recent_txn_value
        from 
        (
            select token_address
                ,if(from_address='-1','0x0000000000000000000000000000000000000000',from_address) as account_address
                ,`value`*-1 as `value`
                ,block_number
                ,log_index
            from prod.transfer_event_eth
            WHERE block_number > (select max(block_number) from dw.dws_token_balance_eth)
            union all
            select token_address
                ,if(to_address='-1','0x0000000000000000000000000000000000000000',to_address) as account_address
                ,`value`
                ,block_number
                ,log_index
            from prod.transfer_event_eth
            WHERE block_number > (select max(block_number) from dw.dws_token_balance_eth)
        ) as a
        group by 1,2

        -- part2 eth token
        union all

        select '0xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxeth' as token_address
                ,address as account_address
                ,sum(`value`) as recent_txn_value
        from
        (
            select address,block_number,concat('trace__',trace_id) as id,`value`
            from
            (
                SELECT to_address AS address,block_number,transaction_hash,concat(trace_address,',') trace_address,trace_id,
                    (CAST(CONV(SUBSTR(trim(REPLACE (REPLACE (VALUE,CHAR (10),''),CHAR (13),'')),3,char_length(trim(REPLACE (REPLACE (VALUE,CHAR (10),''),CHAR (13),'')))-2-5),16,10)/power(10,18)*power(16,5) AS DECIMAL (20,10))) AS VALUE
                FROM prod.traces_eth
                WHERE STATUS=0
                AND (call_type NOT IN ('delegatecall','callcode','staticcall'))
                and block_number > (select max(block_number) from dw.dws_token_balance_eth)
                
                UNION ALL
                SELECT from_address AS address,block_number,transaction_hash,concat(trace_address,',') trace_address,trace_id,
                    -(CAST(CONV(SUBSTR(trim(REPLACE (REPLACE (VALUE,CHAR(10),''),CHAR(13),'')),3,char_length(trim(REPLACE (REPLACE (VALUE,CHAR (10),''),CHAR (13),'')))-2-5),16,10)/power(10,18)*power(16,5) AS DECIMAL (20,10))) AS VALUE
                FROM prod.traces_eth 
                WHERE STATUS=0
                AND (call_type NOT IN ('delegatecall','callcode','staticcall'))
                and block_number > (select max(block_number) from dw.dws_token_balance_eth)
            ) as trade_all
            LEFT JOIN
            (
                SELECT transaction_hash
                        ,if(trace_address='',trace_address,concat(trace_address,',')) as trace_address
                FROM prod.traces_eth WHERE STATUS=1 and block_number > (select max(block_number) from dw.dws_token_balance_eth)
            ) as  fail_trace
            ON trade_all.transaction_hash = fail_trace.transaction_hash 
            AND trade_all.trace_address LIKE CONCAT(fail_trace.trace_address,'%')
            WHERE fail_trace.transaction_hash IS NULL AND fail_trace.trace_address IS NULL

            UNION ALL
            SELECT lower(from_address) as address,block_number,concat('transactions__',transaction_index) AS id,
                -(effective_gas_price / POWER(10.0, 18) * gas_used) as value
            from prod.transactions_eth transactions
            WHERE block_number > (select max(block_number) from dw.dws_token_balance_eth)
            and (effective_gas_price / POWER(10.0, 18) * gas_used) !=0
        ) as detail
        left join
        (
            select block_number,from_unixtime(timestamp) as ts 
            from prod.blocks 
            where chain='eth'
        ) as b
        on detail.block_number=b.block_number
        where value!=0 and (detail.address not in (select address from tmp.`token_balance_ethToken_except`) or detail.block_number > 1920000)
        group by 1,2
    ) as r
    group by 1,2
) as t2
on t1.token_address = t2.token_address
and t1.account_address = t2.account_address
order by recent_txn_value desc
limit 100
