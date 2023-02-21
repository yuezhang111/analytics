from utils.doris_connector import DorisClient


class BackfillNFTBalanceCost(object):
    def __init__(self,is_truncate=True):
        self.doris_db = DorisClient("dm")
        self.is_truncate = is_truncate

    def get_block_bucket(self,bucket_size=1000000):
        db = self.doris_db
        bucket_sql = """
    SELECT block_number
        ,records
        ,cum_records
        ,rnk
    FROM
    (
        SELECT block_number
            ,records
            ,cum_records
            ,rank() over(PARTITION BY floor(cum_records/{bucket_size}) ORDER BY cum_records%{bucket_size} asc) as rnk
        FROM
        (
            SELECT block_number
                ,records
                ,sum(records) over(ORDER BY block_number asc) as cum_records
            FROM
            (
                select block_number
                    ,count(*) as records
                from dw.dws_nft_balance_eth
                where `value` > 0
                GROUP BY 1
            ) as a
        ) as aa
    ) as t
    WHERE rnk = 1
        """.format(bucket_size=bucket_size)
        res_rows, field_names = db.read_sql(bucket_sql)
        block_bucket = []
        for row in res_rows:
            block_bucket.append(row[0])
        return block_bucket

    def get_latest_block_number(self):
        blo_sql = """
        SELECT max(block_number) as block_number
        FROM dw.dws_nft_balance_eth as a
        """
        res_rows,_ = self.doris_db.read_sql(blo_sql)
        block_number = res_rows[0][0]
        return block_number

    @staticmethod
    def get_backfill_sql(start,end):
        backfill_sql = """
    INSERT INTO dm.dm_nft_balance_cost_eth_di_test
    SELECT token_address
        ,token_id
        ,account_address
        ,`value` as balance_value
        ,block_number
        ,transaction_hash
        ,txn_block
        ,cost_eth_value as cost_eth_amt
        ,cost_ts
        ,trade_type
        ,is_scalp
        ,now() as etl_time
    FROM
    (
        SELECT a.token_address
            ,a.token_id
            ,a.account_address
            ,a.`value`
            ,a.block_number
            ,b.cost_eth_value
            ,b.trade_type
            ,b.transaction_hash
            ,b.block_number as txn_block
            ,b.ts as cost_ts
            ,b.is_scalp
            ,row_number() over(partition by a.token_address,a.token_id ORDER BY b.ts DESC) as rnk
        FROM 
        (
            SELECT a.token_address,a.token_id,a.account_address,a.`value`,a.block_number
                ,FROM_UNIXTIME(b.`timestamp`) as block_ts
            FROM dw.dws_nft_balance_eth as a
            left join prod.blocks as b
            ON a.block_number = b.block_number
            AND b.`chain` = 'eth'
            INNER JOIN dw.dwm_nft_detail_ha as c
            ON a.token_address = c.token_address
            AND c.token_type = 'ERC721'
            WHERE a.`value` > 0
            AND a.`block_number` >= {start_blo}
            AND a.`block_number` < {end_blo}
        ) as a
        LEFT JOIN 
        (
        select to_address as account_address
            ,token_address
            ,token_ids as token_id
            ,token_num
            ,buyer_pay_eth_amount as cost_eth_value
            ,trade_time as ts
            ,transaction_hash
            ,block_number
            ,trade_type
            ,is_scalp
        from dw.dwb_nft_trade_eth_detail_hi
        where erc_standard not in ('ERC1155')
        and block_number < {end_blo}
        union all
        select account_address
            ,token_address
            ,token_id
            ,mint_amount as token_num
            ,(gas_cost+mint_cost) as cost_eth_value
            ,mint_time as ts
            ,transaction_hash
            ,block_number
            ,'mint' as trade_type
            ,0 as is_scalp
        from dw.dwb_nft_mint_detail_eth_hi
        where block_number < {end_blo}
        ) as b
        ON a.token_address = b.token_address
        AND a.token_id = b.token_id
    ) as t
    WHERE t.rnk = 1
    """.format(
            start_blo=start,
            end_blo=end
        )
        return backfill_sql

    def truncate_test_table(self,db_name,table_name):
        sql = """
        DROP TABLE IF EXISTS {db_name}.{table_name};
        CREATE TABLE {db_name}.{table_name} (
          `token_address` varchar(42) NOT NULL COMMENT "",
          `token_id` varchar(80) NOT NULL COMMENT "",
          `account_address` varchar(200) NOT NULL COMMENT "",
          `balance_value` int(11) NULL COMMENT "NFT Balance",
          `block_number` bigint(20) NULL COMMENT "NFT Balance Updated Block",
          `transaction_hash` varchar(500) NULL COMMENT "NFT Trade/Mint Transaction",
          `txn_block` bigint(20) NULL COMMENT "Block Number of NFT Trade/Mint Transaction",
          `cost_eth_amt` double NULL COMMENT "cost_eth_amt",
          `cost_ts` datetime NULL COMMENT "NFT入手时间",
          `trade_type` varchar(20) NULL COMMENT "NFT入手方式(buy/mint)",
          `is_scalp` tinyint(4) NULL COMMENT "是否刷单交易",
          `etl_time` datetime NULL DEFAULT CURRENT_TIMESTAMP COMMENT ""
        ) ENGINE=OLAP
        UNIQUE KEY(`token_address`, `token_id`, `account_address`)
        COMMENT "nft_balance_cost"
        DISTRIBUTED BY HASH(`token_address`, `token_id`) BUCKETS 32 
        PROPERTIES (
            "replication_num" = "3",
            "in_memory" = "false",
            "storage_format" = "V2"
        );
        """.format(
            db_name=db_name,
            table_name=table_name
        )
        self.doris_db.run_sql(sql)

    def run_backfill(self,start_blo=0,end_blo=1,bucket_size=1000000):
        if self.is_truncate:
            self.truncate_test_table("dm","dm_nft_balance_cost_eth_di_test")
        block_bucket = self.get_block_bucket(bucket_size)
        start_blo = start_blo if start_blo > 0 else block_bucket[0]
        end_blo = end_blo if end_blo > 1 else block_bucket[-1]
        current_end_blo = 0
        for i in range(1,len(block_bucket)):
            current_blo = block_bucket[i-1]
            if current_blo < start_blo:
                continue
            elif current_blo >= end_blo:
                break
            else:
                current_end_blo = min(block_bucket[i],end_blo)
                print(current_blo, current_end_blo)
                sql = BackfillNFTBalanceCost.get_backfill_sql(current_blo, current_end_blo)
                self.doris_db.run_sql(sql)
        return current_end_blo


def main():
    backfill_task = BackfillNFTBalanceCost()
    start_block = 0
    end_block = backfill_task.get_latest_block_number()
    finished_block = backfill_task.run_backfill(start_block,end_block)
    print(finished_block)


if __name__ == "__main__":
    main()
