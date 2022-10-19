CREATE SCHEMA_CHANGE JOB schema_change_job_AddExportedLocalVETypes_JT8B8XV85X78MVIO FOR GRAPH nft_profit {
  ADD VERTEX account(PRIMARY_ID account_address STRING) WITH STATS="OUTDEGREE_BY_EDGETYPE", PRIMARY_ID_AS_ATTRIBUTE="true";
  ADD VERTEX nft_transfer(PRIMARY_ID trade_primary_id STRING, txn_hash STRING, block_number INT, from_address STRING, to_address STRING, buyer_pay_amt DOUBLE, seller_receive_amt DOUBLE, token_address STRING, token_id STRING, token_num INT, log_index INT, trade_type INT DEFAULT "0", trade_time DATETIME, trade_category INT, gas_cost DOUBLE) WITH STATS="OUTDEGREE_BY_EDGETYPE", PRIMARY_ID_AS_ATTRIBUTE="true";
  ADD DIRECTED EDGE send_nft(FROM account, TO nft_transfer) WITH REVERSE_EDGE="reverse_send_nft";
  ADD DIRECTED EDGE receive_nft(FROM nft_transfer, TO account) WITH REVERSE_EDGE="reverse_receive_nft";
}
RUN SCHEMA_CHANGE JOB schema_change_job_AddExportedLocalVETypes_JT8B8XV85X78MVIO
DROP JOB schema_change_job_AddExportedLocalVETypes_JT8B8XV85X78MVIO
CREATE SCHEMA_CHANGE JOB schema_change_job_AddAttributeIndex_JT8B8XV85X78MVIO FOR GRAPH nft_profit {
ALTER VERTEX nft_transfer ADD INDEX txn_hash_6s1hzdwlsq6 ON (txn_hash);
ALTER VERTEX nft_transfer ADD INDEX block_number_v8m9fh58uel ON (block_number);
}
RUN SCHEMA_CHANGE JOB schema_change_job_AddAttributeIndex_JT8B8