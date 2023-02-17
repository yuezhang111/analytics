:CREATE OR REPLACE Distributed QUERY nft_profit_calculation(SET<VERTEX<account>> account_list,BOOL print_edge=FALSE)
FOR GRAPH nft_profit {
  TYPEDEF TUPLE <
      INT block_number,
      STRING sell_date,
      INT token_num,
      VERTEX<nft_transfer> v>
  order_value;

  TYPEDEF TUPLE <
      STRING nft_address,
      STRING nft_id>
  nft_key;

  MapAccum<nft_key, ListAccum<order_value>> @received_records;
  MapAccum<nft_key, ListAccum<order_value>> @bought_records;
  MapAccum<nft_key, ListAccum<order_value>> @sold_records;
  MapAccum<STRING, SumAccum<DOUBLE>> @@sold_value;
  MapAccum<STRING, SumAccum<DOUBLE>> @@buy_value;
  SetAccum<Vertex<nft_transfer>> @@sell_nodes;
  SetAccum<Vertex<nft_transfer>> @@buy_nodes;
  SetAccum<Vertex<nft_transfer>> @@transfer_nodes;
  SetAccum<Vertex<nft_transfer>> @@not_finish;
  SetAccum<Vertex<nft_transfer>> @local_not_finish;
  SumAccum<INT> @valid_num = -1;
  SetAccum<EDGE> @@print_edges;
  MaxAccum<STRING> @sell_date;
  MapAccum<STRING, SumAccum<DOUBLE>> @@date_sell;
  MapAccum<STRING, SumAccum<DOUBLE>> @@date_buy;
  MapAccum<STRING, MapAccum<STRING, SumAccum<DOUBLE>>> @@date_project_sell;
  MapAccum<STRING, MapAccum<STRING, SumAccum<DOUBLE>>> @@date_project_buy;
  MapAccum<STRING, MapAccum<STRING, SumAccum<DOUBLE>>> @@date_project_profit;


  // 测试通过用例
  // base case：0xe89afaad1f5fa099add609606adc969e3203435f
  // transfer case: 0xbb34d62e24def6543470a9fd1d05f70375ce5ec5
  // Bundle Trade: 0xc8150d7f2c604ee6792b5fafb3d213cb3674e064
  // ERC-1155: 0x05861bf310629d56736a957a59eec2af1daa47b1
  // vopa: 0x2329a3412ba3e49be47274a72383839bbf1cde88
  // 测试未通过用例
  // 无

  // 输出sold集合
  start = account_list;
  r = SELECT t
      FROM start:s-(send_nft:e)-nft_transfer:t
      WHERE t.trade_type == 1
      ACCUM
        @@sold_value += (t.token_address -> t.seller_receive_amt),
        @@date_sell += (datetime_format(t.trade_time,"%Y-%m-%d") -> t.seller_receive_amt),
        @@date_project_sell += (datetime_format(t.trade_time,"%Y-%m-%d") -> (t.token_address -> t.seller_receive_amt)),
        @@date_project_profit += (datetime_format(t.trade_time,"%Y-%m-%d") -> (t.token_address -> t.seller_receive_amt)),
        s.@sold_records += (
          nft_key(t.token_address, t.token_id) ->
          order_value(t.block_number,datetime_format(t.trade_time,"%Y-%m-%d"),t.token_num, t)
        ),
        @@sell_nodes += t,
        @@print_edges += e,
        // 卖出场景下需要计算buy_offer订单的gas
        IF t.trade_category == 2 THEN
          @@date_gas += (datetime_format(t.trade_time,"%Y-%m-%d") -> t.gas_cost)
        END;
  PRINT r.size();



  seed = account_list;
  int seed_size = 0;
  seed_size = seed.size();
  while seed_size > 0 limit 50 DO
    get_trades = SELECT t FROM seed:s-(reverse_receive_nft:e)-nft_transfer:t
                  WHERE s.@sold_records.containsKey(nft_key(t.token_address, t.token_id))
                  ACCUM
                    // 将用户的买入分别存入@bought_records和@received_records中
                    List<order_value> tmp = s.@sold_records.get(nft_key(t.token_address, t.token_id)),
                    FOREACH v in tmp DO
                      IF t.block_number <= v.block_number
                      THEN
                        IF t.trade_type >= 1 THEN
                          s.@bought_records += (
                            nft_key(t.token_address, t.token_id) -> 
                            order_value(t.block_number, datetime_format(t.trade_time,"%Y-%m-%d"), t.token_num, t)
                          )
                        ELSE
                          s.@received_records += (
                            nft_key(t.token_address, t.token_id) -> 
                            order_value(t.block_number, datetime_format(t.trade_time,"%Y-%m-%d"), t.token_num, t)
                          )
                        END,
                        BREAK
                      END
                    END
                  POST-ACCUM
                    //bought::遍历每个nft，匹配买入和卖出，输出结果集并更新local accum数值
                    FOREACH (nft, orders) in s.@sold_records DO
                      FOREACH i2 in range[0, s.@bought_records.get(nft).size()] DO
                        while s.@bought_records.get(nft).get(i2).token_num > 0 DO
                          // 找到该nft最早的一笔卖出，及其对应的min_block
                          int index = -1,
                          int min_block = GSQL_INT_MAX,
                          int len = 0,
                          len = count(orders),
                          FOREACH selling_index in RANGE [0, len - 1] DO
                            order_value odr = orders.get(selling_index),
                            if (orders.get(selling_index).token_num != 0 and orders.get(selling_index).block_number < min_block) 
                              and orders.get(selling_index).v != s.@bought_records.get(nft).get(i2).v THEN
                              min_block = orders.get(selling_index).block_number,
                              index = selling_index
                            END
                          END,
  
                          // 根据匹配结果，更新数据
                          IF (min_block != GSQL_INT_MAX) THEN
                            @@buy_nodes += s.@bought_records.get(nft).get(i2).v,
                            s.@local_not_finish += s.@bought_records.get(nft).get(i2).v,

                            if (orders.get(index).token_num >= s.@bought_records.get(nft).get(i2).token_num) THEN
                              //卖出数量大于等于买入数量，更新数据，寻找下一个买入
                              orders.update(index,
                                order_value(
                                  orders.get(index).block_number,
                                  orders.get(index).sell_date,
                                  orders.get(index).token_num - s.@bought_records.get(nft).get(i2).token_num,
                                  orders.get(index).v
                                )
                              ),
                              /***
                              vertex<nft_transfer> __v = orders.get(index).v,
                              @@date_gas += (
                                datetime_format(__v.trade_time,"%Y-%m-%d") -> __v.gas_cost
                              ),
                              ***/
                              
                              s.@bought_records.get(nft).update(i2,
                                order_value(
                                  s.@bought_records.get(nft).get(i2).block_number,
                                  orders.get(index).sell_date,
                                  0,
                                  s.@bought_records.get(nft).get(i2).v
                                )
                              )
                            ELSE
                              // 卖出数量小于买入数量，更新数据，寻找下一个卖出
                              s.@bought_records.get(nft).update(i2,
                                order_value(
                                  s.@bought_records.get(nft).get(i2).block_number,
                                  orders.get(index).sell_date,
                                  s.@bought_records.get(nft).get(i2).token_num - orders.get(index).token_num,
                                  s.@bought_records.get(nft).get(i2).v
                                )
                              ),
                              orders.update(index, 
                                order_value(orders.get(index).block_number, orders.get(index).sell_date, 0, orders.get(index).v))
                            END
                          ELSE
                            BREAK
                          END
                        END
                      END
                    END
                    
  
                    //Transfer::遍历每个nft，匹配卖出和转入，输出结果集并更新local accum数值
                    ,FOREACH (nft, orders) in s.@sold_records DO
                      FOREACH i2 in RANGE[0, s.@received_records.get(nft).size()] DO
                        while s.@received_records.get(nft).get(i2).token_num > 0 DO
                          // 找到该nft最早的一笔卖出，及其对应的min_block
                          int index = -1,
                          int min_block = GSQL_INT_MAX,
                          int len = 0,
                          len = count(orders),
                          FOREACH selling_index in RANGE [0, len - 1] DO
                            if (orders.get(selling_index).token_num != 0 and orders.get(selling_index).block_number < min_block) 
                              and orders.get(selling_index).v != s.@received_records.get(nft).get(i2).v THEN
                              min_block = orders.get(selling_index).block_number,
                              index = selling_index
                            END
                          END,
  
                          // 根据匹配结果，更新数据
                          IF (min_block != GSQL_INT_MAX) THEN
                            @@transfer_nodes += s.@received_records.get(nft).get(i2).v,
                            s.@local_not_finish += s.@received_records.get(nft).get(i2).v,

                            if (orders.get(index).token_num >= s.@received_records.get(nft).get(i2).token_num) THEN
                              orders.update(index, order_value(
                                orders.get(index).block_number,
                                orders.get(index).sell_date,
                                orders.get(index).token_num - s.@received_records.get(nft).get(i2).token_num,
                                orders.get(index).v
                              )),
                              s.@received_records.get(nft).update(i2, order_value(
                                s.@received_records.get(nft).get(i2).block_number,
                                orders.get(index).sell_date,
                                0,
                                s.@received_records.get(nft).get(i2).v
                              ))
                            ELSE
                              s.@received_records.get(nft).update(i2, order_value(
                                s.@received_records.get(nft).get(i2).block_number,
                                orders.get(index).sell_date,
                                s.@received_records.get(nft).get(i2).token_num - orders.get(index).token_num,
                                s.@received_records.get(nft).get(i2).v
                              )),
                              orders.update(index, order_value(
                                orders.get(index).block_number,
                                orders.get(index).sell_date,
                                0,
                                orders.get(index).v
                              ))
                            END
                          ELSE
                            BREAK
                          END
                        END
                      END
                    END
                ;
  
    // 所有买入和转入，有匹配上的 计算更新到输出output中
    tmp_unfinish = SELECT t FROM seed:s-(reverse_receive_nft:e)-nft_transfer:t
                  WHERE s.@local_not_finish.contains(t)
                  ACCUM
                    FOREACH odr in s.@received_records.get(nft_key(t.token_address, t.token_id)) DO
                      if (odr.v == t and odr.token_num < t.token_num) THEN
                        @@print_edges += e,
                        t.@valid_num = (t.token_num - odr.token_num),
                        t.@sell_date = odr.sell_date,
                        @@not_finish += t,
                        BREAK
                      END
                    END,
                    FOREACH odr in s.@bought_records.get(nft_key(t.token_address, t.token_id)) DO
                      if (odr.v == t and odr.token_num < t.token_num) THEN
                        @@print_edges += e,
                        @@date_buy += (odr.sell_date -> (t.buyer_pay_amt * (t.token_num - odr.token_num / t.token_num))),
                        @@date_project_buy += (odr.sell_date -> (t.token_address -> (t.buyer_pay_amt * (t.token_num - odr.token_num / t.token_num)))),
                        @@date_project_profit += (odr.sell_date -> (t.token_address -> -(t.buyer_pay_amt * (t.token_num - odr.token_num / t.token_num)))),
                        @@buy_value += (t.token_address -> (t.buyer_pay_amt * (t.token_num - odr.token_num / t.token_num))),
                        // 买入订单sell_offer或者transfer 都计算gas cost
                        IF t.trade_category == 1 or t.trade_type == 0 then 
                          @@date_gas += (odr.sell_date -> t.gas_cost)
                        END
                      END
                    END;
    reset_collection_accum(@sold_records);
    reset_collection_accum(@bought_records);
    reset_collection_accum(@received_records);
    reset_collection_accum(@local_not_finish);
    next_step = @@not_finish;
    @@not_finish.clear();
    seed = SELECT t FROM next_step:s-(reverse_send_nft:e)-account:t
           ACCUM
             t.@sold_records += (nft_key(s.token_address, s.token_id) -> order_value(s.block_number,s.@sell_date, s.@valid_num, s)),
             @@print_edges += e;
    seed_size = seed.size();
    PRINT seed_size;
  END;
  //PRINT @@sell_nodes;
  //PRINT @@buy_nodes;
  //PRINT @@transfer_nodes;
  PRINT @@buy_nodes.size() as buy_nft_cnt;
  PRINT @@sell_nodes.size() as sell_nft_cnt;
  
  if print_edge THEN
    if @@print_edges.size() < 100 THEN
      PRINT @@print_edges;
    ELSE
      PRINT "100+ edges to print";
    END;
  END;
  
  // profit chart, 盈利最多的几个nft项目
  MapAccum<STRING, SumAccum<DOUBLE>> @@top_projects;
  DOUBLE profit = 0;
  foreach (nft,sell) in @@sold_value DO
    profit = profit+sell;
    @@top_projects += (nft -> sell);
  end;
  foreach (nft,buy) in @@buy_value DO
    profit = profit-buy;
    @@top_projects += (nft -> -buy);
  end;
  PRINT profit as nft_cum_profit;
  PRINT @@top_projects;
  
  // profit chart曲线原始数据
  //PRINT @@date_sell;
  //PRINT @@date_buy;
  DOUBLE profit2 = 0;
  double max_daily_profit = 0;
  STRING best_day;
  MapAccum<STRING, SumAccum<DOUBLE>> @@date_profit;

  FOREACH (date,cost) in @@date_buy DO
    DOUBLE p = 0;
    p = p + @@date_sell.get(date);
    p = p - cost;
    profit2 += p;
    if p > max_daily_profit THEN
      max_daily_profit = p;
      best_day = date;
    END;
    @@date_profit+=(date -> p);
  END;
  PRINT profit2 as date_cum_profit;
  PRINT @@date_profit;
  
  
  // 个人收益最大的一天 卖出的项目
  PRINT best_day;
  PRINT max_daily_profit as best_day_profit;
  //PRINT @@date_project_buy.get(best_day);
  //PRINT @@date_project_sell.get(best_day);
  
  MapAccum<STRING,DOUBLE> @@bestday_project_profit;
  FOREACH (nft,sell_amt) in @@date_project_sell.get(best_day) DO
    double buy_amt = @@date_project_buy.get(best_day).get(nft);
    @@bestday_project_profit += (nft -> sell_amt - buy_amt);
  END;
  PRINT @@bestday_project_profit;
  PRINT @@date_project_profit.get(best_day);
  
  PRINT @@date_gas;
}