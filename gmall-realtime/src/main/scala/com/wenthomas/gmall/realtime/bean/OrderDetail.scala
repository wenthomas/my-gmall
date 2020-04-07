package com.wenthomas.gmall.realtime.bean

/**
 * @author Verno
 * @create 2020-04-07 14:42 
 */
case class OrderDetail(
                      id: String,
                      order_id: String,
                      sku_name: String,
                      sku_id: String,
                      order_price: String,
                      img_url: String,
                      sku_num: String
                      )
