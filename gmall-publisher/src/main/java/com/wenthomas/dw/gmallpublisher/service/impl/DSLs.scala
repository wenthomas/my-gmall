package com.wenthomas.dw.gmallpublisher.service.impl

/**
 * @author Verno
 * @create 2020-04-08 14:21 
 */
object DSLs {

    def getSaleDetailDSL(date: String,
                         keyWord: String,
                         startPage: Int,
                         sizePerPage: Int,
                         aggField: String,
                         aggCount: Int) = {
        s"""
            |{
            |  "query": {
            |    "bool": {
            |      "filter": {
            |        "term": {
            |          "dt": "${date}"
            |        }
            |      },
            |      "must": [
            |        {
            |          "match": {
            |            "sku_name": {
            |              "query": "${keyWord}",
            |              "operator": "and"
            |            }
            |          }
            |        }
            |      ]
            |    }
            |  },
            |  "aggs": {
            |    "group_by_${aggField}": {
            |      "terms": {
            |        "field": "${aggField}",
            |        "size": ${aggCount}
            |      }
            |    }
            |  },
            |  "from": ${(startPage - 1) * sizePerPage},
            |  "size": ${sizePerPage}
            |}
            |""".stripMargin
    }

}
