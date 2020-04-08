package com.wenthomas.dw.gmallpublisher.service.impl;

import com.wenthomas.dw.gmallpublisher.mapper.DauMapper;
import com.wenthomas.dw.gmallpublisher.mapper.OrderInfoMapper;
import com.wenthomas.dw.gmallpublisher.service.PublisherService;
import com.wenthomas.gmall.common.Constant;
import com.wenthomas.gmall.common.ESUtil;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Verno
 * @create 2020-03-31 18:00
 */
@Service
public class PublisherServiceImpl implements PublisherService {

    private static Logger logger = LoggerFactory.getLogger(PublisherServiceImpl.class);

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderInfoMapper orderInfoMapper;

    @Override
    public Long getDau(String date) {
        return dauMapper.getDauTotal(date);
    }

    @Override
    public Map<String, Long> getHourDau(String date) {
        List<Map<String, Object>> hourDauList = dauMapper.getDauHour(date);

        Map<String, Long> resultMap = new HashMap<>();

        for (Map<String, Object> map : hourDauList) {
            String hour = (String) map.get("HOUR");
            Long count = (Long) map.get("COUNT");
            resultMap.put(hour, count);
            logger.info("{}: {}----{}", date, hour, count);
        }

        return resultMap;
    }

    /**
     * 查询当日总销售额
     * @param date
     * @return
     */
    @Override
    public Double getTotalAmount(String date) {
        Double result = orderInfoMapper.getTotalAmount(date);
        return result == null ? 0 : result;
    }

    /**
     * 查询当日每小时销售额明细
     * @param date
     * @return
     */
    @Override
    public Map<String, Double> getHourOrderAmount(String date) {
        Map<String, Double> result = new HashMap<>();
        List<Map<String, Object>> hourAmount = orderInfoMapper.getHourAmount(date);
        for (Map<String, Object> map : hourAmount) {
            String hour = (String) map.get("CREATE_HOUR");
            Double amount = ((BigDecimal) map.get("SUM")).doubleValue();
            result.put(hour, amount);
        }
        return result;
    }

    @Override
    public Map<String, Object> getSaleDetailAndAggGroupByFields(String date,
                                                                String keyWord,
                                                                int startPage,
                                                                int sizePerPage,
                                                                String aggField,
                                                                int aggCount) throws IOException {
        Map<String, Object> resultMap = new HashMap<>();

        //1，获取ES客户端
        JestClient client = ESUtil.getClient();

        //2，查询数据（聚合）
        String dsls = DSLs.getSaleDetailDSL(date, keyWord, startPage, sizePerPage, aggField, aggCount);
        System.out.println(dsls);
        Search search = new Search.Builder(dsls)
                .addIndex(Constant.INDEX_SALE_DETAIL)
                .addType("_doc")
                .build();
        SearchResult searchResult = client.execute(search);

        //3，从返回的结果中，解析出来我们需要的数据
        //3.1 总数
        Integer total = searchResult.getTotal();
        resultMap.put("total", total);

        //3.2 明细
        List<Map> detail = new ArrayList<>();
        List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);
        for (SearchResult.Hit<HashMap, Void> hit : hits) {
            HashMap source = hit.source;
            detail.add(source);
        }
        resultMap.put("detail", detail);

        //3.3 聚合
        Map<String, Object> ageMap = new HashMap<>();

        List<TermsAggregation.Entry> buckets = searchResult.getAggregations()
                .getTermsAggregation("group_by_" + aggField)
                .getBuckets();

        for (TermsAggregation.Entry bucket : buckets) {
            String key = bucket.getKey();
            Long count = bucket.getCount();
            ageMap.put(key, count);
        }
        resultMap.put("agg", ageMap);

        //4，返回最终结果

        return resultMap;
    }
}
