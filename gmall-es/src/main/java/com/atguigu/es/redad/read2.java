package com.atguigu.es.redad;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class read2 {
    public static void main(String[] args) {
//        1. 创建jest客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();
//        2. 设置连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);
//        3. 获取连接
        JestClient jestClient = jestClientFactory.getObject();
        // TODO: 读取数据
        // todo 1. 编辑查询语句
//        todo 相当于查询语句最外围的{}
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//------------------querry-----------------------------------------
        // TODO: 相当于"bool"
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        // TODO: 相同于term
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex", "男");
        // TODO 相当于“filter”
        boolQueryBuilder.filter(termQueryBuilder);
        // TODO 相当于 “query”
        sourceBuilder.query(termQueryBuilder);
        // TODO: 相当于“match”
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo", "球");
        // TODO 相当于 “must”
    boolQueryBuilder.must(termQueryBuilder);
// --------------------agg----------------------------------------
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupByClass").field("class_id").size(10);
        // TODO 相当于 “max”
        MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("groupByAge").field("age");
        jestClient.shutdownClient();
    }
}
