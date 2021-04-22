package com.atguigu.es.redad;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class read {
    public static void main(String[] args) throws IOException {
        // TODO: 2021/4/20 获取jest连接
        JestClientFactory factory = new JestClientFactory();
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        factory.setHttpClientConfig(httpClientConfig);
        JestClient jestClient = factory.getObject();
        // TODO: 读取数据
        Search build = new Search.Builder("{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"sex\": \"男\"\n" +
                "        }\n" +
                "      },\"must\": [\n" +
                "        {\n" +
                "          \"match\": {\n" +
                "            \"favo\": \"球\"\n" +
                "            \n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"groupByClass\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"class_id\",\n" +
                "        \"size\": 10\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"groupByAge\": {\n" +
                "          \"max\": {\n" +
                "            \"field\": \"age\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"from\": 0,\n" +
                "  \"size\": 2\n" +
                "}"
        )
                .addIndex("student123")
                .addType("_doc")
                .build();
        SearchResult result = jestClient.execute(build);
        // TODO: 解析数据
        System.out.println("命中" + result.getTotal() + "条数据");

        // TODO: 获取具体数据
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println("_index:" + hit.index);
            System.out.println("_type:" + hit.type);
            System.out.println("_id:" + hit.id);
            Map sou = hit.source;
            for (Object o : sou.keySet()) {
                System.out.println(o+":"+sou.get(o));

            }
            System.out.println("********************************************");
            // TODO: 获取聚合数据
            MetricAggregation aggregations = result.getAggregations();
            TermsAggregation groupByClass = aggregations.getTermsAggregation("groupByClass");
            // TODO: 解析班级聚合数据
            List<TermsAggregation.Entry> buckets = groupByClass.getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                System.out.println("key: "+":"+bucket.getKey());
                System.out.println("doc_count"+bucket.getCount());
                // TODO: 解析嵌套的年龄聚合组数据
                MaxAggregation groupByAge = bucket.getMaxAggregation("groupByAge");
                System.out.println("value: "+groupByAge.getMax());
            }
            jestClient.shutdownClient();

        }
    }
}
