package com.atguigu.es.write;


import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class write {
    public static void main(String[] args) throws IOException {
        JestClientFactory Factory = new JestClientFactory();
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        Factory.setHttpClientConfig(httpClientConfig);
        JestClient jestClient= Factory.getObject();
        // TODO: 2021/4/20 构建ES插入数据对象
        Index index = new Index.Builder(" \"id\": \"001\",\n" +
                "    \"name\": \"红红\",\n" +
                "    \"sex\": 0,\n" +
                "    \"birth\": \"1999-01-01\"").index("movie_test1").type("_doc").id("001").build();
        jestClient.execute(index);
        jestClient.shutdownClient();

    }
}
