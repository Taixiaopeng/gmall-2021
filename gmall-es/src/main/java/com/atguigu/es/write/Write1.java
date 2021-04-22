package com.atguigu.es.write;

import com.atguigu.es.bean.Movie2;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;


public class Write1 {
    public static void main(String[] args) throws IOException {
        // TODO: 1. 获取jest客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();
        // TODO 2. 设置连接ES的地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        // TODO: 3. 获取连接
        JestClient jestClient = jestClientFactory.getObject();
        // TODO: 4. 操作数据，往ES中写入数据
        Movie2 movie2 = new Movie2("1003", "一路向北");
        Index index = new Index.Builder(movie2)
                .index("movie2")
                .type("_doc")
                .id("1003")
                .build();
        jestClient.execute(index);
        // TODO: 最后一步 关闭连接
        jestClient.shutdownClient();

    }
}
