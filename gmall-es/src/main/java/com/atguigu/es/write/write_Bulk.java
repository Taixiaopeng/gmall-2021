package com.atguigu.es.write;

import com.atguigu.es.bean.Movie2;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class write_Bulk {
    public static void main(String[] args) throws IOException {
        // TODO 1. 创建Jest客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();
        // TODO 2. 设置连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        // TODO: 3. 获取连接
        JestClient jestClient = jestClientFactory.getObject();
        // TODO: 2021/4/20 批量写入数据
        Movie2 movie3 = new Movie2("1003", "一路向西");
        Movie2 movie4 = new Movie2("1004", "一路向北");
        Movie2 movie5 = new Movie2("1005", "一路向东");
        Movie2 movie6 = new Movie2("1006", "一路向南");
        Movie2 movie7 = new Movie2("1007", "一路向上");
        Index index = new Index.Builder(movie3).id("1003").build();
        Index index1 = new Index.Builder(movie4).id("1004").build();
        Index index2 = new Index.Builder(movie5).id("1005").build();
        Index index3 = new Index.Builder(movie6).id("1006").build();
        Index index4 = new Index.Builder(movie7).id("1006").build();

        Bulk build = new Bulk.Builder()
                .defaultIndex("movie2")
                .defaultType("_doc")
                .addAction(index)
                .addAction(index4)
                .addAction(index1)
                .addAction(index2)
                .addAction(index3)
                .build();

        jestClient.execute(build);
        jestClient.shutdownClient();

    }
}
