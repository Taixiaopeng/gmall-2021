package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {
    // TODO: 2021/4/18 日活数据总数接口 => service

    Integer getDauToTal(String date);
// TODO: 2021/4/18 日活数据分时抽象方法
    public Map<String, Long> getDauHour(String date);
}
