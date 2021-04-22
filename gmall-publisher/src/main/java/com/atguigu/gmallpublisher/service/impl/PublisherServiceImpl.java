package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;

    @Override
    public Integer getDauToTal(String date) {

        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map<String, Long> getDauHour(String date) {
        // 获取数据 => 获取mapper传过来的数据
        List<Map> mapList = dauMapper.selectDauTotalHourMap(date);
        // 创建Map集合存放结果数据
        HashMap<String, Long> result = new HashMap<>();
        // 解析mapper传过来的数据,并封装到新的Mapper集合中去
        for (Map map : mapList) {
            result.put(map.get("LH").toString(),(Long) map.get("CT"));
        }
        return result;
    }
}
