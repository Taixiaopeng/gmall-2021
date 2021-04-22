package com.atguigu.gmallpublisher.controlle;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class Controller {
    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String realtimeTotal(@RequestParam("date") String date) {
        // TODO: 2021/4/18 1.创建List<Map>集合用来存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        // TODO: 2021/4/18 2.从service层获取处理好的数据
        Integer dauToTal = publisherService.getDauToTal(date);

        // TODO: 2021/4/18 3.将数据封装到结果集合中
        HashMap<String, Object> dauMap = new HashMap<>();
//        todo 3.1 将新增日活数据封装到map中
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauToTal);

//       todo 3.2 将新增设备的数据封装到设备中
        HashMap<String, Object> devMap = new HashMap<>();
        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", "233");
        // TODO: 2021/4/18 将map集合放到list集合中
        result.add(devMap);
        result.add(dauMap);
        // TODO: 2021/4/18 返回数据
        return JSONObject.toJSONString(result);

    }
@RequestMapping("realtime-hours")
    public String realtimeHours(@RequestParam("id") String id, @RequestParam("date") String date) {
        //todo 根据传进来的日期 获取昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        // TODO: 2021/4/18 获取server层数据

        Map<String, Long> todatHourDau = publisherService.getDauHour(date);
        //todo 查询昨天的数据
        Map<String, Long> yesterdayHourDau = publisherService.getDauHour(yesterday);
        // todo 创建map集合，存放结果数据
        HashMap<String, Map> result = new HashMap<>();
        // TODO: 2021/4/18

        result.put("yesterday", yesterdayHourDau);
        result.put("today", todatHourDau);

        return JSONObject.toJSONString(result);
    }

}
