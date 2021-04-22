package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    //    todo 查询日活总数据的抽象方法，实现类在resource包下mapper中DauMapper.xml
    public Integer selectDauTotal(String date);

    // TODO: 2021/4/18 查询日活分时数据抽象方法
    public List<Map> selectDauTotalHourMap(String date);

}
