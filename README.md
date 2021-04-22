# spark实时项目
1. 父工程gmall-202
    + 此模块的主要功能:管理依赖
2. 子模块 gmall-common
    + 编写整个项目中公用的常量，工具类
3. 子模块 gmall-realtime
   + 实时处理的核心
   
## 日活需求分析
1. 消费KAFKA中的数据 KAFKA_GMALL_START_UP
2. 利用Redis过滤当日已经计入的日活设备
3. 把每批次新增的当日日活信息保存到HBase中
4. 从HBase中查询除数据，发布成数据接口，通过可视化工程调用

### 具体实现
1. 数据来源 kafka -> startUp topic中的数据 启动日志
2. 业务处理 统计日活 去重 mid
   + 先做批次间去重  redis的Set集合
   + 再做批次内去重  groupByKey -->对时间排序(sort) -->取第一个数据
   + 结论:先用去重能力强的，再用去重能力弱的可以避免数据量太大的压力。
3. 数据精准一次性消费(数据不丢-->手动维护偏移量，数据不重复 -> 幂等性)
   + 数据去向 
   + Hbase 存明细数据
     + 优点:灵活性高
     + 缺点:数据量大，占用磁盘 
   + 数据存至mysql 存结果数据 --> 开启事务 精准一次性消费
      + 优点:
      + 缺点:
#### 需求1:
1. 筛选出用户最基本的活跃行为（打开应用程序 是否有start项，打开第一个页面(page项中，没有last_page_id)）
2. 去重操作，以什么字段为准进行去重(用户id,ip,mid) 用Redis存储已访问用户列表
    + 用户id可以作假。
    + mid是设备的内置号,以设备为单位进行统计。
3. 得到用户的日活明细后
    + 直接保存到某个数据库中，使用OLAP进行统计汇总
    + 进行统计汇总之后再保存。
    
4. 发布展示 
    + 自定义可视化界面 包括datav 需要制作数据服务接口 写代码
    + BI工具 可以直接读取数据库 通过配置完成数据展示 配置就行
5. 去重,以什么字段为准进行去重(mid),用redis来存储已访问列表 什么数据对象来存储列表
    + 提取对象mid
    + 查询mid 提取对象中的mid
    + 设计定义: 已访问列表
      + redis存储已访问列表
        + redis type? key? (filed score) value? expire？ 读api? 写api？
        + String(每个mid-每天 成为一个key,极端情况下利于分布式)         setnx mid:0101:2020-01-22 1  keys mid*2021-01-22
        + Set(把当天已访问存入 一个set key )                         sadd dau:2021-01-22 mid_0101; smembers dau:2020-01-22
        + key？dau:[2020-01-22] value? mid expire expire？ 24小时；读写api sadd 自带判断是否存在
    + 如果有 过滤掉该对象 如果没有则保留 //插入到该列表中

6. 日活数据查询接口
7. springboot分层
    + Controller 接受请求   返回请求
    + Service    处理请求   处理数据
    + DAO        操作数据   获取数据  mapper(mybatis 通过xml文件直接写SQL实现JDBC相关的功能 )
    