# 本地演示 -hive维表

## 说明

来源

`Debezium mysql binlog`

下游

`Topic1 : realtime_ads.ord.top`

`Topic2 : realtime_ads.ord.list_cdc`

`Topic3 : realtime_ads.ord.list_all`

### 版本改动

* [0513 ]  ord_distribution 对该流过滤 去掉删除数据
* [0513 ] executeRate 指标分母 计算修改
* [0513 ]  【计划】 修改 增量 ord_distribution ->  全量 ord_distribution_cur 实时关联 配送单数两表增量关联 -> 直接读 mysql 全量变化表 判断 其中时间
* [0513 ]  修复 率 除法报错 numberformatexceptio BUG
* [0516 ]  修复 所有率指标 有可能为null & 以及学校 id名字 为null 过滤
* [0516 ]  修改维表topic 清理策略为压缩 修复过期删除无数据问题 （drop 后 create 待解决）
* [0517 ]  修复 执行率 分母 totalnum 统计口径 count（1） -> count（detail.id）
* [0517 ]  修复 执行率 分子 转成 double 前 不能为null 异常 ，最好先对 分子判断 if
* [0902 ]   修改认证用户名
* [0905 ]   去除log4j2日志依赖


### 当前隐患

* 测试阶段 （针对当天） 每次执行刷入dw 导致数据冗余

       // mark  所有维表
       //晨检
       //tods_ops_safeter_d_
       //tods_ops_school_d_
       //tods_ops_catering_d_
       //取样
       //tdwd_base_restaurant_info_d_
       //tods_ops_safeter_d_
       //查验
       //tdwd_base_restaurant_info_d_



