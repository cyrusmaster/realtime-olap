


# 实时OLAP - 验收

### 说明

source

`ord_distribution` `ord_order_detail`


sink 

`Topic1 : realtime_ads.ord.top` 

`Topic2 : realtime_ads.ord.list_cdc`

`Topic3 : realtime_ads.ord.list_all`




### 版本改动

* [0513 下午]  ord_distribution 对该流过滤 去掉删除数据  
* [0513 晚上] executeRate  指标分母  计算修改
* [0513 晚上]  【计划】   修改  增量 ord_distribution  ->  全量 ord_distribution_cur 实时关联  配送单数两表增量关联 -> 直接读 mysql 全量变化表 判断 其中时间
* [0513 晚上]  修复 率 除法报错 numberformatexceptio BUG
* [0516 上午]  修复 所有率指标 有可能为null & 以及学校 id名字 为null 过滤
* [0516 下午]  修改维表topic 清理策略为压缩  修复过期删除无数据问题 （drop 后 create 待解决）
* [0517 上午]  修复 执行率 分母 totalnum 统计口径  count（1） -> count（detail.id）
* [0517 下午]  修复 list执行率 分子 转成 double 前 不能为null 异常 ，最好先对 分子判断 if 
* [0520 上午]  修复 top执行率 分子 转成 double 前 不能为null 异常 ，最好先对 分子判断 if
* [0616 下午]  修改 独立表ETL任务，修改新的表模型字段
* [0617 下午]  修改 top list 统计口径（已与楼上讨论核对数据校准） 










