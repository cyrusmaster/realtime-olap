package com.hzsun.zbp.ord;


import com.hzsun.zbp.ord.constant.Kafka;
import com.hzsun.zbp.ord.utils.KerberosAuth;
import com.hzsun.zbp.ord.utils.TimeUtils;
import com.hzsun.zbp.ord.utils.getHiveCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;


/**
 * REMARK   维表视图处理  每日更新
 *
 * @author cyf
 * @className DimJob
 * @date 2022/5/9 20:04
 */
public class DimJob {


    public static void main(String[] args) throws Exception {


        System.setProperty("log4j.skipJansi", "false");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        new KerberosAuth().kerberosAuth(true);
        HiveCatalog hive = getHiveCatalog.getHiveCatalog();
        //注册Catalog （和连接信息一致）
        tenv.registerCatalog("hive_big", hive);
        // 使用注册的Catalog(必须存在) ，不使用的话查不到数据
        tenv.useCatalog("hive_big");
        tenv.useDatabase("jinhua_app");


        //埋点
        System.out.println(TimeUtils.getYest());


        //mark mc

        //1 sch
        Table table = tenv.sqlQuery("select a.id id, a.school_id school_id, b.name name\n" +
                "from " + "tods_" + "ops_catering" + "_d_" + TimeUtils.getYest() + " a\n" +
                "         left join  " + "tods_" + "ops_school" + "_d_" + TimeUtils.getYest() + " b\n" +
                "                   on b.id = a.school_id\n" +
                "where b.name is not null");

        //tenv.toRetractStream(table, Row.class).print();

        tenv.createTemporaryView("hive_sch", table);


        //tenv.executeSql("drop table dim_sch ");
        tenv.executeSql("CREATE TABLE  if not exists  dim_sch  (" +
                "  id  string  ," +
                "  school_id  string  ," +
                "  name string , " +
                "  PRIMARY KEY (id) NOT ENFORCED " +
                ") " +
                "WITH (" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = '" + Kafka.DIM_SCH + "',\n" +
                "    'properties.bootstrap.servers' = '" + Kafka.KAFKA_BROKER_LIST + "'  ,\n" +
                " 'key.format' = 'csv',  \n" +
                " 'value.format' = 'csv')");

        tenv.executeSql("INSERT INTO  dim_sch  SELECT id,school_id, name FROM hive_sch");


        //2 staff   按学校 从业人数 （计算所有需要 sum）


        Table table2 = tenv.sqlQuery("select catering_id, CAST( count(*) as BIGINT )  staff_num\n" +
                "from  " + "tods_" + "ops_safeter" + "_d_" + TimeUtils.getYest() + " \n" +
                "where del_flag = '0' \n" +
                "  and (job = '1' or job = '2' or job = '4' or job = '5')\n" +
                "  and status = '1' \n" +
                "  and catering_id in (\n" +
                "    select id\n" +
                "    from " + "tods_" + "ops_catering" + "_d_" + TimeUtils.getYest() + "  \n" +
                "    where superior_id = '4f69898aa6ec9e506ec4bd4526a7cfb7'\n" +
                "      and status = '1' \n" +
                "      and del_flag = '0' \n" +
                "                      )" +
                "  group by catering_id");
        tenv.createTemporaryView("hive_staff", table2);

        //tenv.toRetractStream(table2, Row.class).print();


        // 消息被删除策略清空后  需要先drop
        // drop 第一次可以 第二天就会报错找不到
        //
        //tenv.executeSql("Drop Table dim_staff");
        tenv.executeSql("CREATE TABLE  if not exists  dim_staff (" +
                "  catering_id  STRING  ," +
                "  staff_num  BIGINT   , " +
                "  PRIMARY KEY (catering_id) NOT ENFORCED " +
                ") " +
                "WITH (" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = '" + Kafka.DIM_STAFF + "',\n" +
                "    'properties.bootstrap.servers' = '" + Kafka.KAFKA_BROKER_LIST + "'  ,\n" +
                " 'key.format' = 'csv',  \n" +
                " 'value.format' = 'csv')");

        tenv.executeSql("INSERT INTO  dim_staff  SELECT catering_id, staff_num  FROM hive_staff");


        //mark iot、ord

        //tenv.executeSql("drop table dim_sch_p ");
        tenv.executeSql("CREATE TABLE  if not exists dim_sch_p   (" +
                "  restaurant_id  string  ," +
                "  organ_no  string  ," +
                "  organ_name string , " +
                "  PRIMARY KEY (restaurant_id) NOT ENFORCED " +
                ") " +
                "WITH (" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = '" + Kafka.SCH_PRINT_DIM_TOPIC + "',\n" +
                "    'properties.bootstrap.servers' = '" + Kafka.KAFKA_BROKER_LIST + "'  ,\n" +
                "    'key.format' = 'csv',  \n" +
                "    'value.format' = 'csv')");

        tenv.executeSql("INSERT INTO  dim_sch_p  SELECT restaurant_id,organ_no,organ_name FROM tdwd_base_restaurant_info_d_" + TimeUtils.getYest() + " \n");
        //tenv.executeSql("INSERT INTO  dim_sch_p  SELECT restaurant_id,organ_no,organ_name FROM tdwd_base_restaurant_info_d_20220515");

        //————————————————————————————————————————————————2——————————————————————————————————————————————————


        //tenv.executeSql("drop table dim_sch_s ");
        tenv.executeSql("CREATE TABLE  if not exists dim_sch_s   (" +
                "  id  string  ," +
                "  organ_no  string  ," +
                "  organ_name string , " +
                "  PRIMARY KEY (id) NOT ENFORCED " +
                ") " +
                "WITH (" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = '" + Kafka.SCH_SAMPLE_DIM_TOPIC + "',\n" +
                "    'properties.bootstrap.servers' = '" + Kafka.KAFKA_BROKER_LIST + "'  ,\n" +
                "    'key.format' = 'csv',  \n" +
                "    'value.format' = 'csv')"
        );

        tenv.executeSql("INSERT INTO  dim_sch_s  \n" +
                "SELECT   a.id,b.organ_no,b.organ_name \n" +
                "from  tods_ops_safeter_d_" + TimeUtils.getYest() + " a  \n" +
                "left join  tdwd_base_restaurant_info_d_" + TimeUtils.getYest() + " b \n" +
                "on b.restaurant_id = a.catering_id  \n" +
                "where b.organ_no is not null");


        //tenv.createTemporaryView("hive_view1",table);
        //Table table = tenv.sqlQuery(" SELECT count(*)  FROM tdwd_base_restaurant_info_d_20220517 \n");
        //Table table = tenv.sqlQuery(" SELECT  restaurant_id,organ_no,organ_name FROM tdwd_base_restaurant_info_d_"+ TimeUtils.getYest()+" \n");
        //tenv.toChangelogStream(table).print();
        //env.execute("dim");
    }

}
