package com.hzsun.zbp.ord.utils;

import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class getHiveCatalog {


    public static String name = "hive_big";
    public static String defaultDatabase = "jinhua_app";
    //public static String hiveConfDir = "D:\\project\\flink\\code\\东阳\\食品安全\\dysa-stream\\src\\main\\resources\\conf";
    public static String hiveConfDir = "src/main/resources/";
    //public static String hiveConfDir = "resources/";

    public static HiveCatalog getHiveCatalog() throws Exception {
        HiveCatalog hiveCatalog = null;

        try {
            hiveCatalog = UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<HiveCatalog>() {
                @Override
                public HiveCatalog run() throws Exception {
                    return new HiveCatalog(name, defaultDatabase, hiveConfDir);
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return hiveCatalog;


    }

}
