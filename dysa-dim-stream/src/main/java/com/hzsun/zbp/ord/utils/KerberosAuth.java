package com.hzsun.zbp.ord.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class KerberosAuth {


    //public class KerberosAuth {

    public void kerberosAuth(Boolean debug) {
        try {
            //System.setProperty("java.security.krb5.conf", "D:\\project\\flink\\code\\东阳\\食品安全\\dysa-stream\\src\\main\\resources\\conf\\krb5.conf");
            System.setProperty("java.security.krb5.conf", "src/main/resources/krb5.conf");
            //System.setProperty("java.security.krb5.conf", "resources/krb5.conf");

            // fixme
            System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");
            if (debug) {
                System.setProperty("sun.security.krb5.debug", "true");
            }
            ;
            Configuration conf = new Configuration();
            conf.set("hadoop.security.authentication", "Kerberos");

            UserGroupInformation.setConfiguration(conf);

            //UserGroupInformation.loginUserFromKeytab("hive/hive@HADOOP.COM", "D:\\project\\flink\\code\\东阳\\食品安全\\dysa-stream\\src\\main\\resources\\conf\\hive.keytab");
            //0902 注释  修改用户名
            //UserGroupInformation.loginUserFromKeytab("krbtgt/hive@HADOOP.COM", "src/main/resources/hive.keytab");
            UserGroupInformation.loginUserFromKeytab("hive/hive@HADOOP.COM", "/var/keytab/hive.keytab");
            //UserGroupInformation.loginUserFromKeytab("hive/hive@HADOOP.COM", "resources/hive.keytab");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}