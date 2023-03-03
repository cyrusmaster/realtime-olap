package com.hzsun.zbp.mc.utils;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TimeUtils {


    // 今日



    // stream kafka source
    public static long getTodayZeroPointL() {
        long now = System.currentTimeMillis();
        long daySeconds = 1000 * 60 * 60 * 24;
        // 东八区加八小时
        // 获取零点时间戳
        long t = now - (now + 8 * 60 * 60 * 1000) % daySeconds;

        return t;
    }
    //kafka从昨天0点开始读 因为有迟到数据
    public static long getYestZeroPointL() {
        long now = System.currentTimeMillis();
        long daySeconds = 1000 * 60 * 60 * 24;
        // 东八区加八小时
        // 获取零点时间戳
        long t = now - (now + 8 * 60 * 60 * 1000) % daySeconds;

        return t-daySeconds;
    }

    // 今日
    public static String getTodayZeroPointS() {
        long now = System.currentTimeMillis();
        long daySeconds = 1000 * 60 * 60 * 24;
        // 东八区加八小时
        // 获取零点时间戳
        long t = now - (now + 8 * 60 * 60 * 1000) % daySeconds;

        String s = String.valueOf( now - (now + 8 * 60 * 60 * 1000) % daySeconds);
        return s;
    }

    // 昨天
    public static String getYest() {

        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE,-1);
        //long now = System.currentTimeMillis();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        //simpleDateFormat.format(now);

        return  simpleDateFormat.format(calendar.getTime());
    }


    // -8h timestamp  map调用
    public static Long getTrueTimestamp(String s){

        //before
        Long aLong = Long.valueOf(s);
        //after
        long eightSeconds = 1000 * 60 * 60 * 8;
        Long now = aLong - eightSeconds;

        return  now;
    }








    public static void main(String[] args) {

        System.out.println(getTodayZeroPointL());




    }


}
