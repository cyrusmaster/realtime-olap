package com.hzsun.zbp.ord.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class TimeUtils {

    public static long getTodayZeroPoint0() {
        long now = System.currentTimeMillis();
        long daySeconds = 1000 * 60 * 60 * 24;
        // 获取零点时间戳
        long t = now - (now) % daySeconds;
        return t;
    }


    // stream kafka source
    public static long getTodayZeroPointL() {
        long now = System.currentTimeMillis();
        long daySeconds = 1000 * 60 * 60 * 24;
        // 东八区加八小时
        // 获取零点时间戳
        long t = now - (now + 8 * 60 * 60 * 1000) % daySeconds;
        return t;
    }


    public static String getTodayZeroPointS() {
        long now = System.currentTimeMillis();
        long daySeconds = 1000 * 60 * 60 * 24;
        // 东八区加八小时
        // 获取零点时间戳
        long t = now - (now + 8 * 60 * 60 * 1000) % daySeconds;

        String s = String.valueOf(now - (now + 8 * 60 * 60 * 1000) % daySeconds);
        return s;
    }


    public static String getYest() {


        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);


        //long now = System.currentTimeMillis();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        //simpleDateFormat.format(now);


        return simpleDateFormat.format(calendar.getTime());
    }


    public static String formatDate(String inputDate) throws ParseException {

        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.CHINA);
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.ENGLISH);
        //默认 2022-05-07 06:13:13 早8
        inputFormat.setTimeZone(TimeZone.getTimeZone("GMT-08:00"));
        Date date = inputFormat.parse(inputDate);
        return outputFormat.format(date);

    }


    public static void main(String[] args) throws ParseException {


        String s = "2022-05-06T01:25:23Z";

        //System.out.println(formatDate(s));


        //System.out.println(getTodayZeroPointL());

        System.out.println(getTodayZeroPointS());


    }


}
