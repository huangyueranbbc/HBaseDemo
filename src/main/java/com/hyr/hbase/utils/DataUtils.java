package com.hyr.hbase.utils;

import org.apache.hadoop.hbase.client.Put;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/*******************************************************************************
 * @date 2018-08-14 下午 4:19
 * @author: huangyueran
 * @Description: 数据工具类
 ******************************************************************************/
public class DataUtils {

    public static List<Put> getPuts() {
        List<Put> puts = new ArrayList<Put>(); // 装入集合一起插入记录

        for (int i = 0; i < 10; i++) {
            String rowkey;
            String phoneNum = getPhoneNum("183");

            // 100条通话记录
            for (int j = 0; j < 100; j++) {
                String phoneDate = getData("2016");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
                try {
                    long dateLong = sdf.parse(phoneDate).getTime();
                    // 降序
                    rowkey = phoneNum + (Long.MAX_VALUE - dateLong);

                    Put put = new Put(rowkey.getBytes());
                    put.add("cf1".getBytes(), "type".getBytes(), (new Random().nextInt(2) + "").getBytes());
                    put.add("cf1".getBytes(), "time".getBytes(), phoneDate.getBytes());
                    put.add("cf1".getBytes(), "dest".getBytes(), getPhoneNum("159").getBytes());

                    puts.add(put);
                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }
        }

        return puts;
    }

    /**
     * 随机生成手机号
     *
     * @param prefix
     * @return
     */
    public static String getPhoneNum(String prefix) {
        return prefix + String.format("%08d", new Random().nextInt(99999999));
    }

    /**
     * 随机生成时间
     *
     * @param year
     * @return
     */
    public static String getData(String year) {
        Random r = new Random();
        return year + String.format("%02d%02d%02d%02d%02d",
                new Object[]{r.nextInt(12) + 1, r.nextInt(28) + 1, r.nextInt(60), r.nextInt(60), r.nextInt(60)});
    }
}
