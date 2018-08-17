package com.hyr.hbase.data;

import com.hyr.hbase.Conf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/*******************************************************************************
 * @date 2018-08-15 下午 5:17
 * @author: huangyueran
 * @Description: 预分区
 ******************************************************************************/
public class DataPrePartition {

    private final Logger log = LoggerFactory.getLogger(DataPrePartition.class);

    // HBase 基本 API
    private Connection connection; // HTablePool被弃用 因为线程不安全。使用Connection代替，只要保证全局是同一个Connection就可以。
    private Admin admin;
    private Table table;

    TableName tableName = TableName.valueOf(Conf.TEST_PRE_PARTITION_TABLE);

    @Before
    public void begin() throws IOException {
        Configuration conf = new Configuration();
        // 指定 Zookeeper集群
        conf.set("hbase.zookeeper.quorum", Conf.HOST);

        connection = ConnectionFactory.createConnection(conf);

        table = connection.getTable(tableName);

        admin = connection.getAdmin();
    }

    /**
     * @throws IOException
     * @category 预分区插入数据
     */
    @Test
    public void insertByRegion() throws IOException {
        table.put(batchPut());
    }

    private List<Put> batchPut() {
        List<Put> list = new ArrayList<Put>();
        for (int i = 1; i <= 10000; i++) {
            String rowkeyStr = getRandomNumber() + "-" + System.currentTimeMillis() + "-" + i;
            log.info("rowkey:{}", rowkeyStr);
            byte[] rowkey = Bytes.toBytes(rowkeyStr);
            Put put = new Put(rowkey);
            put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zs" + i));
            list.add(put);
        }
        return list;
    }

    private String getRandomNumber() {
        String ranStr = new Random().nextInt(11) + "";
        return ranStr;
    }


    @After
    public void end() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
