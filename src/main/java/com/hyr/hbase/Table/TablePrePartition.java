package com.hyr.hbase.Table;

import com.hyr.hbase.Conf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/*******************************************************************************
 * @date 2018-08-15 下午 5:17
 * @author: huangyueran
 * @Description: 预分区
 ******************************************************************************/
public class TablePrePartition {

    private final Logger log = LoggerFactory.getLogger(TablePrePartition.class);

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
     * 预分区 创建表
     */
    @Test
    public void createPrePartitionTable() {
        try {
            List<String> columnFamily = new ArrayList<String>();
            columnFamily.add("info");

            if (admin.tableExists(tableName)) {
                System.out.println("table has exist!" + tableName);
            } else {
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                for (String cf : columnFamily) {
                    tableDescriptor.addFamily(new HColumnDescriptor(cf));
                }
                byte[][] splitKeys = getSplitKeys();
                admin.createTable(tableDescriptor, splitKeys);//指定splitkeys
                log.info("create table:{} success. columnFamily:{}", tableName, columnFamily.toString());
            }
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private byte[][] getSplitKeys() {
        String[] keys = new String[]{"1|", "2|", "3|", "4|", "5|",
                "6|", "7|", "8|", "9|"};
        byte[][] splitKeys = new byte[keys.length][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
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
