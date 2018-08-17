package com.hyr.hbase.table;

import com.hyr.hbase.Conf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/*******************************************************************************
 * @date 2018-08-14 下午 3:59
 * @author: huangyueran
 * @Description:
 ******************************************************************************/
public class TableOperation {
    private final Logger log = LoggerFactory.getLogger(TableOperation.class);

    // HBase 基本 API
    private Connection connection; // HTablePool被弃用 因为线程不安全。使用Connection代替，只要保证全局是同一个Connection就可以。
    private Admin admin;
    private Table table;

    TableName tableName = TableName.valueOf(Conf.TEST_TABLE);

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
     * @category 创建表
     */
    @Test
    public void createTable() throws IOException {
        // 判断表是否存在 如果存在 删除表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        HTableDescriptor desc = new HTableDescriptor(tableName); // 创建表描述

        // 创建列族 1-3个列族最佳
        HColumnDescriptor family = new HColumnDescriptor("cf1");
        family.setBlockCacheEnabled(true); // 打开读缓存
        family.setInMemory(true); // 打开写缓存
        family.setMaxVersions(1); // 最大版本数

        desc.addFamily(family);

        admin.createTable(desc);
    }


    /**
     * 获取表的状态信息
     *
     * @throws IOException
     */
    @Test
    public void getTableStatus() {
        // 判断表是否存在 如果存在 删除表
        try {
            boolean result = admin.isTableAvailable(tableName);// 判断表物理上是否存在 如果被disable 也会返回true
            log.info("isTableAvailable(tableName):{}", result);

            result = admin.isTableDisabled(tableName);
            log.info("isTableDisabled(tableName):{}", result);

            result = admin.tableExists(tableName);
            log.info("tableExists(tableName):{}", result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取表结构 描述
     */
    @Test
    public void getTableDescribe() {
        // 判断表是否存在 如果存在 删除表
        try {
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
            log.info("tableDescriptor:{}", tableDescriptor.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 修改表
     */
    @Test
    public void alterTable() {
        // 判断表是否存在 如果存在 删除表
        try {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor("newcloumn1");
            // add cloumn family
            admin.addColumn(tableName, columnDescriptor);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * delete cloumn family
     */
    @Test
    public void deleteTableCF() {
        // 判断表是否存在 如果存在 删除表
        try {
            // add cloumn family
            admin.deleteColumn(tableName, "newcloumn1".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * disable Table and enable Table
     */
    @Test
    public void disableAndEnableTable() throws IOException {
        Boolean bool = admin.isTableDisabled(tableName);
        log.info("table is disable ? " + bool);

        if (!bool) {
            admin.disableTable(tableName);
            log.info("table is disable");
        }

        admin.enableTable(tableName);
        log.info("table is enable");

        bool = admin.isTableDisabled(tableName);
        log.info("table is disable ? " + bool);
    }

    /**
     * drop Table
     */
    @Test
    public void dropTable() throws IOException {
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    /**
     * truncate table : 禁止、删除表，并重新创建一个表
     */
    @Test
    public void truncateTable() throws IOException {
        TableName testyufenqu = TableName.valueOf("testyufenqu");
        if (admin.tableExists(testyufenqu)) {
            boolean isSaveRegion = false; // 是否保留预分区region,如果为false,预分区会丢失。
            admin.disableTable(testyufenqu);
            admin.truncateTable(testyufenqu, isSaveRegion);
        }
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
