package com.hyr.hbase.Table;

import com.hyr.hbase.Conf;
import org.apache.hadoop.conf.Configuration;
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
 * @date 2018-08-14 下午 5:02
 * @author: huangyueran
 * @Description:
 ******************************************************************************/
public class HBaseAdmin {
    private final Logger log = LoggerFactory.getLogger(HBaseAdmin.class);

    // HBase 基本 API
    private Connection connection; // HTablePool被弃用 因为线程不安全。使用Connection代替，只要保证全局是同一个Connection就可以。
    private Admin admin;

    @Before
    public void begin() throws IOException {
        Configuration conf = new Configuration();
        // 指定 Zookeeper集群
        conf.set("hbase.zookeeper.quorum", Conf.HOST);

        connection = ConnectionFactory.createConnection(conf);

        admin = connection.getAdmin();
    }

    /**
     * shutdown hbase
     */
    @Test
    public void shutDownHbase() {
        try {
            log.info("shuttint down hbase ......");
            admin.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void end() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
