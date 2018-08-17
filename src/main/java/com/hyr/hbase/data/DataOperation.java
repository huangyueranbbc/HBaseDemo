package com.hyr.hbase.data;

import com.hyr.hbase.Conf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import static com.hyr.hbase.utils.DataUtils.getPuts;

/*******************************************************************************
 * @date 2018-08-14 下午 3:59
 * @author: huangyueran
 * @Description:
 ******************************************************************************/
public class DataOperation {
    private final Logger log = LoggerFactory.getLogger(DataOperation.class);

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
     * @category 单条插入数据
     */
    @Test
    public void insert() throws IOException {
        byte[] rowkey = ("155623184659223370584996432807".getBytes());
        Put puts = new Put(rowkey);
        puts.addColumn("cf1".getBytes(), "dest".getBytes(), "15562318465".getBytes());
        puts.addColumn("cf1".getBytes(), "type".getBytes(), "1".getBytes());
        puts.addColumn("cf1".getBytes(), "time".getBytes(), "2015-09-09 16:55:29".getBytes());
        table.put(puts);
    }


    /**
     * 异步批量插入
     *
     * @return long   返回执行时间
     * @throws IOException
     */
    @Test
    public void asynBatchInsert() throws Exception {
        long startTime = System.currentTimeMillis();
        List<Put> puts = getPuts();
        // 批量插入异常监听
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    System.out.println("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(tableName)
                .listener(listener);
        params.writeBufferSize(5 * 1024 * 1024);

        final BufferedMutator mutator = connection.getBufferedMutator(params);
        try {
            mutator.mutate(puts);
            mutator.flush();
        } finally {
            mutator.close();
        }
        log.info("cost time:{}ms", System.currentTimeMillis() - startTime);
    }

    /**
     * 阻塞试批量插入
     *
     * @return long   返回执行时间
     * @throws IOException
     */
    @Test
    public void batchInsert() {
        long startTime = System.currentTimeMillis();
        List<Put> puts = getPuts();
        // 插入数据
        try {
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("cost time:{}ms", System.currentTimeMillis() - startTime);
    }

    /**
     * scan 扫描表
     */
    @Test
    public void scan() throws ParseException {
        log.info("start scan DB...");

        // 查询 18399123150 二月份 通话账单
        Scan scan = new Scan();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

        String startRowkey = "18399123150" + (Long.MAX_VALUE - sdf.parse("20160201000000").getTime());
        String stopRowkey = "18399123150" + (Long.MAX_VALUE - sdf.parse("20160101000000").getTime());
        scan.setStartRow(startRowkey.getBytes());
        scan.setStopRow(stopRowkey.getBytes());

        try {
            ResultScanner resultScanner = table.getScanner(scan);
            log.info("resultScanner:{}", resultScanner);

            for (Result result : resultScanner) {
                String row = Bytes.toString(result.getRow());
                log.info("rowkey:{}", row);
                Cell c1 = result.getColumnLatestCell("cf1".getBytes(), "type".getBytes());
                log.info(new String(CellUtil.cloneValue(c1)));
                Cell c2 = result.getColumnLatestCell("cf1".getBytes(), "dest".getBytes());
                log.info(new String(CellUtil.cloneValue(c2)));
                Cell c3 = result.getColumnLatestCell("cf1".getBytes(), "time".getBytes());
                log.info(new String(CellUtil.cloneValue(c3)));
                log.info("===============================");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 单条查询
     * @throws IOException
     */
    @Test
    public  void select() throws IOException {
        String rowkey = "155623184659223370584996432807";
        Get get = new Get(rowkey.getBytes());

        Result result = table.get(get);

        Cell c1 = result.getColumnLatestCell("cf1".getBytes(), "type".getBytes());
        log.info(new String(c1.getValue()));
        Cell c2 = result.getColumnLatestCell("cf1".getBytes(), "dest".getBytes());
        log.info(new String(c2.getValue()));
        Cell c3 = result.getColumnLatestCell("cf1".getBytes(), "time".getBytes());
        log.info(new String(c3.getValue()));
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
