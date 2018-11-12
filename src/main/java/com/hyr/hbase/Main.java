package com.hyr.hbase;

import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
 
import com.yammer.metrics.stats.EWMA;
 
/**
 *	BaseRegionObserver
 */
public class Main {
	
	private static String tableName="test";
	private static String family="test";
	
	public static void main(String[] args)
	{
		Main main=new Main();
		main.putCoprocessExample();
	}
	
	public Configuration init()
	{
		Configuration conf=new Configuration();
		conf.set("hbase.master", "hadoop1:16000");
		conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3,hadoop4,hadoop5,hadoop6");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		return conf;
	}
	
	/**
	 *  进行数据
	 */
	public void putCoprocessExample()
	{
		Configuration conf=init();
		HBaseAdmin admin=null;
		try {
			admin=new HBaseAdmin(conf);
			if(!admin.tableExists(tableName))
			{
				HTableDescriptor tableDescriptor=new HTableDescriptor(tableName);
				HColumnDescriptor columnDescriptor=new HColumnDescriptor(family);
				tableDescriptor.addFamily(columnDescriptor);
				Path path=new Path("hdfs://192.98.12.234:9000/Coprocessor/Copro.jar");
				tableDescriptor.setValue("COPROCESSOR$1",path.toString()+"|"+"com.hyr.hbase.RegionObverserExample|"+Coprocessor.PRIORITY_USER);
				admin.createTable(tableDescriptor);
			}
			//想数据库中插入数据
			Put put=new Put(Bytes.toBytes("row-2"));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes("qualifier1"), Bytes.toBytes("value2"));
			HTable table=new HTable(conf, tableName);
			table.put(put);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
 
}
