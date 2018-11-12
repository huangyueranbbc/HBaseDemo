package com.hyr.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
 
/**
 * @author wozipa
 * @Date 2016-3-20 20:57
 * 创建两张一模一样的表
 */
public class RegionObverserExample extends BaseRegionObserver{
	
	private static String tableName="testCopy";
	private static String family="copy";
	
	/**
	 * @author wozipa
	 * @Date 2016-3-20 21：00
	 *  进行表格的配置
	 * @return
	 */
	public Configuration init()
	{
		Configuration conf=new Configuration();
		conf.set("hbase.master", "hadoop1:16000");
		conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3,hadoop4,hadoop5,hadoop6");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		return conf;
	}
	
 
	/**
	 * @author wozipa
	 * @Date 2016-3-20 21:02
	 *  在插入数据之前将数据插入到指定的表格中
	 */
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
			throws IOException {
		// TODO Auto-generated method stub
		Configuration conf=init();
		try {
			HBaseAdmin admin=new HBaseAdmin(conf);
			if(!admin.tableExists(tableName))
			{
				HTableDescriptor tableDescriptor=new HTableDescriptor(tableName);
				HColumnDescriptor columnDescriptor=new HColumnDescriptor(family);
				tableDescriptor.addFamily(columnDescriptor);
				admin.createTable(tableDescriptor);
				admin.close();
			}
			//创建数据插入该保重，保持行键值和列名、value值的相同
			Map<byte[],List<KeyValue>> map=put.getFamilyMap();
			List<KeyValue> list=map.get(put.getRow());
			Put put2=new Put(put.getRow());
			for(KeyValue kv:list)
			{
				put2.addColumn(Bytes.toBytes(family),kv.getQualifier(),kv.getValue());
			}
			//创建表连接
			HTable table=new HTable(conf, tableName);
			table.put(put2);
			//
			table.close();
		} catch (Exception e1) {
			// TODO: handle exception
			e1.printStackTrace();
		}
	}
}
