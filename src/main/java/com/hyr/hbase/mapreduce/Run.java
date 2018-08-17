package com.hyr.hbase.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @category MapperReduce 分布式计算 基于hbase
 * @author huangyueran
 *
 */
public class Run {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://master:9000");
		conf.set("yarn.resourcemanager.hostname", "master");
		conf.set("hbase.zookeeper.quorum", "master"); // 交给zookeeper处理
		// conf.set("mapred.jar", "H:\\jar\\hb_wc.jar");

		Job job = Job.getInstance(conf);

		job.setMapperClass(WcMapper.class);
		job.setReducerClass(WCTableReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// job.setNumReduceTasks(20); // 设置Reduce任务个数

		FileInputFormat.setInputPaths(job, "/user/input/qq");

		// hbase reducer
		String targetTable = "qqfriend";
		TableMapReduceUtil.initTableReducerJob(targetTable, // 输出到哪一张表
				WCTableReducer.class, // reducer class
				job);

		job.waitForCompletion(true); // 是否等待完成

	}
}
