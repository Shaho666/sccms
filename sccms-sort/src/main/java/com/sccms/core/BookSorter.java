package com.sccms.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.sccms.mapper.BookPriceMapper;
import com.sccms.reducer.BookPriceReducer;

public class BookSorter {

	public static void main(String[] args) throws Exception {
		
		System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop-common-2.7.4-bin");
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "192.168.16.129");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");

		Job job = Job.getInstance(configuration);
		job.setJarByClass(BookSorter.class);

		Scan scan = new Scan();
		//scan.addFamily("bookName-price".getBytes());
		scan.addColumn("bookName-price".getBytes(), "price".getBytes());

		TableMapReduceUtil.initTableMapperJob("BookPrice", // 数据库表名
				scan, // Scan实例，控制列族及属性的选择
				BookPriceMapper.class, // mapper类
				FloatWritable.class, // mapper输出键
				Text.class, // mapper输出值
				job);
		TableMapReduceUtil.initTableReducerJob("BookPriceSorted", // 设置输出表
				BookPriceReducer.class, // 设置TableReducer类
				job);

		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
		
	}
	
}
