package com.sccms.orm.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.sccms.orm.dao.AccessObject3;

@SuppressWarnings({ "deprecation", "unused" })
public class HBaseFilterTester {

	public static void main(String[] args) throws IOException {

		System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop-common-2.7.4-bin");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.16.129");
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		HTablePool pool = new HTablePool(conf, 10); // we leave the connection pool management up to the consuming
													// environment.

		AccessObject3 ao = new AccessObject3(pool);

		// create a row filter f1 that returns rows less than or equal to 20070920
		Filter f1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
				new BinaryComparator(Bytes.toBytes("20070920")));
		// create a row filter f2 that returns rows less than or equal to 20050920
		Filter f2 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
				new BinaryComparator(Bytes.toBytes("20050920")));
		// create a row filter f3 that returns rows equal to the regular expression
		// .*2006.
		Filter f3 = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(".*2006."));
		// create a column filter f4 that returns rows less than or equal to the column
		// q
		Filter f4 = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
				new BinaryComparator(Bytes.toBytes("q")));
		// create a value filter f5 that is equal to the value 136.90
		Filter f5 = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("136.90")));
		ao.getInfo(f5);

	}
}
