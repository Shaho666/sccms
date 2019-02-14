package com.sccms.orm.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

import com.sccms.orm.dao.AccessObject3;

@SuppressWarnings("deprecation")
public class HBaseCounterTester {
	public static void main(String[] args) throws IOException{
		
		System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop-common-2.7.4-bin");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.16.129");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
		HTablePool pool = new HTablePool(conf, 10); // we leave the connection pool management up to the consuming environment.
		AccessObject3 ao = new AccessObject3(pool);
		
		ao.performIncrement("20061222",1, 20);
	}

}

