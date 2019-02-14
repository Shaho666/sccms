package com.sccms.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HbaseConnUtils {

	public static Map<String, Object> createConn(Map<String, String> connConf) {

		Configuration configuration = null;
		Connection connection = null;
		Admin admin = null;

		Map<String, Object> connMap = new HashMap<String, Object>();

		try {

			System.setProperty("hadoop.home.dir", connConf.get("hadoop.home.dir"));

			configuration = HBaseConfiguration.create();
			configuration.set("hbase.zookeeper.quorum", connConf.get("hbase.zookeeper.quorum"));
			configuration.set("hbase.zookeeper.property.clientPort",
					connConf.get("hbase.zookeeper.property.clientPort"));

			connection = ConnectionFactory.createConnection(configuration);
			admin = connection.getAdmin();

			connMap.put("connection", connection);
			connMap.put("admin", admin);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return connMap;
	}

	public static void close(Connection connection, Admin admin) {

		try {
			if (admin != null) {
				admin.close();
			}
			if (null != connection) {
				connection.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
