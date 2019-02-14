package com.sccms.orm.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("unused")
public class HBaseSchemaTester {
	
	public static void main(String[] args) throws IOException{
		
		System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop-common-2.7.4-bin");
		// Defining the tableName and the columnFamily.
		byte[] tableName = Bytes.toBytes("tableFromJava");
		byte[] columnFamily = Bytes.toBytes("columnFamilyFromJava");
		
		// need to grab the configuration from HBase
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.16.129");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
		Admin admin = ConnectionFactory.createConnection(conf).getAdmin();
		
//		// create the HBaseAdmin object by passing in the conf
//		// create the HTableDescriptor desc by passing in the tableName
//		HTableDescriptor desc = new HTableDescriptor(tableName);
//		// create the HColumnDescriptor colFamilyDesc by passing in the columnFamily
//		HColumnDescriptor colFamilyDesc = new HColumnDescriptor(columnFamily);
//		// add the columnFamilyDesc to the desc
//		desc.addFamily(colFamilyDesc);
//		// invoke the createTable() method by passing in the desc
//		admin.createTable(desc);
//		// Once your above code has been completed, uncomment the code below to test */
//		
//		boolean available = admin.isTableAvailable(TableName.valueOf("tableFromJava"));
//		System.out.println("Table available: " + available);
		
		
		
		
		
		
		
		
		/* The CODE BELOW is to modify an existing table */
		 
		// create a HTableDescriptor htd1 for the tableName
		HTableDescriptor htd1 = admin.getTableDescriptor(TableName.valueOf(tableName));
		// invoke the getMaxFileSize() method and store the value in a variable. You will be changing this value later, so you will use this to verify your change.
		long oldMaxFileSize = htd1.getMaxFileSize();
		// create a HColumnDescriptor colFamilyDesc2 for the column family cf2
		HColumnDescriptor colFamilyDesc2 = new HColumnDescriptor(Bytes.toBytes("cf2"));
		// add the colFamilyDesc2 to the htd1
		htd1.addFamily(colFamilyDesc2);
		// setMaxFileSize of 1024*1024*1024L for htd1
		htd1.setMaxFileSize(1024 * 1024 * 1024L);
		// disable the table admin.disableTable(tableName);
		admin.disableTable(TableName.valueOf(tableName));
		// modify the table admin.modifyTable(tableName, htd1);
		admin.modifyTable(TableName.valueOf(tableName), htd1);
		// enable the table admin.enableTable(tableName);
		admin.enableTable(TableName.valueOf(tableName));
		// get the HTableDescriptor htd2 for comparison
		HTableDescriptor htd2 = admin.getTableDescriptor(TableName.valueOf(tableName));		
		// Uncomment out the below when you are ready to test.
		System.out.println("Old max file size = " + oldMaxFileSize);
		System.out.println("New max file size = " + htd2.getMaxFileSize());
		System.out.println("Equals: " + htd1.equals(htd2));
		System.out.println("New schema: " + htd2);
		
		
		
		
		
		
	}

}
