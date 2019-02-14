package com.sccms.orm.dao;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings({ "unused", "deprecation" })
public class AccessObject3 {

	// declaring the table and column family CONSTANTS
	public static final byte[] TABLE_NAME = 
		Bytes.toBytes("sales_fact");
	public static final byte[] COLUMN_FAMILY =
		Bytes.toBytes("cf");
	
	
	// declaring the columns CONSTANTS
	private static final byte[] UNIT_PRICE =
		Bytes.toBytes("up");
	private static final byte[] QUANTITY = 
		Bytes.toBytes("q");
	
	private HTablePool pool;
		
	public AccessObject3(HTablePool pool){
		this.pool = pool;
	}
	
	
	public static Scan mkScan(){
		Scan s = new Scan();
		s.addFamily(COLUMN_FAMILY);
		return s;
	}
	
	
	public List<Result> getInfo(Filter filter) throws IOException{
		ArrayList<Result> list = new ArrayList<Result>();
		
		HTableInterface sales_fact = pool.getTable(TABLE_NAME);
		Scan scan = mkScan();
		
		/* add the column UNIT_PRICE */
		scan.addColumn(COLUMN_FAMILY, UNIT_PRICE);
		/* add the column QUANTITY */
		scan.addColumn(COLUMN_FAMILY, QUANTITY);		
		/* set the filter */
		scan.setFilter(filter);
		
		ResultScanner scanner = sales_fact.getScanner(scan);
		for(Result r : scanner){
			System.out.println(r);
			list.add(r);
		}
		return list;
	}
	
	
	public void performIncrement(String rowkey, int viewCountValue, int anotherCountValue) throws IOException{
		HTableInterface sales_fact = pool.getTable(TABLE_NAME);
		
		// create the Increment increment1 object with the passed in rowkey
		Increment increment1 = new Increment(Bytes.toBytes(rowkey));
		
		// add the column "ViewCount" with the viewCountValue to the increment1 object
		increment1.addColumn(COLUMN_FAMILY, Bytes.toBytes("ViewCount"), viewCountValue);
		// add the column "AnotherCount" with the anotherCountValue to the increment1 object
		increment1.addColumn(COLUMN_FAMILY, Bytes.toBytes("AnotherCount"), anotherCountValue);
		
		//invoke the increment() method by passing in increment1 object
		Result result1 = sales_fact.increment(increment1);
		
		// uncomment out this section when you have your written your code above 
		
		for(KeyValue kv : result1.raw()){
			System.out.println("KV: " + kv + "Value: " + Bytes.toLong(kv.getValue()));;
		}
	}
}
