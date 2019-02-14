package com.sccms.dao.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import com.sccms.annotation.Autowired;
import com.sccms.dao.ScDdlDmlDao;
import com.sccms.utils.HbaseConnUtils;

public class ScDdlDmlDaoImpl implements ScDdlDmlDao {

	@Autowired
	private Connection connection;

	@Autowired
	private Admin admin;

	@Override
	public Integer createTable(String tableName, String[] fields) throws Exception {

		TableName tableNames = TableName.valueOf(tableName);
		if (admin.tableExists(tableNames)) {
			admin.disableTable(tableNames);
			admin.deleteTable(tableNames);
		}

		HTableDescriptor hTableDescriptor = new HTableDescriptor(tableNames);
		StringBuilder colFamilyName = new StringBuilder();
		for (String str : fields) {
			colFamilyName.append(str + "-");
		}
		colFamilyName.deleteCharAt(colFamilyName.length() - 1);

		HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamilyName.toString());
		hTableDescriptor.addFamily(hColumnDescriptor);
		admin.createTable(hTableDescriptor);

		return 0;
	}

	@Override
	public Integer addRecord(String tableName, String row, String[] fields, String[] values) throws Exception {

		if (fields.length != values.length) {
			return -1;
		}

		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(row.getBytes());

		for (int i = 0; i < fields.length; i++) {
			String[] split = fields[i].split(":");
			if (split.length != 2) {
				return -1;
			}
			put.addColumn(split[0].getBytes(), split[1].getBytes(), values[i].getBytes());
		}

		table.put(put);
		table.close();

		return 0;
	}

	@Override
	public Map<String, String> scanColumn(String tableName, String column) throws Exception {

		Table table = connection.getTable(TableName.valueOf(tableName));

		Scan scan = new Scan();
		scan.addFamily(column.getBytes());
		ResultScanner resultScanner = table.getScanner(scan);

		Map<String, String> resultMap = new HashMap<String, String>();

		for (Result result : resultScanner) {
			byte[] rowKey = result.getRow();
			Cell[] cells = result.rawCells();
			StringBuilder strValue = new StringBuilder();
			strValue.append("{");

			for (Cell cell : cells) {
				strValue.append(new String(CellUtil.cloneQualifier(cell)));
				strValue.append(":");
				strValue.append(new String(CellUtil.cloneValue(cell)));
				strValue.append(";");
			}
			strValue.deleteCharAt(strValue.length() - 1);
			strValue.append("}");

			resultMap.put(new String(rowKey), strValue.toString());
		}

		table.close();
		return resultMap;
	}

	@Override
	public Integer modifyData(String tableName, String row, String column, String value) throws Exception {

		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(row.getBytes());

		String[] split = column.split(":");
		put.addColumn(split[0].getBytes(), split[1].getBytes(), value.getBytes());
		table.put(put);
		table.close();

		return 0;
	}

	@Override
	public Integer deleteRow(String tableName, String row) throws Exception {

		Table table = connection.getTable(TableName.valueOf(tableName));
		Delete delete = new Delete(row.getBytes());
		
		table.delete(delete);
		table.close();
		return 0;
	}

	@Override
	public boolean closeConn() {

		HbaseConnUtils.close(connection, admin);
		return true;
	}

}
