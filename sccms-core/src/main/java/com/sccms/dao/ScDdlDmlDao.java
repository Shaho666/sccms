package com.sccms.dao;

import java.util.Map;

public interface ScDdlDmlDao {

	public Integer createTable(String tableName, String[] fields) throws Exception;
	
	public Integer addRecord(String tableName, String row, String[] fields, String[] values) throws Exception;
	
	public Map<String, String> scanColumn(String tableName, String column) throws Exception;
	
	public Integer modifyData(String tableName, String row, String column, String value) throws Exception;
	
	public Integer deleteRow(String tableName, String row) throws Exception;
	
	public boolean closeConn();
	
}
