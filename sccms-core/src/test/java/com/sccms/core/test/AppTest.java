package com.sccms.core.test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sccms.annotation.Autowired;
import com.sccms.dao.ScDdlDmlDao;
import com.sccms.dao.impl.ScDdlDmlDaoImpl;
import com.sccms.factory.ScDdlDmlDaoFactory;
import com.sccms.utils.HbaseConnUtils;

public class AppTest {

	private ScDdlDmlDao scDdlDmlDao;

	@Before
	public void init() {
		Map<String, String> confMap = new HashMap<String, String>();
		confMap.put("hadoop.home.dir", "E:\\hadoop\\hadoop-common-2.7.4-bin");
		confMap.put("hbase.zookeeper.quorum", "192.168.16.129");
		confMap.put("hbase.zookeeper.property.clientPort", "2181");

		ScDdlDmlDaoFactory factory = new ScDdlDmlDaoFactory();
		scDdlDmlDao = factory.getBean(ScDdlDmlDaoImpl.class, confMap);
	}

	@After
	public void closeConn() {
		scDdlDmlDao.closeConn();
	}

	@Test
	public void testField() {
		try {

			Map<String, String> confMap = new HashMap<String, String>();
			confMap.put("hadoop.home.dir", "E:\\hadoop\\hadoop-common-2.7.4-bin");
			confMap.put("hbase.zookeeper.quorum", "192.168.16.129");
			confMap.put("hbase.zookeeper.property.clientPort", "2181");

			Map<String, Object> connMap = HbaseConnUtils.createConn(confMap);
			ScDdlDmlDao scDdlDmlDao = (ScDdlDmlDao) ScDdlDmlDaoImpl.class.newInstance();
			Field[] fields = ScDdlDmlDaoImpl.class.getDeclaredFields();
			for (Field field : fields) {
				if (field.getAnnotation(Autowired.class) != null) {
					field.setAccessible(true);
					if (field.getType().equals(Connection.class)) {
						field.set(scDdlDmlDao, connMap.get("connection"));
					}
					if (field.getType().equals(Admin.class)) {
						field.set(scDdlDmlDao, connMap.get("admin"));
					}
				}
			}

			HbaseConnUtils.close((Connection) connMap.get("connection"), (Admin) connMap.get("admin"));

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testCreateTable() throws Exception {

		scDdlDmlDao.createTable("BookPrice", new String[] { "bookName","price" });
	}

	@Test
	public void testAddRecord() throws Exception {

		String tableName = "BookPrice";
		String rowBase = "book";

		String[] fields = { "bookName-price:bookName", "bookName-price:price" };

		String[] values0 = { "Database System Concept", "30" };
		String[] values1 = { "Thinking in Java", "60" };
		String[] values2 = { "Data Mining", "25" };
		String[] values3 = { "Beginning Java Objects", "80" };
		String[] values4 = { "JVM Principles", "25" };
		String[] values5 = { "Deep Learning", "45" };
		String[] values6 = { "Image Retrieval", "30" };
		String[] values7 = { "Web Development", "60" };
		String[] values8 = { "C++ Programing", "85" };
		String[] values9 = { "Operating System Principles", "55" };

		scDdlDmlDao.addRecord(tableName, rowBase + 0, fields, values0);
		scDdlDmlDao.addRecord(tableName, rowBase + 1, fields, values1);
		scDdlDmlDao.addRecord(tableName, rowBase + 2, fields, values2);
		scDdlDmlDao.addRecord(tableName, rowBase + 3, fields, values3);
		scDdlDmlDao.addRecord(tableName, rowBase + 4, fields, values4);
		scDdlDmlDao.addRecord(tableName, rowBase + 5, fields, values5);
		scDdlDmlDao.addRecord(tableName, rowBase + 6, fields, values6);
		scDdlDmlDao.addRecord(tableName, rowBase + 7, fields, values7);
		scDdlDmlDao.addRecord(tableName, rowBase + 8, fields, values8);
		scDdlDmlDao.addRecord(tableName, rowBase + 9, fields, values9);
		
	}

	@Test
	public void testScanColumn() throws Exception {

		Map<String, String> resultMap = scDdlDmlDao.scanColumn("Student", "S_No-S_Name-S_Sex-S_Age");
		Set<String> keySet = resultMap.keySet();
		for (String string : keySet) {
			System.out.println(string + ": " + resultMap.get(string));
		}
	}

	@Test
	public void testModifyData() throws Exception {

		scDdlDmlDao.modifyData("Student", "stu3", "S_No-S_Name-S_Sex-S_Age:S_Sex", "female");
	}

	@Test
	public void testDeleteRow() throws Exception {

		scDdlDmlDao.deleteRow("Student", "stu3");
	}

}
