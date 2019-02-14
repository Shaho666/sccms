package com.sccms.factory;

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import com.sccms.annotation.Autowired;
import com.sccms.dao.ScDdlDmlDao;
import com.sccms.utils.HbaseConnUtils;

public class ScDdlDmlDaoFactory {

	public ScDdlDmlDao getBean(Class<?> clazz, Map<String, String> confMap) {
		
		ScDdlDmlDao scDdlDmlDao = null;
		
		try {
			
			Map<String, Object> connMap = HbaseConnUtils.createConn(confMap);
			
			scDdlDmlDao = (ScDdlDmlDao) clazz.newInstance();
			Field[] fields = clazz.getDeclaredFields();
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
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return scDdlDmlDao;
	}
	
}
