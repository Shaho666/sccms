package com.sccms.orm.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("deprecation")
public class AccessObject2 {
	// declaring the table and column family CONSTANTS
	public static final byte[] TABLE_NAME = 
		Bytes.toBytes("users");
	public static final byte[] INFO_FAMILY =
		Bytes.toBytes("info");
	
	// declaring the columns CONSTANTS
	private static final byte[] USER_COL =
		Bytes.toBytes("user");
	private static final byte[] NAME_COL =
		Bytes.toBytes("name");
	private static final byte[] EMAIL_COL =
		Bytes.toBytes("email");
	
	private HTablePool pool;
	
	public AccessObject2(HTablePool pool){
		this.pool = pool;
	}
	
	public static Get mkGet(String user){
		Get g = new Get(Bytes.toBytes(user));
		/* need to add the family member*/
		return g;
	}
	
	public static Put mkPut(User u){
		Put p = new Put(Bytes.toBytes(u.user)); 
		/* need to add user_col, name_col, email_col */
		return p;
	}
	
	public static Delete mkDelete(String user){
		Delete d = new Delete (Bytes.toBytes(user));
		return d;
	}
	
	public static Scan mkScan(){
		Scan s = new Scan();
		/* need to add the column family */
		return s;
	}
	
	
	
	public User getUser(String user) throws IOException{
		HTableInterface users = pool.getTable(TABLE_NAME);
		
		Get g = mkGet(user);
		//g.
		/* invoke the table's get command*/
		Result result = users.get(g);
		/* create a User from the result object */
		User userInfo = new User(result);
		/* return the user back to the caller */
		
		return userInfo;
	}
	
	public void addUser(String user, String name, String email) throws IOException{
		/* get the table handle from the HTablePool*/
		HTableInterface users = pool.getTable(TABLE_NAME);
		/* create the Put object with the parameters */
		User userInfo = new User(user, name, email);
		Put p = mkPut(userInfo);
		p.addColumn(INFO_FAMILY, USER_COL, user.getBytes());
		p.addColumn(INFO_FAMILY, NAME_COL, name.getBytes());
		p.addColumn(INFO_FAMILY, EMAIL_COL, email.getBytes());
		/* invoke the table's Put command */
		users.put(p);
		/* close the table's connection resource */
		users.close();
		
	}
	
	public List<User> getUsers() throws IOException{
		
		List<User> usersInfo = new ArrayList<>();
		/* get the handle to the table */
		HTableInterface users = pool.getTable(TABLE_NAME);
		/* get the scanner from the table */
		ResultScanner scanner = users.getScanner(new Scan());
		/* Iterate through the ResultScanner and add each Result to the list*/
		for (Result result : scanner) {
			
			Cell[] cells = result.rawCells();
			String user = null;
			String name = null;
			String email = null;
			for (Cell cell : cells) {
				if (new String(CellUtil.cloneQualifier(cell)).equals("user")) {
					user = new String(CellUtil.cloneValue(cell));
				}
				if (new String(CellUtil.cloneQualifier(cell)).equals("name")) {
					name = new String(CellUtil.cloneValue(cell));
				}
				if (new String(CellUtil.cloneQualifier(cell)).equals("email")) {
					email = new String(CellUtil.cloneValue(cell));
				}
			}
			
			usersInfo.add(new User(user, name, email));
		}
		/* return the list */
		return usersInfo;
	}
	
	public void deleteUser(String user) throws IOException{
		/* get table handle */
		HTableInterface users = pool.getTable(TABLE_NAME);
		/* create Delete object */
		Delete d = mkDelete(user);
		/* invoke table Delete command */
		users.delete(d);
		/* close connection */
		users.close();
	}
	
	
	static class User extends com.sccms.orm.pojo.User{
			
		private User (Result r){
			this(r.getValue(INFO_FAMILY, USER_COL),
				r.getValue(INFO_FAMILY, NAME_COL),
				r.getValue(INFO_FAMILY, EMAIL_COL));
		}
		
		private User(byte[] user, byte[] name, byte[] email){
			this(Bytes.toString(user),
					Bytes.toString(name),
					Bytes.toString(email));
		}
		
		private User(String user, String name, String email){
			this.user = user;
			this.name = name;
			this.email = email;
		}
	}
}
