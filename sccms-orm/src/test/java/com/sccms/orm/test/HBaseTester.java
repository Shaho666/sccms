package com.sccms.orm.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

import com.sccms.orm.dao.AccessObject2;
import com.sccms.orm.pojo.User;

@SuppressWarnings("deprecation")
public class HBaseTester {
	public static final String usage = 
		"add user name email - add a new user\n"+
		"get user - retrieve the user\n"+
		"delete user - remove the user\n"+
		"list - list all users\n";
	
	
	public static void main(String[] args) throws IOException{
		
		System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop-common-2.7.4-bin");
		
		if(args.length == 0){
			System.out.println(usage);
			System.exit(0);
		}
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.16.129");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		HTablePool pool = new HTablePool(conf, 10);
		
		AccessObject2 ao = new AccessObject2(pool);
		if("get".equals(args[0])){
			System.out.println("Getting user " + args[1]);
			User u = ao.getUser(args[1]);
			System.out.println(u);
		}
		if("add".equals(args[0])){
			System.out.println("Adding user...");
			ao.addUser(args[1],args[2],args[3]);
			User u = ao.getUser(args[1]);
			System.out.println("Successfully added user " + u);
		}
		
		if("list".equals(args[0])){
			for(User u : ao.getUsers()){
				System.out.println(u);
			}
		}
		if("delete".equals(args[0])){
			System.out.println("Deleting user...");
			ao.deleteUser(args[1]);
			System.out.println("Successfully deleted user " + args[1]);
		}
		pool.closeTablePool(AccessObject2.TABLE_NAME);
		
	}
}
