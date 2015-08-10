package MainFunc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

import DAO.UserDAO;

public class Main {
	private static Configuration conf = null;
	public static void main(String[] args) throws Exception {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "NewMaster");
		HTablePool pool = new HTablePool(conf, 10);
		
		java.util.Date date = new java.util.Date();
		long sTime = date.getTime();
		
		DAO.UserDAO userDAO = new UserDAO(pool, conf);
		//userDAO.createTable();
//		userDAO.addRecord("TheRealMT","Mark Tain","test@123.com","999999", sTime);
		userDAO.addRecord("Momo","Steffi","steffi@micron.com","111111", sTime);
//		userDAO.addRecord("Micron", "Soway", "sowaychang@micron.com", "222222", sTime);
		
		/*
		userDAO.getAllRecord();
		userDAO.delRecord("Momo");
		System.out.println("");
		
		*/
		
		//userDAO.getUser("Micron");
		
		userDAO.getAllRecord();
		System.out.println("OK");
	}
}
