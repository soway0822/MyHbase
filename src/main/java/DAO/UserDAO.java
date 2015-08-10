package DAO;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import Util.UserTO;

public class UserDAO {
	public static final byte[] TABLE_NAME = Bytes.toBytes("users");
	public static final byte[] INFO_FAM = Bytes.toBytes("info");
	public static final byte[] USER_COL = Bytes.toBytes("user");
	public static final byte[] NAME_COL = Bytes.toBytes("name");
	public static final byte[] EMAIL_COL = Bytes.toBytes("email");
	public static final byte[] PASS_COL = Bytes.toBytes("password");
	public static final byte[] TWEETS_COL = Bytes.toBytes("tweet_count");
	public static final byte[] HAMLET_COL = Bytes.toBytes("hamlet_tag");
	private static HTablePool pool;
	private static Configuration conf;

	public UserDAO(HTablePool pool, Configuration conf) {
		this.pool = pool;
		this.conf = conf;
	}

	private static Get mkGet(String user) {
		Get g = new Get(Bytes.toBytes(user));
		g.addFamily(INFO_FAM);
		return g;
	}

	private static Put mkPut(User u) {
		Put p = new Put(Bytes.toBytes(u.user));
		p.add(INFO_FAM, USER_COL, Bytes.toBytes(u.user));
		p.add(INFO_FAM, NAME_COL, Bytes.toBytes(u.name));
		p.add(INFO_FAM, EMAIL_COL, Bytes.toBytes(u.email));
		p.add(INFO_FAM, PASS_COL, Bytes.toBytes(u.password));
		p.add(INFO_FAM, TWEETS_COL, Bytes.toBytes(u.tweetCount));
		return p;
	}

	private static Delete mkDel(String user) {
		Delete d = new Delete(Bytes.toBytes(user));
		return d;
	}

	public static void createTable() throws IOException {
		HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
		HColumnDescriptor c = new HColumnDescriptor(INFO_FAM);
		c.setMaxVersions(3);
		desc.addFamily(c);
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.createTable(desc);
	}

	public static void addRecord(String user, String name, String email,
			String password, long tweet_count) {
		try {
			HTableInterface users = pool.getTable(TABLE_NAME);
			Put p = mkPut(new User(user, name, email, password, tweet_count));
			users.put(p);
			users.close();

			System.out.println("insert recored " + user + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void getAllRecord() {
		try {
			HTableInterface table = pool.getTable(TABLE_NAME);

			Scan s = new Scan();
			s.addFamily(INFO_FAM);
			// s.setStartRow(Bytes.toBytes("a"));
			// s.setStopRow(Bytes.toBytes("a" + 1));

			ResultScanner ss = table.getScanner(s);
			for (Result r : ss) {
				User u = new User(r);
				System.out.println(u);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void getUser(String username) {
		try {
			HTableInterface table = pool.getTable(TABLE_NAME);
			Get g = mkGet(username);
			Result result = table.get(g);
			if (!result.isEmpty()) {
				List<KeyValue> passwords = result.getColumn(INFO_FAM, PASS_COL);
				byte[] b = passwords.get(0).getValue();
				System.out.println(Bytes.toString(b));
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void delRecord(String user) {
		try {
			HTableInterface users = pool.getTable(TABLE_NAME);
			Delete d = mkDel(user);
			users.delete(d);
			users.close();

			System.out.println("del recored " + user + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// << User
	private static class User extends UserTO {
		private User(Result r) {
			this(r.getValue(INFO_FAM, USER_COL),
					r.getValue(INFO_FAM, NAME_COL), r.getValue(INFO_FAM,
							EMAIL_COL), r.getValue(INFO_FAM, PASS_COL), r
							.getValue(INFO_FAM, TWEETS_COL));
		}

		private User(byte[] user, byte[] name, byte[] email, byte[] password,
				byte[] tweet_count) {
			this(Bytes.toString(user), Bytes.toString(name), Bytes
					.toString(email), Bytes.toString(password), Bytes
					.toLong(tweet_count));
		}

		private User(String user, String name, String email, String password,
				long tweet_count) {
			this.user = user;
			this.name = name;
			this.email = email;
			this.password = password;
			this.tweetCount = tweet_count;

		}
	}
	// >>
}
