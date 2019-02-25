package clientDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.Before;
import org.junit.Test;

public class HbaseClientDemo {

	Configuration conf=null;
	Connection connection=null;
	
	//建立hbase的连接
	@Before
	public void getConn() throws Exception{
		//构造一个连接对象
		conf = HBaseConfiguration.create();
		
		conf.set("hbase.zookeeper.quorum", "namenode:2181,datanode1:2181,datanode2:2181,datanode3:2181");
		
		connection = ConnectionFactory.createConnection(conf);
	}
	
	//hbase的创建表
	
	@Test
	public void testCreateTable() throws Exception{
		
		//从连接 中构造一个DDL操作器
		Admin admin = connection.getAdmin();
		
		//创建一个表定义描述对象
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("user_info"));
		
		//创建列族定义描述对象
		HColumnDescriptor hColumnDescriptor_1 = new HColumnDescriptor("base_info");
		hColumnDescriptor_1.setMaxVersions(3); //设置该列祖中存储数据的最大版本数，默认是1
		
		HColumnDescriptor hColumnDescriptor_2 = new HColumnDescriptor("extra_info");

		//将列族定义信息对象装入表定义对象中
		hTableDescriptor.addFamily(hColumnDescriptor_1);
		hTableDescriptor.addFamily(hColumnDescriptor_2);
		
		//用DDL操作对象：admin来建表
		admin.createTable(hTableDescriptor);
		
		//关闭链接
		admin.close();
		connection.close();
	}
	
	//hbase的修改表
	@Test
	public void testDropTable() throws Exception {
		
		Admin admin = connection.getAdmin();
		
		//删除表
		admin.disableTable(TableName.valueOf("user_info"));
		admin.deleteTable(TableName.valueOf("user_info"));
	
		admin.close();
		connection.close();
	}
	
	//修改表定义--添加一个列族
	@Test
	public void testAlterTable() throws Exception{

		Admin admin = connection.getAdmin();
		
		//取出旧的表定义信息
		HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf("user_info"));
		//新构造一个列族定义
		HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("other_info");
		//设置该列族的布隆过滤器类型
		hColumnDescriptor.setBloomFilterType(BloomType.ROWCOL);
		
		//将列族定义添加到表定义对象中
		tableDescriptor.addFamily(hColumnDescriptor);
		
		//将修改过的表定义交给admin去提交
		admin.modifyTable(TableName.valueOf("user_info"),tableDescriptor );
		
		admin.close();
		connection.close();
		
	}
	

	
	
	
}
