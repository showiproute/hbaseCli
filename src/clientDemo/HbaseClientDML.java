package clientDemo;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HbaseClientDML {

	Configuration conf=null;
	Connection conn=null;
	
	@Before
	public void getConn() throws Exception{
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "namenode:2181,datanode1:2181,datanode2:2181,datanode3:2181");

		conn = ConnectionFactory.createConnection(conf);
	}
	
	/*
	 * 增、改（一样的，改就是put来覆盖）
	 */
	
	@Test
	public void testPut() throws Exception{
		
		//获取一个操作指定表的table对象，进行DML操作
		Table table = conn.getTable(TableName.valueOf("user_info"));
		
		//构造要插入的数据为一个Put类型的对象(一个put对象只能对应一个rowkey)的对象
		Put put1 = new Put(Bytes.toBytes("001"));
		//args: 列族，key，value
		put1.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhangsan"));
		put1.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("18"));
		put1.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("home"), Bytes.toBytes("beijing"));

		
		Put put2 = new Put(Bytes.toBytes("002"));
		//args: 列族，key，value
		put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("lisi"));
		put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("28"));
		put2.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("home"), Bytes.toBytes("tianjing"));
		
		//插进去
//		table.put(put);
		
		ArrayList<Put> puts = new ArrayList<>();
		
		puts.add(put1);
		puts.add(put2);
		
		table.put(puts);
		
		conn.close();
	}
	
	
	/*
	 * 删
	 */
	
	@Test
	public void testRm() throws Exception {
		 Table table = conn.getTable(TableName.valueOf("user_info"));
		 Delete delete1 = new Delete(Bytes.toBytes("001"));
		 Delete delete2 = new Delete(Bytes.toBytes("002"));
		 delete2.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("home"));
		 
		 ArrayList<Delete> dels = new ArrayList<>();
		 
		 dels.add(delete1);
		 dels.add(delete2);
		 
		 
		 table.delete(dels);
		 
		 table.close();
		 conn.close();
	}
	
	/*
	 * 查
	 */
	@Test
	public void testGet()  throws Exception{
		
		Table table = conn.getTable(TableName.valueOf("user_info"));
		
		Get get = new Get("002".getBytes());
		
		Result result = table.get(get);
		CellScanner cellScanner = result.cellScanner();
		
		//遍历整行所有的key value
		while(cellScanner.advance()) {
			Cell cell = cellScanner.current();
			byte[] rowArray = cell.getRowArray();
			byte[] familyArray = cell.getFamilyArray(); //列族名的字节数组
			byte[] qualifierArray = cell.getQualifierArray(); //列名的字节数组
			byte[] valueArray = cell.getValueArray(); //拿到value的字节数组
			System.out.println("行键"+new String(rowArray,cell.getRowOffset(),cell.getRowLength()));
			System.out.println("列族名"+new String(familyArray,cell.getFamilyOffset(),cell.getFamilyLength()));
			System.out.println("列名"+new String(qualifierArray,cell.getQualifierOffset(),cell.getQualifierLength()));
			System.out.println("value:"+new String(valueArray,cell.getValueOffset(),cell.getValueLength()));
			
		}
		
		//直接从结果中取用户指定的key-value
		byte[] value = result.getValue("base_info".getBytes(), "age".getBytes());
		System.out.println(new String(value));
		
		
		table.close();
		conn.close();
		
	}
	
	/*
	 * 按行键范围查询数据
	 */
	@Test
	public void testScan() throws Exception{
		/*查询指定行范围数据
		 0
		 1
		 10
		 100
		 1000
		 10000
		 
		 Scan scan = new Scan("10".getBytes(), "10000\000".getBytes());
		 //查询的就是10 ---- 100000之间的数据
		*/
		
		Table table = conn.getTable(TableName.valueOf("user_info"));
		//包含起始行键 不包含最终行键,如果真的想查询出末尾行键，就可以在末尾行键插入一个不可见字符\0
		Scan scan = new Scan("10".getBytes(), "1000\0".getBytes());
		ResultScanner scanner = table.getScanner(scan);
		Iterator<Result> iterator = scanner.iterator();
		while(iterator.hasNext()) {
			Result result = iterator.next();
			
			CellScanner cellScanner = result.cellScanner();
			
			//遍历整行所有的key value
			while(cellScanner.advance()) {
				Cell cell = cellScanner.current();
				byte[] rowArray = cell.getRowArray();
				byte[] familyArray = cell.getFamilyArray(); //列族名的字节数组
				byte[] qualifierArray = cell.getQualifierArray(); //列名的字节数组
				byte[] valueArray = cell.getValueArray(); //拿到value的字节数组
				System.out.println("行键"+new String(rowArray,cell.getRowOffset(),cell.getRowLength()));
				System.out.println("列族名"+new String(familyArray,cell.getFamilyOffset(),cell.getFamilyLength()));
				System.out.println("列名"+new String(qualifierArray,cell.getQualifierOffset(),cell.getQualifierLength()));
				System.out.println("value:"+new String(valueArray,cell.getValueOffset(),cell.getValueLength()));
				
			}
			System.out.println("--------------------");
		}
	}
	
	/*
	 * 循环插入大量数据
	 */
	
	@Test
	public void testManyPuts() throws Exception{
		
		Table table = conn.getTable(TableName.valueOf("user_info"));
		ArrayList<Put> arrayList = new ArrayList<>();
		
		for(int i=0;i<1000;i++) {
			Put put = new Put(Bytes.toBytes(""+i));
			put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhangsan"+i));
			put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes(18+i));
			put.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("home"), Bytes.toBytes("zhangsan"+i));

			arrayList.add(put);
		}
		
		table.put(arrayList);
		
	}
	
}
