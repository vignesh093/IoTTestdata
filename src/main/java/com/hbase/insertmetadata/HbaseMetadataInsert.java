package com.hbase.insertmetadata;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.common.utils.SetTestProperties;

public class HbaseMetadataInsert {
	public static ByteArrayOutputStream generateRowKey(int connectorid, int deviceid, int variableid) throws IOException {
		ByteArrayOutputStream byte1 = new ByteArrayOutputStream();
	    DataOutputStream data = new DataOutputStream(byte1);
	    data.writeInt(deviceid);
	    data.writeInt(connectorid);
	    data.writeInt(variableid);
	    return byte1;
	}
	public static void main(String[] args) throws IOException {
		SetTestProperties properties = new SetTestProperties();
		String zkQuorum = properties.zkQuorum;
		String tableName = properties.metadatatablename;
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", zkQuorum);
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		// ** Connection to the cluster. A single connection shared by all
		// application threads. *//
		Connection connection = null;
		// ** A lightweight handle to a specific table. Used for a single
		// thread. *//
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)));
			ByteArrayOutputStream rowkey = generateRowKey(1111,111,1);
			Put put = new Put(rowkey.toByteArray());
			put.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("deviceguid"), Bytes.toBytes("40a3a852-959e-4243-85a9-534ca7b10db6"));
			put.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("deviceid"), Bytes.toBytes(111));
			put.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("connectorguid"), Bytes.toBytes("94d39560-0aad-4a03-8a84-80ed2d1e42ba"));
			put.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("connectorid"), Bytes.toBytes(1111));
			put.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variablename"), Bytes.toBytes("temp"));
			put.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variableid"), Bytes.toBytes(1));
			put.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variableDataType"), Bytes.toBytes("int"));
			table.put(put);
			
			ByteArrayOutputStream rowkey1 = generateRowKey(1111,111,2);
			Put put1 = new Put(rowkey1.toByteArray());
			put1.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("deviceguid"), Bytes.toBytes("40a3a852-959e-4243-85a9-534ca7b10db6"));
			put1.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("deviceid"), Bytes.toBytes(111));
			put1.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("connectorguid"), Bytes.toBytes("94d39560-0aad-4a03-8a84-80ed2d1e42ba"));
			put1.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("connectorid"), Bytes.toBytes(1111));
			put1.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variablename"), Bytes.toBytes("pressure"));
			put1.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variableid"), Bytes.toBytes(2));
			put1.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variableDataType"), Bytes.toBytes("double"));
			table.put(put1);
			
			ByteArrayOutputStream rowkey2 = generateRowKey(2222,222,1);
			Put put2 = new Put(rowkey2.toByteArray());
			put2.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("deviceguid"), Bytes.toBytes("7ac70d77-5fb7-4069-a82c-c33910b5b1e5"));
			put2.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("deviceid"), Bytes.toBytes(222));
			put2.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("connectorguid"), Bytes.toBytes("9140d444-bfad-4ef3-9fd3-d7da9038a27f"));
			put2.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("connectorid"), Bytes.toBytes(2222));
			put2.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variablename"), Bytes.toBytes("temp"));
			put2.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variableid"), Bytes.toBytes(1));
			put2.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variableDataType"), Bytes.toBytes("int"));
			table.put(put2);
			
			ByteArrayOutputStream rowkey3 = generateRowKey(2222,333,1);
			Put put3 = new Put(rowkey3.toByteArray());	
			put3.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("deviceguid"), Bytes.toBytes("cbae2be5-b8a3-4e7b-acff-45741f0f18aa"));
			put3.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("deviceid"), Bytes.toBytes(333));
			put3.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("connectorguid"), Bytes.toBytes("9140d444-bfad-4ef3-9fd3-d7da9038a27f"));
			put3.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("connectorid"), Bytes.toBytes(2222));
			put3.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variablename"), Bytes.toBytes("temp"));
			put3.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variableid"), Bytes.toBytes(1));
			put3.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variableDataType"), Bytes.toBytes("int"));
			table.put(put3);
			
			ByteArrayOutputStream rowkey4 = generateRowKey(4444,444,1);
			Put put4 = new Put(rowkey4.toByteArray());			
			put4.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("deviceguid"), Bytes.toBytes("e6330f53-d3f2-47c9-a3c1-ee98ca91ec76"));
			put4.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("deviceid"), Bytes.toBytes(444));
			put4.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("connectorguid"), Bytes.toBytes("e27b487b-dd35-40ce-8f89-05d74877a824"));
			put4.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("connectorid"), Bytes.toBytes(4444));
			put4.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variablename"), Bytes.toBytes("temp"));
			put4.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variableid"), Bytes.toBytes(1));
			put4.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variableDataType"), Bytes.toBytes("int"));
			table.put(put4);
			
			ByteArrayOutputStream rowkey5 = generateRowKey(5555,555,1);
			Put put5 = new Put(rowkey5.toByteArray());		
			put5.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("deviceguid"), Bytes.toBytes("4c5feab4-8f15-4d45-a588-08df46612def"));
			put5.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("deviceid"), Bytes.toBytes(555));
			put5.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("connectorguid"), Bytes.toBytes("6a4a2997-1fcc-434e-94e2-59c68d064803"));
			put5.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("connectorid"), Bytes.toBytes(5555));
			put5.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variablename"), Bytes.toBytes("temp"));
			put5.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variableid"), Bytes.toBytes(1));
			put5.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variableDataType"), Bytes.toBytes("int"));
			table.put(put5);
			System.out.println("Metadata successfully inserted");
		} finally {
			if (table != null)
				table.close();
			if (connection != null)
				connection.close();
		}
		
	}

}
