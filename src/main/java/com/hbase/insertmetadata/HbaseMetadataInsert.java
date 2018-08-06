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

public class HbaseMetadataInsert {

	public static void main(String[] args) throws IOException {

		String zkQuorum = "clouderavm01.centralindia.cloudapp.azure.com,clouderavm02.centralindia.cloudapp.azure.com,clouderavm03.centralindia.cloudapp.azure.com";
		String tableName = "IOTMETADATA";
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
			ByteArrayOutputStream byte1 = new ByteArrayOutputStream();
		    DataOutputStream data = new DataOutputStream(byte1);
		    data.writeInt(111);
		    data.writeInt(1111);
		    data.writeInt(2);
			Put put = new Put(byte1.toByteArray());
			put.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("deviceguid"), Bytes.toBytes("4d0a159d-65ff-441d-87d5-17af708afc14"));
			put.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("deviceid"), Bytes.toBytes(111));
			put.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("connectorguid"), Bytes.toBytes("5a9fb010-843d-480c-a94f-c12dac48c187"));
			put.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("connectorid"), Bytes.toBytes(1111));
			put.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variablename"), Bytes.toBytes("pressure"));
			put.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variableid"), Bytes.toBytes(2));
			put.addColumn(Bytes.toBytes("IOT"), Bytes.toBytes("variableDataType"), Bytes.toBytes("double"));
			table.put(put);
			System.out.println("Metadata successfully inserted");
		} finally {
			if (table != null)
				table.close();
			if (connection != null)
				connection.close();
		}
		
	}

}
