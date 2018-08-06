package com.test.generate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class GetDeviceMetadata {
	List<MetadataPOJO> metadatalist = new ArrayList<MetadataPOJO>();

	public List<MetadataPOJO> getmetadata() throws IOException {
		
		String zkQuorum = "clouderavm01.centralindia.cloudapp.azure.com,clouderavm02.centralindia.cloudapp.azure.com,clouderavm03.centralindia.cloudapp.azure.com";
		String tableName = "IOTMETADATA";
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", zkQuorum);
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Connection connection = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)));
			Scan scan = new Scan();
			ResultScanner scanner = table.getScanner(scan);
			for(Result res : scanner){
				MetadataPOJO metadata = new MetadataPOJO();
				metadata.setDeviceguid(Bytes.toString(res.getValue(Bytes.toBytes("IOT"), Bytes.toBytes("deviceguid"))));
				metadata.setDeviceid(Bytes.toInt(res.getValue(Bytes.toBytes("IOT"), Bytes.toBytes("deviceid"))));
				metadata.setConnectorguid(Bytes.toString(res.getValue(Bytes.toBytes("IOT"), Bytes.toBytes("connectorguid"))));
				metadata.setConnectorid(Bytes.toInt(res.getValue(Bytes.toBytes("IOT"), Bytes.toBytes("connectorid"))));
				metadata.setVariablename(Bytes.toString(res.getValue(Bytes.toBytes("IOT"), Bytes.toBytes("variablename"))));
				metadata.setVariableid(Bytes.toInt(res.getValue(Bytes.toBytes("IOT"), Bytes.toBytes("variableid"))));
				metadata.setVariableDataType(Bytes.toString(res.getValue(Bytes.toBytes("IOT"), Bytes.toBytes("variableDataType"))));
				metadatalist.add(metadata);
			}
		}
		finally {
			if (table != null)
				table.close();
			if (connection != null)
				connection.close();
		}
		return metadatalist;
	}
}
