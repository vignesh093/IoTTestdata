package com.test.generate;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONObject;

public class GenerateJSON {
	Random random = new Random();
	public String generateSingleFieldJsonTimeseries(int count,MetadataPOJO metadata){
		int deviceid = metadata.getDeviceid();
		Random random = new Random();
		Timestamp timestamp = new Timestamp(new Date().getTime());
		if(count % 3 == 0){ //send some record with eventtime delay(max delay of 10 minutes) (1 record for every 3 records)
		int minute_delay = (-1) * random.nextInt(10);
		
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MINUTE, minute_delay);
		
		timestamp = new Timestamp(cal.getTime().getTime());
		}
		if(count % 7 == 0){			//send some error record invalid deviceid. Once every 7 records
			deviceid = 999;
		}
		System.out.println("curr Time "+new Timestamp(new Date().getTime()) +" sent timestamp "+timestamp+" deviceid "+deviceid);

		JSONObject complete_json = new JSONObject();
		complete_json.put("name", "iottesttimeseries");
		JSONObject header_item = new JSONObject();
		header_item.put("timeseries", 1);
		header_item.put("connectorid", metadata.getConnectorid());
		header_item.put("deviceid", deviceid);
		header_item.put("eventtime", timestamp);
		complete_json.put("header", header_item);
		
		JSONArray array_values = new JSONArray();
		JSONObject value_item = new JSONObject();
		value_item.put("variableid", metadata.getVariableid());
		value_item.put("value", random.nextInt(100)+1 );
		array_values.put(value_item);
		
		JSONObject value_item1 = new JSONObject();
		value_item1.put("values", array_values);
		complete_json.put("iotdata",value_item1);
		
		return complete_json.toString();
	}
	
	public String generateDoubleFieldJsonTimeseries(int count,MetadataPOJO metadata){
		Random random = new Random();
		Timestamp timestamp = new Timestamp(new Date().getTime());
		if(count % 3 == 0){
		int minute_delay = (-1) * random.nextInt(10);
		
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MINUTE, minute_delay);
		
		timestamp = new Timestamp(cal.getTime().getTime());
		}
		System.out.println("curr Time "+new Timestamp(new Date().getTime()) +" sent timestamp "+timestamp+" deviceid "+metadata.getDeviceid());
		JSONObject complete_json = new JSONObject();
		complete_json.put("name", "iottesttimeseries");
		JSONObject header_item = new JSONObject();
		header_item.put("timeseries", 1);
		header_item.put("connectorid", metadata.getConnectorid());
		header_item.put("deviceid", metadata.getDeviceid());
		header_item.put("eventtime", timestamp);
		complete_json.put("header", header_item);
		
		JSONArray array_values = new JSONArray();
		JSONObject value_item1 = new JSONObject();
		//variableid embedded
		value_item1.put("variableid", 1);
		value_item1.put("value", random.nextInt(100)+1);
		JSONObject value_item2 = new JSONObject();
		value_item2.put("variableid", 2);
		value_item2.put("value", random.nextDouble());
		array_values.put(value_item1);
		array_values.put(value_item2);
		
		
		JSONObject value_item3 = new JSONObject();
		value_item3.put("values", array_values);
		complete_json.put("iotdata",value_item3);
		return complete_json.toString();
	}
	
	public String generateEventData(MetadataPOJO metadata,String EventStatus){
		Timestamp timestamp = new Timestamp(new Date().getTime());
		
		JSONObject complete_json = new JSONObject();
		complete_json.put("name", "iottestevent");
		JSONObject header_item = new JSONObject();
		header_item.put("timeseries", 0);
		header_item.put("connectorid", metadata.getConnectorid());
		header_item.put("deviceid", metadata.getDeviceid());
		header_item.put("eventtime", timestamp);
		complete_json.put("header", header_item);
		
		JSONObject value_item = new JSONObject();
		value_item.put("status",EventStatus);
		
		complete_json.put("values", value_item);
		return complete_json.toString();
	}
}
