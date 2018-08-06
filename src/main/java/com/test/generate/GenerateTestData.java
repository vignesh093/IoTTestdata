package com.test.generate;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.http.client.ClientProtocolException;

class GenerateEventData implements Runnable {

	static GenerateJSON generatejson = new GenerateJSON();
	static SendHTTPRequest httprequest = new SendHTTPRequest();
	String EventStatus;
	List<MetadataPOJO> metadatalist = null;

	GenerateEventData() throws IOException {
		GetDeviceMetadata devmetadata = new GetDeviceMetadata();
		List<MetadataPOJO> metadatalist = devmetadata.getmetadata();
		this.metadatalist = metadatalist;
		//start with a DEAD event(for testing purpose)
		this.EventStatus = "DEAD";
	}

	public void run() {
		int count = 0; //count added to change EVENT's ALIVE and DEAD
		while (true) {
			for (MetadataPOJO metadata : metadatalist) {
				try {
					String kafkakey = metadata.getDeviceid() + "," + "0"; //key has been added so that the schema can be assigned to event/timeseries data at spark end.
					//Done by filtering 0 which is event data and 1 as timeseries. Deviceid added otherwise having 0 and 1 would not distrbute the data evenly to partitions
					httprequest.sendrequest(generatejson.generateEventData(metadata, EventStatus), kafkakey);
					//send a event and sleep for mentioned ms
					Thread.sleep(50000);
				} catch (ClientProtocolException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			count++;
			if(count % 2 == 0) //Send DEAD and ALIVE events alternatively for each device
				EventStatus = "DEAD";
			else
				EventStatus = "ALIVE";
		}
	}
}

class GenerateTimeseriesData implements Runnable {
	static GenerateJSON generatejson = new GenerateJSON();
	static SendHTTPRequest httprequest = new SendHTTPRequest();
	int count;
	List<MetadataPOJO> metadatalist = null;

	GenerateTimeseriesData() throws IOException {
		GetDeviceMetadata devmetadata = new GetDeviceMetadata();
		this.count = 0;
		List<MetadataPOJO> metadatalist = devmetadata.getmetadata();
		this.metadatalist = metadatalist;
	}

	public void run() {
		while (true) {
			for (MetadataPOJO metadata : metadatalist) {
				String jsontobesent = "";
				String kafkakey = metadata.getDeviceid() + "," + "1";
				//Only deviceid 111 has 2 variables hence the check
				if (metadata.getDeviceid() == 111)
					jsontobesent = generatejson.generateDoubleFieldJsonTimeseries(count++, metadata);
				else
					jsontobesent = generatejson.generateSingleFieldJsonTimeseries(count++, metadata);
				try {
					httprequest.sendrequest(jsontobesent, kafkakey);
				} catch (ClientProtocolException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					Thread.sleep(20000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}

public class GenerateTestData {
	static GenerateJSON generatejson = new GenerateJSON();
	static SendHTTPRequest httprequest = new SendHTTPRequest();

	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {

		while (true) {
			//Two threads. One for timeseries and other for event so that both run and send events parallely.
			//Join is to make sure both run and wait for other success
			GenerateEventData generateeventdata = new GenerateEventData();
			Thread eventthread = new Thread(generateeventdata);
			eventthread.start();

			GenerateTimeseriesData generatetimeseriesdata = new GenerateTimeseriesData();
			Thread timeseriesthread = new Thread(generatetimeseriesdata);
			timeseriesthread.start();
			eventthread.join();
			timeseriesthread.join();

		}
	}

}
