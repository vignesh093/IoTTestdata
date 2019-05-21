package com.test.generate;

import java.io.IOException;
import java.util.Random;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import com.common.utils.SetTestProperties;

public class SendHTTPRequest {
	Random random = new Random();
	SetTestProperties properties = new SetTestProperties();
	public void sendrequest(String payload,String kafkakey) throws ClientProtocolException, IOException{
		//String server_name = Integer.toString(random.nextInt(3) + 1); //send http message to any one of 3 VM's
		//final String postserver = "http://xxx%s.xxx.cloudapp.azure.com:5555/";
		String posturi = properties.posturi;
		StringEntity entity = new StringEntity(payload,
                ContentType.APPLICATION_FORM_URLENCODED);
		
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost request = new HttpPost(posturi);
        request.setHeader("kafkakey", kafkakey);
        request.setEntity(entity);

        HttpResponse response = httpClient.execute(request);
        System.out.println(response.getStatusLine().getStatusCode() +","+posturi);
	}
}
