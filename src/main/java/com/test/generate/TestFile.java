package com.test.generate;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;
public class TestFile {
	public static void main(String args[]){
		Random random = new Random();
	
/*		System.out.println(UUID.randomUUID().toString());
		System.out.println(UUID.randomUUID().toString());
		System.out.println(UUID.randomUUID().toString());
		System.out.println(UUID.randomUUID().toString());
		System.out.println(UUID.randomUUID().toString());
*/
		Timestamp timestamp = new Timestamp(new Date().getTime());
		/*int minute_delay = (-1) * random.nextInt(10);
		System.out.println(minute_delay);
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MINUTE, minute_delay);
		
		timestamp = new Timestamp(cal.getTime().getTime());*/
		System.out.println(timestamp);
	}
}
