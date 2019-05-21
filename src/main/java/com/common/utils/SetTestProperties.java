package com.common.utils;

import java.util.Properties;
import java.util.ResourceBundle;

public class SetTestProperties {
	Properties prop = new Properties();
	ResourceBundle rb = ResourceBundle.getBundle("testconf");
	
	public final String zkQuorum = rb.getString("zkQuorum");
	public final String zkport = rb.getString("zkport");
	public final String metadatatablename = rb.getString("metadatatablename");
	public final String posturi = rb.getString("posturi");
	
}
