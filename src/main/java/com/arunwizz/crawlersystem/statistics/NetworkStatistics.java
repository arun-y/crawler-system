package com.arunwizz.crawlersystem.statistics;

public class NetworkStatistics {
	
	public enum Parameter {ROBO, DNS, CONN, REQ, RES};
		
	public String url;
	public String status;
	public long robo;
	public long dns;
	public long conn;
	public long req;
	public long res;

	public String toString() {
		return url + ":" + status + "," + robo + "," + dns + "," + conn + ","
				+ conn + "," + req + "," + res;
	}
	
}
