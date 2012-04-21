package com.arunwizz.crawlersystem.statistics;

import java.util.HashMap;

import com.arunwizz.crawlersystem.statistics.NetworkStatistics.Parameter;

public class Statistician {

	private static HashMap<String, NetworkStatistics> networkStatisticsTable = new HashMap<String, NetworkStatistics>();
	
	public static synchronized void status(String url, String status) {
		NetworkStatistics networkStatistics = null;
		if (networkStatisticsTable.containsKey(url)) {
			networkStatistics = networkStatisticsTable
					.get(url);
		} else {
			networkStatistics = new NetworkStatistics();
			networkStatisticsTable.put(url, networkStatistics);
		}
		networkStatistics.status = status;
	}

	public static synchronized void put(String url, Parameter para, long value) {
		NetworkStatistics networkStatistics = null;
		if (networkStatisticsTable.containsKey(url)) {
			networkStatistics = networkStatisticsTable
					.get(url);
		} else {
			networkStatistics = new NetworkStatistics();
			networkStatisticsTable.put(url, networkStatistics);
		}
		switch (para) {
		case ROBO:
			networkStatistics.robo = value;
			break;
		case DNS:
			networkStatistics.dns = value;
			break;
		case CONN:
			networkStatistics.conn = value;
			break;
		case REQ:
			networkStatistics.req = value;
			break;
		case RES:
			networkStatistics.res = value;
			break;
		default:
			break;
		}
	}
	
	private static HashMap<String, Long> hostWaitQueueStatistics = new HashMap<String, Long>();
	
	public static synchronized void hostWaitQueueEnter(String host, long enterTime) {
		hostWaitQueueStatistics.put(host, enterTime);		
	}

	public static synchronized void hostWaitQueueExit(String host, long exitTime) {
		long hostWaitTime = exitTime - hostWaitQueueStatistics.get(host);
		hostWaitQueueStatistics.put(host, hostWaitTime);
	}

	public static HashMap<String, Long> getHostWaitQueueStatistics() {
		return hostWaitQueueStatistics;
	}
}
