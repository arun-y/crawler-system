package com.arunwizz.crawlersystem.statistics;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.arunwizz.crawlersystem.statistics.NetworkStatistics.Parameter;

public class Statistician {

	private static Map<String, NetworkStatistics> networkStatisticsTable = Collections.synchronizedMap(new HashMap<String, NetworkStatistics>());
	
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
	
	private static Map<String, Long> hostWaitQueueStatistics = Collections.synchronizedMap(new HashMap<String, Long>());
	
	public static synchronized void hostWaitQueueEnter(String host, long enterTime) {
		hostWaitQueueStatistics.put(host, enterTime);		
	}

	public static synchronized void hostWaitQueueExit(String host, long exitTime) {
		final long hostWaitTime = exitTime - hostWaitQueueStatistics.get(host);
		hostWaitQueueStatistics.put(host, hostWaitTime);
	}

	public static Map<String, Long> getHostWaitQueueStatistics() {
		return hostWaitQueueStatistics;
	}
}
