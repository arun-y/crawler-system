package com.arunwizz.crawlersystem.statistics;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arunwizz.crawlersystem.core.CrawlerManager;
import com.arunwizz.crawlersystem.statistics.NetworkStatistics.Parameter;

public class Statistician {

	private static final Logger LOGGER = LoggerFactory.getLogger(Statistician.class);
	
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
	
	private static Map<String, Long> hostWaitQueueStatistics = new ConcurrentHashMap<String, Long>(CrawlerManager.MAX_HOST_COUNT, 0.75f, 5);//max 5 thread assuming
	
	public static synchronized void hostWaitQueueEnter(String host) {
		hostWaitQueueStatistics.put(host, System.currentTimeMillis());		
	}

	public static synchronized void hostWaitQueueExit(String host) {
		LOGGER.trace("Host Wait Queue Entry Time: {}", hostWaitQueueStatistics.get(host));
		final long hostWaitTime = System.currentTimeMillis() - hostWaitQueueStatistics.get(host);
		hostWaitQueueStatistics.put(host, hostWaitTime);
	}

	public static Map<String, Long> getHostWaitQueueStatistics() {
		return hostWaitQueueStatistics;
	}
}
