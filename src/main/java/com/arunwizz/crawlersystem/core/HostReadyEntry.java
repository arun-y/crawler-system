package com.arunwizz.crawlersystem.core;

/**
 * Entry for ready priority queue, entries are ordered by request number
 * 
 * @author aruny
 * 
 */
class HostReadyEntry implements Comparable<HostReadyEntry> {

	private String hostname;
	private long requestNumber;

	public HostReadyEntry(String hostname, long requestNumber) {
		this.hostname = hostname;
		this.requestNumber = requestNumber;
	}

	@Override
	public int compareTo(HostReadyEntry o) {
		if (requestNumber < o.requestNumber) {
			return -1;
		} else if (requestNumber > o.requestNumber) {
			return +1;
		} else {
			return 0;
		}
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public long getRequestNumber() {
		return requestNumber;
	}

	public void setRequestNumber(long requestNumber) {
		this.requestNumber = requestNumber;
	}

}