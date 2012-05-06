package com.arunwizz.crawlersystem.core;

public class CrawlingRequestMessage implements Comparable<CrawlingRequestMessage> {

	private int priority;
	private String contentLocation;

	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public String getContentLocation() {
		return contentLocation;
	}

	public void setContentLocation(String contentLocation) {
		this.contentLocation = contentLocation;
	}

	@Override
	public int compareTo(CrawlingRequestMessage o) {
		if (priority < o.priority) {
			return -1;
		} else if (priority > o.priority) {
			return +1;
		}
		return 0;
	}

}
