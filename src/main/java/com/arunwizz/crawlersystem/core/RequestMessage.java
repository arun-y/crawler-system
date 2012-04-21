package com.arunwizz.crawlersystem.core;

public class RequestMessage implements Comparable<RequestMessage> {

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
	public int compareTo(RequestMessage o) {
		if (priority < o.priority) {
			return -1;
		} else if (priority > o.priority) {
			return +1;
		}
		return 0;
	}

}
