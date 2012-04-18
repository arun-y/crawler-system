package com.arunwizz.crawlersystem.core;

public class RequestMessage {
	
	private int priority;
	private String  contentLocation;
	
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

}
