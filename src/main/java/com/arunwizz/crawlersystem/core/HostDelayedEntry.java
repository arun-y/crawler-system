package com.arunwizz.crawlersystem.core;

import java.util.Date;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Entry for wait queue, entries are ordered by remaining delay time
 * 
 * @author aruny
 * 
 */
class HostDelayedEntry implements Delayed {

	private String hostReady;

	public String getHostReadyEntry() {
		return hostReady;
	}

	public void setHostReadyEntry(String hostReadyEntry) {
		this.hostReady = hostReadyEntry;
	}

	public long getDelayTime() {
		return delayTime;
	}

	public void setDelayTime(long delayTime) {
		this.delayTime = delayTime;
	}

	public long getCreationTime() {
		return creationTime;
	}

	public void setCreationTime(long creationTime) {
		this.creationTime = creationTime;
	}

	private long delayTime;
	private long creationTime;

	public HostDelayedEntry(String hostReadyEntry, long delayTime) {
		this.hostReady = hostReadyEntry;
		this.delayTime = delayTime;
		this.creationTime = new Date().getTime();
	}

	@Override
	public int compareTo(Delayed o) {
		if (o instanceof HostDelayedEntry) {

			if (delayTime < ((HostDelayedEntry) o).delayTime) {
				return -1;
			} else if (delayTime > ((HostDelayedEntry) o).delayTime) {
				return +1;
			} else {
				return 0;
			}
		} else {
			throw new ClassCastException();
		}
	}

	@Override
	public long getDelay(TimeUnit unit) {

		long delay = delayTime - (new Date().getTime() - creationTime);

		switch (unit) {
		case DAYS:
			return delay / (24 * 60 * 60 * 1000);
		case HOURS:
			return delay / (60 * 60 * 1000);
		case MINUTES:
			return delay / (60 * 1000);
		case SECONDS:
			return delay / (1000);
		case MICROSECONDS:
			return delay * 10 ^ 3;
		case NANOSECONDS:
			return delay * 10 ^ 6;
		default:
			return delay;
		}
	}

}