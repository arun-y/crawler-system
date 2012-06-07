package com.arunwizz.crawlersystem.core;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Entry for wait queue, entries are ordered by remaining delay time
 * 
 * @author aruny
 * 
 */
class HostDelayedEntry implements Delayed {

	private final String host;
	private final long delayTime;
	private final long creationTime;

	public HostDelayedEntry(String host, long delayTime) {
		this.host = host;
		this.delayTime = delayTime;
		this.creationTime = System.currentTimeMillis();
	}

	public String getHost() {
		return host;
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

		long delay = delayTime - (System.currentTimeMillis() - creationTime);

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
	
	@Override
	public String toString() {
		return host + ":" + delayTime;
	}

}