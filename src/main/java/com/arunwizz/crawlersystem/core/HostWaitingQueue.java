package com.arunwizz.crawlersystem.core;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arunwizz.crawlersystem.statistics.Statistician;

/**
 * A self-checking queue for delayed items
 * 
 * @author aruny
 * 
 * @param <T>
 * @param <U>
 */
public class HostWaitingQueue<T extends Delayed> implements Runnable {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(HostWaitingQueue.class);

	// waiting queue
	private DelayQueue<HostDelayedEntry> delayQueue;

	// call back queue once host is out from wait queue
	private BlockingQueue<String> readyQueue;

	public HostWaitingQueue(BlockingQueue<String> readyQueue) {
		this.delayQueue = new DelayQueue<HostDelayedEntry>();
		this.readyQueue = readyQueue;
	}

	public void put(HostDelayedEntry delayedObj) {
		delayQueue.put(delayedObj);
		Statistician.hostWaitQueueEnter(delayedObj.getHost());
	}

	public void run() {
		while (true) {
			try {
				HostDelayedEntry delayedObj = null;
				delayedObj = delayQueue.take();
				if (delayedObj != null) {
					Statistician.hostWaitQueueExit(delayedObj.getHost());
					LOGGER.trace("Adding host {} into ready queue",
							delayedObj.getHost());
					readyQueue.put(delayedObj.getHost());
				}
			} catch (InterruptedException e) {
				LOGGER.error("Interrupted!! - {}", e.getMessage());
			}
		}
	}

	@Override
	public String toString() {
		return delayQueue.toString();
	}

}
