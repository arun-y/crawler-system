package com.arunwizz.crawlersystem.core;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelayCallBackQueue<T extends Delayed, U> implements Runnable {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DelayCallBackQueue.class);
	
	private Object mutex = new Object(); 
	
	private DelayQueue<T> delayQueue;
	private ICallBackClass<T, U> callBackClass;
	
	public DelayCallBackQueue(ICallBackClass<T, U> callBackClass) {
		this.delayQueue = new DelayQueue<T>();
		this.callBackClass = callBackClass;
	}
	
	public void put(T delayedObj) {
		synchronized (mutex) {
			delayQueue.put(delayedObj);
			mutex.notify();
		}
	}
	
	public void run() {
		while (true) {
			T delayedObj = delayQueue.poll(); 
			if (delayedObj != null) {
				callBackClass.callBack(delayedObj);
			} else {
				try {
					synchronized (mutex) {
						mutex.wait(100);
					}
				} catch (InterruptedException e) {
					LOGGER.error(e.getMessage());
				}
			}
		}
	}

}
