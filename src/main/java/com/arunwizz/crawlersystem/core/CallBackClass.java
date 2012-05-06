package com.arunwizz.crawlersystem.core;

import java.util.Date;
import java.util.Queue;

import com.arunwizz.crawlersystem.statistics.Statistician;

class CallBackClass implements ICallBackClass<HostDelayedEntry, HostReadyEntry> {

	private Queue<HostReadyEntry> readyQueue;

	public CallBackClass(Queue<HostReadyEntry> readyQueue) {
		this.readyQueue = readyQueue;
	}

	@Override
	public void callBack(HostDelayedEntry hostDelayedEntry) {
		Statistician.hostWaitQueueExit(hostDelayedEntry.getHostReadyEntry()
				.getHostname(), new Date().getTime());
		synchronized (readyQueue) {
			readyQueue.add(hostDelayedEntry.getHostReadyEntry());
			readyQueue.notify();
		}
	}

}