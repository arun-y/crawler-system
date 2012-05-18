package com.arunwizz.crawlersystem.core;

import java.util.Date;
import java.util.Queue;

import com.arunwizz.crawlersystem.statistics.Statistician;

class CallBackClass implements ICallBackClass<HostDelayedEntry, String> {

	private Queue<String> readyQueue;

	public CallBackClass(Queue<String> readyQueue) {
		this.readyQueue = readyQueue;
	}

	@Override
	public void callBack(HostDelayedEntry hostDelayedEntry) {
		Statistician.hostWaitQueueExit(hostDelayedEntry.getHostReadyEntry(),
				new Date().getTime());
		synchronized (readyQueue) {
			if (!readyQueue.contains(hostDelayedEntry.getHostReadyEntry())) {
				readyQueue.add(hostDelayedEntry.getHostReadyEntry());
			}
			readyQueue.notify();
		}
	}

}