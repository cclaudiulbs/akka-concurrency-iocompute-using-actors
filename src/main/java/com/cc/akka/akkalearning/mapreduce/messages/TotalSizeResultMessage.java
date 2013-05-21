package com.cc.akka.akkalearning.mapreduce.messages;

public class TotalSizeResultMessage {

	private final long totalSize;
	
	public TotalSizeResultMessage(long totalSize) {
		this.totalSize = totalSize;
	}

	public long getTotalSize() {
		return totalSize;
	}
}
