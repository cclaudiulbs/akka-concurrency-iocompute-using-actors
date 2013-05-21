package com.cc.akka.akkalearning.mapreduce.messages;

public class StartPathMessage {
	
	private final String startPath;
	
	public StartPathMessage(String startPath) {
		this.startPath = startPath;
	}

	public String getStartPath() {
		return startPath;
	}
}
