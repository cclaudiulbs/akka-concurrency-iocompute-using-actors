package com.cc.akka.akkalearning.mapreduce.messages;

import java.util.List;

public class ParsedFileSizeMessage {
	
	private final List<Long> fileSizes;

	public ParsedFileSizeMessage(List<Long> fileSizes) {
		this.fileSizes = fileSizes;
	}

	public List<Long> getFileSizes() {
		return fileSizes;
	}
}