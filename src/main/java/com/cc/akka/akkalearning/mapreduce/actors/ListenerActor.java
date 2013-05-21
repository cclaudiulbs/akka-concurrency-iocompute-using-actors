package com.cc.akka.akkalearning.mapreduce.actors;

import com.cc.akka.akkalearning.mapreduce.messages.TotalSizeResultMessage;

import akka.actor.UntypedActor;

/**
 * Listener actor responsibility is ONLY to display the results on a terminal
 * DisplayResultMessage
 * 
 * @author cclaudiu
 */
public class ListenerActor extends UntypedActor {
	
	private final String startPath;
	
	public ListenerActor(String startPath) {
		this.startPath = startPath;
	}

	@Override
	public void onReceive(Object rawMessage) throws Exception {
		if(rawMessage instanceof TotalSizeResultMessage) {
			TotalSizeResultMessage decodedMessage = (TotalSizeResultMessage) rawMessage;
			System.out.println("File Size Computation starting from PATH: [" + startPath + "] completed");
			System.out.println("Total Size in Kbytes: " + decodedMessage.getTotalSize());
			
			
			getContext().system().shutdown();
			
		} else {
			unhandled(rawMessage);
		}
	}
}
