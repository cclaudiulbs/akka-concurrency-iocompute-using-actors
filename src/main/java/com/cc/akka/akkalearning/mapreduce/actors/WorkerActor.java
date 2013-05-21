package com.cc.akka.akkalearning.mapreduce.actors;

import com.cc.akka.akkalearning.mapreduce.messages.ParsedFileSizeMessage;
import com.cc.akka.akkalearning.mapreduce.messages.TotalSizeResultMessage;

import akka.actor.UntypedActor;

public class WorkerActor extends UntypedActor {
	
	private int totalSize;
	
	/**
	 * Invoked by the Mailbox core component, which invokes the Actor's onReceive method
	 */
	@Override
	public void onReceive(Object rawMessage) throws Exception {
		
		if(rawMessage instanceof ParsedFileSizeMessage) {
			
			ParsedFileSizeMessage decodedMessage = (ParsedFileSizeMessage) rawMessage;
			for(Long each: decodedMessage.getFileSizes()) {
				totalSize += each;
			}
			
			getSender().tell(new TotalSizeResultMessage(totalSize), getSelf());
			
		} else {
			unhandled(rawMessage);
		}
		
	}
}
