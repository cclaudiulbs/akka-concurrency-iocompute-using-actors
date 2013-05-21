package com.cc.akka.akkalearning.mapreduce.actors;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.cc.akka.akkalearning.mapreduce.messages.ParsedFileSizeMessage;
import com.cc.akka.akkalearning.mapreduce.messages.StartPathMessage;

import akka.actor.UntypedActor;

public class ParserActor extends UntypedActor {

	private final List<Long> fileSizes = new ArrayList<Long>();
	
	@Override
	public void onReceive(Object rawMessage) throws Exception {
		
		if(rawMessage instanceof StartPathMessage) {
			StartPathMessage decodedMessage = (StartPathMessage) rawMessage;
			List<File> fetchedFiles = recursivelyGetFiles(new File(decodedMessage.getStartPath()));
			
			for(File each: fetchedFiles) {
				fileSizes.add(each.getTotalSpace() / 1024);
			}
			
			getSender().tell(new ParsedFileSizeMessage(fileSizes), getSelf());
		} else {
			unhandled(rawMessage);
		}
	}
	
	private static List<File> recursivelyGetFiles(File each) {
		List<File> container = new ArrayList<File>();
		
		if(each.isFile()) {
			container.add(each);
		} else if(each.isDirectory()) {
			for(File eachFile: each.listFiles()) {
				container.addAll(recursivelyGetFiles(eachFile));
			}
		}
		return container;
	}
	
	// Test
	public static void main(String[] args) {
		for(File each: recursivelyGetFiles(new File("/home/cclaudiu"))) {
			System.out.println(each.getName());
		}
	}
}
