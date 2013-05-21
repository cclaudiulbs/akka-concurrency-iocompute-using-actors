package com.cc.akka.akkalearning.mapreduce.actors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.cc.akka.akkalearning.mapreduce.messages.ParsedFileSizeMessage;
import com.cc.akka.akkalearning.mapreduce.messages.StartPathMessage;
import com.cc.akka.akkalearning.mapreduce.messages.TotalSizeResultMessage;
import com.google.common.collect.Lists;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorFactory;
import akka.routing.RoundRobinRouter;

/**
 * Master actor responsibility is to communicate with: Worker Actors which process the computation, and with the Listener Actor
 * sends message: StartPathMessage(String startPath) to Parser Actors, which fetch data from I/O, and return a List of Files
 * sends message: ComputeFileSizeMessage to worker-actors which compute the sum, by passing a chunk of files, and return a result:
 * FileSizeResultMessage, which Master Actor aggregates
 * @author cclaudiu
 *
 */
public class MasterActor extends UntypedActor {

	private final ActorRef parserActor;
	private final ActorRef workerActor;
	private final ActorRef listenerActor;
	private final int nrOfWorkers;
	private int totalSizeAggregated;
	private int counterCalls;
	
	@SuppressWarnings("serial")
	public MasterActor(int nrOfWorkers, ActorRef listenerActor) {
		this.nrOfWorkers = nrOfWorkers;
		this.listenerActor = listenerActor;
		
		// Create a new Actor, and using the utility UntypedActorFactory return an instance of the WorkerActor, this actor may be parameterized for more Control
		parserActor = getContext().actorOf(new Props(new UntypedActorFactory() {
			@Override
			public Actor create() throws Exception {
				return new ParserActor();
			}
		}).withRouter(new RoundRobinRouter(nrOfWorkers)), "parserActor");
		// means the router distributes the task to a nrOfWorkers, which are relying on ForkJoin -- work-stealing-algorithm
		
		workerActor = getContext().actorOf(new Props(new UntypedActorFactory() {
			@Override
			public Actor create() throws Exception {
				return new WorkerActor();
			}
		}).withRouter(new RoundRobinRouter(nrOfWorkers)), "workerActor");
	}
	
	@Override
	public void onReceive(Object rawMessage) throws Exception {
		
		if(rawMessage instanceof StartPathMessage) {
			final StartPathMessage decodedMessage = (StartPathMessage) rawMessage;
			parserActor.tell(decodedMessage, getSelf());
			
		// the parser actor finished tasks to aggregate the files into a container of sizes
		} else if(rawMessage instanceof ParsedFileSizeMessage) {
			ParsedFileSizeMessage decodedMesssage = (ParsedFileSizeMessage) rawMessage;
			
			// using google's guava partition utility
			List<List<Long>> partitionedList = Lists.partition(decodedMesssage.getFileSizes(), 100);
			
			System.out.println("Total files: " + decodedMesssage.getFileSizes().size());
			System.out.println("Master Actor partitionedList elements Size: " + partitionedList.size());
			
			for(int startWorkerIdx = 0; startWorkerIdx < nrOfWorkers; ++startWorkerIdx) {
				workerActor.tell(new ParsedFileSizeMessage(partitionedList.get(startWorkerIdx)), getSelf());
			}
			
		} else if(rawMessage instanceof TotalSizeResultMessage) {
			TotalSizeResultMessage decodedMessage = (TotalSizeResultMessage) rawMessage;
			
			totalSizeAggregated += decodedMessage.getTotalSize();
					
			if(++counterCalls == nrOfWorkers) {
				// inform the Listener and shutdown the Master-actor 
				listenerActor.tell(new TotalSizeResultMessage(totalSizeAggregated), getSelf());
				
				// stop the master actor from other computation
				getContext().stop(getSelf());
			}
		} else {
			unhandled(rawMessage);
		}
	}
	
	
	// TODO: Note: not yet done! using recursion, since it selects only the even numbers and puts them into partitions
	private static List<List<Integer>> partitionContainer(List<Integer> container, int partitionSize) {
		List<List<Integer>> returnedContainer = new ArrayList<List<Integer>>();
		List<Integer> partition = new ArrayList<Integer>(partitionSize);
		int count = container.size();
		
		if(count == 0) {
			return returnedContainer;
		} else {
			partition.add(container.get(count - 1));
			
			returnedContainer.add(partition);
		}
		
		if(partition.size() == partitionSize) {
			returnedContainer.addAll(partitionContainer(container.subList(0, count - 1), partitionSize));
		}
		
		return returnedContainer;
	}
	
	// Test 
	public static void main(String[] args) {
		List<Integer> testNumbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12);
		@SuppressWarnings("unused")
		List<List<Integer>> result = partitionContainer(testNumbers, 2);
	}
}