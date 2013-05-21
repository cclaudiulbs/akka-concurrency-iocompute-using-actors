package com.cc.akka.akkalearning.mapreduce.client;

import com.cc.akka.akkalearning.mapreduce.actors.ListenerActor;
import com.cc.akka.akkalearning.mapreduce.actors.MasterActor;
import com.cc.akka.akkalearning.mapreduce.messages.StartPathMessage;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActorFactory;

/**
 * The next Demo will demonstrate the real use of Actors in a concurrent real application
 * What it does it that it divides/splits the heavy task into smaller pieces which then distribute
 * them acrros a range of 10 worker actors, by defering the computation to each worker.
 * each worker-actor processes its piece of computation and when finish each of them Publish a message
 * back to the Master Actor;
 * The master-actor aggregates all those results and tells via a message-passing mechanism to the Listener Actor
 * that it can display the result of the computation.
 * The responsibility of the listener-actor, is to display the result aggregated by the master-actor, and then
 * being the last actor in the scene, it shuts down the akka-system.
 * 
 * 
 * @author cclaudiu
 *
 */
public class AkkaMapReduceClient {

	private static final int NR_OF_WORKERS = 10;
	private static final String START_PATH = "/home/cclaudiu";
	
	public static void main(String[] args) {
		ActorSystem actorSystem = ActorSystem.create("MapReduceAkkaSystem");
		
//		This approach does not let us parameterize the ListenerActor, hence replace it for the UntypedActorFactory
//		final ActorRef listenerActor = actorSystem.actorOf(new Props(ListenerActor.class), "listenerActor");
		
		final ActorRef listenerActor = actorSystem.actorOf(new Props(new UntypedActorFactory() {
			
			@Override
			public Actor create() throws Exception {
				return new ListenerActor(START_PATH);
			}
		}), "listenerActor");
		
		
		final ActorRef masterActor = actorSystem.actorOf(new Props(new UntypedActorFactory() {
			@Override
			public Actor create() throws Exception {
				return new MasterActor(NR_OF_WORKERS, listenerActor);
			}
		}), "masterActor");
		
		masterActor.tell(new StartPathMessage(START_PATH), masterActor);
	}
}