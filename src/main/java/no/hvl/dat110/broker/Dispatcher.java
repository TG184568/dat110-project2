package no.hvl.dat110.broker;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import no.hvl.dat110.common.Logger;
import no.hvl.dat110.common.Stopable;
import no.hvl.dat110.messages.ConnectMsg;
import no.hvl.dat110.messages.CreateTopicMsg;
import no.hvl.dat110.messages.DeleteTopicMsg;
import no.hvl.dat110.messages.DisconnectMsg;
import no.hvl.dat110.messages.Message;
import no.hvl.dat110.messages.MessageType;
import no.hvl.dat110.messages.PublishMsg;
import no.hvl.dat110.messages.SubscribeMsg;
import no.hvl.dat110.messages.UnsubscribeMsg;
import no.hvl.dat110.messagetransport.Connection;

public class Dispatcher extends Stopable {

	private Storage storage;
	private Map<String, ClientHandler> clientHandlers;

	public Dispatcher(Storage storage) {
		super("Dispatcher");
		this.storage = storage;
		this.clientHandlers = new ConcurrentHashMap<>();
	}

	@Override
	public void doProcess() {
		// In the multi-threaded version, the dispatcher doesn't need to poll clients
		// Each client has its own handler thread
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void doStop() {
		super.doStop();
		
		// Stop all client handler threads
		for (ClientHandler handler : clientHandlers.values()) {
			handler.doStop();
		}
	}

	public void dispatch(ClientSession client, Message msg) {

		MessageType type = msg.getType();

		// invoke the appropriate handler method
		switch (type) {

		case DISCONNECT:
			onDisconnect((DisconnectMsg) msg);
			break;

		case CREATETOPIC:
			onCreateTopic((CreateTopicMsg) msg);
			break;

		case DELETETOPIC:
			onDeleteTopic((DeleteTopicMsg) msg);
			break;

		case SUBSCRIBE:
			onSubscribe((SubscribeMsg) msg);
			break;

		case UNSUBSCRIBE:
			onUnsubscribe((UnsubscribeMsg) msg);
			break;

		case PUBLISH:
			onPublish((PublishMsg) msg);
			break;

		default:
			Logger.log("broker dispatch - unhandled message type");
			break;

		}
	}

	// called from Broker after having established the underlying connection
	public void onConnect(ConnectMsg msg, Connection connection) {

		String user = msg.getUser();

		Logger.log("onConnect:" + msg.toString());

		storage.addClientSession(user, connection);
		
		// Create and start a handler thread for this client
		ClientSession session = storage.getSession(user);
		ClientHandler handler = new ClientHandler(session, this);
		clientHandlers.put(user, handler);
		handler.start();
	}

	// called by dispatch upon receiving a disconnect message
	public void onDisconnect(DisconnectMsg msg) {

		String user = msg.getUser();

		Logger.log("onDisconnect:" + msg.toString());
		
		// Stop the client handler thread
		ClientHandler handler = clientHandlers.get(user);
		if (handler != null) {
			handler.doStop();
			clientHandlers.remove(user);
		}

		storage.removeClientSession(user);
	}

	public void onCreateTopic(CreateTopicMsg msg) {

		Logger.log("onCreateTopic:" + msg.toString());

		// create the topic in the broker storage
		String topic = msg.getTopic();
		storage.createTopic(topic);
		
		// Log information about topic creation
		Logger.log("Topic : " + storage.getTopics().size());

	}

	public void onDeleteTopic(DeleteTopicMsg msg) {

		Logger.log("onDeleteTopic:" + msg.toString());

		// delete the topic from the broker storage
		String topic = msg.getTopic();
		storage.deleteTopic(topic);
		
		// Log information about topic deletion
		Logger.log("Topic : " + storage.getTopics().size());
	}

	public void onSubscribe(SubscribeMsg msg) {

		Logger.log("onSubscribe:" + msg.toString());

		// subscribe user to the topic
		String user = msg.getUser();
		String topic = msg.getTopic();
		
		storage.addSubscriber(user, topic);
		
		// Log information about subscription
		Set<String> subscribers = storage.getSubscribers(topic);
		Logger.log("Subscribers : " + topic + " : " + subscribers.size());

	}

	public void onUnsubscribe(UnsubscribeMsg msg) {

		Logger.log("onUnsubscribe:" + msg.toString());

		// unsubscribe user from the topic
		String user = msg.getUser();
		String topic = msg.getTopic();
		
		storage.removeSubscriber(user, topic);
		
		// Log information about unsubscription
		Set<String> subscribers = storage.getSubscribers(topic);
		Logger.log("Subscribers : " + topic + " : " + subscribers.size());
	}

	public void onPublish(PublishMsg msg) {

		Logger.log("onPublish:" + msg.toString());

		// publish the message to clients subscribed to the topic
		String topic = msg.getTopic();
		
		// Get all subscribers for this topic
		Set<String> subscribers = storage.getSubscribers(topic);
		
		if (subscribers != null) {
			// For each subscriber, send the message
			for (String subscriber : subscribers) {
				ClientSession session = storage.getSession(subscriber);
				
				// If the client is currently connected (has a session), send the message
				if (session != null) {
					session.send(msg);
				} else {
					// If the client is disconnected, buffer the message for later delivery
					storage.bufferMessage(subscriber, msg);
				}
			}
		}
	}
}
