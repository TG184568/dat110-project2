package no.hvl.dat110.broker;

import java.util.Collection;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import no.hvl.dat110.common.TODO;
import no.hvl.dat110.common.Logger;
import no.hvl.dat110.messagetransport.Connection;
import no.hvl.dat110.messages.Message;
import no.hvl.dat110.messages.PublishMsg;

public class Storage {

	// data structure for managing subscriptions
	// maps from a topic to set of subscribed users
	protected ConcurrentHashMap<String, Set<String>> subscriptions;
	
	// data structure for managing currently connected clients
	// maps from user to corresponding client session object
	protected ConcurrentHashMap<String, ClientSession> clients;
	
	// data structure for buffering messages for disconnected clients
	// maps from user to a list of messages that were published while the client was disconnected
	protected ConcurrentHashMap<String, List<PublishMsg>> bufferedMessages;

	public Storage() {
		subscriptions = new ConcurrentHashMap<String, Set<String>>();
		clients = new ConcurrentHashMap<String, ClientSession>();
		bufferedMessages = new ConcurrentHashMap<String, List<PublishMsg>>();
	}

	public Collection<ClientSession> getSessions() {
		return clients.values();
	}

	public Set<String> getTopics() {
		return subscriptions.keySet();
	}

	// get the session object for a given user
	// session object can be used to send a message to the user
	public ClientSession getSession(String user) {
		ClientSession session = clients.get(user);
		return session;
	}

	public Set<String> getSubscribers(String topic) {
		return (subscriptions.get(topic));
	}

	public void addClientSession(String user, Connection connection) {
		// add corresponding client session to the storage
		ClientSession session = new ClientSession(user, connection);
		clients.put(user, session);
		
		// Check if there are buffered messages for this user
		List<PublishMsg> messages = bufferedMessages.get(user);
		if (messages != null && !messages.isEmpty()) {
			// Send all buffered messages to the client
			for (PublishMsg msg : messages) {
				session.send(msg);
			}
			// Clear the buffer after sending
			bufferedMessages.remove(user);
		}
	}

	public void removeClientSession(String user) {
		// disconnect the client (user) 
		// and remove client session for user from the storage
		ClientSession session = clients.get(user);
		
		if (session != null) {
			session.disconnect();
			clients.remove(user);
		}
	}

	public void createTopic(String topic) {
		// create topic in the storage
		// Using newKeySet to create a concurrent set
		subscriptions.put(topic, ConcurrentHashMap.newKeySet());
	}

	public void deleteTopic(String topic) {
		// delete topic from the storage
		subscriptions.remove(topic);
	}

	public void addSubscriber(String user, String topic) {
		// add the user as subscriber to the topic
		Set<String> subscribers = subscriptions.get(topic);
		
		if (subscribers != null) {
			subscribers.add(user);
		}
	}

	public void removeSubscriber(String user, String topic) {
		// remove the user as subscriber to the topic
		Set<String> subscribers = subscriptions.get(topic);
		
		if (subscribers != null) {
			subscribers.remove(user);
		}
	}
	
	// Method to buffer a message for a disconnected client
	public void bufferMessage(String user, PublishMsg msg) {
		bufferedMessages.computeIfAbsent(user, k -> new ArrayList<>()).add(msg);
	}
}
