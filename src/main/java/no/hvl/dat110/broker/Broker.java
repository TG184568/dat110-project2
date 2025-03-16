package no.hvl.dat110.broker;

import no.hvl.dat110.common.Logger;
import no.hvl.dat110.common.Stopable;
import no.hvl.dat110.messages.ConnectMsg;
import no.hvl.dat110.messages.MessageUtils;
import no.hvl.dat110.messagetransport.Connection;
import no.hvl.dat110.messagetransport.MessagingServer;

public class Broker extends Stopable {

	private MessagingServer server;
	private Dispatcher dispatcher;
	private boolean stopable = false;
	private int maxaccept = 0;
	
	public Broker(int port) {
		super("Broker");
		server = new MessagingServer(port);
		dispatcher = new Dispatcher(new Storage());
	}

	public void setMaxAccept(int n) {
		this.stopable = true;
		this.maxaccept = n;
	}
	
	@Override
	public void doProcess() {
		
		Logger.lg(".");
		
		Connection connection = server.accept();
		
		Logger.log("Broker accept [0]");
		
		boolean success = true;
		
		String user = "";
		
		if (connection != null) {
			
			try {
				
				ConnectMsg msg = (ConnectMsg) MessageUtils.receive(connection);
				
				user = msg.getUser();
				
				Logger.log("onConnect:" + msg.toString());
				
				dispatcher.onConnect(msg, connection);
				
			} catch (Exception e) {
				Logger.log("Exception in broker accept/connect");
				success = false;
			}
			
		}
		
		if (stopable) {
            maxaccept--;
            
            if (maxaccept < 1) {
                super.doStop();
            }
        }
	}

	public void startBroker() {
		dispatcher.start();
		super.start();
	}
	
	public void stopBroker() {
		super.doStop();
		dispatcher.doStop();
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
