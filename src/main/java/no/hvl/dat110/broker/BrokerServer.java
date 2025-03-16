package no.hvl.dat110.broker;

import no.hvl.dat110.common.Logger;

public class BrokerServer {

	public static void main(String[] args) {
		
		int BROKER_DEFAULTPORT = 8080;
		
		int port = BROKER_DEFAULTPORT;
		
		if (args != null) {
			if (args.length > 0) {
				try {
					port = Integer.parseInt(args[0]);
				} catch (NumberFormatException ex) {
					System.out.println("Usage: java BrokerServer [port]");
					System.exit(1);
				}
			}
		}
		
		Logger.log("Broker server : " + port);
		
		Broker broker = new Broker(port);
		
		broker.startBroker();
		
		// wait for the broker to terminate
		try {
			broker.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		Logger.log("Broker server stopping ... ");
	}
}
