package no.hvl.dat110.iotsystem;

import no.hvl.dat110.client.Client;

public class TemperatureDevice {

	private static final int COUNT = 10;

	public static void main(String[] args) {

		// simulated / virtual temperature sensor
		TemperatureSensor sn = new TemperatureSensor();

		// create a client object and use it to
		Client client = new Client("temperaturesensor", Common.BROKERHOST, Common.BROKERPORT);

		// - connect to the broker
		client.connect();

		// - publish the temperature(s)
		for (int i = 0; i < COUNT; i++) {
			int temp = sn.read();
			System.out.println("READING: " + temp);
			
			client.publish(Common.TEMPTOPIC, Integer.toString(temp));
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// - disconnect from the broker
		client.disconnect();

		System.out.println("Temperature device stopping ... ");
	}
}
