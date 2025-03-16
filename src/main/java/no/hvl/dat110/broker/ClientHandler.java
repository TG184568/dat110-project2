package no.hvl.dat110.broker;

import no.hvl.dat110.common.Stopable;
import no.hvl.dat110.messages.Message;

public class ClientHandler extends Stopable {
    
    private ClientSession client;
    private Dispatcher dispatcher;
    
    public ClientHandler(ClientSession client, Dispatcher dispatcher) {
        super("ClientHandler-" + client.getUser());
        this.client = client;
        this.dispatcher = dispatcher;
    }
    
    @Override
    public void doProcess() {
        Message msg = null;
        
        if (client.hasData()) {
            msg = client.receive();
        }
        
        // a message was received
        if (msg != null) {
            dispatcher.dispatch(client, msg);
        }
        
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
} 