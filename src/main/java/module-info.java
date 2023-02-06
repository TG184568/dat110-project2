module no.hvl.dat110 {
    requires javafx.controls;
    requires com.google.gson;
    opens no.hvl.dat110.messages to com.google.gson;
    exports no.hvl.dat110.chapp;
}