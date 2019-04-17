package com.nataliaar.sethefinalproject.eventgenerator;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URISyntaxException;
import java.util.Arrays;
import com.opencsv.CSVWriter;

public class RandomEventGenerator {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        int eventCount = 100;
        if (args.length > 0) {
            eventCount = Integer.parseInt(args[0]);
        }
        boolean printEvent = false;
        if (args.length > 1) {
            printEvent = Boolean.parseBoolean(args[1]);
        }
        RandomEventProducer eventProducer = new RandomEventProducer();
        Socket flumeSocket = new Socket(InetAddress.getByName("0.0.0.0"), 44444);
        PrintWriter flumeWriter = new PrintWriter(flumeSocket.getOutputStream(), true);
        CSVWriter csvWriter = new CSVWriter(
                                flumeWriter, 
                                CSVWriter.DEFAULT_SEPARATOR, 
                                CSVWriter.NO_QUOTE_CHARACTER, 
                                CSVWriter.NO_ESCAPE_CHARACTER, 
                                CSVWriter.DEFAULT_LINE_END);
        System.out.println("Starting event generation...");
        for (int i = 0; i < eventCount; i++) {
            ProductPurchaseEvent nextEvent = eventProducer.buildRandomEvent();
            if (printEvent) {
                System.out.println("Generated event: " + nextEvent);
                System.out.println("Generated event array: " + Arrays.toString(nextEvent.toStringArray()));
            }
            csvWriter.writeNext(nextEvent.toStringArray(), false);
            Thread.sleep(1000);
        }
        System.out.println("Generated " + eventCount + " events");
        csvWriter.flush();
        csvWriter.close();
        flumeSocket.close();
    }

}
