package com.dellemc.course;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.UTF8StringSerializer;

import java.net.URI;

import static com.dellemc.course.Common.createClientFactory;

public class Reader {
    public static void createReaderGroup(URI uri, String scope, String stream, String readerGroupName) {
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, uri);
        readerGroupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().stream(Stream.of(scope,stream)).build());
        readerGroupManager.close();
    }
    public static void deleteReaderGroup(URI uri, String scope,String readerGroupName) {
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, uri);
        readerGroupManager.deleteReaderGroup(readerGroupName);
        readerGroupManager.close();
    }
    public static EventStreamReader<String> createReader(EventStreamClientFactory clientFactory, String groupName) {
        EventStreamReader<String> reader = clientFactory.createReader("reader1", groupName, new UTF8StringSerializer(), ReaderConfig.builder().build());
        return reader;
    }
    public static void main(String[] args) throws Exception {
        URI uri = new URI(Common.Url);
        String readerGroupName = "readergroup";
        createReaderGroup(uri, Common.Scope, Common.Stream,readerGroupName);
        EventStreamClientFactory clientFactory = createClientFactory(uri, Common.Scope);
        EventStreamReader<String> reader = createReader(clientFactory,readerGroupName);
        while (true) {
            EventRead<String> eventRead = reader.readNextEvent(2000);
            if (eventRead.isCheckpoint()) {
                continue;
            }
            String event = eventRead.getEvent();
            if (event == null) {
                System.out.println("No more event");
                break;
            }
            System.out.println(event);
        }
        reader.close();
        clientFactory.close();
        deleteReaderGroup(uri, Common.Scope,readerGroupName);
    }

}
