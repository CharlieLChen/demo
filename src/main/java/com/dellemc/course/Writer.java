package com.dellemc.course;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.UTF8StringSerializer;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;

import static com.dellemc.course.Common.createClientFactory;

public class Writer {
    public static void createStream(URI uri, String scope, String stream) throws Exception{
        StreamManager streamManager = StreamManager.create(uri);
        streamManager.createScope(scope);
        StreamConfiguration config = StreamConfiguration.builder().build();
        streamManager.createStream(scope,stream,config);
        streamManager.close();
    }
    public static EventStreamWriter<String> createWriter(String stream, EventStreamClientFactory factory) {
        return factory.createEventWriter(stream, new UTF8StringSerializer(), EventWriterConfig.builder().build());
    }

    public static void main(String[] args) throws Exception {
        URI uri = new URI(Common.Url);
        createStream(uri, Common.Scope, Common.Stream);
        EventStreamClientFactory clientFactory = createClientFactory(uri, Common.Scope);
        EventStreamWriter<String> writer = createWriter(Common.Stream, clientFactory);
        CompletableFuture<Void> future = writer.writeEvent("Hello World");
        future.get();

        System.out.println("Write data successfully");
        writer.close();
        clientFactory.close();
    }

}
