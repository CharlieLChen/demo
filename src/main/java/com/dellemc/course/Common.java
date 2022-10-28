package com.dellemc.course;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;

import java.net.URI;

public class Common {
    public static String Url = "tcp://10.247.101.103:9090";
    public static String Scope = "dell";
    public static String Stream = "demo";


    public static EventStreamClientFactory createClientFactory(URI uri, String scope) {
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(uri).build();
        return EventStreamClientFactory.withScope(scope, clientConfig);
    }

}
