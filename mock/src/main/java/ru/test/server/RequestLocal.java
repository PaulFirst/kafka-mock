package ru.test.server;

public class RequestLocal {

    public static RequestLocal withThreadConfinedCaching() {
        return new RequestLocal();
    }

    public void close() {

    }
}
