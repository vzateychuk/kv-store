package ru.vez.kvstore;

import java.io.IOException;

public class MainApp {

    public static void main(String[] args) throws IOException {
        StoreEngine engine = new StoreEngine("store.db");
        new KeyValueServer(engine).start();
    }
}
