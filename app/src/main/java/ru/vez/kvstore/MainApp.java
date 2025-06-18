package ru.vez.kvstore;

public class MainApp {
    public String getGreeting() {
        return "Hello World!";
    }

    public static void main(String[] args) {
        System.out.println(new MainApp().getGreeting());
    }
}
