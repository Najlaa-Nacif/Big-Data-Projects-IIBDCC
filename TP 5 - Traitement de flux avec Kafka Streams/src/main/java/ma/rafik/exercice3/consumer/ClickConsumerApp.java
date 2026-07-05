package ma.rafik.exercice3.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ClickConsumerApp {

    public static void main(String[] args) {
        System.setProperty("spring.profiles.active", "consumer");
        SpringApplication.run(ClickConsumerApp.class, args);
    }
}
