package ma.rafik.exercice3.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ClickProducerApp {

    public static void main(String[] args) {
        System.setProperty("spring.profiles.active", "producer");
        SpringApplication.run(ClickProducerApp.class, args);
    }
}
