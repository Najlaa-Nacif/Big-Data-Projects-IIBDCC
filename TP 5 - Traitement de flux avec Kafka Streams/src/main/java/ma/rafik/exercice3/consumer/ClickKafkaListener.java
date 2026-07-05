package ma.rafik.exercice3.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ClickKafkaListener {

    private final ClickCountService clickCountService;

    public ClickKafkaListener(ClickCountService clickCountService) {
        this.clickCountService = clickCountService;
    }

    @KafkaListener(topics = "click-counts", groupId = "click-consumer-group")
    public void listen(ConsumerRecord<String, String> record) {
        try {
            long count = Long.parseLong(record.value());
            clickCountService.updateCount(count);
            System.out.println("Clics reçus - clé: " + record.key() + ", total: " + count);
        } catch (NumberFormatException e) {
            System.err.println("Valeur invalide reçue: " + record.value());
        }
    }
}
