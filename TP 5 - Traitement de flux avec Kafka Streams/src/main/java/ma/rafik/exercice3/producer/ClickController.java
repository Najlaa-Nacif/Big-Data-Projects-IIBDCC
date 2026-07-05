package ma.rafik.exercice3.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class ClickController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public ClickController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/")
    public String index() {
        return "index";
    }

    @PostMapping("/click")
    @ResponseBody
    public String click() {
        kafkaTemplate.send("clicks", "user1", "click");
        return "OK";
    }
}
