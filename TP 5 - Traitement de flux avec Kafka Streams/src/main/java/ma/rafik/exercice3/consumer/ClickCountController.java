package ma.rafik.exercice3.consumer;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/clicks")
public class ClickCountController {

    private final ClickCountService clickCountService;

    public ClickCountController(ClickCountService clickCountService) {
        this.clickCountService = clickCountService;
    }

    @GetMapping("/count")
    public Map<String, Long> getCount() {
        return Map.of("totalClicks", clickCountService.getCount());
    }


    // 👇 AJOUT ICI
    @GetMapping("/test")
    public String test() {
        return "OK";
    }
}

