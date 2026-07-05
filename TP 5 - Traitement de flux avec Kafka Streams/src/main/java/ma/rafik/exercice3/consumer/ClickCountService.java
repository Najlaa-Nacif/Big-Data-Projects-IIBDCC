package ma.rafik.exercice3.consumer;

import org.springframework.stereotype.Service;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class ClickCountService {

    private final AtomicLong totalClicks = new AtomicLong(0);

    public void updateCount(long count) {
        totalClicks.set(count);
    }

    public long getCount() {
        return totalClicks.get();
    }
}
