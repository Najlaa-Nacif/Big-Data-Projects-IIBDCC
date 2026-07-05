package ma.rafik.exercice1;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

    public class TextCleanerApp {

        public static void main(String[] args) {

            // 1. CONFIGURATION KAFKA STREAMS
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "text-cleaner-app");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

            // 2. BUILD STREAM
            StreamsBuilder builder = new StreamsBuilder();

            KStream<String, String> stream = builder.stream("text-input");

            // 3. NETTOYAGE + VALIDATION
            KStream<String, String> cleaned = stream.mapValues(value ->
                    value == null ? "" :
                            value.trim()
                                    .replaceAll("\\s+", " ")
                                    .toUpperCase()
            );

            // 4. VALID MESSAGE → text-clean
            cleaned.filter((key, value) -> isValid(value))
                    .to("text-clean");

            // 5. INVALID MESSAGE → text-dead-letter
            cleaned.filter((key, value) -> !isValid(value))
                    .to("text-dead-letter");

            // 6. START APPLICATION
            KafkaStreams streams = new KafkaStreams(builder.build(), props);

            streams.start();

            // 7. SHUTDOWN PROPERLY
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        }

        // 8. VALIDATION RULES
        private static boolean isValid(String value) {

            if (value == null || value.trim().isEmpty()) return false;
            if (value.length() > 100) return false;

            String[] forbidden = {"HACK", "SPAM", "XXX"};

            for (String word : forbidden) {
                if (value.contains(word)) {
                    return false;
                }
            }

            return true;
        }
    }

