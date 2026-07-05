package ma.rafik.exercice3.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class ClickStreamsApp {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "click-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "./kafka-streams-state");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put("auto.offset.reset", "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        // Consommer le topic "clicks"
        KStream<String, String> clicks = builder.stream("clicks");

        // Compter les clics par clé (groupByKey + count)
        KTable<String, Long> counts = clicks.groupByKey().count();

        // Publier les résultats dans "click-counts" sous forme de String
        counts.toStream()
              .mapValues(count -> Long.toString(count))
              .to("click-counts", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.out.println("ClickStreamsApp démarré. Topic 'clicks' → comptage → 'click-counts'");
    }
}
