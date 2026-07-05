package ma.rafik.exercice2;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WeatherAnalyzerApp {

    private static final String INPUT_TOPIC = "weather-data";
    private static final String OUTPUT_TOPIC = "station-averages";

    // ================= PROMETHEUS =================
    private static final Gauge avgTempGauge = Gauge.build()
            .name("weather_avg_temperature_fahrenheit")
            .help("Average temperature per station")
            .labelNames("station")
            .register();

    private static final Gauge avgHumidityGauge = Gauge.build()
            .name("weather_avg_humidity")
            .help("Average humidity per station")
            .labelNames("station")
            .register();

    // ================= MAIN =================
    public static void main(String[] args) throws Exception {

        createTopics();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-analyzer-final");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put("auto.offset.reset", "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        // 1. READ STREAM
        KStream<String, String> source = builder.stream(INPUT_TOPIC);

        // 2. PARSE SAFE
        KStream<String, WeatherData> parsed = source
                .mapValues(WeatherAnalyzerApp::parse)
                .filter((k, v) -> v != null);

        // 3. FILTER > 30°C
        KStream<String, WeatherData> filtered = parsed
                .filter((k, v) -> v.temperature > 30);

        // 4. CONVERT + KEY = station
        KStream<String, WeatherData> processed = filtered
                .mapValues(v -> new WeatherData(
                        v.station,
                        celsiusToFahrenheit(v.temperature),
                        v.humidity
                ))
                .selectKey((k, v) -> v.station);

        // 5. GROUP BY KEY
        KGroupedStream<String, WeatherData> grouped =
                processed.groupByKey(Grouped.with(Serdes.String(), new WeatherDataSerde()));

        // 6. AGGREGATION
        KTable<String, StationAggregate> result = grouped.aggregate(
                StationAggregate::new,
                (station, value, agg) -> {

                    agg.add(value.temperature, value.humidity);

                    avgTempGauge.labels(station).set(agg.avgTemp());
                    avgHumidityGauge.labels(station).set(agg.avgHumidity());

                    return agg;
                },
                Materialized.with(Serdes.String(), new StationAggregateSerde())
        );

        // 7. OUTPUT (FIXED - NO station FIELD)
        result.toStream()
                .mapValues((key, a) ->
                        key + " : Temp moyenne = " +
                                String.format("%.1f", a.avgTemp()) +
                                " F, Humidité = " +
                                String.format("%.1f", a.avgHumidity()) + " %"
                )
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // ================= RUN =================
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        DefaultExports.initialize();
        HTTPServer server = new HTTPServer(1234);

        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            server.stop();
            latch.countDown();
        }));

        try {
            streams.start();
            System.out.println("Weather Analyzer Started...");
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ================= PARSE SAFE =================
    private static WeatherData parse(String value) {
        try {
            String[] p = value.split(",");
            if (p.length != 3) return null;

            return new WeatherData(
                    p[0].trim(),
                    Double.parseDouble(p[1].trim()),
                    Double.parseDouble(p[2].trim())
            );
        } catch (Exception e) {
            return null;
        }
    }

    private static double celsiusToFahrenheit(double c) {
        return (c * 9 / 5) + 32;
    }

    // ================= TOPICS =================
    private static void createTopics() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        try (AdminClient admin = AdminClient.create(props)) {
            admin.createTopics(List.of(
                    new NewTopic(INPUT_TOPIC, 1, (short) 1),
                    new NewTopic(OUTPUT_TOPIC, 1, (short) 1)
            ));
        } catch (Exception ignored) {}
    }

    // ================= MODELS =================
    static class WeatherData {
        String station;
        double temperature;
        double humidity;

        WeatherData(String station, double temperature, double humidity) {
            this.station = station;
            this.temperature = temperature;
            this.humidity = humidity;
        }
    }

    static class StationAggregate {
        double tempSum = 0;
        double humiditySum = 0;
        long count = 0;

        void add(double temp, double hum) {
            tempSum += temp;
            humiditySum += hum;
            count++;
        }

        double avgTemp() {
            return count == 0 ? 0 : tempSum / count;
        }

        double avgHumidity() {
            return count == 0 ? 0 : humiditySum / count;
        }
    }

    // ================= SERDES =================
    static class WeatherDataSerde implements Serde<WeatherData> {

        public Serializer<WeatherData> serializer() {
            return (t, d) -> (d.station + "," + d.temperature + "," + d.humidity)
                    .getBytes(StandardCharsets.UTF_8);
        }

        public Deserializer<WeatherData> deserializer() {
            return (t, b) -> {
                try {
                    String[] p = new String(b, StandardCharsets.UTF_8).split(",");
                    return new WeatherData(
                            p[0],
                            Double.parseDouble(p[1]),
                            Double.parseDouble(p[2])
                    );
                } catch (Exception e) {
                    return null;
                }
            };
        }

        public void configure(Map<String, ?> configs, boolean isKey) {}
        public void close() {}
    }

    static class StationAggregateSerde implements Serde<StationAggregate> {

        public Serializer<StationAggregate> serializer() {
            return (t, d) -> (d.tempSum + "," + d.humiditySum + "," + d.count)
                    .getBytes(StandardCharsets.UTF_8);
        }

        public Deserializer<StationAggregate> deserializer() {
            return (t, b) -> {
                try {
                    String[] p = new String(b, StandardCharsets.UTF_8).split(",");
                    StationAggregate a = new StationAggregate();
                    a.tempSum = Double.parseDouble(p[0]);
                    a.humiditySum = Double.parseDouble(p[1]);
                    a.count = Long.parseLong(p[2]);
                    return a;
                } catch (Exception e) {
                    return null;
                }
            };
        }

        public void configure(Map<String, ?> configs, boolean isKey) {}
        public void close() {}
    }
}