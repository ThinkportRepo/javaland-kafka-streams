package digital.thinkport.javaland;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

public class StreamsApplication {

    private static final String NEW_INSURANCE_REQUEST_TOPIC = "new-insurance-request";
    private static final String CREDIT_RATING_RESULT_TOPIC = "credit-rating-result";
    private static final String INSURANCE_CONCLUDED_TOPIC = "insurance-concluded";
    private static final String CREDIT_RATING_INSUFFICIENT_TOPIC = "credit-rating-insufficient";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");



        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, NewInsuranceRequest> newInsuranceRequestStream = builder.stream(NEW_INSURANCE_REQUEST_TOPIC);
        KStream<String, CreditRatingResult> creditRatingResultStream = builder.stream(CREDIT_RATING_RESULT_TOPIC);

        // 1.
        newInsuranceRequestStream
                .filter((key, value) -> value.getTypeOfInsurance().equals("Hausrat"))
                .mapValues(((key, value) -> String.format("Neue Hausratversicherung angefragt von %s %s", value.getFirstName(), value.getLastName())))
                .foreach((key, value) -> System.out.println(value));
        // 2.
        newInsuranceRequestStream.join(creditRatingResultStream, (newInsuranceRequest, creditRatingResult) -> getBuild(newInsuranceRequest, creditRatingResult), JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5)))
                .to(INSURANCE_CONCLUDED_TOPIC);

        // 3.
        newInsuranceRequestStream.join(creditRatingResultStream, (newInsuranceRequest, creditRatingResult) -> getBuild(newInsuranceRequest, creditRatingResult), JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5)))
                .to(((key, value, recordContext) -> value.getRatingResult() > 50 ? INSURANCE_CONCLUDED_TOPIC : CREDIT_RATING_INSUFFICIENT_TOPIC));
        builder.stream(INSURANCE_CONCLUDED_TOPIC).foreach((key, value) -> System.out.println("concluded:" + value));
        builder.stream(CREDIT_RATING_INSUFFICIENT_TOPIC).foreach((key, value) -> System.out.println("credit insufficient: " + value));

        KafkaStreams kafkaStreams = new KafkaStreams(buildTopology(builder), properties);
        kafkaStreams.start();
    }

    public static Topology buildTopology(StreamsBuilder streamsBuilder) {
        KStream<String, NewInsuranceRequest> newInsuranceRequestStream = streamsBuilder.stream(NEW_INSURANCE_REQUEST_TOPIC);
        KStream<String, CreditRatingResult> creditRatingResultStream = streamsBuilder.stream(CREDIT_RATING_RESULT_TOPIC);
        newInsuranceRequestStream.join(creditRatingResultStream, (newInsuranceRequest, creditRatingResult) -> getBuild(newInsuranceRequest, creditRatingResult), JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5)))
                .to(((key, value, recordContext) -> value.getRatingResult() > 50 ? INSURANCE_CONCLUDED_TOPIC : CREDIT_RATING_INSUFFICIENT_TOPIC));
        return streamsBuilder.build();
    }

    private static InsuranceConcluded getBuild(NewInsuranceRequest newInsuranceRequest, CreditRatingResult creditRatingResult) {
        return InsuranceConcluded.newBuilder()
                .setFirstName(newInsuranceRequest.getFirstName())
                .setLastName(newInsuranceRequest.getLastName())
                .setRatingResult(creditRatingResult.getRatingResult())
                .setConclusionTimestamp(Instant.now().getEpochSecond())
                .setTypeOfInsurance(newInsuranceRequest.getTypeOfInsurance())
                .build();
    }


}
