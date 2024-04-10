package digital.thinkport.javaland;


import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class StreamsApplicationTest {
    @Test
    void test() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        Topology topology = StreamsApplication.buildTopology(streamsBuilder);
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put("schema.registry.url", "mock://localhost:8081");

        Serde<NewInsuranceRequest> newInsuranceRequestSerde = createMockSerde();
        Serde<CreditRatingResult> creditRatingResultSerde = createMockSerde();
        Serde<InsuranceConcluded> insuranceConcludedSerde = createMockSerde();



        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, properties)) {
            TestInputTopic<String, NewInsuranceRequest> inputTopic = topologyTestDriver.createInputTopic("new-insurance-request", Serdes.String().serializer(), newInsuranceRequestSerde.serializer());
            TestInputTopic<String, CreditRatingResult> inputTopic2 = topologyTestDriver.createInputTopic("credit-rating-result", Serdes.String().serializer(), creditRatingResultSerde.serializer());
            TestOutputTopic<String, InsuranceConcluded> output = topologyTestDriver.createOutputTopic("insurance-concluded", Serdes.String().deserializer(), insuranceConcludedSerde.deserializer());

            TestRecord<String, NewInsuranceRequest> input1 = new TestRecord<>("key", new NewInsuranceRequest("test", "test", "Hausrat"));
            TestRecord<String, CreditRatingResult> input2 = new TestRecord<>("key", new CreditRatingResult(71));
            inputTopic.pipeInput(input1);
            inputTopic2.pipeInput(input2);
            TestRecord<String, InsuranceConcluded> result = output.readRecord();
            assertNotNull(result);
            assertEquals(71, result.value().getRatingResult());
        }
    }

    private static <T extends SpecificRecord> SpecificAvroSerde<T> createMockSerde() {
        var serde = new SpecificAvroSerde<T>();
        serde.configure(
                Map.of("schema.registry.url", "mock://localhost:8081"),
                false
        );
        return serde;
    }
}
