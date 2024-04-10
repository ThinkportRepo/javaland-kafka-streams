package digital.thinkport.javaland;

import com.github.javafaker.Faker;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class FakeDataGenerator {
    private static List<String> insuranceTypes = List.of("Hausrat", "Haftpflicht", "Wohngebäude", "Rechtsschutz", "Hund");

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "true");

        Faker faker = new Faker(Locale.GERMAN);

        KafkaProducer<String, NewInsuranceRequest> kafkaProducerNewInsurance = new KafkaProducer<>(properties);
        KafkaProducer<String, CreditRatingResult> kafkaProducerCreditRating = new KafkaProducer<>(properties);
        while (true) {
            String id = UUID.randomUUID().toString();
            sendNewInsuranceRequest(id, faker, kafkaProducerNewInsurance);
            sendCreditRatingResult(id, kafkaProducerCreditRating);
            Thread.sleep(1000);
        }


    }

    private static void sendNewInsuranceRequest(String id, Faker faker, KafkaProducer<String, NewInsuranceRequest> kafkaProducerNewInsurance) {
        var newInsuranceRequest = NewInsuranceRequest.newBuilder()
                .setTypeOfInsurance(pickRandom(insuranceTypes))
                .setFirstName(faker.name().firstName())
                .setLastName(faker.name().lastName())
                        .build();
        kafkaProducerNewInsurance.send(new ProducerRecord<>("new-insurance-request",id,  newInsuranceRequest));
        kafkaProducerNewInsurance.flush();
    }

    private static void sendCreditRatingResult(String id, KafkaProducer<String, CreditRatingResult> kafkaProducerCreditRating) {
        var creditRatingResult = CreditRatingResult.newBuilder()
                        .setRatingResult(ThreadLocalRandom.current().nextInt(100))
                                .build();
        kafkaProducerCreditRating.send(new ProducerRecord<>("credit-rating-result",id, creditRatingResult));
        kafkaProducerCreditRating.flush();
    }

    private static String pickRandom(List<String> list) {
        int randomIndex = ThreadLocalRandom.current().nextInt(list.size());
        return list.get(randomIndex);
    }
}
