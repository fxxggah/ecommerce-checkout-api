package dio.ecommerce.checkout.controller;

import dio.ecommerce.checkout.controller.checkout.CheckoutRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CheckoutControllerTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.cloud.stream.kafka.binder.brokers", kafka::getBootstrapServers);
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void deveCriarCheckoutERetornar201() {
        CheckoutRequest request = new CheckoutRequest();
        request.setFirstName("Gabriel");
        request.setLastName("Oliveira");
        request.setEmail("gabriel@email.com");
        request.setAddress("Rua das Flores, 123");
        request.setComplement("Apto 45");
        request.setCountry("Brasil");
        request.setState("SP");
        request.setCep("18600-000");
        request.setSaveAddress(true);
        request.setSaveInfo(false);
        request.setPaymentMethod("CREDIT");
        request.setCardName("GABRIEL OLIVEIRA");
        request.setCardNumber("0000000000000000");
        request.setCardDate("12/30");
        request.setCardCvv("000");
        request.setProducts(List.of("notebook", "mouse"));

        ResponseEntity<Map> response = restTemplate.postForEntity("/v1/checkout", request, Map.class);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertNotNull(response.getBody().get("code"));
    }
}