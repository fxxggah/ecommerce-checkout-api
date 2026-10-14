package dio.ecommerce.checkout.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import dio.ecommerce.checkout.entity.Checkout;
import dio.ecommerce.checkout.event.PaymentCreatedEvent;
import dio.ecommerce.checkout.service.CheckoutService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Consumer;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class PaymentPaidListener {

    private final CheckoutService checkoutService;

    @Bean
    public Consumer<byte[]> paymentPaid(ObjectMapper objectMapper) {
        return payload -> {
            try {
                final PaymentCreatedEvent event = objectMapper.readValue(payload, PaymentCreatedEvent.class);
                log.info("paymentCreatedEvent={}", event);
                checkoutService.updateStatus(event.checkoutCode(), Checkout.Status.APPROVED);
            } catch (IOException e) {
                throw new UncheckedIOException("Payload inválido em payment-paid: " + new String(payload), e);
            }
        };
    }
}