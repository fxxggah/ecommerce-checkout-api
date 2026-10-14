package dio.ecommerce.checkout.listener;

import dio.ecommerce.checkout.entity.Checkout;
import dio.ecommerce.checkout.event.PaymentCreatedEvent;
import dio.ecommerce.checkout.service.CheckoutService;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
@RequiredArgsConstructor
public class PaymentPaidListener {

    private final CheckoutService checkoutService;

    @Bean
    public Consumer<PaymentCreatedEvent> paymentPaid() {
        return event -> checkoutService.updateStatus(event.checkoutCode(), Checkout.Status.APPROVED);
    }
}