package dio.ecommerce.checkout.event;

public record CheckoutCreatedEvent(String checkoutCode, String status) {}