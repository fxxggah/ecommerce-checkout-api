package dio.ecommerce.checkout.service;

import dio.ecommerce.checkout.controller.checkout.CheckoutRequest;
import dio.ecommerce.checkout.entity.Checkout;

import java.util.Optional;

public interface CheckoutService {

    Optional<Checkout> create(CheckoutRequest checkoutRequest);

    Optional<Checkout> updateStatus(String checkoutCode, Checkout.Status status);
}