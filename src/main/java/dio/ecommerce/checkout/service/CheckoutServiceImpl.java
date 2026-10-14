package dio.ecommerce.checkout.service;

import dio.ecommerce.checkout.controller.checkout.CheckoutRequest;
import dio.ecommerce.checkout.entity.Checkout;
import dio.ecommerce.checkout.entity.CheckoutItem;
import dio.ecommerce.checkout.entity.Shipping;
import dio.ecommerce.checkout.event.CheckoutCreatedEvent;
import dio.ecommerce.checkout.repository.CheckoutRepository;
import dio.ecommerce.checkout.util.UUIDUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class CheckoutServiceImpl implements CheckoutService {

    private static final String CHECKOUT_CREATED_OUTPUT = "checkout-created-output";

    private final CheckoutRepository checkoutRepository;
    private final StreamBridge streamBridge;
    private final UUIDUtil uuidUtil;

    @Override
    public Optional<Checkout> create(CheckoutRequest checkoutRequest) {
        log.info("M=create, checkoutRequest={}", checkoutRequest);

        final Checkout checkoutEntity = Checkout.builder()
                .code(uuidUtil.createUUID().toString())
                .status(Checkout.Status.CREATED)
                .saveAddress(checkoutRequest.getSaveAddress())
                .saveInformation(checkoutRequest.getSaveInfo())
                .shipping(Shipping.builder()
                        .address(checkoutRequest.getAddress())
                        .complement(checkoutRequest.getComplement())
                        .country(checkoutRequest.getCountry())
                        .state(checkoutRequest.getState())
                        .cep(checkoutRequest.getCep())
                        .build())
                .build();

        checkoutEntity.setItems(checkoutRequest.getProducts()
                .stream()
                .map(product -> CheckoutItem.builder()
                        .checkout(checkoutEntity)
                        .product(product)
                        .build())
                .collect(Collectors.toList()));

        final Checkout entity = checkoutRepository.save(checkoutEntity);

        streamBridge.send(CHECKOUT_CREATED_OUTPUT,
                new CheckoutCreatedEvent(entity.getCode(), entity.getStatus().name()));
        return Optional.of(entity);
    }

    @Override
    public Optional<Checkout> updateStatus(String checkoutCode, Checkout.Status status) {
        return checkoutRepository.findByCode(checkoutCode).map(checkout -> {
            checkout.setStatus(status);
            return checkoutRepository.save(checkout);
        });
    }
}