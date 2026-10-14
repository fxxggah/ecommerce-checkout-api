package dio.ecommerce.checkout.repository;

import dio.ecommerce.checkout.entity.Checkout;
import org.springframework.data.repository.CrudRepository;

import java.util.Optional;

public interface CheckoutRepository extends CrudRepository<Checkout, Long> {

    Optional<Checkout> findByCode(String code);

}
