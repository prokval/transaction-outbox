package com.gruelbox.transactionoutbox.spring.example;


import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;

public class NoTransactionalOutboxTest extends AbstractSpringBootTest {

    @Autowired
    private NonOutboxExternalQueueService nonOutboxExternalQueueService;

    Customer joe = new Customer(1L, "Joe", "Strummer");

    @Test
    void test_NonOutboxService_works_without_TransactionalOutbox_processing() {
        assertThat(nonOutboxExternalQueueService.sendCustomerCreatedEvent(joe)).isEqualTo(100L);
        assertThat(nonOutboxExternalQueueService.addCustomerPayment(joe, 10)).isEqualTo(10);
    }
}
