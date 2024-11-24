package com.gruelbox.transactionoutbox.spring.example;

import com.gruelbox.transactionoutbox.spring.NoTransactionalOutbox;
import lombok.Getter;
import org.springframework.stereotype.Service;

@Getter
@Service
@NoTransactionalOutbox
class NonOutboxExternalQueueService implements IExternalQueueService {

  @Override
  public Long sendCustomerCreatedEvent(Customer customer) {
    return 100L;
  }

  @Override
  public Integer addCustomerPayment(Customer customer, int payment) {
    return 10;
  }

  @Override
  public void updateCustomerStatus(Customer customer, String status) {}
}
