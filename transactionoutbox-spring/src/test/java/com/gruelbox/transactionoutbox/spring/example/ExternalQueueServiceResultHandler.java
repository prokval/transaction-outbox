package com.gruelbox.transactionoutbox.spring.example;

import com.gruelbox.transactionoutbox.spring.TransactionalOutboxResultHandler;
import lombok.Getter;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

@Getter
@Service
class ExternalQueueServiceResultHandler {

  private final List<ResultRecord> records = new CopyOnWriteArrayList<>();

  record ResultRecord(Customer customer, Object parameter, Object result, String errorMessage) { }

  @TransactionalOutboxResultHandler
  public void sendCustomerCreatedEvent(Customer customer, Long result, Throwable error) {
    records.add(new ResultRecord(customer, null, result, error != null ? error.getMessage() : null));
  }

  @TransactionalOutboxResultHandler
  public void addCustomerPayment(Customer customer, int payment, Integer result, Throwable error) {
    records.add(new ResultRecord(customer, payment, result, error != null ? error.getMessage() : null));
  }

  @TransactionalOutboxResultHandler
  public void updateCustomerStatus(Customer customer, String status, Throwable error) {
    records.add(new ResultRecord(customer, status, null, error != null ? error.getMessage() : null));
  }

  void clear() {
    records.clear();
  }
}
