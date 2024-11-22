package com.gruelbox.transactionoutbox.spring.example;

import lombok.Getter;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Getter
@Service
class ExternalQueueService implements IExternalQueueService {

  private final Set<Long> attempted = new HashSet<>();
  private final List<Customer> sent = new CopyOnWriteArrayList<>();

  private final Map<Long, List<Integer>> payments = new ConcurrentHashMap<>();

  private final Map<Long, List<String>> statuses = new ConcurrentHashMap<>();


  @Override
  public Long sendCustomerCreatedEvent(Customer customer) {
    if (attempted.add(customer.getId())) {
      throw new RuntimeException("Temporary failure, try again");
    }
    sent.add(customer);
    return customer.getId();
  }

  @Override
  public Integer addCustomerPayment(Customer customer, int payment) {
    if (!payments.containsKey(customer.getId())) {
      payments.put(customer.getId(), new ArrayList<>());
      throw new RuntimeException("Temporary failure, try again");
    }
    List<Integer> payments = this.payments.get(customer.getId());
    payments.add(payment);
    return payments.stream().mapToInt(i -> i).sum();
  }

  @Override
  public void updateCustomerStatus(Customer customer, String status) {
    if (!statuses.containsKey(customer.getId())) {
      statuses.put(customer.getId(), new ArrayList<>());
      throw new RuntimeException("Temporary failure, try again");
    }
    statuses.get(customer.getId()).add(status);
  }

  public void clear() {
    attempted.clear();
    sent.clear();
    payments.clear();
    statuses.clear();
  }
}
