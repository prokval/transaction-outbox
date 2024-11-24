package com.gruelbox.transactionoutbox.spring.example;

import static org.springframework.http.HttpStatus.NOT_FOUND;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@SuppressWarnings("unused")
@RestController
@Slf4j
class EventuallyConsistentController {

  @Autowired private CustomerRepository customerRepository;

  @Autowired private ExternalQueueService externalQueueService;

  @PostMapping(path = "/customer")
  @Transactional
  public void createCustomer(@RequestBody Customer customer) {
    customerRepository.save(customer);
    externalQueueService.sendCustomerCreatedEvent(customer);
  }

  @PostMapping(path = "/customer/status/{status}")
  @Transactional
  public void updateStatus(
      @RequestBody Customer customer, @PathVariable(name = "status") String status) {
    customerRepository.save(customer);
    externalQueueService.updateCustomerStatus(customer, status);
  }

  @PostMapping(path = "/customer/payment/{value}")
  @Transactional
  public void addPayment(
      @RequestBody Customer customer, @PathVariable(name = "value") int paymentValue) {
    customerRepository.save(customer);
    externalQueueService.addCustomerPayment(customer, paymentValue);
  }

  @GetMapping("/customer/{id}")
  public Customer getCustomer(@PathVariable long id) {
    return customerRepository
        .findById(id)
        .orElseThrow(() -> new ResponseStatusException(NOT_FOUND));
  }
}
