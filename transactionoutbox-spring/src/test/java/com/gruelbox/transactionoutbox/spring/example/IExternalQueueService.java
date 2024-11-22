package com.gruelbox.transactionoutbox.spring.example;

import com.gruelbox.transactionoutbox.spring.TransactionalOutbox;

public interface IExternalQueueService {
    @TransactionalOutbox(requireResultHandler = true)
    Long sendCustomerCreatedEvent(Customer customer);

    @TransactionalOutbox(useBeanMethodAsTopic = true, orderExpression = "#customer.id", requireResultHandler = true)
    Integer addCustomerPayment(Customer customer, int payment);

    @TransactionalOutbox(useBeanMethodAsTopic = true, orderExpression = "#customer.id", orderedTakeLast = true, requireResultHandler = true)
    void updateCustomerStatus(Customer customer, String status);
}
