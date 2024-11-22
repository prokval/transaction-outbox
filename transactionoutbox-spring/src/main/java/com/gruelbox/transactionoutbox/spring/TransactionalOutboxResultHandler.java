package com.gruelbox.transactionoutbox.spring;

import java.lang.annotation.*;

/**
 * Mark a method to handle result of {@link TransactionalOutbox} method call.
 *
 * <p>The link between {@code @TransactionalOutbox} and {@code @TransactionalOutboxResultHandler}
 * detected by {@link TransactionalOutboxMethodRegistry} if the following conditions are met:
 *
 * <ul>
 *     <li> {@code @TransactionalOutbox} has {@code requireResultHandler} set to {@code true}
 *     <li> Method names are equal
 *     <li> Handler method has exactly the same parameter types plus return type of original method and {@code Throwable}
 *          type. If the return type of original method is void, then this parameter should be skipped in the handler.
 * </ul>
 *
 * Here are examples of Result Handler methods:
 * <pre>
 *  &#064;Service
 *  ExternalQueueService {
 *     &#064;TransactionalOutbox(requireResultHandler = true)
 *     Long sendCustomerCreatedEvent(Customer customer) { ... }
 *
 *     &#064;TransactionalOutbox(useBeanMethodAsTopic = true, orderExpression = "#customer.id", requireResultHandler = true)
 *     Integer addCustomerPayment(Customer customer, int payment) { ... }
 *
 *     &#064;TransactionalOutbox(useBeanMethodAsTopic = true, orderExpression = "#customer.id", orderedTakeLast = true, requireResultHandler = true)
 *     void updateCustomerStatus(Customer customer, String status) { ... }
 *  }
 *
 *  &#064;Service
 *  class ExternalQueueServiceResultHandler {
 *
 *     &#064;TransactionalOutboxResultHandler
 *     public void sendCustomerCreatedEvent(Customer customer, Long result, Throwable error) { ... }
 *
 *     &#064;TransactionalOutboxResultHandler
 *     public void addCustomerPayment(Customer customer, int payment, Integer result, Throwable error) { ... }
 *
 *     &#064;TransactionalOutboxResultHandler
 *     public void updateCustomerStatus(Customer customer, String status, Throwable error) { ... }

 *  }
 * </pre>
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Inherited
@Documented
public @interface TransactionalOutboxResultHandler {

}
