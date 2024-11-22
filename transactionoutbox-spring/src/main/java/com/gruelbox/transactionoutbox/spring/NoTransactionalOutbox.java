package com.gruelbox.transactionoutbox.spring;

import java.lang.annotation.*;

/**
 * Disables Transactional Outbox for the specific method or whole type.
 * Useful when {@code @TransactionalOutbox} is defined on interface level, and you
 * want to disable it for specific implementation.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
@Inherited
@Documented
public @interface NoTransactionalOutbox {

}
