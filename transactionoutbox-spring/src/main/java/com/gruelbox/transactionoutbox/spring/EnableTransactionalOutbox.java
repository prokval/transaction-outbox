package com.gruelbox.transactionoutbox.spring;

import java.lang.annotation.*;
import org.springframework.context.annotation.Import;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(TransactionalOutboxConfiguration.class)
public @interface EnableTransactionalOutbox {}
