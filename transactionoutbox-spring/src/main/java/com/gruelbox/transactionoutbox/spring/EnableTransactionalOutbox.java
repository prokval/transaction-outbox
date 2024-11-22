package com.gruelbox.transactionoutbox.spring;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(TransactionalOutboxConfiguration.class)
public @interface EnableTransactionalOutbox {

}
