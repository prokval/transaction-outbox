package com.gruelbox.transactionoutbox.spring.example;

import com.gruelbox.transactionoutbox.spring.EnableTransactionalOutbox;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@SpringBootApplication
@EnableTransactionalOutbox
@EnableTransactionManagement
public class SpringTestsConfiguration {}
