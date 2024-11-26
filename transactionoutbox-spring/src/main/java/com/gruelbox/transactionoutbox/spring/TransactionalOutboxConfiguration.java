package com.gruelbox.transactionoutbox.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.jackson.JacksonInvocationSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@Import({
  TransactionOutboxProperties.class,
  SpringInstantiator.class,
  SpringTransactionManager.class,
  DelegatedTransactionOutbox.class,
  TransactionalOutboxBeanPostProcessor.class,
  TransactionalOutboxResultHandlerBeanPostProcessor.class,
  TransactionalOutboxMethodRegistry.class
})
@EnableScheduling
public class TransactionalOutboxConfiguration {

  @Autowired private ObjectMapper objectMapper;

  @Autowired private TransactionOutboxProperties properties;

  @Bean
  public Persistor persistor() {

    InvocationSerializer serializer =
        properties.isUseJackson()
            ? JacksonInvocationSerializer.builder().mapper(objectMapper).build()
            : InvocationSerializer.createDefaultJsonSerializer();

    if (properties.getSqlDialect() == TransactionOutboxProperties.OutboxSqlDialect.POSTGRESQL_SEQ) {
      return PgSeqPersistor.builder()
          .serializer(serializer)
          .dialect(properties.getSqlDialect().getDialect())
          .build();
    } else {
      return DefaultPersistor.builder()
          .serializer(serializer)
          .dialect(properties.getSqlDialect().getDialect())
          .build();
    }
  }

  @Bean
  public TransactionOutboxBackgroundProcessor transactionOutboxBackgroundProcessor(
      TransactionOutbox transactionOutbox) {
    return new TransactionOutboxBackgroundProcessor(transactionOutbox);
  }
}
