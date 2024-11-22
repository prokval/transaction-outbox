package com.gruelbox.transactionoutbox.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.PgSeqDialect;
import com.gruelbox.transactionoutbox.PgSeqPersistor;
import com.gruelbox.transactionoutbox.TransactionOutbox;
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
        TransactionalOutboxBeanPostProcessor.class,
        TransactionalOutboxResultHandlerBeanPostProcessor.class,
        TransactionalOutboxMethodRegistry.class
})
@EnableScheduling
public class TransactionalOutboxConfiguration {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private TransactionOutboxProperties properties;

    @Bean
    public Persistor persistor() {
        if (properties.isUseJackson()) {
            return PgSeqPersistor.builder()
                    .serializer(JacksonInvocationSerializer.builder().mapper(objectMapper).build())
                    .dialect(PgSeqDialect.POSTGRESQL_SEQ)
                    .build();
        } else {
            return PgSeqPersistor.builder()
                    .dialect(PgSeqDialect.POSTGRESQL_SEQ)
                    .build();
        }
    }

    @Bean
    public TransactionOutbox transactionOutbox(
            SpringInstantiator instantiator,
            SpringTransactionManager transactionManager,
            Persistor persistor,
            TransactionalOutboxMethodRegistry registry) {
        return TransactionOutbox.builder()
                .instantiator(instantiator)
                .transactionManager(transactionManager)
                .persistor(persistor)
                .attemptFrequency(properties.getAttemptFrequency())
                .blockAfterAttempts(properties.getBlockAfterAttempts())
                .listener(registry)
                .build();
    }

    @Bean
    public TransactionOutboxBackgroundProcessor transactionOutboxBackgroundProcessor(TransactionOutbox transactionOutbox) {
        return new TransactionOutboxBackgroundProcessor(transactionOutbox);
    }
}
