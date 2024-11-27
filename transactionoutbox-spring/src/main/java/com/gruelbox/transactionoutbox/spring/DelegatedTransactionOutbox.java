package com.gruelbox.transactionoutbox.spring;

import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.Submitter;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import java.util.concurrent.Executor;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

@Component
public class DelegatedTransactionOutbox implements TransactionOutbox {

  private volatile TransactionOutbox delegate;

  /**
   * This separate initializer is needed as DelegatedTransactionOutbox itself is a dependency of
   * TransactionalOutboxBeanPostProcessor, thus it has different lifecycle: {@code
   * DelegatedTransactionOutbox.afterPropertiesSet} would be called before all BeanPostProcessors
   * finish their work. We don't want to initialize PlatformTransactionManager prematurely as it may
   * lead to some undesired side effects.
   */
  @RequiredArgsConstructor
  @Component
  static class DelegatedTransactionOutboxInitializer implements InitializingBean {
    private final SpringInstantiator instantiator;
    private final SpringTransactionManager transactionManager;
    private final Persistor persistor;
    private final Submitter submitter;
    private final TransactionalOutboxMethodRegistry registry;
    private final TransactionOutboxProperties properties;

    private final DelegatedTransactionOutbox delegatedTransactionOutbox;

    @Override
    public void afterPropertiesSet() throws Exception {
      delegatedTransactionOutbox.delegate =
          TransactionOutbox.builder()
              .instantiator(instantiator)
              .transactionManager(transactionManager)
              .persistor(persistor)
              .submitter(submitter)
              .attemptFrequency(properties.getAttemptFrequency())
              .blockAfterAttempts(properties.getBlockAfterAttempts())
              .listener(registry)
              .build();
    }
  }

  @Override
  public void initialize() {
    delegate.initialize();
  }

  @Override
  public <T> T schedule(Class<T> clazz) {
    return delegate.schedule(clazz);
  }

  @Override
  public ParameterizedScheduleBuilder with() {
    return delegate.with();
  }

  @Override
  public boolean flush() {
    return delegate.flush();
  }

  @Override
  public boolean flush(Executor executor) {
    return delegate.flush(executor);
  }

  @Override
  public boolean unblock(String entryId) {
    return delegate.unblock(entryId);
  }

  @Override
  public boolean unblock(String entryId, Object transactionContext) {
    return delegate.unblock(entryId, transactionContext);
  }

  @Override
  public void processNow(TransactionOutboxEntry entry) {
    delegate.processNow(entry);
  }
}
