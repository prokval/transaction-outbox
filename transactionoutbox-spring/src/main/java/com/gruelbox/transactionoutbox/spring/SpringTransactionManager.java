package com.gruelbox.transactionoutbox.spring;

import static com.gruelbox.transactionoutbox.spi.Utils.uncheck;
import static com.gruelbox.transactionoutbox.spi.Utils.uncheckedly;

import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.spi.Utils;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Transaction manager which uses spring-tx and Hibernate. */
@Slf4j
@Service
@RequiredArgsConstructor
public class SpringTransactionManager implements ThreadLocalContextTransactionManager {

  private final SpringTransaction transactionInstance = new SpringTransaction();
  private final DataSource dataSource;
  private final TransactionTemplate requiresNewTxTemplate;
  private final TransactionTemplate txTemplate;
  private final TransactionTemplate mandatoryTxTemplate;

  @Autowired
  public SpringTransactionManager(
      DataSource dataSource, PlatformTransactionManager transactionManager) {
    this.dataSource = dataSource;
    requiresNewTxTemplate = createTransactionTemplate(transactionManager, Propagation.REQUIRES_NEW);
    txTemplate = createTransactionTemplate(transactionManager, Propagation.REQUIRED);
    mandatoryTxTemplate = createTransactionTemplate(transactionManager, Propagation.MANDATORY);
  }

  private static TransactionTemplate createTransactionTemplate(
      PlatformTransactionManager transactionManager, Propagation propagation) {
    TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
    txTemplate.setPropagationBehavior(propagation.value());
    return txTemplate;
  }

  @Override
  public void inTransaction(Runnable runnable) {
    uncheck(() -> inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromRunnable(runnable)));
  }

  @Override
  public void inTransaction(TransactionalWork work) {
    inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromWork(work));
  }

  @Override
  public <T> T inTransactionReturns(TransactionalSupplier<T> supplier) {
    return inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromSupplier(supplier));
  }

  @Override
  public <E extends Exception> void inTransactionThrows(ThrowingTransactionalWork<E> work)
      throws E {
    inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromWork(work));
  }

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E> work) throws E {
    try {
      return requiresNewTxTemplate.execute(
          t -> uncheckedly(() -> work.doWork(transactionInstance)));
    } catch (UncheckedException e) {
      //noinspection unchecked
      throw (E) e.getCause();
    }
  }

  @Override
  public <T, E extends Exception> T requireTransactionReturns(
      ThrowingTransactionalSupplier<T, E> work) throws E, NoTransactionActiveException {
    try {
      return mandatoryTxTemplate.execute(t -> uncheckedly(() -> work.doWork(transactionInstance)));
    } catch (IllegalTransactionStateException e) {
      throw new NoTransactionActiveException(e);
    } catch (UncheckedException e) {
      //noinspection unchecked
      throw (E) e.getCause();
    }
  }

  @Override
  public <T> T inCurrentOrNewTransaction(TransactionalSupplier<T> supplier) {
    return txTemplate.execute(t -> supplier.doWork(transactionInstance));
  }

  private final class SpringTransaction implements Transaction {

    @Override
    public Connection connection() {
      return DataSourceUtils.getConnection(dataSource);
    }

    @Override
    public PreparedStatement prepareBatchStatement(String sql) {
      BatchCountingStatement preparedStatement =
          Utils.uncheckedly(
              () -> BatchCountingStatementHandler.countBatches(connection().prepareStatement(sql)));
      TransactionSynchronizationManager.registerSynchronization(
          new TransactionSynchronization() {
            @Override
            public void beforeCommit(boolean readOnly) {
              if (preparedStatement.getBatchCount() != 0) {
                log.debug("Flushing batches");
                uncheck(preparedStatement::executeBatch);
              }
            }

            @Override
            public void afterCompletion(int status) {
              Utils.safelyClose(preparedStatement);
            }
          });
      return preparedStatement;
    }

    @Override
    public void addPostCommitHook(Runnable runnable) {
      TransactionSynchronizationManager.registerSynchronization(
          new TransactionSynchronization() {
            @Override
            public void afterCommit() {
              runnable.run();
            }
          });
    }
  }

  private interface BatchCountingStatement extends PreparedStatement {
    int getBatchCount();
  }

  private static final class BatchCountingStatementHandler implements InvocationHandler {

    private final PreparedStatement delegate;
    private int count = 0;

    private BatchCountingStatementHandler(PreparedStatement delegate) {
      this.delegate = delegate;
    }

    static BatchCountingStatement countBatches(PreparedStatement delegate) {
      return (BatchCountingStatement)
          Proxy.newProxyInstance(
              BatchCountingStatementHandler.class.getClassLoader(),
              new Class[] {BatchCountingStatement.class},
              new BatchCountingStatementHandler(delegate));
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if ("getBatchCount".equals(method.getName())) {
        return count;
      }
      try {
        return method.invoke(delegate, args);
      } finally {
        if ("addBatch".equals(method.getName())) {
          ++count;
        }
      }
    }
  }
}
