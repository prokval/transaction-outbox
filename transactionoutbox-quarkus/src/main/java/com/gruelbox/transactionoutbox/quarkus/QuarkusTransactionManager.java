package com.gruelbox.transactionoutbox.quarkus;

import static com.gruelbox.transactionoutbox.spi.Utils.uncheck;

import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.spi.Utils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;

/** Transaction manager which uses cdi and quarkus. */
@ApplicationScoped
public class QuarkusTransactionManager implements ThreadLocalContextTransactionManager {

  private final CdiTransaction transactionInstance = new CdiTransaction();

  private final DataSource datasource;

  private final TransactionSynchronizationRegistry tsr;

  @Inject
  public QuarkusTransactionManager(DataSource datasource, TransactionSynchronizationRegistry tsr) {
    this.datasource = datasource;
    this.tsr = tsr;
  }

  @Override
  @Transactional(value = TxType.REQUIRES_NEW)
  public void inTransaction(Runnable runnable) {
    uncheck(() -> inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromRunnable(runnable)));
  }

  @Override
  @Transactional(value = TxType.REQUIRES_NEW)
  public void inTransaction(TransactionalWork work) {
    uncheck(() -> inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromWork(work)));
  }

  @Override
  @Transactional(value = TxType.REQUIRES_NEW)
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E> work) throws E {
    return work.doWork(transactionInstance);
  }

  @Override
  @Transactional(value = TxType.REQUIRED)
  public <T> T inCurrentOrNewTransaction(TransactionalSupplier<T> supplier) {
    return supplier.doWork(transactionInstance);
  }

  @Override
  public <T, E extends Exception> T requireTransactionReturns(
      ThrowingTransactionalSupplier<T, E> work) throws E, NoTransactionActiveException {
    if (tsr.getTransactionStatus() != Status.STATUS_ACTIVE) {
      throw new NoTransactionActiveException();
    }

    return work.doWork(transactionInstance);
  }

  private final class CdiTransaction implements Transaction {

    public Connection connection() {
      try {
        return datasource.getConnection();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public PreparedStatement prepareBatchStatement(String sql) {
      BatchCountingStatement preparedStatement =
          Utils.uncheckedly(
              () -> BatchCountingStatementHandler.countBatches(connection().prepareStatement(sql)));

      tsr.registerInterposedSynchronization(
          new Synchronization() {
            @Override
            public void beforeCompletion() {
              if (preparedStatement.getBatchCount() != 0) {
                Utils.uncheck(preparedStatement::executeBatch);
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
      tsr.registerInterposedSynchronization(
          new Synchronization() {
            @Override
            public void beforeCompletion() {}

            @Override
            public void afterCompletion(int status) {
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
