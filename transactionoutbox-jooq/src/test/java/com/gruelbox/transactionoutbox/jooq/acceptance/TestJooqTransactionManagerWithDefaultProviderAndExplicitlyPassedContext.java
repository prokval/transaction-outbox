package com.gruelbox.transactionoutbox.jooq.acceptance;

import static com.gruelbox.transactionoutbox.jooq.acceptance.JooqTestUtils.createTestTable;
import static com.gruelbox.transactionoutbox.spi.Utils.uncheck;
import static com.gruelbox.transactionoutbox.testing.TestUtils.runSql;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.junit.jupiter.api.Assertions.*;

import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.jooq.JooqTransactionManager;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.*;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
class TestJooqTransactionManagerWithDefaultProviderAndExplicitlyPassedContext {

  private HikariDataSource dataSource;
  private DSLContext dsl;

  @BeforeEach
  void beforeEach() {
    dataSource = pooledDataSource();
    dsl = createDsl();
    createTestTable(dsl);
  }

  @AfterEach
  void afterEach() {
    dataSource.close();
  }

  private HikariDataSource pooledDataSource() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(
        "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=2000;LOB_TIMEOUT=2000;MV_STORE=TRUE;DATABASE_TO_UPPER=FALSE");
    config.setUsername("test");
    config.setPassword("test");
    config.addDataSourceProperty("cachePrepStmts", "true");
    return new HikariDataSource(config);
  }

  private DSLContext createDsl() {
    dsl = DSL.using(dataSource, SQLDialect.H2);
    dsl.configuration().set(JooqTransactionManager.createListener());
    return dsl;
  }

  private TransactionManager createTransactionManager() {
    return JooqTransactionManager.create(dsl);
  }

  @Test
  void testSimplePassingTransaction() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(createTransactionManager())
            .persistor(Persistor.forDialect(Dialect.H2))
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry, Object result) {
                    latch.countDown();
                  }
                })
            .build();

    clearOutbox(createTransactionManager());

    createTransactionManager()
        .inTransaction(
            tx -> {
              outbox.schedule(Worker.class).process(1, tx);
              try {
                // Should not be fired until after commit
                assertFalse(latch.await(2, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                fail("Interrupted");
              }
            });

    // Should be fired after commit
    assertTrue(latch.await(2, TimeUnit.SECONDS));
    JooqTestUtils.assertRecordExists(dsl, 1);
  }

  @Test
  void testSimplePassingContext() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(createTransactionManager())
            .persistor(Persistor.forDialect(Dialect.H2))
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry, Object result) {
                    latch.countDown();
                  }
                })
            .build();

    clearOutbox(createTransactionManager());

    DSLContext dsl = createDsl();
    dsl.transaction(
        cx1 -> {
          outbox.schedule(Worker.class).process(1, cx1);
          try {
            // Should not be fired until after commit
            assertFalse(latch.await(2, TimeUnit.SECONDS));
          } catch (InterruptedException e) {
            fail("Interrupted");
          }
        });

    // Should be fired after commit
    assertTrue(latch.await(2, TimeUnit.SECONDS));
    JooqTestUtils.assertRecordExists(dsl, 1);
  }

  @Test
  void testNestedPassingContext() throws Exception {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(createTransactionManager())
            .persistor(Persistor.forDialect(Dialect.H2))
            .attemptFrequency(Duration.of(1, ChronoUnit.SECONDS))
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry, Object result) {
                    if (entry.getInvocation().getArgs()[0].equals(1)) {
                      latch1.countDown();
                    } else {
                      latch2.countDown();
                    }
                  }
                })
            .build();

    clearOutbox(createTransactionManager());

    withRunningFlusher(
        outbox,
        () -> {
          dsl.transaction(
              ctx -> {
                outbox.schedule(Worker.class).process(1, ctx);
                ctx.dsl().transaction(cx1 -> outbox.schedule(Worker.class).process(2, ctx));

                // Neither should be fired - the second job is in a nested transaction
                CompletableFuture.allOf(
                        runAsync(
                            () -> uncheck(() -> assertFalse(latch1.await(2, TimeUnit.SECONDS)))),
                        runAsync(
                            () -> uncheck(() -> assertFalse(latch2.await(2, TimeUnit.SECONDS)))))
                    .get();
              });

          // Both should be fired after commit
          CompletableFuture.allOf(
                  runAsync(() -> uncheck(() -> assertTrue(latch1.await(2, TimeUnit.SECONDS)))),
                  runAsync(() -> uncheck(() -> assertTrue(latch2.await(2, TimeUnit.SECONDS)))))
              .get();
        });
    JooqTestUtils.assertRecordExists(dsl, 1);
    JooqTestUtils.assertRecordExists(dsl, 2);
  }

  @Test
  void testNestedPassingTransaction() throws Exception {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(createTransactionManager())
            .persistor(Persistor.forDialect(Dialect.H2))
            .attemptFrequency(Duration.of(1, ChronoUnit.SECONDS))
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry, Object result) {
                    if (entry.getInvocation().getArgs()[0].equals(1)) {
                      latch1.countDown();
                    } else {
                      latch2.countDown();
                    }
                  }
                })
            .build();

    clearOutbox(createTransactionManager());

    withRunningFlusher(
        outbox,
        () -> {
          createTransactionManager()
              .inTransactionThrows(
                  tx1 -> {
                    outbox.schedule(Worker.class).process(1, tx1);

                    createTransactionManager()
                        .inTransactionThrows(tx2 -> outbox.schedule(Worker.class).process(2, tx2));

                    // The inner transaction should be committed - these are different semantics
                    CompletableFuture.allOf(
                            runAsync(
                                () ->
                                    uncheck(() -> assertFalse(latch1.await(2, TimeUnit.SECONDS)))),
                            runAsync(
                                () -> uncheck(() -> assertTrue(latch2.await(2, TimeUnit.SECONDS)))))
                        .get();

                    JooqTestUtils.assertRecordExists(dsl, 2);
                  });

          // Should be fired after commit
          assertTrue(latch1.await(2, TimeUnit.SECONDS));
        });

    JooqTestUtils.assertRecordExists(dsl, 1);
  }

  /**
   * Ensures that given the rollback of an inner transaction, any outbox work scheduled in the inner
   * transaction is rolled back while the outer transaction's works.
   */
  @Test
  void testNestedWithInnerFailure() throws Exception {
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    TransactionOutbox outbox =
        TransactionOutbox.builder()
            .transactionManager(createTransactionManager())
            .persistor(Persistor.forDialect(Dialect.H2))
            .attemptFrequency(Duration.of(1, ChronoUnit.SECONDS))
            .listener(
                new TransactionOutboxListener() {
                  @Override
                  public void success(TransactionOutboxEntry entry, Object result) {
                    if (entry.getInvocation().getArgs()[0].equals(1)) {
                      latch1.countDown();
                    } else {
                      latch2.countDown();
                    }
                  }
                })
            .build();

    clearOutbox(createTransactionManager());

    withRunningFlusher(
        outbox,
        () -> {
          dsl.transaction(
              ctx -> {
                outbox.schedule(Worker.class).process(1, ctx);

                assertThrows(
                    UnsupportedOperationException.class,
                    () ->
                        ctx.dsl()
                            .transaction(
                                cx2 -> {
                                  outbox.schedule(Worker.class).process(2, cx2);
                                  throw new UnsupportedOperationException();
                                }));

                CompletableFuture.allOf(
                        runAsync(
                            () -> uncheck(() -> assertFalse(latch1.await(2, TimeUnit.SECONDS)))),
                        runAsync(
                            () -> uncheck(() -> assertFalse(latch2.await(2, TimeUnit.SECONDS)))))
                    .get();
              });

          CompletableFuture.allOf(
                  runAsync(() -> uncheck(() -> assertTrue(latch1.await(2, TimeUnit.SECONDS)))),
                  runAsync(() -> uncheck(() -> assertFalse(latch2.await(2, TimeUnit.SECONDS)))))
              .get();
        });
    JooqTestUtils.assertRecordExists(dsl, 1);
    JooqTestUtils.assertRecordNotExists(dsl, 2);
  }

  private void clearOutbox(TransactionManager transactionManager) {
    runSql(transactionManager, "DELETE FROM TXNO_OUTBOX");
  }

  private void withRunningFlusher(TransactionOutbox outbox, ThrowingRunnable runnable)
      throws Exception {
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    try {
      scheduler.scheduleAtFixedRate(
          () -> {
            if (Thread.interrupted()) {
              return;
            }
            outbox.flush();
          },
          500,
          500,
          TimeUnit.MILLISECONDS);
      runnable.run();
    } finally {
      scheduler.shutdown();
      assertTrue(scheduler.awaitTermination(20, TimeUnit.SECONDS));
    }
  }

  @SuppressWarnings("EmptyMethod")
  static class Worker {

    @SuppressWarnings("SameParameterValue")
    void process(int i, Transaction transaction) {
      JooqTestUtils.writeRecord(transaction, i);
    }

    void process(int i, Configuration configuration) {
      JooqTestUtils.writeRecord(configuration, i);
    }
  }
}
