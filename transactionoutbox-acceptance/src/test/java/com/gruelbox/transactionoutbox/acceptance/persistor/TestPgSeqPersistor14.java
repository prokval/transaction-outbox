package com.gruelbox.transactionoutbox.acceptance.persistor;

import com.gruelbox.transactionoutbox.*;
import com.gruelbox.transactionoutbox.testing.AbstractPersistorTest;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

@Testcontainers
class TestPgSeqPersistor14 extends AbstractPersistorTest {

  @Container
  @SuppressWarnings({"rawtypes", "resource"})
  private static final JdbcDatabaseContainer container =
      (JdbcDatabaseContainer)
          new PostgreSQLContainer("postgres:14")
              .withStartupTimeout(Duration.ofHours(1))
              .withReuse(true);

  private final PgSeqPersistor persistor =
          PgSeqPersistor.builder().build();
  private final TransactionManager txManager =
      TransactionManager.fromConnectionDetails(
          "org.postgresql.Driver",
          container.getJdbcUrl(),
          container.getUsername(),
          container.getPassword());

  @Override
  protected PgSeqPersistor persistor() {
    return persistor;
  }

  @Override
  protected TransactionManager txManager() {
    return txManager;
  }

  @Override
  protected Dialect dialect() {
    return PgSeqDialect.POSTGRESQL_SEQ;
  }
}
