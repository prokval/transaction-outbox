package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.PgSeqDialect;
import com.gruelbox.transactionoutbox.PgSeqPersistor;
import com.gruelbox.transactionoutbox.testing.AbstractAcceptanceTest;
import java.time.Duration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SuppressWarnings("WeakerAccess")
@Testcontainers
class TestPostgresSeq14 extends AbstractAcceptanceTest {

  @Container
  @SuppressWarnings({"rawtypes", "resource"})
  private static final JdbcDatabaseContainer container =
      (JdbcDatabaseContainer)
          new PostgreSQLContainer("postgres:14")
              .withStartupTimeout(Duration.ofHours(1))
              .withReuse(true);

  @Override
  protected ConnectionDetails connectionDetails() {
    return ConnectionDetails.builder()
        .dialect(PgSeqDialect.POSTGRESQL_SEQ)
        .driverClassName("org.postgresql.Driver")
        .url(container.getJdbcUrl())
        .user(container.getUsername())
        .password(container.getPassword())
        .build();
  }

  @Override
  protected Persistor persistor() {
    return PgSeqPersistor.builder().build();
  }
}
