package com.gruelbox.transactionoutbox.spring;

import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.PgSeqDialect;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import lombok.*;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@ConfigurationProperties("outbox")
@Setter(AccessLevel.PACKAGE)
@Validated
public class TransactionOutboxProperties {
  @NotNull private Duration repeatEvery;
  private boolean useJackson = true;
  @NotNull private Duration attemptFrequency;
  @Positive private int blockAfterAttempts;
  @NotNull private OutboxSqlDialect sqlDialect = OutboxSqlDialect.H2;

  @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
  @Getter
  public enum OutboxSqlDialect {
    POSTGRESQL_SEQ(PgSeqDialect.POSTGRESQL_SEQ),
    MY_SQL_5(Dialect.MY_SQL_5),
    MY_SQL_8(Dialect.MY_SQL_8),
    POSTGRESQL_9(Dialect.POSTGRESQL_9),
    H2(Dialect.H2),
    ORACLE(Dialect.ORACLE),
    MS_SQL_SERVER(Dialect.MS_SQL_SERVER);

    private final Dialect dialect;
  }
}
