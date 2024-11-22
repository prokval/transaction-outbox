package com.gruelbox.transactionoutbox.spring;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Data
@ConfigurationProperties("outbox")
@Setter(AccessLevel.PACKAGE)
public class TransactionOutboxProperties {
  private Duration repeatEvery;
  private boolean useJackson;
  private Duration attemptFrequency;
  private int blockAfterAttempts;
}
