package com.gruelbox.transactionoutbox;

import static java.util.stream.Collectors.joining;

import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.Callable;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.MDC;

/**
 * Internal representation of a {@link TransactionOutbox} task. Generally only directly of interest
 * to implementers of SPIs such as {@link Persistor} or {@link Submitter}.
 */
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode
@ToString
public class TransactionOutboxEntry implements Validatable {

  /**
   * @param id The id of the record. Usually a UUID.
   * @return The id of the record. Usually a UUID.
   */
  @SuppressWarnings("JavaDoc")
  @Getter
  private final String id;

  /**
   * @param uniqueRequestId A unique, client-supplied key for the entry. If supplied, it must be
   *     globally unique
   */
  @SuppressWarnings("JavaDoc")
  @Getter
  private final String uniqueRequestId;

  /**
   * @param topic An optional scope for ordered sequencing.
   */
  @SuppressWarnings("JavaDoc")
  @Getter
  private final String topic;

  @Getter private final boolean orderedTakeLast;

  /**
   * @param sequence The ordered sequence within the {@code topic}.
   */
  @SuppressWarnings("JavaDoc")
  @Getter
  @Setter
  private Long sequence;

  /**
   * @param invocation The method invocation to perform.
   * @return The method invocation to perform.
   */
  @SuppressWarnings("JavaDoc")
  @Getter
  @Setter(AccessLevel.PACKAGE)
  private Invocation invocation;

  /**
   * @param lastAttemptTime The timestamp at which the task was last processed.
   * @return The timestamp at which the task was last processed.
   */
  @SuppressWarnings("JavaDoc")
  @Getter
  @Setter
  private Instant lastAttemptTime;

  /**
   * @param nextAttemptTime The timestamp after which the task is available for re-attempting.
   * @return The timestamp after which the task is available for re-attempting.
   */
  @SuppressWarnings("JavaDoc")
  @Getter
  @Setter
  private Instant nextAttemptTime;

  /**
   * @param attempts The number of unsuccessful attempts so far made to run the task.
   * @return The number of unsuccessful attempts so far made to run the task.
   */
  @SuppressWarnings("JavaDoc")
  @Getter
  @Setter
  private int attempts;

  /**
   * @param blocked True if the task has exceeded the configured maximum number of attempts.
   * @return True if the task has exceeded the configured maximum number of attempts.
   */
  @SuppressWarnings("JavaDoc")
  @Getter
  @Setter
  private boolean blocked;

  /**
   * @param processed True if the task has been processed but has been retained to prevent duplicate
   *     requests.
   * @return True if the task has been processed but has been retained to prevent * duplicate
   *     requests.
   */
  @SuppressWarnings("JavaDoc")
  @Getter
  @Setter
  private boolean processed;

  /**
   * @param version The optimistic locking version. Monotonically increasing with each update.
   * @return The optimistic locking version. Monotonically increasing with each update.
   */
  @SuppressWarnings("JavaDoc")
  @Getter
  @Setter
  private int version;

  @EqualsAndHashCode.Exclude @ToString.Exclude private volatile boolean initialized;
  @EqualsAndHashCode.Exclude @ToString.Exclude private String description;

  /**
   * @return A textual description of the task.
   */
  public String description() {
    if (!this.initialized) {
      synchronized (this) {
        if (!this.initialized) {
          String description =
              String.format(
                  "%s.%s(%s) [%s]%s%s",
                  invocation.getClassName(),
                  invocation.getMethodName(),
                  invocation.getArgs() == null
                      ? null
                      : Arrays.stream(invocation.getArgs())
                          .map(this::stringify)
                          .collect(joining(", ")),
                  id,
                  uniqueRequestId == null ? "" : " uid=[" + uniqueRequestId + "]",
                  topic == null ? "" : " seq=[" + topic + "/" + sequence + "]");
          this.description = description;
          this.initialized = true;
          return description;
        }
      }
    }
    return this.description;
  }

  private String stringify(Object o) {
    if (o == null) {
      return "null";
    }
    if (o.getClass().isArray()) {
      return "[" + Arrays.stream((Object[]) o).map(this::stringify).collect(joining(", ")) + "]";
    }
    if (o instanceof String) {
      return "\"" + o + "\"";
    }
    return o.toString();
  }

  @Override
  public void validate(Validator validator) {
    validator.notNull("id", id);
    validator.nullOrNotBlank("uniqueRequestId", uniqueRequestId);
    validator.nullOrNotBlank("topic", topic);
    validator.notNull("invocation", invocation);
    validator.positiveOrZero("attempts", attempts);
    validator.positiveOrZero("version", version);
    validator.isTrue("topic", !"*".equals(topic), "Topic may not be *");
  }

  void withinMDC(Runnable runnable) {
    if (invocation != null && invocation.getMdc() != null && MDC.getMDCAdapter() != null) {
      var oldMdc = MDC.getCopyOfContextMap();
      fillMDC();
      try {
        runnable.run();
      } finally {
        if (oldMdc == null) {
          MDC.clear();
        } else {
          MDC.setContextMap(oldMdc);
        }
      }
    } else {
      runnable.run();
    }
  }

  <T> T withinMDC(Callable<T> callable) throws Exception {
    if (invocation != null && invocation.getMdc() != null && MDC.getMDCAdapter() != null) {
      var oldMdc = MDC.getCopyOfContextMap();
      fillMDC();
      try {
        return callable.call();
      } finally {
        if (oldMdc == null) {
          MDC.clear();
        } else {
          MDC.setContextMap(oldMdc);
        }
      }
    } else {
      return callable.call();
    }
  }

  private void fillMDC() {
    MDC.setContextMap(invocation.getMdc());
    MDC.put("outbox.class", invocation.getClassName());
    MDC.put("outbox.method", invocation.getMethodName());
    MDC.put("outbox.id", id);
    MDC.put("outbox.attempt", Integer.toString(attempts));
  }
}
