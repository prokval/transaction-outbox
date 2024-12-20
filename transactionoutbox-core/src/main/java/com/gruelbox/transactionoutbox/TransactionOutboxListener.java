package com.gruelbox.transactionoutbox;

import java.lang.reflect.InvocationTargetException;

/** A listener for events fired by {@link TransactionOutbox}. */
public interface TransactionOutboxListener {

  TransactionOutboxListener EMPTY = new TransactionOutboxListener() {};

  /**
   * Fired when a transaction outbox task is scheduled.
   *
   * <p>This event is not guaranteed to fire in the event of a JVM failure or power loss. It is
   * fired <em>after</em> the commit to the database adding the scheduled task but before the task
   * is submitted for processing. It will, except in extreme circumstances (although this is not
   * guaranteed), fire prior to any subsequent {@link #success(TransactionOutboxEntry, Object)} or
   * {@link #failure(TransactionOutboxEntry, Throwable)}.
   *
   * @param entry The outbox entry scheduled.
   */
  default void scheduled(TransactionOutboxEntry entry) {
    // No-op
  }

  /**
   * Implement this method to intercept and decorate all outbox invocations. In general, you should
   * call {@code invocation.run()} which actually calls the underlying method, unless you are
   * deliberately trying to suppress the method call.
   *
   * @param entry The outbox entry scheduled being invoked.
   * @param invocator A runnable which performs the work of the scheduled task.
   * @throws IllegalAccessException If thrown by the method invocation.
   * @throws IllegalArgumentException If thrown by the method invocation.
   * @throws InvocationTargetException If thrown by the method invocation.
   */
  default Object wrapInvocation(TransactionOutboxEntry entry, Invocator invocator)
      throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    return invocator.invoke();
  }

  @FunctionalInterface
  interface Invocator {
    Object invoke()
        throws IllegalAccessException, IllegalArgumentException, InvocationTargetException;
  }

  /**
   * Fired when a transaction outbox task is successfully completed <em>and</em> recorded as such in
   * the database such that it will not be re-attempted. Note that:
   *
   * <ul>
   *   <li>{@link TransactionOutbox} uses "at least once" semantics, so the actual processing of a
   *       task may complete any number of times before this event is fired.
   *   <li>This event is not guaranteed to fire in the event of a JVM failure or power loss. It is
   *       fired <em>after</em> the commit to the database removing the completed task and all bets
   *       are off after this point.
   * </ul>
   *
   * @param entry The outbox entry completed.
   * @param result The result returned from the invocation
   */
  default void success(TransactionOutboxEntry entry, Object result) {
    // No-op
  }

  /**
   * Fired when a transaction outbox task fails. This may occur multiple times until the maximum
   * number of retries, at which point this will be fired <em>and then</em> {@link
   * #blocked(TransactionOutboxEntry, Throwable)}. This event is not guaranteed to fire in the event
   * of a JVM failure or power loss. It is fired <em>after</em> the commit to the database marking
   * the task as failed.
   *
   * @param entry The outbox entry failed.
   * @param cause The cause of the most recent failure.
   */
  default void failure(TransactionOutboxEntry entry, Throwable cause) {
    // No-op
  }

  /**
   * Fired when a transaction outbox task has passed the maximum number of retries and has been
   * blocked. This event is not guaranteed to fire in the event of a JVM failure or power loss. It
   * is fired <em>after</em> the commit to the database marking the task as blocked.
   *
   * @param entry The outbox entry to be marked as blocked.
   * @param cause The cause of the most recent failure.
   */
  default void blocked(TransactionOutboxEntry entry, Throwable cause) {
    // No-op
  }

  /**
   * Chains this listener with another and returns the result.
   *
   * @param other The other listener. It will always be called after this one.
   * @return The combined listener.
   */
  default TransactionOutboxListener andThen(TransactionOutboxListener other) {
    var self = this;
    return new TransactionOutboxListener() {

      @Override
      public void scheduled(TransactionOutboxEntry entry) {
        self.scheduled(entry);
        other.scheduled(entry);
      }

      @Override
      public Object wrapInvocation(TransactionOutboxEntry entry, Invocator invocator)
          throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        return self.wrapInvocation(entry, () -> other.wrapInvocation(entry, invocator));
      }

      @Override
      public void success(TransactionOutboxEntry entry, Object result) {
        self.success(entry, result);
        other.success(entry, result);
      }

      @Override
      public void failure(TransactionOutboxEntry entry, Throwable cause) {
        self.failure(entry, cause);
        other.failure(entry, cause);
      }

      @Override
      public void blocked(TransactionOutboxEntry entry, Throwable cause) {
        self.blocked(entry, cause);
        other.blocked(entry, cause);
      }
    };
  }
}
