package com.gruelbox.transactionoutbox.spi;

import com.gruelbox.transactionoutbox.*;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuperBuilder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class AbstractThreadLocalTransactionManager<TX extends SimpleTransaction>
    implements ThreadLocalContextTransactionManager {

  private final ThreadLocal<Deque<TX>> transactions = ThreadLocal.withInitial(LinkedList::new);

  @Override
  public final void inTransaction(Runnable runnable) {
    ThreadLocalContextTransactionManager.super.inTransaction(runnable);
  }

  @Override
  public final void inTransaction(TransactionalWork work) {
    ThreadLocalContextTransactionManager.super.inTransaction(work);
  }

  @Override
  public final <T> T inTransactionReturns(TransactionalSupplier<T> supplier) {
    return ThreadLocalContextTransactionManager.super.inTransactionReturns(supplier);
  }

  @Override
  public final <E extends Exception> void inTransactionThrows(ThrowingTransactionalWork<E> work)
      throws E {
    ThreadLocalContextTransactionManager.super.inTransactionThrows(work);
  }

  @Override
  public <T, E extends Exception> T requireTransactionReturns(
      ThrowingTransactionalSupplier<T, E> work) throws E, NoTransactionActiveException {
    return work.doWork(peekTransaction().orElseThrow(NoTransactionActiveException::new));
  }

  @Override
  public <T> T inCurrentOrNewTransaction(TransactionalSupplier<T> supplier) {
    return peekTransaction().map(supplier::doWork).orElseGet(() -> inTransactionReturns(supplier));
  }

  public final TX pushTransaction(TX transaction) {
    transactions.get().push(transaction);
    return transaction;
  }

  public final TX popTransaction() {
    TX result = transactions.get().pop();
    if (transactions.get().isEmpty()) {
      transactions.remove();
    }
    return result;
  }

  public Optional<TX> peekTransaction() {
    return Optional.ofNullable(transactions.get().peek());
  }
}
