package com.gruelbox.transactionoutbox.spring;

import com.gruelbox.transactionoutbox.Submitter;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

/**
 * The purpose of this class is to wait for completion of currently executing tasks on Spring
 * context shutdown
 */
@Slf4j
@Component
public class DelegatedSubmitter implements Submitter, InitializingBean, DisposableBean {
  private volatile Submitter submitter;
  private volatile ThreadPoolExecutor executor;

  @Override
  public void afterPropertiesSet() throws Exception {
    this.executor =
        new ThreadPoolExecutor(
            1,
            Math.max(1, ForkJoinPool.commonPool().getParallelism()),
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(16384));

    this.submitter = Submitter.withExecutor(this.executor);
  }

  @Override
  public void submit(TransactionOutboxEntry entry, Consumer<TransactionOutboxEntry> localExecutor) {
    submitter.submit(entry, localExecutor);
  }

  @Override
  public void destroy() throws Exception {
    executor.shutdown();
    if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
      log.warn("Was not able to terminate TransactionalOutbox's executor within 10 seconds");
    }
  }
}
