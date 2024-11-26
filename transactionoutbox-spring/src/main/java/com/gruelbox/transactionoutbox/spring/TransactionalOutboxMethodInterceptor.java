package com.gruelbox.transactionoutbox.spring;

import com.gruelbox.transactionoutbox.TransactionOutbox;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

class TransactionalOutboxMethodInterceptor implements MethodInterceptor {

  private final Map<Method, TransactionalOutbox> annotatedMethods;
  private final TransactionOutbox transactionOutbox;
  private final SpringExpressionTopicGenerator topicGenerator;

  public TransactionalOutboxMethodInterceptor(
      String beanName,
      Map<Method, TransactionalOutbox> annotatedMethods,
      TransactionOutbox transactionOutbox) {
    this.annotatedMethods = annotatedMethods;
    this.transactionOutbox = transactionOutbox;
    this.topicGenerator = new SpringExpressionTopicGenerator(beanName);
  }

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    TransactionalOutbox annotation = annotatedMethods.get(invocation.getMethod());
    if (annotation != null) {
      Duration delayForAtLeast =
          annotation.delayMillis() > 0 ? Duration.ofMillis(annotation.delayMillis()) : null;

      String topic =
          topicGenerator.generateTopic(
              annotation.useBeanMethodAsTopic(),
              annotation.orderExpression(),
              invocation.getMethod(),
              invocation.getArguments());

      transactionOutbox
          .with()
          .ordered(topic)
          .orderedTakeLast(annotation.orderedTakeLast())
          .delayForAtLeast(delayForAtLeast)
          .persistInvocationAndAddPostCommitHook(
              invocation.getMethod(), invocation.getArguments(), annotation.requireInTransaction());

      return null;

    } else {
      return invocation.proceed();
    }
  }
}
