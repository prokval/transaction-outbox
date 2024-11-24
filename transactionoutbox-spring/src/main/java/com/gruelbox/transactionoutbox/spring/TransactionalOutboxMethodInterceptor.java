package com.gruelbox.transactionoutbox.spring;

import com.gruelbox.transactionoutbox.TransactionOutbox;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ObjectUtils;

@RequiredArgsConstructor
class TransactionalOutboxMethodInterceptor implements MethodInterceptor {

  private static final ParameterNameDiscoverer parameterNameDiscoverer =
      new DefaultParameterNameDiscoverer();

  private final String beanName;
  private final Map<Method, TransactionalOutbox> annotatedMethods;
  private final TransactionOutbox transactionOutbox;

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    TransactionalOutbox annotation = annotatedMethods.get(invocation.getMethod());
    if (annotation != null) {
      Duration delayForAtLeast =
          annotation.delayMillis() > 0 ? Duration.ofMillis(annotation.delayMillis()) : null;

      String topic =
          generateTopic(
              annotation.useBeanMethodAsTopic(), annotation.orderExpression(), invocation);

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

  private String generateTopic(
      boolean useBeanMethodAsTopic, String topicExpression, MethodInvocation invocation) {
    String topic = useBeanMethodAsTopic ? beanName + "." + invocation.getMethod().getName() : "";

    if (!ObjectUtils.isEmpty(topicExpression)) {
      ExpressionParser parser = new SpelExpressionParser();
      Expression exp = parser.parseExpression(topicExpression);
      EvaluationContext context =
          new MethodBasedEvaluationContext(
              null, invocation.getMethod(), invocation.getArguments(), parameterNameDiscoverer);
      String topicValue = exp.getValue(context, String.class);
      if (topic.isEmpty()) {
        topic = topicValue != null ? topicValue : "null";
      } else {
        topic += "-" + topicValue;
      }
    }
    topic = topic.strip();
    return topic.isEmpty() ? null : topic;
  }
}
