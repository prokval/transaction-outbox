package com.gruelbox.transactionoutbox.spring;

import java.lang.reflect.Method;
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ObjectUtils;

public class SpringExpressionTopicGenerator {
  private static final ParameterNameDiscoverer defaultParameterNameDiscoverer =
      new DefaultParameterNameDiscoverer();

  private final ParameterNameDiscoverer parameterNameDiscoverer;
  private final String beanName;

  public SpringExpressionTopicGenerator(String beanName) {
    this(defaultParameterNameDiscoverer, beanName);
  }

  SpringExpressionTopicGenerator(ParameterNameDiscoverer parameterNameDiscoverer, String beanName) {
    this.parameterNameDiscoverer = parameterNameDiscoverer;
    this.beanName = beanName;
  }

  public String generateTopic(
      boolean useBeanMethodAsTopic, String topicExpression, Method method, Object[] args) {
    String topic = useBeanMethodAsTopic ? beanName + "." + method.getName() : "";

    if (!ObjectUtils.isEmpty(topicExpression)) {
      ExpressionParser parser = new SpelExpressionParser();
      Expression exp = parser.parseExpression(topicExpression);
      EvaluationContext context =
          new MethodBasedEvaluationContext(null, method, args, parameterNameDiscoverer);
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
