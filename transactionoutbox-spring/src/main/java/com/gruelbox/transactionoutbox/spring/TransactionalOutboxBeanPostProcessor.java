package com.gruelbox.transactionoutbox.spring;

import com.gruelbox.transactionoutbox.TransactionOutbox;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContextException;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class TransactionalOutboxBeanPostProcessor implements BeanPostProcessor {

  private final TransactionOutbox transactionOutbox;
  private final TransactionalOutboxMethodRegistry methodRegistry;

  private boolean isMarkedWithNoTransactionalOutbox(AnnotatedElement annotatedElement) {
    return AnnotatedElementUtils.findMergedAnnotation(annotatedElement, NoTransactionalOutbox.class)
        != null;
  }

  @Override
  public Object postProcessBeforeInitialization(Object bean, String beanName)
      throws BeansException {
    Class<?> targetClass = AopProxyUtils.ultimateTargetClass(bean);

    Map<Method, TransactionalOutbox> annotatedMethods =
        isMarkedWithNoTransactionalOutbox(targetClass)
            ? Map.of()
            : Map.copyOf(
                MethodIntrospector.selectMethods(
                    targetClass,
                    (MethodIntrospector.MetadataLookup<TransactionalOutbox>)
                        method ->
                            isMarkedWithNoTransactionalOutbox(method)
                                ? null
                                : AnnotatedElementUtils.findMergedAnnotation(
                                    method, TransactionalOutbox.class)));

    if (annotatedMethods.isEmpty()) {
      return bean;
    }

    validateMethods(annotatedMethods.keySet());

    annotatedMethods.forEach(
        (method, annotation) ->
            methodRegistry.registerTransactionalOutboxMethod(beanName, method, annotation));

    ProxyFactory proxyFactory = new ProxyFactory(bean);
    proxyFactory.setProxyTargetClass(true);

    proxyFactory.addInterface(TransactionalOutboxProxy.class);
    proxyFactory.addAdvice(new TransactionalOutboxProxy.GetTargetMethodInterceptor(bean));

    proxyFactory.addAdvice(
        new TransactionalOutboxMethodInterceptor(beanName, annotatedMethods, transactionOutbox));

    return proxyFactory.getProxy();
  }

  private void validateMethods(Collection<Method> methods) {
    for (Method method : methods) {
      if (method.getReturnType().isPrimitive() && !Void.TYPE.equals(method.getReturnType())) {
        throw new ApplicationContextException(
            "Method "
                + method
                + " has primitive return type. This is not supported by @TransactionalOutbox");
      }
    }
  }
}
