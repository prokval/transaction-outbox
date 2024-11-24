package com.gruelbox.transactionoutbox.spring;

import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.TransactionOutboxListener;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationContextException;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

@Component
@Slf4j
public class TransactionalOutboxMethodRegistry
    implements TransactionOutboxListener,
        ApplicationContextAware,
        ApplicationListener<ContextRefreshedEvent> {

  @Override
  public void success(TransactionOutboxEntry entry, Object result) {
    callResultHandler(entry, result, null);
  }

  @Override
  public void failure(TransactionOutboxEntry entry, Throwable cause) {
    callResultHandler(entry, null, cause);
  }

  private void callResultHandler(TransactionOutboxEntry entry, Object result, Throwable cause) {
    Invocation invocation = entry.getInvocation();
    BeanMethodKey key =
        new BeanMethodKey(
            invocation.getClassName(), invocation.getMethodName(), invocation.getParameterTypes());

    ResultHandlerCaller caller = resultHandlerCallers.get(key);

    if (caller != null) {
      log.debug("Calling ");
      caller.call(invocation.getArgs(), result, cause);
    }
  }

  private Map<BeanMethodKey, ResultHandlerCaller> resultHandlerCallers;

  @RequiredArgsConstructor
  @Getter
  private static final class ResultHandlerCaller implements MethodDescriptor {
    private final Object resultHandlerInstance;
    private final Method method;
    private final boolean noResultParameter;

    void call(Object[] args, Object result, Throwable error) {
      Object[] methodArgs =
          noResultParameter
              ? ArrayUtils.addAll(args, error)
              : ArrayUtils.addAll(args, result, error);
      try {
        method.setAccessible(true);
        method.invoke(resultHandlerInstance, methodArgs);
      } catch (InvocationTargetException e) {
        log.error("Error while executing {}", this.toString(), e.getTargetException());
      } catch (Exception e) {
        log.error("Error while executing {}", this.toString(), e);
      }
    }

    @Override
    public String toString() {
      return resultHandlerInstance.getClass().getName() + "." + toShortSignature();
    }
  }

  @Data
  private static final class BeanMethodKey {
    private final String beanName;
    private final String methodName;
    private final Class<?>[] parameterTypes;
  }

  @Data
  private static final class MethodKey {
    private final String methodName;
    private final Class<?>[] parameterTypes;
  }

  private interface MethodDescriptor {
    Method getMethod();

    default String toShortSignature() {
      Method method = getMethod();
      StringJoiner sj = new StringJoiner(",", method.getName() + "(", ")");
      for (Class<?> parameterType : method.getParameterTypes()) {
        sj.add(parameterType.getTypeName());
      }
      return sj.toString();
    }
  }

  @Data
  private static final class TransactionalOutboxMethod implements MethodDescriptor {
    private final String beanName;
    private final Method method;
    private final TransactionalOutbox annotation;

    BeanMethodKey getKey() {
      return new BeanMethodKey(beanName, method.getName(), method.getParameterTypes());
    }

    boolean isVoidReturnType() {
      return Void.TYPE.equals(method.getReturnType());
    }

    MethodKey getRequredResultHandlerMethodKey() {
      Class<?>[] resultHandlerMethodParameterTypes =
          isVoidReturnType()
              ? ArrayUtils.addAll(method.getParameterTypes(), Throwable.class)
              : ArrayUtils.addAll(
                  method.getParameterTypes(), method.getReturnType(), Throwable.class);

      return new MethodKey(method.getName(), resultHandlerMethodParameterTypes);
    }

    @Override
    public String toString() {
      return beanName + "." + toShortSignature();
    }
  }

  @Data
  private static final class ResultHandlerMethod implements MethodDescriptor {
    private final String beanName;
    private final Method method;

    MethodKey getKey() {
      return new MethodKey(method.getName(), method.getParameterTypes());
    }

    @Override
    public String toString() {
      return beanName + "." + toShortSignature();
    }
  }

  private Map<BeanMethodKey, TransactionalOutboxMethod> transactionOutboxMethods = new HashMap<>();

  void registerTransactionalOutboxMethod(
      String beanName, Method method, TransactionalOutbox annotation) {
    TransactionalOutboxMethod m = new TransactionalOutboxMethod(beanName, method, annotation);
    transactionOutboxMethods.put(m.getKey(), m);
  }

  private Set<ResultHandlerMethod> resultHandlerMethods = new HashSet<>();

  void registerResultHandlerMethod(String beanName, Method method) {
    resultHandlerMethods.add(new ResultHandlerMethod(beanName, method));
  }

  @Override
  public void onApplicationEvent(ContextRefreshedEvent event) {
    resultHandlerCallers =
        transactionOutboxMethods.values().stream()
            .filter(m -> m.getAnnotation().requireResultHandler())
            .collect(
                Collectors.toMap(
                    TransactionalOutboxMethod::getKey,
                    this::buildResultHandlerMethodCallerOrThrow));
  }

  private ApplicationContext applicationContext;

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.applicationContext = applicationContext;
  }

  private ResultHandlerCaller buildResultHandlerMethodCallerOrThrow(
      TransactionalOutboxMethod transactionalOutboxMethod) {

    ResultHandlerMethod resultHandlerMethod =
        findResultHandlerMethodOrThrow(transactionalOutboxMethod);
    Object bean = applicationContext.getBean(resultHandlerMethod.getBeanName());

    return new ResultHandlerCaller(
        bean, resultHandlerMethod.getMethod(), transactionalOutboxMethod.isVoidReturnType());
  }

  private ResultHandlerMethod findResultHandlerMethodOrThrow(
      TransactionalOutboxMethod transactionalOutboxMethod) {
    MethodKey keyToFind = transactionalOutboxMethod.getRequredResultHandlerMethodKey();

    Map<MethodKey, List<ResultHandlerMethod>> handlersByKey =
        resultHandlerMethods.stream().collect(Collectors.groupingBy(ResultHandlerMethod::getKey));

    List<ResultHandlerMethod> candidates = handlersByKey.get(keyToFind);
    if (CollectionUtils.isEmpty(candidates)) {
      throw new ApplicationContextException(
          "Can't find @TransactionalOutboxResultHandler for " + transactionalOutboxMethod);
    }

    if (candidates.size() > 1) {
      throw new ApplicationContextException(
          "Ambiguity in @TransactionalOutboxResultHandler among "
              + candidates.stream()
                  .map(ResultHandlerMethod::toString)
                  .collect(Collectors.joining(", ")));
    }

    return candidates.get(0);
  }
}
