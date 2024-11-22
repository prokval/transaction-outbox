package com.gruelbox.transactionoutbox.spring;

import lombok.RequiredArgsConstructor;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.Set;

@Component
@RequiredArgsConstructor
public class TransactionalOutboxResultHandlerBeanPostProcessor implements BeanPostProcessor {

    private final TransactionalOutboxMethodRegistry methodRegistry;

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = AopProxyUtils.ultimateTargetClass(bean);

        Set<Method> methods = MethodIntrospector.selectMethods(targetClass,
                        (MethodIntrospector.MetadataLookup<TransactionalOutboxResultHandler>) method ->
                                AnnotatedElementUtils.findMergedAnnotation(method, TransactionalOutboxResultHandler.class))
                .keySet();

        methods.forEach(m -> methodRegistry.registerResultHandlerMethod(beanName, m));

        return bean;
    }

}
