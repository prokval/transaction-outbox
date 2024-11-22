package com.gruelbox.transactionoutbox.spring;

import lombok.RequiredArgsConstructor;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.RawTargetAccess;
import org.springframework.aop.SpringProxy;

public interface TransactionalOutboxProxy extends SpringProxy, RawTargetAccess {

    Object getTransactionalOutboxProxyTarget();

    @RequiredArgsConstructor
    class GetTargetMethodInterceptor implements MethodInterceptor {
        private final Object target;


        @Override
        public Object invoke(MethodInvocation invocation) throws Throwable {
            if (invocation.getMethod().getName().equals("getTransactionalOutboxProxyTarget")) {
                return target;
            } else {
                return invocation.proceed();
            }
        }
    }
}
