package com.gruelbox.transactionoutbox.spring;

import com.gruelbox.transactionoutbox.TransactionOutbox;

import java.lang.annotation.*;

/**
 *  Methods annotated with {@code @TransactionalOutbox} will be wrapped with <a
 *  href="https://microservices.io/patterns/data/transactional-outbox.html">Transactional Outbox</a>
 *  pattern. All calls to the method will be recorded in the Transactional Outbox. It returns immediately after
 *  the call is recorded in Outbox.
 *
 *  <p>If the method supposed to return something - {@code null} will be returned.
 *  Primitive return types are not supported for this reason. Actual method call will be performed asynchronously
 *  either after current transaction commit, or in separate thread, depending on configuration.
 *  See {@link com.gruelbox.transactionoutbox.TransactionOutboxImpl#persistInvocationAndAddPostCommitHook} for details.
 *
 *  <p>The real result of the method call could be handled by the {@code @TransactionalOutboxResultHandler}
 *  annotated method.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Inherited
@Documented
public @interface TransactionalOutbox {

    /**
     * {@code true} - Throw exception if no current transaction (default) <br>
     * {@code false} - use current transaction,
     * but if there is no active transaction, start new one to persist invocation.
     * This might be useful when you don't care much about atomicity and just want to use
     * retriable method invocation mechanism, which survives app restart.
     */
    boolean requireInTransaction() default true;

    /**
     * @see TransactionOutbox.ParameterizedScheduleBuilder#delayForAtLeast
     */
    long delayMillis() default 0;

    /**
     * Use {@code beanName.method} as the topic value
     *
     * @see TransactionOutbox.ParameterizedScheduleBuilder#ordered
     */
    boolean useBeanMethodAsTopic() default false;

    /**
     * SpEL Expression to extract topic value from method parameters. If {@code useBeanMethodAsTopic}
     * is true, the resulting topic expression will be a concatenation of {@code beanName.method} and the result
     * of {@code orderExpression}
     * <p>
     * Example: {@code "#p0"} {@code "#p0.someProperty"}
     * <p>
     * If -parameters flag was passed to Java compiler, parameter names could be used instead
     * <p>
     * Example: {@code "#someParameter"} {@code "#someParameter.someProperty"}
     *
     * @see TransactionOutbox.ParameterizedScheduleBuilder#ordered
     */
    String orderExpression() default "";

    /**
     * If there are multiple method calls for the same topic in a period between {@link TransactionOutbox#flush()} calls,
     * then only the latest method call will be done.
     *
     * @see TransactionOutbox.ParameterizedScheduleBuilder#orderedTakeLast
     */
    boolean orderedTakeLast() default false;

    boolean requireResultHandler() default false;
}
