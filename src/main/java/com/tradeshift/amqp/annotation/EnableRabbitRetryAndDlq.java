package com.tradeshift.amqp.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Exceptions checking order:
 * 
 * <pre>
 * - Should be discarted
 * - Should be sent to retry
 * - Should be sent to DLQ
 * - Otherwise discart
 * </pre>
 * 
 * The <code>discartWhen</code> attribute has higher precedence over <code>exceptions</code> attribute.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface EnableRabbitRetryAndDlq {
    String event();
    
    /**
     * Define if the exception check should use 'instanceof' operator.
     */
    boolean checkInheritance() default false;
    
    /**
     * Exceptions to ignore and just discard the message. 
     */
    Class[] discartWhen() default {};

    /**
     * Exceptions to verify if the message should be sent to retry.
     * If the number of retries is exceeded the message will be sent to DLQ.
     */
    Class[] exceptions() default Exception.class;
    
    /**
     * Exceptions to verify if the message should be sent to retry.
     * If the number of retries is exceeded the message will be sent to DLQ.
     */
    Class[] retryWhen() default {};
    
    /**
     *  Exceptions to verify if the message should be sent to DLQ. 
     */
    Class[] directToDlqWhen() default {};

}
