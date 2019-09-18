package com.tradeshift.amqp.annotation;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import com.tradeshift.amqp.rabbit.properties.TunedRabbitProperties;
import com.tradeshift.amqp.rabbit.retry.QueueRetryComponent;
import com.tradeshift.amqp.rabbit.properties.TunedRabbitPropertiesMap;
import org.springframework.context.annotation.Configuration;

@Aspect
@Configuration
public class EnableRabbitRetryAndDlqAspect {

    private static final Logger log = LoggerFactory.getLogger(EnableRabbitRetryAndDlqAspect.class);

    private final QueueRetryComponent queueRetryComponent;
    private final TunedRabbitPropertiesMap rabbitCustomPropertiesMap;

    @Autowired
    public EnableRabbitRetryAndDlqAspect(QueueRetryComponent queueRetryComponent, TunedRabbitPropertiesMap rabbitCustomPropertiesMap) {
        this.queueRetryComponent = queueRetryComponent;
        this.rabbitCustomPropertiesMap = rabbitCustomPropertiesMap;
    }

    @Around("com.tradeshift.amqp.annotation.CommonJoinPointConfig.enableRabbitRetryAndDlqAnnotation()")
    public void validateMessage(ProceedingJoinPoint joinPoint) throws Throwable {
    	try {
    		joinPoint.proceed();
    	} catch (Exception e) {
    		handleExceptionUsingEventDefinitions(e, joinPoint);
    	}
    }

    /**
     * Handle the exception as defined by the annotation.
     * Checking order:
     * - Should be discarted
     * - Should be sent to DLQ
     * - Should be sent to retry
     * - Otherwise discart
     */
    private void handleExceptionUsingEventDefinitions(Exception exceptionThrown, ProceedingJoinPoint joinPoint) {
    	Method method = getMethod(joinPoint);
    	log.info("The listener [{}.{}] threw an exception: {}", method.getDeclaringClass().getSimpleName(), method.getName(), exceptionThrown.getMessage());

    	EnableRabbitRetryAndDlq annotation = method.getAnnotation(EnableRabbitRetryAndDlq.class);

    	if (shouldDiscart(annotation, exceptionThrown)) {
    		log.warn("Exception {} was parametrized to be discarted", exceptionThrown.getClass().getSimpleName());
    	} else if (shouldSentDirectToDlq(annotation, exceptionThrown) ) {
    		sendMessageToDlq(joinPoint, annotation);
    	} else if (shouldSentToRetry(annotation, exceptionThrown)) {
    		sendMessageToRetry(joinPoint, annotation);
    	} else {
    		log.error("Discarting message after exception {}: {}", exceptionThrown.getClass().getSimpleName(), exceptionThrown.getMessage());
    	}
    }

    private boolean shouldDiscart(EnableRabbitRetryAndDlq annotation, Exception exceptionThrown) {
    	return checkIfContainsException(annotation, annotation.discartWhen(), exceptionThrown);
    }

    private boolean shouldSentToRetry(EnableRabbitRetryAndDlq annotation, Exception exceptionThrown) {
    	if (annotation.retryWhen() != null && annotation.retryWhen().length > 0) {
    		return checkIfContainsException(annotation, annotation.retryWhen(), exceptionThrown);
    	}
    	// TODO: define if backwards compatibility will be preserved (keep exceptions attribute)
    	return checkIfContainsException(annotation, annotation.exceptions(), exceptionThrown);
    }

    private boolean shouldSentDirectToDlq(EnableRabbitRetryAndDlq annotation, Exception exceptionThrown) {
    	return checkIfContainsException(annotation, annotation.directToDlqWhen(), exceptionThrown);
    }

    private void sendMessageToRetry(ProceedingJoinPoint joinPoint, EnableRabbitRetryAndDlq annotation) {
    	TunedRabbitProperties properties = getPropertiesByAnnotationEvent(annotation);
    	Message message = (Message) joinPoint.getArgs()[0];
    	queueRetryComponent.sendToRetryOrDlq(message, properties);
    }

    private void sendMessageToDlq(ProceedingJoinPoint joinPoint, EnableRabbitRetryAndDlq annotation) {
    	TunedRabbitProperties properties = getPropertiesByAnnotationEvent(annotation);
    	Message message = (Message) joinPoint.getArgs()[0];
    	queueRetryComponent.sendToDlq(message, properties);
    }

    private Method getMethod(ProceedingJoinPoint joinPoint) {
    	MethodSignature signature = (MethodSignature) joinPoint.getSignature();
    	return signature.getMethod();
    }

    private boolean checkIfContainsException(EnableRabbitRetryAndDlq annotation, Class<?>[] acceptableExceptions, Exception exceptionThrown) {
    	if (acceptableExceptions.length == 0)
    		return false;

    	List<Class<?>> exceptions = Arrays.asList(acceptableExceptions);
    	if (annotation.checkInheritance()) {
    		return exceptions.stream()
    				.anyMatch(type -> type.isAssignableFrom(exceptionThrown.getClass()));
    	}
    	return exceptions.contains(Exception.class) || exceptions.contains(exceptionThrown.getClass());
    }

    private TunedRabbitProperties getPropertiesByAnnotationEvent(EnableRabbitRetryAndDlq annotation) {
    	String queueProperty = annotation.event();
    	TunedRabbitProperties properties = rabbitCustomPropertiesMap.get(queueProperty);
    	if (Objects.isNull(properties)) {
    		throw new NoSuchBeanDefinitionException(String.format("Any bean with name %s was found", queueProperty));
    	}
    	return properties;
    }
}
