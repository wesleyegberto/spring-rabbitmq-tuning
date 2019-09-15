package com.tradeshift.amqp.annotation;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.stream.IntStream;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import com.tradeshift.amqp.rabbit.properties.TunedRabbitProperties;
import com.tradeshift.amqp.rabbit.properties.TunedRabbitPropertiesMap;
import com.tradeshift.amqp.rabbit.retry.QueueRetryComponent;

@RunWith(SpringRunner.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class EnableRabbitRetryAndDlqAspectTest {
	private static TunedRabbitProperties createQueueProperties;

	@Mock
	private TunedRabbitPropertiesMap tunnedRabbitPropertiesMap;
	@Mock
	private QueueRetryComponent queueComponent;
	@Mock
	private MethodSignature signature;

	@InjectMocks
	@Spy
	private EnableRabbitRetryAndDlqAspect aspect;

	@BeforeClass
	public static void setUp() {
		createQueueProperties = createQueueProperties();
	}

	@Before
	public void beforeEach() {
		when(tunnedRabbitPropertiesMap.get("some-event")).thenReturn(createQueueProperties);

	}

	@Test
	@EnableRabbitRetryAndDlq(event = "some-event")
	public void should_send_to_retry_with_default_config_and_backwards_compatibility() throws Throwable {
		ProceedingJoinPoint joinPoint = mockJointPointWithDeathAndThrowing(
				"should_send_to_retry_with_default_config_and_backwards_compatibility", 1, RuntimeException.class);

		aspect.validateMessage(joinPoint);

		verifyIfRetryWasCalled(1);
		verifyIfDlqWasCalled(0);
	}

	@Test
	@EnableRabbitRetryAndDlq(event = "some-event", exceptions = NumberFormatException.class)
	public void should_send_to_retry_when_exceptions_contains_exception_thrown() throws Throwable {
		ProceedingJoinPoint joinPoint = mockJointPointWithDeathAndThrowing(
				"should_send_to_retry_when_exceptions_contains_exception_thrown", 1, NumberFormatException.class);

		aspect.validateMessage(joinPoint);

		verifyIfRetryWasCalled(1);
		verifyIfDlqWasCalled(0);
	}

	@Test
	@EnableRabbitRetryAndDlq(event = "some-event", retryWhen = NumberFormatException.class)
	public void should_send_to_retry_when_retryWhen_contains_exception_thrown() throws Throwable {
		ProceedingJoinPoint joinPoint = mockJointPointWithDeathAndThrowing(
				"should_send_to_retry_when_retryWhen_contains_exception_thrown", 1, NumberFormatException.class);

		aspect.validateMessage(joinPoint);

		verifyIfRetryWasCalled(1);
		verifyIfDlqWasCalled(0);
	}

	@Test
	@EnableRabbitRetryAndDlq(event = "some-event", checkInheritance = true, retryWhen = IllegalArgumentException.class)
	public void should_send_to_retry_when_retryWhen_contains_exception_checking_inheritance() throws Throwable {
		ProceedingJoinPoint joinPoint = mockJointPointWithDeathAndThrowing(
				"should_send_to_retry_when_retryWhen_contains_exception_checking_inheritance", 1,
				NumberFormatException.class);

		aspect.validateMessage(joinPoint);

		verifyIfRetryWasCalled(1);
		verifyIfDlqWasCalled(0);
	}

	@Test
	@EnableRabbitRetryAndDlq(event = "some-event",
		discartWhen = NumberFormatException.class,
		retryWhen = NumberFormatException.class
	)
	public void should_send_discart_even_when_retryWhen_contains_same_exception() throws Throwable {
		ProceedingJoinPoint joinPoint = mockJointPointWithDeathAndThrowing(
				"should_send_discart_even_when_retryWhen_contains_same_exception", 1, NumberFormatException.class);

		aspect.validateMessage(joinPoint);

		verifyIfRetryWasCalled(0);
		verifyIfDlqWasCalled(0);
	}

	@Test
	@EnableRabbitRetryAndDlq(event = "some-event",
		discartWhen = NullPointerException.class,
		retryWhen = IllegalArgumentException.class,
		directToDlqWhen = NumberFormatException.class
	)
	public void should_send_dlq_when_only_directToDlqWhen_exceptions_contains() throws Throwable {
		ProceedingJoinPoint joinPoint = mockJointPointWithDeathAndThrowing(
				"should_send_dlq_when_only_directToDlqWhen_exceptions_contains", 1, NumberFormatException.class);

		aspect.validateMessage(joinPoint);

		verifyIfRetryWasCalled(0);
		verifyIfDlqWasCalled(1);
	}

	@Test
	@EnableRabbitRetryAndDlq(event = "some-event", checkInheritance = true,
		discartWhen = NullPointerException.class,
		retryWhen = IllegalStateException.class,
		directToDlqWhen = IllegalArgumentException.class
	)
	public void should_send_dlq_when_only_directToDlqWhen_exceptions_contains_checking_inheritance() throws Throwable {
		ProceedingJoinPoint joinPoint = mockJointPointWithDeathAndThrowing(
				"should_send_dlq_when_only_directToDlqWhen_exceptions_contains_checking_inheritance", 1,
				NumberFormatException.class);

		aspect.validateMessage(joinPoint);

		verifyIfRetryWasCalled(0);
		verifyIfDlqWasCalled(1);
	}

	private ProceedingJoinPoint mockJointPointWithDeathAndThrowing(String testMethodName, int numbreOfDeaths,
			Class<? extends Exception> exceptionToThrown) throws Throwable {
		ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
		Method method = mockMethodUsingTestingMethod(testMethodName);
		when(signature.getMethod()).thenReturn(method);
		when(joinPoint.getArgs()).thenReturn(new Message[] { createMessageWithDeath(numbreOfDeaths) });
		when(joinPoint.proceed()).thenThrow(exceptionToThrown);
		when(joinPoint.getSignature()).thenReturn(signature);
		return joinPoint;
	}

	private void verifyIfRetryWasCalled(int numberOfTimes) {
		verify(queueComponent, times(numberOfTimes)).sendToRetryOrDlq(any(Message.class), eq(createQueueProperties));
	}

	private void verifyIfDlqWasCalled(int numberOfTimes) {
		verify(queueComponent, times(numberOfTimes)).sendToDlq(any(Message.class), eq(createQueueProperties));
	}

	private Method mockMethodUsingTestingMethod(String testingMethodName)
			throws NoSuchMethodException, SecurityException {
		return EnableRabbitRetryAndDlqAspectTest.class.getMethod(testingMethodName);
	}

	private static Message createMessageWithDeath(int numberOfDeaths) {
		return new Message("some".getBytes(), createMessageProperties(numberOfDeaths));
	}

	private static MessageProperties createMessageProperties(Integer numberOfDeaths) {
		MessageProperties messageProperties = new MessageProperties();
		HashMap<String, Integer> map = new HashMap();
		IntStream.range(0, numberOfDeaths).forEach(value -> map.put("count", value));
		messageProperties.getHeaders().put("x-death", Collections.singletonList(map));
		return messageProperties;
	}

	private static TunedRabbitProperties createQueueProperties() {
		TunedRabbitProperties queueProperties = new TunedRabbitProperties();
		queueProperties.setQueue("queue.test");
		queueProperties.setExchange("ex.test");
		queueProperties.setExchangeType("topic");
		queueProperties.setMaxRetriesAttempts(5);
		queueProperties.setQueueRoutingKey("routing.key.test");
		queueProperties.setTtlRetryMessage(5000);
		queueProperties.setPrimary(true);
		queueProperties.setVirtualHost("vh");
		queueProperties.setUsername("guest");
		queueProperties.setPassword("guest");
		queueProperties.setHost("host");
		queueProperties.setPort(5672);
		queueProperties.setSslConnection(false);
		queueProperties.setTtlMultiply(1);
		queueProperties.setMaxRetriesAttempts(1);
		return queueProperties;
	}
}
