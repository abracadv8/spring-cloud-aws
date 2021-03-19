/*
 * Copyright 2013-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.aws.messaging.listener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.messaging.MessagingException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import static org.springframework.cloud.aws.messaging.core.QueueMessageUtils.createMessage;

/**
 * Ugly hack.
 *
 * The current implementation is that there is one polling thread in a loop:
 * 1 - Poll for 10 items, create a coundownlatch(10)
 * 2 - Pass them off to the executing thread (this could be a thread pool/executor service)
 * 3 - Count down via the latch as items are processed
 * 4 - When all 10 are processed, repeat
 *
 * The main limitation is that the polling takes place in a single-threaded loop;
 * even if the items were processed instantaneously (0 nanoseconds), the implementation
 * is still bounded by however long it takes to poll the messages.  Eg, if the polling
 * API has 100ms of latency with instantaneous processing, the fastest you could process
 * is 10 messages / 100ms (or 100 messages / second).  Additional, the max threads you could
 * have would be 11 (this thread, plus the 10 task executors = one for each of the 10 messages
 * being polled).  This basically means you can only achieve a max concurrency of 10.
 *
 *
 * Below is a way to decouple the execution threads from the polling threads with a shared blocking queue
 * as the interface between them.  This way you could have multiple threads polling for 10 messages
 * at the same time and adding to the queue until it reaches capacity and then blocks the pollers until the
 * processing threads consume them. The blocking queue is fair so multiple pollers should all theoretically
 * get a fair chance at adding their items so none of the polling threads should starve or not be able to
 * make progress.
 *
 * The default is to have 1 poller such that it should behave somewhat similarly to the
 * original, however, this also means that it uses at least one additional thread than
 * the original does.
 *
 * It almost always makes sense to have 2-3 pollers minimum; with just 1 poller, when it performs
 * the poll, even with just 100ms network latency, that is time the executor threads are idle,
 * without work to do.
 *
 * Some sample settings might be to:
 * - messages set to 10 (poll 10 messages at a time)
 * - set 3 pollers (so it is less likely executors are idle)
 * - a blocking queue size of pollers * message = 25-30 (each of the 3 pollers can add to the queue without blocking one another)
 * - Create your own ThreadExecutor with 10-30 threads to asyncrhonously process.
 *
 *
 *
 * @author Agim Emruli
 * @author Alain Sahli
 * @author Mete Alpaslan Katırcıoğlu
 * @since 1.0
 */
public class SimpleMessageListenerContainer extends AbstractMessageListenerContainer {

	// A few extra threads to handle submitting cleanup tasks
	private static final int DEFAULT_WORKER_THREADS = 3;

	private static final String DEFAULT_THREAD_NAME_PREFIX = ClassUtils
			.getShortName(SimpleMessageListenerContainer.class) + "-";

	private boolean defaultTaskExecutor;

	private long backOffTime = 10000;

	private long queueStopTimeout = 20000;

	private int blockingQueueMaxSize = 10;

	private int blockingQueuePollWaitMs = 25;

	private int pollerCount = 1;

	private AsyncTaskExecutor taskExecutor;

	private ConcurrentHashMap<String, CountDownLatch> scheduledFutureByQueueListenerShutdown;

	private ConcurrentHashMap<String, List<Future<?>>> scheduledFutureByQueueListener;

	private ConcurrentHashMap<String, Future<?>> scheduledFutureByQueueProcessor;

	private ConcurrentHashMap<String, Boolean> runningStateByQueue;

	protected AsyncTaskExecutor getTaskExecutor() {
		return this.taskExecutor;
	}

	public void setTaskExecutor(AsyncTaskExecutor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	/**
	 * @return The number of milliseconds the polling thread must wait before trying to
	 * recover when an error occurs (e.g. connection timeout)
	 */
	public long getBackOffTime() {
		return this.backOffTime;
	}

	/**
	 * The number of milliseconds the polling thread must wait before trying to recover
	 * when an error occurs (e.g. connection timeout). Default is 10000 milliseconds.
	 * @param backOffTime in milliseconds
	 */
	public void setBackOffTime(long backOffTime) {
		this.backOffTime = backOffTime;
	}

	/**
	 * Get the number of polling threads created.
	 * @return The number of polling threads
	 */
	public int getPollerCount() {
		return pollerCount;
	}

	/**
	 * Sets the number of polling threads.
	 * @param pollerCount
	 */
	public void setPollerCount(int pollerCount) {
		Assert.isTrue(pollerCount > 0, "There must be at least 1 polling thread");
		this.pollerCount = pollerCount;
	}

	/**
	 * Returns the blocking queue size.
	 * @return The blocking queue size
	 */
	public int getBlockingQueueMaxSize() {
		return blockingQueueMaxSize;
	}

	/**
	 * The max size of the queue. All of the polling threads will eagerly try to block and fill it up.
	 * @param blockingQueueMaxSize The max size of the queue.
	 */
	public void setBlockingQueueMaxSize(int blockingQueueMaxSize) {
		this.blockingQueueMaxSize = blockingQueueMaxSize;
	}

	/**
	 * How long the execution threads should wait when polling the blocking queue.  Lower values
	 * will mean more contention.  The executor shouldn't have that much time popping items of
	 * the blocking queue so this doesnt need to be set very low or high.
	 * @return How long for executors to poll-loop when the queue is blocked
	 */
	public int getBlockingQueuePollWaitMs() {
		return blockingQueuePollWaitMs;
	}
	/**
	 * Sets the polling queue wait time for items to become available.
	 * @param blockingQueuePollWaitMs How long executors will wait for an item in the queue.
	 */
	public void setBlockingQueuePollWaitMs(int blockingQueuePollWaitMs) {
		this.blockingQueuePollWaitMs = blockingQueuePollWaitMs;
	}

	/**
	 * @return The number of milliseconds the
	 * {@link SimpleMessageListenerContainer#stop(String)} method waits for a queue to
	 * stop before interrupting the current thread. Default value is 10000 milliseconds
	 * (10 seconds).
	 */
	public long getQueueStopTimeout() {
		return this.queueStopTimeout;
	}

	/**
	 * The number of milliseconds the {@link SimpleMessageListenerContainer#stop(String)}
	 * method waits for a queue to stop before interrupting the current thread. Default
	 * value is 20000 milliseconds (20 seconds).
	 * @param queueStopTimeout in milliseconds
	 */
	public void setQueueStopTimeout(long queueStopTimeout) {
		this.queueStopTimeout = queueStopTimeout;
	}

	@Override
	protected void initialize() {
		super.initialize();

		if (this.taskExecutor == null) {
			this.defaultTaskExecutor = true;
			this.taskExecutor = createDefaultTaskExecutor();
		}

		initializeRunningStateByQueue();
		this.scheduledFutureByQueueListener = new ConcurrentHashMap<>(getRegisteredQueues().size());
		this.scheduledFutureByQueueProcessor = new ConcurrentHashMap<>(getRegisteredQueues().size());
		this.scheduledFutureByQueueListenerShutdown = new ConcurrentHashMap<>(getRegisteredQueues().size());
	}

	private void initializeRunningStateByQueue() {
		this.runningStateByQueue = new ConcurrentHashMap<>(getRegisteredQueues().size());
		for (String queueName : getRegisteredQueues().keySet()) {
			this.runningStateByQueue.put(queueName, false);
		}
	}

	@Override
	protected void doStart() {
		synchronized (this.getLifecycleMonitor()) {
			scheduleMessageListeners();
		}
	}

	@Override
	protected void doStop() {
		notifyRunningQueuesToStop();
		waitForRunningQueuesToStop();
	}

	private void notifyRunningQueuesToStop() {
		getLogger().debug("Stopping notifyRunningQueuesToStop called");
		for (Map.Entry<String, Boolean> runningStateByQueue : this.runningStateByQueue.entrySet()) {
			if (runningStateByQueue.getValue()) {
				stopQueue(runningStateByQueue.getKey());
			}
		}
		getLogger().debug("Stopping notifyRunningQueuesToStop called complete");
	}

	private void waitForRunningQueueToStop(String logicalName) {
		String logicalQueueName = logicalName;
		List<Future<?>> listeners = this.scheduledFutureByQueueListener.get(logicalQueueName);
		Future<?> processors = this.scheduledFutureByQueueProcessor.get(logicalQueueName);

		List<Future<?>> listToStop = new ArrayList<>();
		if (listeners != null) {
			listToStop.addAll(listeners);
		}
		if (processors != null) {
			listToStop.add(processors);
		}

		// Try to stop all the futures at the same time. If the processors are nearing
		// stopping, they  should have a few free threads to asynchronously stop the
		// threads below.
		// Ex, 10 messages + 5 pollers + 2 running = 17 threads. As messages stop
		// executing, more threads  can get freed up to stop the work
		if (listToStop == null || listToStop.isEmpty()) {
			return;
		}

		CountDownLatch latch = new CountDownLatch(listToStop.size());
		for (Future<?> queueSpinningThread : listToStop) {
			getTaskExecutor().execute(() -> {
				try {
					getLogger().debug("Stoppinmg queue[{}] task[{}]", logicalQueueName, queueSpinningThread);
					queueSpinningThread.get(getQueueStopTimeout(), TimeUnit.MILLISECONDS);
					latch.countDown();
				}
				catch (ExecutionException | TimeoutException e) {
					getLogger().warn("An exception occurred while stopping queue '" + logicalQueueName + "'", e);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			});
		}
		try {
			latch.await(getQueueStopTimeout(), TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			getLogger().warn("An Exception occurred while waiting for queue '{}' to finish processing",
					logicalQueueName, e);
		}
	}

	private void waitForRunningQueuesToStop() {
		getLogger().debug("Stoppinmg waitForRunningQueuesToStop called");
		for (Map.Entry<String, Boolean> queueRunningState : this.runningStateByQueue.entrySet()) {
			waitForRunningQueueToStop(queueRunningState.getKey());
		}
	}

	@Override
	protected void doDestroy() {
		if (this.defaultTaskExecutor) {
			((ThreadPoolTaskExecutor) this.taskExecutor).destroy();
		}
	}

	/**
	 * Create a default TaskExecutor. Called if no explicit TaskExecutor has been
	 * specified.
	 * <p>
	 * The default implementation builds a
	 * {@link org.springframework.core.task.SimpleAsyncTaskExecutor} with the specified
	 * bean name (or the class name, if no bean name specified) as thread name prefix.
	 * @return a {@link org.springframework.core.task.SimpleAsyncTaskExecutor} configured
	 * with the thread name prefix
	 * @see org.springframework.core.task.SimpleAsyncTaskExecutor#SimpleAsyncTaskExecutor(String)
	 */
	protected AsyncTaskExecutor createDefaultTaskExecutor() {
		String beanName = getBeanName();
		ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
		threadPoolTaskExecutor.setThreadNamePrefix(beanName != null ? beanName + "-" : DEFAULT_THREAD_NAME_PREFIX);
		int spinningThreads = this.getRegisteredQueues().size();

		if (spinningThreads > 0) {
			threadPoolTaskExecutor.setCorePoolSize(spinningThreads * DEFAULT_WORKER_THREADS + pollerCount);

			int maxNumberOfMessagePerBatch = getMaxNumberOfMessages() != null ? getMaxNumberOfMessages()
					: DEFAULT_MAX_NUMBER_OF_MESSAGES;
			threadPoolTaskExecutor.setMaxPoolSize(spinningThreads * (maxNumberOfMessagePerBatch + 1 + pollerCount));
		}

		// No use of a thread pool executor queue to avoid retaining message too long in memory
		threadPoolTaskExecutor.setQueueCapacity(0);

		// On rejection, just block that thread until it is able to put a job in the queue
		threadPoolTaskExecutor.setRejectedExecutionHandler((r, executor) -> {
			try {
				// The queue is synchronous
				executor.getQueue().put(r);
			}
			catch (InterruptedException e) {
				throw new RejectedExecutionException(
						"InterruptedException while waiting to add  Task to ThreadPoolExecutor queue...", e);
			}
		});
		threadPoolTaskExecutor.afterPropertiesSet();

		return threadPoolTaskExecutor;

	}

	private void scheduleMessageListeners() {
		for (Map.Entry<String, QueueAttributes> registeredQueue : getRegisteredQueues().entrySet()) {
			startQueue(registeredQueue.getKey(), registeredQueue.getValue());
		}
	}

	protected void executeMessage(org.springframework.messaging.Message<String> stringMessage) {
		getMessageHandler().handleMessage(stringMessage);
	}

	/**
	 * Stops and waits until the specified queue has stopped. If the wait timeout
	 * specified by {@link SimpleMessageListenerContainer#getQueueStopTimeout()} is
	 * reached, the current thread is interrupted.
	 * @param logicalQueueName the name as defined on the listener method
	 */
	public void stop(String logicalQueueName) {
		stopQueue(logicalQueueName);
		waitForRunningQueueToStop(logicalQueueName);
	}

	protected void stopQueue(String logicalQueueName) {
		Assert.isTrue(this.runningStateByQueue.containsKey(logicalQueueName),
				"Queue with name '" + logicalQueueName + "' does not exist");
		this.runningStateByQueue.put(logicalQueueName, false);
	}

	public void start(String logicalQueueName) {
		Assert.isTrue(this.runningStateByQueue.containsKey(logicalQueueName),
				"Queue with name '" + logicalQueueName + "' does not exist");

		QueueAttributes queueAttributes = this.getRegisteredQueues().get(logicalQueueName);
		startQueue(logicalQueueName, queueAttributes);
	}

	/**
	 * Checks if the spinning thread for the specified queue {@code logicalQueueName} is
	 * still running (polling for new messages) or not.
	 * @param logicalQueueName the name as defined on the listener method
	 * @return {@code true} if the spinning thread for the specified queue is running
	 * otherwise {@code false}.
	 */
	public boolean isRunningListener(String logicalQueueName) {
		List<Future<?>> futures = this.scheduledFutureByQueueListener.get(logicalQueueName);
		if (futures == null) {
			return false;
		}
		for (Future<?> future : futures) {
			if (!future.isCancelled() && !future.isDone()) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Checks if the spinning thread for the specified queue {@code logicalQueueName} is
	 * still running (polling for new messages) or not.
	 * @param logicalQueueName the name as defined on the listener method
	 * @return {@code true} if the spinning thread for the specified queue is running
	 * otherwise {@code false}.
	 */
	public boolean isRunningProcessor(String logicalQueueName) {
		Future<?> future = this.scheduledFutureByQueueProcessor.get(logicalQueueName);
		return future != null && !future.isCancelled() && !future.isDone();
	}

	protected void startQueue(String queueName, QueueAttributes queueAttributes) {
		if (this.runningStateByQueue.containsKey(queueName) && this.runningStateByQueue.get(queueName)) {
			return;
		}

		this.runningStateByQueue.put(queueName, true);

		// The queue will be fair and blocking takes until something is put
		BlockingQueue<Message> blockingQueue = new ArrayBlockingQueue<>(blockingQueueMaxSize, true);
		List<Future<?>> futures = new ArrayList<>();
		this.scheduledFutureByQueueListenerShutdown.put(queueName, new CountDownLatch(pollerCount));
		for (int i = 0; i < pollerCount; i++) {
			Future<?> futureListener1 = getTaskExecutor()
					.submit(new AsynchronousMessageListener(queueName, queueAttributes, blockingQueue));
			futures.add(futureListener1);
		}

		this.scheduledFutureByQueueListener.put(queueName, futures);
		Future<?> futureProc = getTaskExecutor()
				.submit(new AsynchronousMessageProcessor(queueName, queueAttributes, blockingQueue));
		this.scheduledFutureByQueueProcessor.put(queueName, futureProc);
	}

	private static final class SignalExecutingRunnable implements Runnable {

		private final CountDownLatch countDownLatch;

		private final Runnable runnable;

		private SignalExecutingRunnable(CountDownLatch endSignal, Runnable runnable) {
			this.countDownLatch = endSignal;
			this.runnable = runnable;
		}

		@Override
		public void run() {
			try {
				this.runnable.run();
			}
			finally {
				this.countDownLatch.countDown();
			}
		}

	}

	private static final class SignalExecutingSemRunnable implements Runnable {

		private final Semaphore countDownLatch;

		private final Runnable runnable;

		private SignalExecutingSemRunnable(Semaphore endSignal, Runnable runnable) {
			this.countDownLatch = endSignal;
			this.runnable = runnable;
		}

		@Override
		public void run() {
			try {
				this.runnable.run();
			}
			finally {
				this.countDownLatch.release();
			}
		}

	}

	private final class AsynchronousMessageListener implements Runnable {

		private final QueueAttributes queueAttributes;

		private final String logicalQueueName;

		private final BlockingQueue<Message> blockingQueue;

		private AsynchronousMessageListener(String logicalQueueName, QueueAttributes queueAttributes,
				BlockingQueue<Message> blockingQueue) {
			this.logicalQueueName = logicalQueueName;
			this.queueAttributes = queueAttributes;
			this.blockingQueue = blockingQueue;
		}

		@Override
		public void run() {
			while (isQueueRunning()) {
				try {
					ReceiveMessageResult receiveMessageResult = getAmazonSqs()
							.receiveMessage(this.queueAttributes.getReceiveMessageRequest());
					// TODO what to do about countdown?
					getLogger().debug("PollerThread[{}] Got [{}] messages", Thread.currentThread().getName(),
							receiveMessageResult.getMessages().size());
					for (Message message : receiveMessageResult.getMessages()) {
						// Synchronize as the state of the queue could stop running at any point
						blockingQueue.put(message);
					}
					getLogger().debug("PollerThread[{}] put [{}] messages - current queue size[{}]",
							Thread.currentThread().getName(), receiveMessageResult.getMessages().size(),
							blockingQueue.size());
				}
				catch (Exception e) {
					getLogger().warn("An Exception occurred while polling queue '{}'. The failing operation will be "
							+ "retried in {} milliseconds", this.logicalQueueName, getBackOffTime(), e);
					try {
						// noinspection BusyWait
						Thread.sleep(getBackOffTime());
					}
					catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
					}
				}
			}

			SimpleMessageListenerContainer.this.scheduledFutureByQueueListenerShutdown.get(this.logicalQueueName)
					.countDown();
			if (SimpleMessageListenerContainer.this.scheduledFutureByQueueListenerShutdown.get(this.logicalQueueName)
					.getCount() == 0) {
				SimpleMessageListenerContainer.this.scheduledFutureByQueueListenerShutdown
						.remove(this.logicalQueueName);
				SimpleMessageListenerContainer.this.scheduledFutureByQueueListener.remove(this.logicalQueueName);
			}
		}

		private boolean isQueueRunning() {
			if (SimpleMessageListenerContainer.this.runningStateByQueue.containsKey(this.logicalQueueName)) {
				return SimpleMessageListenerContainer.this.runningStateByQueue.get(this.logicalQueueName);
			}
			else {
				getLogger().warn(
						"Stopped queue '" + this.logicalQueueName + "' because it was not listed as running queue.");
				return false;
			}
		}

	}

	private final class AsynchronousMessageProcessor implements Runnable {

		private final QueueAttributes queueAttributes;

		private final String logicalQueueName;

		private final BlockingQueue<Message> blockingQueue;

		private AsynchronousMessageProcessor(String logicalQueueName, QueueAttributes queueAttributes,
				BlockingQueue<Message> blockingQueue) {
			this.logicalQueueName = logicalQueueName;
			this.queueAttributes = queueAttributes;
			this.blockingQueue = blockingQueue;
		}

		@Override
		public void run() {

			Semaphore sem = new Semaphore(blockingQueueMaxSize);
			while (isQueueRunning()) {
				try {
					// Don't wait forever, just wait for X seconds so that the queue can  stop running at some point
					Message message = blockingQueue.poll(blockingQueuePollWaitMs, TimeUnit.MILLISECONDS);
					if (message != null) {
						if (isQueueRunning()) {
							MessageExecutor messageExecutor = new MessageExecutor(this.logicalQueueName, message,
									this.queueAttributes);
							sem.acquire();
							getTaskExecutor().execute(new SignalExecutingSemRunnable(sem, messageExecutor));
						}
					}
				}
				catch (Exception e) {
					getLogger().warn("An Exception occurred while executing tasks for queue '{}'",
							this.logicalQueueName, e);
				}
			}

			// If there are still items, try to process them and block anything else from
			// putting stuff in. Keep track of the total time taken below.
			long startTime = System.currentTimeMillis();
			long endTime = startTime;
			long timeTaken = 0;

			// Making sure the writer has waited for all of its readers to stop
			CountDownLatch latch = SimpleMessageListenerContainer.this.scheduledFutureByQueueListenerShutdown
					.get(this.logicalQueueName);
			if (latch != null) {
				try {
					latch.await(queueStopTimeout - timeTaken, TimeUnit.MILLISECONDS);
					// Calculate total time taken so far
					endTime = System.currentTimeMillis();
					timeTaken = endTime - startTime;
				}
				catch (InterruptedException e) {
					getLogger().warn("An Exception occurred while waiting for queue '{}' to finish processing",
							this.logicalQueueName, e);
				}
			}

			// Block until we can get all the semaphores (eg, any processors can now no  longer add to the queue)
			try {
				// Block the rest of the queues, taking into account total time taken thus far
				// (eg, if stop time is 30s, wait for 30s minus whatever time was taken above)
				sem.tryAcquire(blockingQueueMaxSize, queueStopTimeout - timeTaken, TimeUnit.MILLISECONDS);
			}
			catch (InterruptedException e) {
				getLogger().warn("An Exception occurred while waiting for queue '{}' to finish processing",
						this.logicalQueueName, e);
			}

			// Readers should have stopped by now.
			// No one should be allowed to add to the queue any longer.
			// Process any remaining values.
			if (!blockingQueue.isEmpty()) {
				CountDownLatch messageBatchLatch = new CountDownLatch(blockingQueue.size());
				// Don't wait forever, just wait for X seconds so that the queue can stop
				// running at some point
				try {
					Message message = blockingQueue.poll(blockingQueuePollWaitMs, TimeUnit.MILLISECONDS);
					while (message != null) {
						MessageExecutor messageExecutor = new MessageExecutor(this.logicalQueueName, message,
								this.queueAttributes);
						getTaskExecutor().execute(new SignalExecutingRunnable(messageBatchLatch, messageExecutor));
						message = blockingQueue.poll(blockingQueuePollWaitMs, TimeUnit.MILLISECONDS);
					}

					// Submit all messages and wait till the
					messageBatchLatch.await(queueStopTimeout - timeTaken, TimeUnit.MILLISECONDS);
					endTime = System.currentTimeMillis();
					timeTaken = endTime - startTime;
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				catch (Exception e) {
					getLogger().warn("An Exception occurred while executing tasks for queue '{}'",
							this.logicalQueueName, e);
				}
			}

			SimpleMessageListenerContainer.this.scheduledFutureByQueueProcessor.remove(this.logicalQueueName);
		}

		private boolean isQueueRunning() {
			if (SimpleMessageListenerContainer.this.runningStateByQueue.containsKey(this.logicalQueueName)) {
				return SimpleMessageListenerContainer.this.runningStateByQueue.get(this.logicalQueueName);
			}
			else {
				getLogger().warn(
						"Stopped queue '" + this.logicalQueueName + "' because it was not listed as running queue.");
				return false;
			}
		}

	}

	private final class MessageExecutor implements Runnable {

		private final Message message;

		private final String logicalQueueName;

		private final String queueUrl;

		private final boolean hasRedrivePolicy;

		private final SqsMessageDeletionPolicy deletionPolicy;

		private MessageExecutor(String logicalQueueName, Message message, QueueAttributes queueAttributes) {
			this.logicalQueueName = logicalQueueName;
			this.message = message;
			this.queueUrl = queueAttributes.getReceiveMessageRequest().getQueueUrl();
			this.hasRedrivePolicy = queueAttributes.hasRedrivePolicy();
			this.deletionPolicy = queueAttributes.getDeletionPolicy();
		}

		@Override
		public void run() {
			String receiptHandle = this.message.getReceiptHandle();
			org.springframework.messaging.Message<String> queueMessage = getMessageForExecution();
			try {
				executeMessage(queueMessage);
				applyDeletionPolicyOnSuccess(receiptHandle);
			}
			catch (MessagingException messagingException) {
				applyDeletionPolicyOnError(receiptHandle);
			}
		}

		private void applyDeletionPolicyOnSuccess(String receiptHandle) {
			if (this.deletionPolicy == SqsMessageDeletionPolicy.ON_SUCCESS
					|| this.deletionPolicy == SqsMessageDeletionPolicy.ALWAYS
					|| this.deletionPolicy == SqsMessageDeletionPolicy.NO_REDRIVE) {
				deleteMessage(receiptHandle);
			}
		}

		private void applyDeletionPolicyOnError(String receiptHandle) {
			if (this.deletionPolicy == SqsMessageDeletionPolicy.ALWAYS
					|| (this.deletionPolicy == SqsMessageDeletionPolicy.NO_REDRIVE && !this.hasRedrivePolicy)) {
				deleteMessage(receiptHandle);
			}
		}

		private void deleteMessage(String receiptHandle) {
			getAmazonSqs().deleteMessageAsync(new DeleteMessageRequest(this.queueUrl, receiptHandle),
					new DeleteMessageHandler(receiptHandle));
		}

		private org.springframework.messaging.Message<String> getMessageForExecution() {
			HashMap<String, Object> additionalHeaders = new HashMap<>();
			additionalHeaders.put(QueueMessageHandler.LOGICAL_RESOURCE_ID, this.logicalQueueName);
			if (this.deletionPolicy == SqsMessageDeletionPolicy.NEVER) {
				String receiptHandle = this.message.getReceiptHandle();
				QueueMessageAcknowledgment acknowledgment = new QueueMessageAcknowledgment(
						SimpleMessageListenerContainer.this.getAmazonSqs(), this.queueUrl, receiptHandle);
				additionalHeaders.put(QueueMessageHandler.ACKNOWLEDGMENT, acknowledgment);
			}
			additionalHeaders.put(QueueMessageHandler.VISIBILITY,
					new QueueMessageVisibility(SimpleMessageListenerContainer.this.getAmazonSqs(), this.queueUrl,
							this.message.getReceiptHandle()));

			return createMessage(this.message, additionalHeaders);
		}

	}

}
