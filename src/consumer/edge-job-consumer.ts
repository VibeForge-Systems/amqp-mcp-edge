import { randomUUID } from 'node:crypto';

import type { Channel, ConsumeMessage, Options } from 'amqplib';

import type { EdgeChannelConfig, ResolvedEdgeChannelConfig } from '../config/index.js';
import { buildConsumerQueueName, buildJobBindingKeys, resolveConfig, validateConfig } from '../config/index.js';
import { assertCompatibleEnvelope, edgeFailureCodes, type ExecutionJobEnvelope, type JobFailureEvent } from '../contracts/index.js';
import type { EdgeChannelConnectionManager } from '../connection/connection-manager.js';
import { createHealthSnapshot, type EdgeChannelHealth } from '../health/index.js';
import { checkTopology, EdgeTopologyError } from '../topology/index.js';
import { edgeLibraryName, edgeLibraryVersion } from '../version.js';

export interface JobDeliveryContext {
  deliveryTag?: number;
  routingKey?: string;
  redelivered?: boolean;
  /**
   * Which delivery of this job this is, starting at 1: the first delivery, plus every requeue this
   * consumer made, plus every return the broker counted in `x-delivery-count` (a quorum queue
   * counts a delivery whose channel closed unacked). At least 2 when the broker marks it
   * redelivered.
   */
  deliveryAttempt?: number;
  /**
   * Which subscription of this consumer the job arrived on. The consumer subscribes again after a
   * reconnect, after the broker closes its channel, and after the broker cancels its subscription,
   * and delivery tags restart on a new channel; `ack` and `nack` use this with `deliveryTag` to
   * refuse a delivery from a subscription that has ended. Pass the context the handler received.
   */
  subscriptionId?: number;
}

/**
 * `ack` or `nack` named a delivery from a subscription that has ended: its channel closed (a
 * reconnect, or the broker closed it), the broker cancelled the subscription, or the consumer was
 * stopped. Nothing was sent to the broker. A job left unsettled on a closed channel goes back to
 * its queue and is delivered again, marked redelivered.
 */
export class EdgeStaleDeliveryError extends Error {
  public readonly deliveryTag: number;

  public constructor(deliveryTag: number, options: { cause?: unknown } = {}) {
    super(
      `delivery ${deliveryTag} is from a subscription that has ended, so it can no longer be acked or nacked; nothing was sent to the broker`,
      options,
    );
    this.name = 'EdgeStaleDeliveryError';
    this.deliveryTag = deliveryTag;
  }
}

/** What the consumer needs to report a job it gives up on. `createEdgeChannel` passes its publisher. */
export interface EdgeJobConsumerOptions {
  failurePublisher?: { publishFailure(message: JobFailureEvent): Promise<void> };
}

interface Subscription {
  id: number;
  channel: Channel;
  consumerTag?: string;
  ended: boolean;
  onChannelClose: () => void;
}

interface PendingDelivery {
  message: ConsumeMessage;
  attemptKey: string;
  subscription: Subscription;
}

// Requeue counts are kept per job attempt for this many attempts at most; the oldest is forgotten
// first. A forgotten count only means a later delivery is counted from the broker's flags again.
const maxTrackedAttempts = 10_000;

/** The most characters of an opted-in error description a failure event carries. */
export const maxHandlerErrorDescriptionLength = 1000;

// An error's class name is sent only when it looks like one, so a name cannot carry other text.
const errorNamePattern = /^[A-Za-z_][A-Za-z0-9_]{0,63}$/;

/**
 * What a failure event says about the handler's error: always the stable code `HANDLER_ERROR`;
 * the error's class name when it is a plain identifier; and a description only when the node opts
 * in with `describeHandlerError`, cut to `maxHandlerErrorDescriptionLength` characters. The
 * handler's own message is never sent by default: error messages often carry connection strings,
 * signed URLs, file paths or other secrets.
 */
export function describeHandlerErrorForEvent(
  error: unknown,
  describe?: (error: unknown) => string | undefined,
): { code: string; name?: string; description?: string } {
  const summary: { code: string; name?: string; description?: string } = { code: edgeFailureCodes.handlerError };
  const name = error instanceof Error ? error.name : undefined;

  if (typeof name === 'string' && errorNamePattern.test(name)) {
    summary.name = name;
  }

  if (describe) {
    let description: unknown;

    try {
      description = describe(error);
    } catch {
      // A describer that throws adds nothing; it must not stop the report or the dead-lettering.
      description = undefined;
    }

    if (typeof description === 'string' && description.length > 0) {
      summary.description = truncate(description, maxHandlerErrorDescriptionLength);
    }
  }

  return summary;
}

function truncate(value: string, maxLength: number): string {
  if (value.length <= maxLength) {
    return value;
  }

  // Never end on the first half of a surrogate pair.
  const end = /[\uD800-\uDBFF]/.test(value.charAt(maxLength - 1)) ? maxLength - 1 : maxLength;
  return value.slice(0, end);
}

export type EdgeJobHandler = (job: ExecutionJobEnvelope, context: JobDeliveryContext) => Promise<void>;

export interface EdgeJobConsumer {
  start(): Promise<void>;
  stop(): Promise<void>;
  onJob(handler: EdgeJobHandler): void;
  ack(delivery: JobDeliveryContext): Promise<void>;
  nack(delivery: JobDeliveryContext, options?: { requeue?: boolean; reason?: string }): Promise<void>;
  health(): Promise<EdgeChannelHealth>;
}

/**
 * The `execution.failure` the consumer publishes when the handler threw on the last delivery
 * attempt. Shared with the mock channel, so tests see the same event.
 */
export function createExhaustedFailureEvent(
  config: Pick<ResolvedEdgeChannelConfig, 'maxDeliveryAttempts' | 'producer' | 'siteId' | 'describeHandlerError'>,
  nodeId: string,
  job: ExecutionJobEnvelope,
  deliveryAttempt: number,
  error: unknown,
): JobFailureEvent {
  const { maxDeliveryAttempts } = config;
  const handlerError = describeHandlerErrorForEvent(error, config.describeHandlerError);
  const now = new Date().toISOString();

  return {
    specVersion: '1.0',
    messageType: 'execution.failure',
    messageId: randomUUID(),
    timestamp: now,
    producer: config.producer ?? { service: edgeLibraryName, version: edgeLibraryVersion },
    compatibility: { channelVersion: edgeLibraryVersion },
    payload: {
      jobId: job.payload.jobId,
      attemptId: job.payload.attemptId,
      ...(config.siteId ? { siteId: config.siteId } : {}),
      nodeId,
      failedAt: now,
      retryable: false,
      code: edgeFailureCodes.deliveryAttemptsExhausted,
      message: `the job handler threw on delivery ${deliveryAttempt} of at most ${maxDeliveryAttempts}; the job was dead-lettered`,
      details: { deliveryAttempt, maxDeliveryAttempts, handlerError },
    },
  };
}

export class EdgeJobConsumer implements EdgeJobConsumer {
  private readonly config;

  private readonly connectionManager;

  private jobHandler: EdgeJobHandler = async () => undefined;

  // Keyed by delivery tag. Every entry belongs to a live subscription: entries go when theirs ends.
  private readonly pendingDeliveries = new Map<number, PendingDelivery>();

  private readonly requeueCounts = new Map<string, number>();

  private readonly failurePublisher?: EdgeJobConsumerOptions['failurePublisher'];

  private failureReportErrors = 0;

  private lastFailureReportError?: string;

  // start() was called and stop() was not: the consumer subscribes again whenever it can.
  private started = false;

  private subscription?: Subscription;

  private subscribing?: Promise<void>;

  private readonly liveSubscriptions = new Map<number, Subscription>();

  private subscriptionCount = 0;

  private discardedDeliveries = 0;

  private brokerCancellations = 0;

  private lastResubscribeError?: string;

  private topologyVerification?: Promise<void>;

  private topologyError?: EdgeTopologyError;

  public constructor(
    config: EdgeChannelConfig,
    connectionManager: EdgeChannelConnectionManager,
    options: EdgeJobConsumerOptions = {},
  ) {
    const validationErrors = validateConfig(config);

    if (validationErrors.length > 0) {
      throw new Error(`invalid edge channel config: ${validationErrors.join('; ')}`);
    }

    this.config = resolveConfig(config);
    this.connectionManager = connectionManager;
    this.failurePublisher = options.failurePublisher;
    // The old subscription ended when its channel closed; subscribe again on the new one.
    connectionManager.onReconnect(() => this.resubscribe());
    connectionManager.onChannelReopened('consumer', () => this.resubscribe());
  }

  public onJob(handler: EdgeJobHandler): void {
    this.jobHandler = handler;
  }

  public async start(): Promise<void> {
    if (this.started) {
      return;
    }

    // Started before subscribing, so a channel that closes meanwhile is subscribed again.
    this.started = true;

    try {
      await this.subscribe();
    } catch (error) {
      this.started = false;
      throw error;
    }
  }

  /**
   * Cancels the subscription. Jobs still unsettled stay unacked on the channel until it closes,
   * when the broker delivers them again; their contexts can no longer be acked or nacked.
   */
  public async stop(): Promise<void> {
    if (!this.started) {
      return;
    }

    this.started = false;
    await this.subscribing?.catch(() => undefined);
    const subscription = this.subscription;

    if (subscription) {
      if (subscription.consumerTag) {
        await subscription.channel.cancel(subscription.consumerTag).catch(() => undefined);
      }

      this.endSubscription(subscription);
    }
  }

  /**
   * Acks a job. Rejects with an `EdgeStaleDeliveryError`, sending nothing, when the delivery's
   * subscription has ended: the broker no longer knows its delivery tag.
   */
  public async ack(delivery: JobDeliveryContext): Promise<void> {
    const pending = this.getPendingDelivery(delivery);
    this.settle(pending, (channel, message) => channel.ack(message));
    this.requeueCounts.delete(pending.attemptKey);
  }

  /**
   * Rejects a job. With `requeue: true` the broker delivers it again, and the next delivery's
   * `deliveryAttempt` counts this requeue; the consumer does not limit requeues a handler asks for,
   * so a handler that requeues should check `deliveryAttempt` itself. Without it (the default) the
   * job is dead-lettered. Rejects with an `EdgeStaleDeliveryError` as `ack` does.
   */
  public async nack(delivery: JobDeliveryContext, options: { requeue?: boolean; reason?: string } = {}): Promise<void> {
    const pending = this.getPendingDelivery(delivery);
    const requeue = options.requeue ?? false;
    this.settle(pending, (channel, message) => channel.nack(message, false, requeue));

    if (requeue) {
      this.countRequeue(pending.attemptKey);
    } else {
      this.requeueCounts.delete(pending.attemptKey);
    }
  }

  public async health(): Promise<EdgeChannelHealth> {
    const connectionHealth = await this.connectionManager.health();

    // consumerActive comes from the connection manager: true only while this consumer's
    // subscription is on the open consumer channel.
    return createHealthSnapshot({
      connected: connectionHealth.connected,
      consumerActive: connectionHealth.consumerActive,
      publisherActive: connectionHealth.publisherActive,
      details: {
        // The connection's reconnect state and channel reopens, then this consumer's own details.
        ...connectionHealth.details,
        pendingDeliveries: this.pendingDeliveries.size,
        maxDeliveryAttempts: this.config.maxDeliveryAttempts,
        ...(this.discardedDeliveries > 0 ? { discardedDeliveries: this.discardedDeliveries } : {}),
        ...(this.brokerCancellations > 0 ? { brokerCancellations: this.brokerCancellations } : {}),
        ...(this.lastResubscribeError ? { lastResubscribeError: this.lastResubscribeError } : {}),
        ...(this.topologyError ? { topologyError: this.topologyError.message } : {}),
        ...(this.failureReportErrors > 0
          ? { failureReportErrors: this.failureReportErrors, lastFailureReportError: this.lastFailureReportError }
          : {}),
      },
    });
  }

  /**
   * Checks, without declaring anything, that the consumer queue exists. In passive mode `start()`
   * calls it, and `createEdgeChannel` calls it before returning. A success is cached until the next
   * reconnect; a failure is not, so a later call checks again.
   *
   * Only the queue is checked. The jobs and dead-letter exchanges are reached through bindings and
   * the queue's dead-letter argument, both owned by the control plane, and RabbitMQ 4 refuses a
   * passive check of an exchange the node holds no permission on.
   */
  public async verifyTopology(): Promise<void> {
    if (!this.topologyVerification) {
      const verification = this.checkConsumerTopology().then(
        () => {
          this.topologyError = undefined;
        },
        (error: unknown) => {
          if (this.topologyVerification === verification) {
            this.topologyVerification = undefined;
          }

          if (error instanceof EdgeTopologyError) {
            this.topologyError = error;
          }

          throw error;
        },
      );
      this.topologyVerification = verification;
    }

    await this.topologyVerification;
  }

  private async checkConsumerTopology(): Promise<void> {
    const channel = await this.connectionManager.openTopologyChannel();
    await checkTopology(channel, [{ kind: 'queue', name: buildConsumerQueueName(this.config) }]);
  }

  // One subscription attempt at a time; concurrent callers share it.
  private subscribe(): Promise<void> {
    if (!this.subscribing) {
      const subscribing = this.initializeConsumer().finally(() => {
        if (this.subscribing === subscribing) {
          this.subscribing = undefined;
        }
      });
      this.subscribing = subscribing;
    }

    return this.subscribing;
  }

  /**
   * Subscribes again after the subscription ended with its channel or was cancelled by the broker.
   * A new channel may reach a broker whose topology changed meanwhile, so a passive-mode consumer
   * checks its queue again, once, first. If the queue is gone it stops instead of retrying:
   * restoring the topology is the control plane's job, not this node's, and health() reports why.
   */
  private async resubscribe(): Promise<void> {
    this.topologyVerification = undefined;

    while (this.started && !this.subscription) {
      try {
        await this.subscribe();
        this.lastResubscribeError = undefined;
      } catch (error) {
        if (error instanceof EdgeTopologyError) {
          this.started = false;
          return;
        }

        this.lastResubscribeError = error instanceof Error ? error.message : String(error);
        throw error;
      }
    }
  }

  private async initializeConsumer(): Promise<void> {
    const queueName = buildConsumerQueueName(this.config);

    if (this.config.topologyMode === 'passive') {
      await this.verifyTopology();
    }

    const channel = await this.connectionManager.getConsumerChannel();

    if (this.config.topologyMode === 'declare') {
      await this.declareTopology(channel, queueName);
    }

    await channel.prefetch(this.config.prefetchCount);

    this.subscriptionCount += 1;
    const subscription: Subscription = {
      id: this.subscriptionCount,
      channel,
      ended: false,
      // A channel that closes ends its subscription: the broker returns the unacked jobs to the
      // queue, and their delivery tags mean nothing on any other channel.
      onChannelClose: () => this.endSubscription(subscription),
    };
    this.liveSubscriptions.set(subscription.id, subscription);
    channel.once('close', subscription.onChannelClose);

    try {
      const { consumerTag } = await channel.consume(
        queueName,
        (message) => {
          void this.handleMessage(subscription, message);
        },
        {
          consumerTag: this.config.consumerTag,
        },
      );
      subscription.consumerTag = consumerTag;
    } catch (error) {
      this.endSubscription(subscription);
      throw error;
    }

    if (subscription.ended) {
      // The channel closed before the broker confirmed the subscription; resubscribe() tries again.
      return;
    }

    this.subscription = subscription;
    this.connectionManager.reportActive('consumer', channel);
  }

  // Forgets a subscription and the deliveries still pending on it.
  private endSubscription(subscription: Subscription): void {
    if (subscription.ended) {
      return;
    }

    subscription.ended = true;
    subscription.channel.off('close', subscription.onChannelClose);
    this.liveSubscriptions.delete(subscription.id);

    for (const [deliveryTag, pending] of this.pendingDeliveries) {
      if (pending.subscription === subscription) {
        this.pendingDeliveries.delete(deliveryTag);
        this.discardedDeliveries += 1;
      }
    }

    if (this.subscription === subscription) {
      this.subscription = undefined;
      this.connectionManager.reportInactive('consumer');
    }
  }

  // The broker cancelled the subscription, for example because the queue was deleted. Subscribe
  // again; in passive mode that checks the queue first.
  private handleBrokerCancel(subscription: Subscription): void {
    if (subscription.ended) {
      return;
    }

    this.brokerCancellations += 1;
    this.endSubscription(subscription);
    void this.resubscribe().catch(() => undefined);
  }

  private async declareTopology(channel: Channel, queueName: string): Promise<void> {
    await channel.assertExchange(this.config.jobsExchange, 'topic', { durable: true });
    await channel.assertExchange(this.config.deadLetterExchange, 'topic', { durable: true });

    const queueOptions: Options.AssertQueue = {
      durable: true,
      ...this.config.consumerQueueOptions,
      arguments: {
        ...(this.config.consumerQueueOptions?.arguments ?? {}),
        'x-dead-letter-exchange': this.config.deadLetterExchange,
      },
    };

    await channel.assertQueue(queueName, queueOptions);

    for (const bindingKey of buildJobBindingKeys(this.config)) {
      await channel.bindQueue(queueName, this.config.jobsExchange, bindingKey);
    }
  }

  private async handleMessage(subscription: Subscription, message: ConsumeMessage | null): Promise<void> {
    if (!message) {
      this.handleBrokerCancel(subscription);
      return;
    }

    if (subscription.ended) {
      return;
    }

    // An invalid message is dead-lettered on the channel it arrived on. If that channel has just
    // closed, amqplib throws, and the broker delivers the message again.
    const reject = () => {
      try {
        subscription.channel.nack(message, false, false);
      } catch {
        // The channel closed; nothing to do.
      }
    };

    if (message.content.byteLength > this.config.maxMessageSizeBytes) {
      reject();
      return;
    }

    let parsedPayload: unknown;

    try {
      parsedPayload = JSON.parse(message.content.toString('utf8'));
    } catch {
      reject();
      return;
    }

    let envelope: ExecutionJobEnvelope;

    try {
      const validatedEnvelope = assertCompatibleEnvelope(parsedPayload, {
        role: 'consumer',
        consumerVersion: this.config.runtimeConsumerVersion,
        allowMissingRuntimeVersion: false,
      });

      if (validatedEnvelope.messageType !== 'execution.job') {
        throw new Error(`unexpected messageType ${validatedEnvelope.messageType}`);
      }

      envelope = validatedEnvelope;
    } catch {
      reject();
      return;
    }

    // One job attempt is one messageId and attemptId; the control plane sends a new attempt as a new
    // message, which starts counting from 1 again.
    const attemptKey = `${envelope.messageId}\u0000${envelope.payload.attemptId}`;
    const deliveryAttempt = this.countDeliveryAttempt(message, attemptKey);
    const context: JobDeliveryContext = {
      deliveryTag: message.fields.deliveryTag,
      routingKey: message.fields.routingKey,
      redelivered: message.fields.redelivered,
      deliveryAttempt,
      subscriptionId: subscription.id,
    };

    this.pendingDeliveries.set(message.fields.deliveryTag, { message, attemptKey, subscription });

    try {
      await this.jobHandler(envelope, context);
    } catch (error) {
      if (this.pendingDeliveries.get(message.fields.deliveryTag)?.message !== message) {
        // The handler acked or nacked before it threw, and that outcome stands; or the
        // subscription ended while it ran, and the broker delivers the job again.
        return;
      }

      try {
        if (deliveryAttempt < this.config.maxDeliveryAttempts) {
          await this.nack(context, { requeue: true });
          return;
        }

        // The last attempt: tell the control plane before the job leaves for the dead-letter queue.
        await this.reportExhausted(envelope, deliveryAttempt, error);
        await this.nack(context, { requeue: false });
      } catch (settleError) {
        if (!(settleError instanceof EdgeStaleDeliveryError)) {
          throw settleError;
        }

        // The subscription ended while the handler ran; the broker delivers the job again.
      }
    }
  }

  private countDeliveryAttempt(message: ConsumeMessage, attemptKey: string): number {
    const brokerCount = message.properties.headers?.['x-delivery-count'];
    const brokerReturns = typeof brokerCount === 'number' && Number.isSafeInteger(brokerCount) && brokerCount > 0 ? brokerCount : 0;
    const counted = 1 + brokerReturns + (this.requeueCounts.get(attemptKey) ?? 0);

    // A redelivered job this process has no count for was delivered at least once before: to an
    // earlier run of the node, or on a connection that dropped.
    return message.fields.redelivered ? Math.max(2, counted) : counted;
  }

  private countRequeue(attemptKey: string): void {
    const count = (this.requeueCounts.get(attemptKey) ?? 0) + 1;
    // Delete first, so that set() moves the key to the newest end of the insertion order.
    this.requeueCounts.delete(attemptKey);
    this.requeueCounts.set(attemptKey, count);

    while (this.requeueCounts.size > maxTrackedAttempts) {
      const oldest = this.requeueCounts.keys().next().value;

      if (oldest === undefined) {
        break;
      }

      this.requeueCounts.delete(oldest);
    }
  }

  private async reportExhausted(job: ExecutionJobEnvelope, deliveryAttempt: number, error: unknown): Promise<void> {
    const recordError = (message: string) => {
      this.failureReportErrors += 1;
      this.lastFailureReportError = message;
    };

    if (!this.failurePublisher) {
      recordError(`job ${job.payload.jobId} was dead-lettered without an execution.failure: the consumer has no failure publisher`);
      return;
    }

    if (!this.config.nodeId) {
      recordError(`job ${job.payload.jobId} was dead-lettered without an execution.failure: the config has no nodeId to report it under`);
      return;
    }

    try {
      await this.failurePublisher.publishFailure(
        createExhaustedFailureEvent(this.config, this.config.nodeId, job, deliveryAttempt, error),
      );
    } catch (error) {
      // The job is dead-lettered regardless: requeueing it would start the loop this exists to end.
      recordError(`execution.failure for job ${job.payload.jobId} was not published: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  private getPendingDelivery(delivery: JobDeliveryContext): PendingDelivery {
    const { deliveryTag, subscriptionId } = delivery;

    if (deliveryTag === undefined) {
      throw new Error('deliveryTag is required');
    }

    if (subscriptionId !== undefined && !this.liveSubscriptions.has(subscriptionId)) {
      throw new EdgeStaleDeliveryError(deliveryTag);
    }

    // Delivery tags are unique among live subscriptions: an ended one has no pending entries, and
    // a later one on the same channel continues its tags. So a context without subscriptionId (one
    // the caller built) names a delivery of the current subscription.
    const pending = this.pendingDeliveries.get(deliveryTag);

    if (!pending) {
      throw new Error(`delivery ${deliveryTag} is not pending`);
    }

    return pending;
  }

  // Acks or nacks on the channel the job arrived on, never on a newer one.
  private settle(pending: PendingDelivery, send: (channel: Channel, message: ConsumeMessage) => void): void {
    const { deliveryTag } = pending.message.fields;

    try {
      send(pending.subscription.channel, pending.message);
    } catch (error) {
      // amqplib throws when the channel has closed.
      this.pendingDeliveries.delete(deliveryTag);
      throw new EdgeStaleDeliveryError(deliveryTag, { cause: error });
    }

    this.pendingDeliveries.delete(deliveryTag);
  }
}