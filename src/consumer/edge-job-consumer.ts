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
}

/** What the consumer needs to report a job it gives up on. `createEdgeChannel` passes its publisher. */
export interface EdgeJobConsumerOptions {
  failurePublisher?: { publishFailure(message: JobFailureEvent): Promise<void> };
}

interface PendingDelivery {
  message: ConsumeMessage;
  attemptKey: string;
}

// Requeue counts are kept per job attempt for this many attempts at most; the oldest is forgotten
// first. A forgotten count only means a later delivery is counted from the broker's flags again.
const maxTrackedAttempts = 10_000;

// Room for the handler's error in the failure event, which a control plane may show to people.
const maxReportedErrorLength = 1000;

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
  config: Pick<ResolvedEdgeChannelConfig, 'maxDeliveryAttempts' | 'producer' | 'siteId'>,
  nodeId: string,
  job: ExecutionJobEnvelope,
  deliveryAttempt: number,
  reason: string,
): JobFailureEvent {
  const { maxDeliveryAttempts } = config;
  const handlerError = reason.length > maxReportedErrorLength ? `${reason.slice(0, maxReportedErrorLength)}…` : reason;
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

  private readonly pendingDeliveries = new Map<number, PendingDelivery>();

  private readonly requeueCounts = new Map<string, number>();

  private readonly failurePublisher?: EdgeJobConsumerOptions['failurePublisher'];

  private failureReportErrors = 0;

  private lastFailureReportError?: string;

  private consumerActive = false;

  private consumerTag?: string;

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
    connectionManager.onReconnect(async () => {
      // A new connection may reach a broker whose topology changed while this node was away, so a
      // passive-mode consumer checks its queue again, once, before it subscribes.
      this.topologyVerification = undefined;

      if (!this.consumerActive) {
        return;
      }

      try {
        await this.initializeConsumer();
      } catch (error) {
        if (!(error instanceof EdgeTopologyError)) {
          throw error;
        }

        // The check failed on its own channel, so the connection is still up and nothing else
        // reconnects. Stop consuming instead of retrying: restoring the topology is the control
        // plane's job, not this node's. health() reports the reason.
        this.consumerActive = false;
        this.consumerTag = undefined;
        this.pendingDeliveries.clear();
      }
    });
  }

  public onJob(handler: EdgeJobHandler): void {
    this.jobHandler = handler;
  }

  public async start(): Promise<void> {
    if (this.consumerActive) {
      return;
    }

    await this.initializeConsumer();
    this.consumerActive = true;
  }

  public async stop(): Promise<void> {
    if (!this.consumerActive) {
      return;
    }

    const channel = await this.connectionManager.getConsumerChannel();

    if (this.consumerTag) {
      await channel.cancel(this.consumerTag).catch(() => undefined);
      this.consumerTag = undefined;
    }

    this.consumerActive = false;
    this.pendingDeliveries.clear();
  }

  public async ack(delivery: JobDeliveryContext): Promise<void> {
    const pending = this.getPendingDelivery(delivery.deliveryTag);
    const channel = await this.connectionManager.getConsumerChannel();
    channel.ack(pending.message);
    this.pendingDeliveries.delete(pending.message.fields.deliveryTag);
    this.requeueCounts.delete(pending.attemptKey);
  }

  /**
   * Rejects a job. With `requeue: true` the broker delivers it again, and the next delivery's
   * `deliveryAttempt` counts this requeue; the consumer does not limit requeues a handler asks for,
   * so a handler that requeues should check `deliveryAttempt` itself. Without it (the default) the
   * job is dead-lettered.
   */
  public async nack(delivery: JobDeliveryContext, options: { requeue?: boolean; reason?: string } = {}): Promise<void> {
    const pending = this.getPendingDelivery(delivery.deliveryTag);
    const requeue = options.requeue ?? false;
    const channel = await this.connectionManager.getConsumerChannel();
    channel.nack(pending.message, false, requeue);
    this.pendingDeliveries.delete(pending.message.fields.deliveryTag);

    if (requeue) {
      this.countRequeue(pending.attemptKey);
    } else {
      this.requeueCounts.delete(pending.attemptKey);
    }
  }

  public async health(): Promise<EdgeChannelHealth> {
    const connectionHealth = await this.connectionManager.health();

    return createHealthSnapshot({
      connected: connectionHealth.connected,
      consumerActive: this.consumerActive,
      publisherActive: connectionHealth.publisherActive,
      details: {
        pendingDeliveries: this.pendingDeliveries.size,
        maxDeliveryAttempts: this.config.maxDeliveryAttempts,
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

  private async initializeConsumer(): Promise<void> {
    const existingTag = this.consumerTag;
    const queueName = buildConsumerQueueName(this.config);

    if (this.config.topologyMode === 'passive') {
      await this.verifyTopology();
    }

    const channel = await this.connectionManager.getConsumerChannel();

    if (this.config.topologyMode === 'declare') {
      await this.declareTopology(channel, queueName);
    }

    await channel.prefetch(this.config.prefetchCount);

    const consumeResult = await channel.consume(
      queueName,
      (message) => {
        void this.handleMessage(channel, message);
      },
      {
        consumerTag: this.config.consumerTag,
      },
    );

    if (existingTag) {
      await channel.cancel(existingTag).catch(() => undefined);
    }

    this.consumerTag = consumeResult.consumerTag;
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

  private async handleMessage(channel: Channel, message: ConsumeMessage | null): Promise<void> {
    if (!message) {
      return;
    }

    if (message.content.byteLength > this.config.maxMessageSizeBytes) {
      channel.nack(message, false, false);
      return;
    }

    let parsedPayload: unknown;

    try {
      parsedPayload = JSON.parse(message.content.toString('utf8'));
    } catch {
      channel.nack(message, false, false);
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
      channel.nack(message, false, false);
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
    };

    this.pendingDeliveries.set(message.fields.deliveryTag, { message, attemptKey });

    try {
      await this.jobHandler(envelope, context);
    } catch (error) {
      if (!this.pendingDeliveries.has(message.fields.deliveryTag)) {
        // The handler acked or nacked before it threw; that outcome stands.
        return;
      }

      const reason = error instanceof Error ? error.message : String(error);

      if (deliveryAttempt < this.config.maxDeliveryAttempts) {
        await this.nack(context, { requeue: true, reason });
        return;
      }

      // The last attempt: tell the control plane before the job leaves for the dead-letter queue.
      await this.reportExhausted(envelope, deliveryAttempt, reason);
      await this.nack(context, { requeue: false, reason });
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

  private async reportExhausted(job: ExecutionJobEnvelope, deliveryAttempt: number, reason: string): Promise<void> {
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
        createExhaustedFailureEvent(this.config, this.config.nodeId, job, deliveryAttempt, reason),
      );
    } catch (error) {
      // The job is dead-lettered regardless: requeueing it would start the loop this exists to end.
      recordError(`execution.failure for job ${job.payload.jobId} was not published: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  private getPendingDelivery(deliveryTag?: number): PendingDelivery {
    if (deliveryTag === undefined) {
      throw new Error('deliveryTag is required');
    }

    const pending = this.pendingDeliveries.get(deliveryTag);

    if (!pending) {
      throw new Error(`delivery ${deliveryTag} is not pending`);
    }

    return pending;
  }
}