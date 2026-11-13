import type { Channel, ConsumeMessage, Options } from 'amqplib';

import type { EdgeChannelConfig } from '../config/index.js';
import { buildConsumerQueueName, buildJobBindingKeys, resolveConfig, validateConfig } from '../config/index.js';
import { assertCompatibleEnvelope, type ExecutionJobEnvelope } from '../contracts/index.js';
import type { EdgeChannelConnectionManager } from '../connection/connection-manager.js';
import { createHealthSnapshot, type EdgeChannelHealth } from '../health/index.js';
import { checkTopology, EdgeTopologyError } from '../topology/index.js';

export interface JobDeliveryContext {
  deliveryTag?: number;
  routingKey?: string;
  redelivered?: boolean;
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

export class EdgeJobConsumer implements EdgeJobConsumer {
  private readonly config;

  private readonly connectionManager;

  private jobHandler: EdgeJobHandler = async () => undefined;

  private readonly pendingDeliveries = new Map<number, ConsumeMessage>();

  private consumerActive = false;

  private consumerTag?: string;

  private topologyVerification?: Promise<void>;

  private topologyError?: EdgeTopologyError;

  public constructor(config: EdgeChannelConfig, connectionManager: EdgeChannelConnectionManager) {
    const validationErrors = validateConfig(config);

    if (validationErrors.length > 0) {
      throw new Error(`invalid edge channel config: ${validationErrors.join('; ')}`);
    }

    this.config = resolveConfig(config);
    this.connectionManager = connectionManager;
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
    const message = this.getPendingMessage(delivery.deliveryTag);
    const channel = await this.connectionManager.getConsumerChannel();
    channel.ack(message);
    this.pendingDeliveries.delete(message.fields.deliveryTag);
  }

  public async nack(delivery: JobDeliveryContext, options: { requeue?: boolean; reason?: string } = {}): Promise<void> {
    const message = this.getPendingMessage(delivery.deliveryTag);
    const channel = await this.connectionManager.getConsumerChannel();
    channel.nack(message, false, options.requeue ?? false);
    this.pendingDeliveries.delete(message.fields.deliveryTag);
  }

  public async health(): Promise<EdgeChannelHealth> {
    const connectionHealth = await this.connectionManager.health();

    return createHealthSnapshot({
      connected: connectionHealth.connected,
      consumerActive: this.consumerActive,
      publisherActive: connectionHealth.publisherActive,
      details: {
        pendingDeliveries: this.pendingDeliveries.size,
        ...(this.topologyError ? { topologyError: this.topologyError.message } : {}),
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

    const context: JobDeliveryContext = {
      deliveryTag: message.fields.deliveryTag,
      routingKey: message.fields.routingKey,
      redelivered: message.fields.redelivered,
    };

    this.pendingDeliveries.set(message.fields.deliveryTag, message);

    try {
      await this.jobHandler(envelope, context);
    } catch (error) {
      if (this.pendingDeliveries.has(message.fields.deliveryTag)) {
        await this.nack(context, {
          requeue: true,
          reason: error instanceof Error ? error.message : 'job handler failed',
        });
      }
    }
  }

  private getPendingMessage(deliveryTag?: number): ConsumeMessage {
    if (deliveryTag === undefined) {
      throw new Error('deliveryTag is required');
    }

    const message = this.pendingDeliveries.get(deliveryTag);

    if (!message) {
      throw new Error(`delivery ${deliveryTag} is not pending`);
    }

    return message;
  }
}