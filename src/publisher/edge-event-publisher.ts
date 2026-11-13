import type { Channel, Options } from 'amqplib';

import type { EdgeChannelConfig } from '../config/index.js';
import { buildRoutingKey, resolveConfig, resolveUserId, validateConfig } from '../config/index.js';
import {
  assertCompatibleEnvelope,
  type JobAckMessage,
  type JobFailureEvent,
  type JobProgressEvent,
  type JobResultEvent,
} from '../contracts/index.js';
import type { EdgeChannelConnectionManager } from '../connection/connection-manager.js';
import { createHealthSnapshot, type EdgeChannelHealth } from '../health/index.js';
import { checkTopology } from '../topology/index.js';

export interface EdgeEventPublisher {
  publishAck(message: JobAckMessage): Promise<void>;
  publishProgress(message: JobProgressEvent): Promise<void>;
  publishResult(message: JobResultEvent): Promise<void>;
  publishFailure(message: JobFailureEvent): Promise<void>;
  health(): Promise<EdgeChannelHealth>;
}

type PublishableEvent = JobAckMessage | JobProgressEvent | JobResultEvent | JobFailureEvent;

export class EdgeEventPublisher implements EdgeEventPublisher {
  private readonly config;

  private readonly connectionManager;

  private publisherActive = false;

  private readonly userId: string;

  private topologyVerification?: Promise<void>;

  public constructor(config: EdgeChannelConfig, connectionManager: EdgeChannelConnectionManager) {
    const validationErrors = validateConfig(config);

    if (validationErrors.length > 0) {
      throw new Error(`invalid edge channel config: ${validationErrors.join('; ')}`);
    }

    this.config = resolveConfig(config);
    this.userId = resolveUserId(config);
    this.connectionManager = connectionManager;
    connectionManager.onReconnect(async () => {
      // Check the events exchange again, once, on the new connection.
      this.topologyVerification = undefined;
    });
  }

  /**
   * Checks, without declaring anything, that the events exchange exists. In passive mode the first
   * publish calls it, and `createEdgeChannel` calls it before returning. A success is cached until
   * the next reconnect, so publishes do not repeat the check; a failure is not cached.
   */
  public async verifyTopology(): Promise<void> {
    if (!this.topologyVerification) {
      const verification = this.checkPublisherTopology().catch((error: unknown) => {
        if (this.topologyVerification === verification) {
          this.topologyVerification = undefined;
        }

        throw error;
      });
      this.topologyVerification = verification;
    }

    await this.topologyVerification;
  }

  public async publishAck(message: JobAckMessage): Promise<void> {
    await this.publish(message);
  }

  public async publishProgress(message: JobProgressEvent): Promise<void> {
    await this.publish(message);
  }

  public async publishResult(message: JobResultEvent): Promise<void> {
    await this.publish(message);
  }

  public async publishFailure(message: JobFailureEvent): Promise<void> {
    await this.publish(message);
  }

  public async health(): Promise<EdgeChannelHealth> {
    const connectionHealth = await this.connectionManager.health();

    return createHealthSnapshot({
      connected: connectionHealth.connected,
      publisherActive: this.publisherActive,
      consumerActive: connectionHealth.consumerActive,
    });
  }

  private async publish(message: PublishableEvent): Promise<void> {
    const validatedMessage = assertCompatibleEnvelope(message, {
      role: 'producer',
      producerVersion: this.config.runtimeProducerVersion,
      allowMissingRuntimeVersion: false,
    });

    if (validatedMessage.messageType === 'execution.job') {
      throw new Error('publisher only accepts execution events');
    }

    if (this.config.topologyMode === 'passive') {
      await this.verifyTopology();
    }

    const channel = await this.connectionManager.getPublisherChannel();

    if (this.config.topologyMode === 'declare') {
      await channel.assertExchange(this.config.eventsExchange, 'topic', { durable: true });
    }

    const routingKey = this.resolveRoutingKey(validatedMessage);
    const content = Buffer.from(JSON.stringify(validatedMessage), 'utf8');
    const publishOptions: Options.Publish = {
      persistent: true,
      contentType: 'application/json',
      type: validatedMessage.messageType,
      messageId: validatedMessage.messageId,
      timestamp: Date.parse(validatedMessage.timestamp),
      // The node's identity. The broker refuses a user_id that differs from the connection's user,
      // which is what lets the control plane trust it; see "Node identity" in docs/PROTOCOL.md.
      userId: this.userId,
    };

    const accepted = channel.publish(this.config.eventsExchange, routingKey, content, publishOptions);

    if (!accepted) {
      await this.waitForDrain(channel, this.config.backpressureTimeoutMs);
    }

    this.publisherActive = true;
  }

  private async checkPublisherTopology(): Promise<void> {
    const channel = await this.connectionManager.openTopologyChannel();
    await checkTopology(channel, [{ kind: 'exchange', name: this.config.eventsExchange }]);
  }

  private resolveRoutingKey(message: PublishableEvent): string {
    return buildRoutingKey(message.messageType, {
      siteId: message.payload.siteId ?? this.config.siteId,
      nodeId: message.payload.nodeId,
    });
  }

  private async waitForDrain(channel: Channel, timeoutMs: number): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        channel.off('drain', onDrain);
        reject(new Error(`publisher backpressure timeout after ${timeoutMs}ms`));
      }, timeoutMs);

      const onDrain = () => {
        clearTimeout(timeout);
        channel.off('drain', onDrain);
        resolve();
      };

      channel.on('drain', onDrain);
    });
  }
}