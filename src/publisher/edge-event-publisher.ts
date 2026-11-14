import type { ConfirmChannel, Options } from 'amqplib';

import type { EdgeChannelConfig } from '../config/index.js';
import { buildRoutingKey, resolveConfig, resolveUserId, validateConfig } from '../config/index.js';
import {
  assertCompatibleEnvelope,
  type JobAckMessage,
  type JobFailureEvent,
  type JobProgressEvent,
  type JobResultEvent,
  type NodeHeartbeatEvent,
  type NodeHelloEvent,
} from '../contracts/index.js';
import type { EdgeChannelConnectionManager } from '../connection/connection-manager.js';
import { createHealthSnapshot, type EdgeChannelHealth } from '../health/index.js';
import { checkTopology } from '../topology/index.js';

export interface EdgeEventPublisher {
  publishAck(message: JobAckMessage): Promise<void>;
  publishProgress(message: JobProgressEvent): Promise<void>;
  publishResult(message: JobResultEvent): Promise<void>;
  publishFailure(message: JobFailureEvent): Promise<void>;
  publishNodeHello(message: NodeHelloEvent): Promise<void>;
  publishNodeHeartbeat(message: NodeHeartbeatEvent): Promise<void>;
  health(): Promise<EdgeChannelHealth>;
}

/**
 * The broker did not confirm an event: it nacked it, the channel closed before the confirm, or
 * no confirm arrived within `confirmTimeoutMs`. The event may or may not have reached its queues,
 * so treat it as not delivered: do not ack the job it reports on.
 */
export class EdgePublishError extends Error {
  public readonly reason: 'nacked' | 'channel-closed' | 'timeout';

  public readonly messageType: string;

  public readonly messageId: string;

  public constructor(
    message: string,
    details: { reason: 'nacked' | 'channel-closed' | 'timeout'; messageType: string; messageId: string; cause?: unknown },
  ) {
    super(message, { cause: details.cause });
    this.name = 'EdgePublishError';
    this.reason = details.reason;
    this.messageType = details.messageType;
    this.messageId = details.messageId;
  }
}

type PublishableEvent =
  | JobAckMessage
  | JobProgressEvent
  | JobResultEvent
  | JobFailureEvent
  | NodeHelloEvent
  | NodeHeartbeatEvent;

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

  /** Publishes `node.hello`, which a node sends after each connection; see docs/PROTOCOL.md. */
  public async publishNodeHello(message: NodeHelloEvent): Promise<void> {
    await this.publish(message);
  }

  /** Publishes `node.heartbeat`, which a node sends every `heartbeatIntervalMs` while connected. */
  public async publishNodeHeartbeat(message: NodeHeartbeatEvent): Promise<void> {
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

    await this.publishConfirmed(channel, routingKey, content, publishOptions, validatedMessage);
    this.publisherActive = true;
  }

  // Resolves once the broker acks the publish; rejects with an EdgePublishError if it nacks it, if
  // the channel closes first, or if no confirm arrives in time. Under backpressure it also waits
  // for the channel to drain, as before.
  private async publishConfirmed(
    channel: ConfirmChannel,
    routingKey: string,
    content: Buffer,
    options: Options.Publish,
    message: PublishableEvent,
  ): Promise<void> {
    const { messageType, messageId } = message;
    const subject = `${messageType} ${messageId}`;
    let accepted = true;

    const confirmed = new Promise<void>((resolve, reject) => {
      const timeoutMs = this.config.confirmTimeoutMs;
      const timer = setTimeout(() => {
        reject(
          new EdgePublishError(`the broker did not confirm ${subject} within ${timeoutMs}ms; treat it as not delivered`, {
            reason: 'timeout',
            messageType,
            messageId,
          }),
        );
      }, timeoutMs);

      const onConfirm = (error: unknown) => {
        clearTimeout(timer);

        if (error === null || error === undefined) {
          resolve();
          return;
        }

        const closed = error instanceof Error && /closed/i.test(error.message);
        reject(
          new EdgePublishError(
            closed
              ? `the channel closed before the broker confirmed ${subject}; treat it as not delivered`
              : `the broker nacked ${subject}; treat it as not delivered`,
            { reason: closed ? 'channel-closed' : 'nacked', messageType, messageId, cause: error },
          ),
        );
      };

      try {
        accepted = channel.publish(this.config.eventsExchange, routingKey, content, options, onConfirm);
      } catch (error) {
        // amqplib throws when the channel is already closed.
        clearTimeout(timer);
        reject(
          new EdgePublishError(`could not publish ${subject}: ${error instanceof Error ? error.message : String(error)}`, {
            reason: 'channel-closed',
            messageType,
            messageId,
            cause: error,
          }),
        );
      }
    });

    const waits = [confirmed];

    if (!accepted) {
      waits.push(this.waitForDrain(channel, this.config.backpressureTimeoutMs));
    }

    // allSettled, so a second failure does not surface later as an unhandled rejection.
    const outcomes = await Promise.allSettled(waits);
    const failed = outcomes.find((outcome): outcome is PromiseRejectedResult => outcome.status === 'rejected');

    if (failed) {
      throw failed.reason;
    }
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

  private async waitForDrain(channel: ConfirmChannel, timeoutMs: number): Promise<void> {
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