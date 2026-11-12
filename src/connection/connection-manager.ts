import { connect } from 'amqplib';
import type { Channel, ChannelModel, Options } from 'amqplib';

import type { EdgeChannelConfig } from '../config/index.js';
import { resolveConfig } from '../config/index.js';
import { createHealthSnapshot, type EdgeChannelHealth } from '../health/index.js';

export class EdgeChannelConnectionManager {
  private readonly config;

  private connection?: ChannelModel;

  private consumerChannel?: Channel;

  private publisherChannel?: Channel;

  private connected = false;

  private closedByUser = false;

  private reconnectAttempts = 0;

  private reconnectingPromise?: Promise<void>;

  private readonly reconnectSubscribers = new Set<() => Promise<void>>();

  public constructor(config: EdgeChannelConfig) {
    this.config = resolveConfig(config);
  }

  public async connect(): Promise<void> {
    if (this.connected && this.connection && this.consumerChannel && this.publisherChannel) {
      return;
    }

    this.closedByUser = false;

    if (this.reconnectingPromise) {
      await this.reconnectingPromise;
      if (this.connected) {
        return;
      }
    }

    await this.establishConnection();
  }

  public onReconnect(handler: () => Promise<void>): () => void {
    this.reconnectSubscribers.add(handler);
    return () => {
      this.reconnectSubscribers.delete(handler);
    };
  }

  public async getConsumerChannel(): Promise<Channel> {
    await this.connect();

    if (!this.consumerChannel) {
      throw new Error('consumer channel is not available');
    }

    return this.consumerChannel;
  }

  public async getPublisherChannel(): Promise<Channel> {
    await this.connect();

    if (!this.publisherChannel) {
      throw new Error('publisher channel is not available');
    }

    return this.publisherChannel;
  }

  /**
   * Opens a short-lived channel for passive topology checks; the caller closes it.
   *
   * A failed check makes the broker close the channel it ran on. The 'error' listener keeps that
   * failure on this channel: amqplib treats an unhandled channel 'error' as a frame error and
   * closes the whole connection, which would start the reconnect loop over a missing exchange.
   */
  public async openTopologyChannel(): Promise<Channel> {
    await this.connect();

    if (!this.connection) {
      throw new Error('connection is not available');
    }

    const channel = await this.connection.createChannel();
    channel.on('error', () => undefined);
    return channel;
  }

  public async close(): Promise<void> {
    this.closedByUser = true;
    this.connected = false;

    const consumerChannel = this.consumerChannel;
    const publisherChannel = this.publisherChannel;
    const connection = this.connection;

    this.consumerChannel = undefined;
    this.publisherChannel = undefined;
    this.connection = undefined;
    this.reconnectingPromise = undefined;

    if (consumerChannel) {
      await consumerChannel.close().catch(() => undefined);
    }

    if (publisherChannel) {
      await publisherChannel.close().catch(() => undefined);
    }

    if (connection) {
      await connection.close().catch(() => undefined);
    }
  }

  public async health(): Promise<EdgeChannelHealth> {
    return createHealthSnapshot({
      connected: this.connected,
      details: {
        connectionName: this.config.connectionName ?? 'amqp-mcp-edge',
        reconnectEnabled: this.config.reconnectEnabled,
        reconnecting: Boolean(this.reconnectingPromise),
        reconnectAttempts: this.reconnectAttempts,
      },
    });
  }

  private async establishConnection(): Promise<void> {
    const connection = await connect(this.config.amqpUrl, {
      clientProperties: {
        connection_name: this.config.connectionName ?? 'amqp-mcp-edge',
      },
    } as Options.Connect);

    connection.on('error', () => {
      this.connected = false;
    });

    connection.on('close', () => {
      this.connected = false;
      this.consumerChannel = undefined;
      this.publisherChannel = undefined;
      if (!this.closedByUser) {
        this.scheduleReconnect();
      }
    });

    this.connection = connection;
    this.consumerChannel = await connection.createChannel();
    this.publisherChannel = await connection.createChannel();
    this.connected = true;
    this.reconnectAttempts = 0;
  }

  private scheduleReconnect(): void {
    if (!this.config.reconnectEnabled || this.closedByUser || this.reconnectingPromise) {
      return;
    }

    this.reconnectingPromise = this.reconnectLoop();
  }

  private async reconnectLoop(): Promise<void> {
    try {
      while (!this.closedByUser && !this.connected) {
        const delayMs = Math.min(
          this.config.reconnectDelayMs * Math.max(1, 2 ** this.reconnectAttempts),
          this.config.reconnectMaxDelayMs,
        );

        await this.sleep(delayMs);

        if (this.closedByUser) {
          return;
        }

        try {
          await this.establishConnection();
          await this.notifyReconnectSubscribers();
          return;
        } catch {
          this.reconnectAttempts += 1;
        }
      }
    } finally {
      this.reconnectingPromise = undefined;
    }
  }

  private async notifyReconnectSubscribers(): Promise<void> {
    // Every subscriber runs, even after one fails, so that one component's failure does not leave
    // another holding state from the previous connection.
    let firstError: unknown;
    let failed = false;

    for (const subscriber of this.reconnectSubscribers) {
      try {
        await subscriber();
      } catch (error) {
        if (!failed) {
          firstError = error;
          failed = true;
        }
      }
    }

    if (failed) {
      throw firstError;
    }
  }

  private async sleep(delayMs: number): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }
}