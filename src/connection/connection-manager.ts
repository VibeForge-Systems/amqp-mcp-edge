import { connect } from 'amqplib';
import type { Channel, ChannelModel, ConfirmChannel, Options } from 'amqplib';

import type { EdgeChannelConfig } from '../config/index.js';
import { resolveConfig } from '../config/index.js';
import { createHealthSnapshot, type EdgeChannelHealth } from '../health/index.js';

/** The two long-lived channels of a connection: the consumer's and the publisher's. */
export type EdgeChannelKind = 'consumer' | 'publisher';

function describeError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export class EdgeChannelConnectionManager {
  private readonly config;

  private connection?: ChannelModel;

  private consumerChannel?: Channel;

  // A confirm channel: the broker acks or nacks every publish, and the publisher waits for it.
  private publisherChannel?: ConfirmChannel;

  private connected = false;

  private closedByUser = false;

  private reconnectAttempts = 0;

  private reconnectingPromise?: Promise<void>;

  private readonly reconnectSubscribers = new Set<() => Promise<void>>();

  private readonly channelSubscribers: Record<EdgeChannelKind, Set<() => Promise<void>>> = {
    consumer: new Set(),
    publisher: new Set(),
  };

  // A channel being opened in place of one that closed while the connection stayed up.
  private readonly channelOpening: Partial<Record<EdgeChannelKind, Promise<boolean>>> = {};

  private readonly channelReopens: Record<EdgeChannelKind, number> = { consumer: 0, publisher: 0 };

  private readonly reopenStreak: Record<EdgeChannelKind, number> = { consumer: 0, publisher: 0 };

  private readonly lastChannelCloseAt: Partial<Record<EdgeChannelKind, number>> = {};

  private lastChannelError?: string;

  // The channel each component last reported itself active on. health() reports a component
  // active only while that channel is still the live one, so a closed channel needs no report.
  private readonly activeChannels: Partial<Record<EdgeChannelKind, Channel>> = {};

  public constructor(config: EdgeChannelConfig) {
    this.config = resolveConfig(config);
  }

  public async connect(): Promise<void> {
    // A channel may be missing while it is reopened; the connection is still up, so do not open
    // another one.
    if (this.connected && this.connection) {
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

  /** Runs `handler` after each reconnect, once both channels are open on the new connection. */
  public onReconnect(handler: () => Promise<void>): () => void {
    this.reconnectSubscribers.add(handler);
    return () => {
      this.reconnectSubscribers.delete(handler);
    };
  }

  /**
   * Runs `handler` after the broker closed the channel of this kind while the connection stayed up
   * (for example after a precondition failure), once the new channel is open. With
   * `reconnectEnabled: false` channels are not reopened and the handler is never called.
   */
  public onChannelReopened(kind: EdgeChannelKind, handler: () => Promise<void>): () => void {
    this.channelSubscribers[kind].add(handler);
    return () => {
      this.channelSubscribers[kind].delete(handler);
    };
  }

  public async getConsumerChannel(): Promise<Channel> {
    await this.ensureChannel('consumer');

    if (!this.consumerChannel) {
      throw new Error('consumer channel is not available');
    }

    return this.consumerChannel;
  }

  public async getPublisherChannel(): Promise<ConfirmChannel> {
    await this.ensureChannel('publisher');

    if (!this.publisherChannel) {
      throw new Error('publisher channel is not available');
    }

    return this.publisherChannel;
  }

  /**
   * A component reports that it works on `channel`: the consumer once it is subscribed, the
   * publisher once a publish is confirmed. Until it reports again, health() shows it active only
   * while `channel` is the open channel of its kind.
   */
  public reportActive(kind: EdgeChannelKind, channel: Channel): void {
    this.activeChannels[kind] = channel;
  }

  /** A component reports that it stopped working: the consumer stopped, or a publish failed. */
  public reportInactive(kind: EdgeChannelKind): void {
    this.activeChannels[kind] = undefined;
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
    this.activeChannels.consumer = undefined;
    this.activeChannels.publisher = undefined;

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
      consumerActive: this.isActive('consumer'),
      publisherActive: this.isActive('publisher'),
      details: {
        connectionName: this.config.connectionName ?? 'amqp-mcp-edge',
        reconnectEnabled: this.config.reconnectEnabled,
        reconnecting: Boolean(this.reconnectingPromise),
        reconnectAttempts: this.reconnectAttempts,
        consumerChannelReopens: this.channelReopens.consumer,
        publisherChannelReopens: this.channelReopens.publisher,
        ...(this.lastChannelError ? { lastChannelError: this.lastChannelError } : {}),
      },
    });
  }

  private isActive(kind: EdgeChannelKind): boolean {
    const channel = this.activeChannels[kind];
    return this.connected && channel !== undefined && channel === this.channelOf(kind);
  }

  private channelOf(kind: EdgeChannelKind): Channel | undefined {
    return kind === 'consumer' ? this.consumerChannel : this.publisherChannel;
  }

  private setChannel(kind: EdgeChannelKind, channel: Channel | undefined): void {
    if (kind === 'consumer') {
      this.consumerChannel = channel;
    } else {
      this.publisherChannel = channel as ConfirmChannel | undefined;
    }
  }

  private async ensureChannel(kind: EdgeChannelKind): Promise<void> {
    await this.connect();
    await this.channelOpening[kind];

    // With reconnectEnabled: false a closed channel is not reopened in the background; open it
    // when it is next needed, as connect() does for the connection.
    if (!this.channelOf(kind) && this.connection && this.connected && !this.closedByUser) {
      await this.openChannel(kind, this.connection, 0);
    }
  }

  private watchChannel(kind: EdgeChannelKind, connection: ChannelModel, channel: Channel): void {
    // Without an 'error' listener amqplib treats a channel the broker closed as a frame error and
    // closes the whole connection.
    channel.on('error', (error: unknown) => {
      this.lastChannelError = `${kind} channel: ${describeError(error)}`;
    });

    channel.once('close', () => {
      if (this.channelOf(kind) !== channel) {
        // Closed by close(), or already replaced.
        return;
      }

      this.setChannel(kind, undefined);

      if (this.closedByUser || !this.config.reconnectEnabled) {
        return;
      }

      void this.reopenChannel(kind, connection);
    });
  }

  // A channel that closes again soon after it was reopened waits like the reconnect loop does, so a
  // channel the broker keeps closing does not spin.
  private nextReopenDelay(kind: EdgeChannelKind): number {
    const now = Date.now();
    const previous = this.lastChannelCloseAt[kind];
    this.lastChannelCloseAt[kind] = now;
    this.reopenStreak[kind] =
      previous !== undefined && now - previous < this.config.reconnectMaxDelayMs ? this.reopenStreak[kind] + 1 : 0;

    if (this.reopenStreak[kind] === 0) {
      return 0;
    }

    return Math.min(this.config.reconnectDelayMs * 2 ** (this.reopenStreak[kind] - 1), this.config.reconnectMaxDelayMs);
  }

  private async reopenChannel(kind: EdgeChannelKind, connection: ChannelModel): Promise<void> {
    // Wait a turn first: amqplib closes every channel before it emits the connection's 'close', so
    // a channel that closed with its connection is left to the reconnect loop.
    await this.sleep(0);

    if (this.closedByUser || this.connection !== connection || !this.connected) {
      return;
    }

    if (!(await this.openChannel(kind, connection, this.nextReopenDelay(kind)))) {
      return;
    }

    this.channelReopens[kind] += 1;

    // Every subscriber runs, even after one fails.
    for (const subscriber of this.channelSubscribers[kind]) {
      try {
        await subscriber();
      } catch (error) {
        this.lastChannelError = `${kind} channel reopened, but restoring its use failed: ${describeError(error)}`;
      }
    }
  }

  // Opens a channel of this kind on `connection` unless one is open; resolves true when one is.
  // Concurrent calls share one attempt.
  private openChannel(kind: EdgeChannelKind, connection: ChannelModel, delayMs: number): Promise<boolean> {
    const pending = this.channelOpening[kind];

    if (pending) {
      return pending;
    }

    const opening = (async (): Promise<boolean> => {
      if (delayMs > 0) {
        await this.sleep(delayMs);
      }

      if (this.closedByUser || this.connection !== connection || !this.connected) {
        return false;
      }

      if (this.channelOf(kind)) {
        return true;
      }

      try {
        const channel = kind === 'consumer' ? await connection.createChannel() : await connection.createConfirmChannel();

        if (this.closedByUser || this.connection !== connection) {
          await channel.close().catch(() => undefined);
          return false;
        }

        this.watchChannel(kind, connection, channel);
        this.setChannel(kind, channel);
        return true;
      } catch (error) {
        this.lastChannelError = `could not open a ${kind} channel: ${describeError(error)}`;

        // A connection that cannot open a channel is closed, so the reconnect loop starts over.
        if (this.connection === connection && !this.closedByUser) {
          await connection.close().catch(() => undefined);
        }

        return false;
      }
    })().finally(() => {
      if (this.channelOpening[kind] === opening) {
        this.channelOpening[kind] = undefined;
      }
    });

    this.channelOpening[kind] = opening;
    return opening;
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
      if (this.connection !== connection) {
        return;
      }

      this.connected = false;
      this.consumerChannel = undefined;
      this.publisherChannel = undefined;
      if (!this.closedByUser) {
        this.scheduleReconnect();
      }
    });

    this.connection = connection;
    const consumerChannel = await connection.createChannel();
    this.watchChannel('consumer', connection, consumerChannel);
    this.consumerChannel = consumerChannel;
    const publisherChannel = await connection.createConfirmChannel();
    this.watchChannel('publisher', connection, publisherChannel);
    this.publisherChannel = publisherChannel;
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