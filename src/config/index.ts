import type { Options } from 'amqplib';

import type { EdgeMessageType } from '../contracts/index.js';
import type { TargetSelector } from '../contracts/jobs.js';

export interface ExchangeConfig {
  jobsExchange: string;
  eventsExchange: string;
  deadLetterExchange: string;
}

export interface QueueNameInput {
  siteId?: string;
  nodeId?: string;
  nodeGroup?: string;
  explicitName?: string;
}

export interface QueueNamingStrategy {
  buildConsumerQueueName(input: QueueNameInput): string;
}

export interface RoutingKeySelector {
  siteId?: string;
  nodeId?: string;
  nodeGroup?: string;
  capabilityKind?: string;
  capabilityName?: string;
}

export interface RoutingKeyStrategy {
  build(messageType: EdgeMessageType, selector: RoutingKeySelector): string;
}

/**
 * How the consumer and publisher treat broker topology (exchanges, queues and bindings).
 *
 * - `declare`: declare the exchanges, the consumer queue and its bindings on startup, and the events
 *   exchange before each publish. Needs the configure permission on all of them. Convenient for
 *   local development; the 0.x default.
 * - `passive`: never declare or bind anything. Check with passive declares, once at startup and
 *   once after each reconnect, that the consumer queue and the events exchange exist, and fail with
 *   an `EdgeTopologyError` if they do not. The node's broker user needs no configure permission.
 *   The production setting.
 */
export type TopologyMode = 'declare' | 'passive';

export const topologyModes: readonly TopologyMode[] = ['declare', 'passive'];

/** The username amqplib authenticates with when `amqpUrl` carries no credentials. */
export const defaultAmqpUsername = 'guest';

export interface EdgeChannelConfig {
  amqpUrl: string;
  /**
   * The broker username this node authenticates as. The publisher sets it as the AMQP `user_id`
   * property on every event, and the broker refuses an event whose `user_id` differs from the
   * connection's user. Defaults to the URL-decoded username in `amqpUrl`, or `guest` when the URL
   * has no credentials (amqplib's default).
   */
  userId?: string;
  /** Defaults to `declare`. Use `passive` in production; see {@link TopologyMode}. */
  topologyMode?: TopologyMode;
  jobsExchange?: string;
  eventsExchange?: string;
  deadLetterExchange?: string;
  siteId?: string;
  nodeId?: string;
  nodeGroup?: string;
  consumerQueueName?: string;
  consumerQueueOptions?: Options.AssertQueue;
  consumerTag?: string;
  prefetchCount?: number;
  runtimeConsumerVersion?: string;
  runtimeProducerVersion?: string;
  connectionName?: string;
  maxMessageSizeBytes?: number;
  reconnectEnabled?: boolean;
  reconnectDelayMs?: number;
  reconnectMaxDelayMs?: number;
  backpressureTimeoutMs?: number;
  queueNamingStrategy?: QueueNamingStrategy;
  routingKeyStrategy?: RoutingKeyStrategy;
}

export interface ResolvedEdgeChannelConfig extends EdgeChannelConfig {
  topologyMode: TopologyMode;
  jobsExchange: string;
  eventsExchange: string;
  deadLetterExchange: string;
  prefetchCount: number;
  maxMessageSizeBytes: number;
  reconnectEnabled: boolean;
  reconnectDelayMs: number;
  reconnectMaxDelayMs: number;
  backpressureTimeoutMs: number;
}

export const defaultExchangeConfig: ExchangeConfig = {
  jobsExchange: 'mcp.edge.jobs',
  eventsExchange: 'mcp.edge.events',
  deadLetterExchange: 'mcp.edge.dlx',
};

function sanitizeRoutingSegment(value: string | undefined): string {
  if (!value) {
    return 'any';
  }

  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9-_.]/g, '-')
    .replace(/\.+/g, '-');
}

export const defaultQueueNamingStrategy: QueueNamingStrategy = {
  buildConsumerQueueName(input: QueueNameInput): string {
    if (input.explicitName) {
      return input.explicitName;
    }

    const siteId = sanitizeRoutingSegment(input.siteId);
    const worker = sanitizeRoutingSegment(input.nodeId ?? input.nodeGroup);
    return `mcp.edge.consumer.${siteId}.${worker}`;
  },
};

export const defaultRoutingKeyStrategy: RoutingKeyStrategy = {
  build(messageType: EdgeMessageType, selector: RoutingKeySelector): string {
    const siteId = sanitizeRoutingSegment(selector.siteId);
    const nodeSegment = sanitizeRoutingSegment(selector.nodeId ?? selector.nodeGroup);

    switch (messageType) {
      case 'execution.job': {
        const capabilityKind = sanitizeRoutingSegment(selector.capabilityKind);
        const capabilityName = sanitizeRoutingSegment(selector.capabilityName);
        return `edge.job.${siteId}.${nodeSegment}.${capabilityKind}.${capabilityName}`;
      }
      case 'execution.ack':
        return `edge.event.ack.${siteId}.${nodeSegment}`;
      case 'execution.progress':
        return `edge.event.progress.${siteId}.${nodeSegment}`;
      case 'execution.result':
        return `edge.event.result.${siteId}.${nodeSegment}`;
      case 'execution.failure':
        return `edge.event.failure.${siteId}.${nodeSegment}`;
      default:
        return `edge.unknown.${siteId}.${nodeSegment}`;
    }
  },
};

export function buildRoutingKey(messageType: EdgeMessageType, selector: RoutingKeySelector): string {
  return defaultRoutingKeyStrategy.build(messageType, selector);
}

export function buildJobRoutingKey(target: TargetSelector, capability: { kind: string; name: string }): string {
  return buildRoutingKey('execution.job', {
    siteId: target.siteId,
    nodeId: target.nodeId,
    nodeGroup: target.nodeGroup,
    capabilityKind: capability.kind,
    capabilityName: capability.name,
  });
}

export function resolveConfig(config: EdgeChannelConfig): ResolvedEdgeChannelConfig {
  return {
    ...config,
    topologyMode: config.topologyMode ?? 'declare',
    jobsExchange: config.jobsExchange ?? defaultExchangeConfig.jobsExchange,
    eventsExchange: config.eventsExchange ?? defaultExchangeConfig.eventsExchange,
    deadLetterExchange: config.deadLetterExchange ?? defaultExchangeConfig.deadLetterExchange,
    prefetchCount: config.prefetchCount ?? 1,
    maxMessageSizeBytes: config.maxMessageSizeBytes ?? 1024 * 1024,
    reconnectEnabled: config.reconnectEnabled ?? true,
    reconnectDelayMs: config.reconnectDelayMs ?? 1000,
    reconnectMaxDelayMs: config.reconnectMaxDelayMs ?? 30_000,
    backpressureTimeoutMs: config.backpressureTimeoutMs ?? 5000,
  };
}

/**
 * Returns the broker username the publisher stamps on every event as the AMQP `user_id` property.
 *
 * It is `config.userId` when set. Otherwise it is the username amqplib authenticates with for
 * `config.amqpUrl`: the URL-decoded username, or `guest` when the URL has neither a username nor a
 * password. Throws when no username can be derived; set `userId` explicitly in that case.
 */
export function resolveUserId(config: Pick<EdgeChannelConfig, 'amqpUrl' | 'userId'>): string {
  if (config.userId !== undefined) {
    if (config.userId.length === 0) {
      throw new Error('userId must not be empty');
    }

    return config.userId;
  }

  let url: URL;

  try {
    url = new URL(config.amqpUrl);
  } catch {
    throw new Error('cannot derive userId: amqpUrl is not a valid URL; set userId explicitly');
  }

  // amqplib falls back to guest/guest only when the URL has neither a username nor a password.
  if (url.username === '' && url.password === '') {
    return defaultAmqpUsername;
  }

  if (url.username === '') {
    throw new Error('cannot derive userId: amqpUrl has a password but no username; set userId explicitly');
  }

  try {
    return decodeURIComponent(url.username);
  } catch {
    // Malformed percent-encoding: amqplib's unescape() leaves such a sequence as it is, so do the same.
    return url.username;
  }
}

export function buildConsumerQueueName(config: EdgeChannelConfig): string {
  const strategy = config.queueNamingStrategy ?? defaultQueueNamingStrategy;
  return strategy.buildConsumerQueueName({
    siteId: config.siteId,
    nodeId: config.nodeId,
    nodeGroup: config.nodeGroup,
    explicitName: config.consumerQueueName,
  });
}

export function buildJobBindingKeys(config: EdgeChannelConfig): string[] {
  const siteCandidates = [config.siteId, 'any'].filter((value, index, values) => value && values.indexOf(value) === index);
  const workerCandidates = [config.nodeId, config.nodeGroup, 'any'].filter(
    (value, index, values) => value && values.indexOf(value) === index,
  );

  const keys: string[] = [];

  for (const siteId of siteCandidates) {
    for (const worker of workerCandidates) {
      keys.push(`edge.job.${sanitizeRoutingSegment(siteId)}.${sanitizeRoutingSegment(worker)}.*.*`);
    }
  }

  return keys;
}

export function validateConfig(config: EdgeChannelConfig): string[] {
  const errors: string[] = [];

  if (!config.amqpUrl?.trim()) {
    errors.push('amqpUrl is required');
  }

  if (config.topologyMode !== undefined && !topologyModes.includes(config.topologyMode)) {
    errors.push(`topologyMode must be one of ${topologyModes.map((mode) => `'${mode}'`).join(', ')}`);
  }

  if (config.userId !== undefined || config.amqpUrl?.trim()) {
    try {
      resolveUserId(config);
    } catch (error) {
      errors.push(error instanceof Error ? error.message : String(error));
    }
  }

  if (config.prefetchCount !== undefined && (!Number.isInteger(config.prefetchCount) || config.prefetchCount <= 0)) {
    errors.push('prefetchCount must be a positive integer');
  }

  if (
    config.maxMessageSizeBytes !== undefined &&
    (!Number.isInteger(config.maxMessageSizeBytes) || config.maxMessageSizeBytes <= 0)
  ) {
    errors.push('maxMessageSizeBytes must be a positive integer');
  }

  if (config.reconnectDelayMs !== undefined && (!Number.isInteger(config.reconnectDelayMs) || config.reconnectDelayMs <= 0)) {
    errors.push('reconnectDelayMs must be a positive integer');
  }

  if (
    config.reconnectMaxDelayMs !== undefined &&
    (!Number.isInteger(config.reconnectMaxDelayMs) || config.reconnectMaxDelayMs <= 0)
  ) {
    errors.push('reconnectMaxDelayMs must be a positive integer');
  }

  if (
    config.reconnectDelayMs !== undefined &&
    config.reconnectMaxDelayMs !== undefined &&
    config.reconnectDelayMs > config.reconnectMaxDelayMs
  ) {
    errors.push('reconnectDelayMs must be less than or equal to reconnectMaxDelayMs');
  }

  if (
    config.backpressureTimeoutMs !== undefined &&
    (!Number.isInteger(config.backpressureTimeoutMs) || config.backpressureTimeoutMs <= 0)
  ) {
    errors.push('backpressureTimeoutMs must be a positive integer');
  }

  return errors;
}