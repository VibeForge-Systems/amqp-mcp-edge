import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import { ZodError } from 'zod';

import {
  buildNodeKey,
  buildRoutingKey,
  defaultMaxDeliveryAttempts,
  canonicalJson,
  checkEventIdentity,
  computeCapabilitiesDigest,
  createCapabilitySnapshot,
  createMockEdgeChannel,
  defaultAmqpUsername,
  edgeFailureCodes,
  edgeLibraryName,
  edgeLibraryVersion,
  edgeMessageTypes,
  EdgeConventionError,
  EdgeEventPublisher,
  EdgeJobConsumer,
  EdgePublishError,
  EdgeTopologyError,
  EdgeUnsupportedMessageTypeError,
  eventRoutingTypes,
  maxHandlerErrorDescriptionLength,
  isPackageInstallJob,
  isToolJob,
  packageInstallResultOutputSchema,
  parseEventRoutingKey,
  parsePackageInstallJob,
  parseToolJob,
  parseToolResultOutput,
  resolveConfig,
  resolveUserId,
  sanitizeRoutingSegment,
  validateCompatibility,
  validateConfig,
  validateEnvelope,
  type JobDeliveryContext,
  type JobFailureEvent,
  type NodeCapabilitySnapshot,
} from '../index.js';
import {
  createAckMessage,
  createBaseEnvelope,
  createExecutionJob,
  createFailureMessage,
  createHeartbeatMessage,
  createHelloMessage,
  createPackageInstallJob,
  createProgressMessage,
  createResultMessage,
  createSnapshot,
  signedDownloadUrl,
} from './fixtures.js';

// Shaped like the error amqplib rejects a channel RPC with when the broker closes the channel.
function amqpChannelError(replyCode: number, text: string): Error & { code: number } {
  const error = new Error(`Operation failed: QueueDeclare; ${replyCode} with message "${text}"`) as Error & { code: number };
  error.code = replyCode;
  return error;
}

// What a fake broker holds, for passive checks: entities are present unless listed here.
class FakeBrokerTopology {
  public readonly missing = new Set<string>();

  public readonly refused = new Set<string>();

  public nextError?: Error;

  public verify(kind: 'exchange' | 'queue', name: string): void {
    const key = `${kind} ${name}`;

    if (this.nextError) {
      const error = this.nextError;
      this.nextError = undefined;
      throw error;
    }

    if (this.refused.has(key)) {
      throw amqpChannelError(403, `ACCESS_REFUSED - configure access to ${kind} '${name}' in vhost '/' refused for user 'node-1'`);
    }

    if (this.missing.has(key)) {
      throw amqpChannelError(404, `NOT_FOUND - no ${kind} '${name}' in vhost '/'`);
    }
  }
}

class FakeTopologyChannel {
  public readonly calls: string[] = [];

  public closed = false;

  public constructor(private readonly topology: FakeBrokerTopology) {}

  public async checkExchange(name: string): Promise<Record<string, never>> {
    this.calls.push(`checkExchange ${name}`);
    this.topology.verify('exchange', name);
    return {};
  }

  public async checkQueue(name: string): Promise<{ queue: string; messageCount: number; consumerCount: number }> {
    this.calls.push(`checkQueue ${name}`);
    this.topology.verify('queue', name);
    return { queue: name, messageCount: 0, consumerCount: 0 };
  }

  public async close(): Promise<void> {
    this.calls.push('close');
    this.closed = true;
  }
}

const declaringCalls = /^(assertExchange|assertQueue|bindQueue|bindExchange|deleteExchange|deleteQueue)\b/;

type FakeDelivery = {
  fields: { deliveryTag: number; routingKey: string; redelivered: boolean };
  properties?: { headers?: Record<string, unknown> };
  content: Buffer;
};

class FakeConsumerChannel {
  public readonly calls: string[] = [];

  public consumeCount = 0;

  public bindCount = 0;

  public cancelledTags: string[] = [];

  public nackCount = 0;

  public lastNackDeliveryTag?: number;

  // Acks and nacks in order, shared with a fake publisher when a test wants the interleaving.
  public readonly settlements: string[] = [];

  private consumeHandler?: (message: FakeDelivery | null) => void;

  public constructor(public readonly log: string[] = []) {}

  public ack(message: { fields: { deliveryTag: number } }): void {
    this.settlements.push(`ack ${message.fields.deliveryTag}`);
    this.log.push(`ack ${message.fields.deliveryTag}`);
  }

  public nack(message: { fields: { deliveryTag: number } }, _allUpTo?: boolean, requeue?: boolean): void {
    this.nackCount += 1;
    this.lastNackDeliveryTag = message.fields.deliveryTag;
    const entry = `nack ${message.fields.deliveryTag} requeue=${String(requeue)}`;
    this.settlements.push(entry);
    this.log.push(entry);
  }

  public async assertExchange(name: string): Promise<void> {
    this.calls.push(`assertExchange ${name}`);
  }

  public async prefetch(count: number): Promise<void> {
    this.calls.push(`prefetch ${count}`);
  }

  public async assertQueue(name: string): Promise<void> {
    this.calls.push(`assertQueue ${name}`);
  }

  public async bindQueue(queueName: string, exchange: string, pattern: string): Promise<void> {
    this.calls.push(`bindQueue ${queueName} ${exchange} ${pattern}`);
    this.bindCount += 1;
  }

  public async consume(
    queueName: string,
    handler: (message: FakeDelivery | null) => void,
    _options?: { consumerTag?: string },
  ): Promise<{ consumerTag: string }> {
    this.calls.push(`consume ${queueName}`);
    this.consumeCount += 1;
    this.consumeHandler = handler;
    return {
      consumerTag: `ctag-${this.consumeCount}`,
    };
  }

  public async cancel(consumerTag: string): Promise<void> {
    this.cancelledTags.push(consumerTag);
  }

  public emitMessage(message: FakeDelivery): void {
    assert.ok(this.consumeHandler, 'expected consume handler to be registered');
    // amqplib always sets properties; headers is empty unless the publisher or broker set one.
    this.consumeHandler({ properties: { headers: {} }, ...message });
  }
}

class FakeConsumerConnectionManager {
  public readonly topologyChannels: FakeTopologyChannel[] = [];

  private reconnectHandler?: () => Promise<void>;

  public constructor(
    private readonly channel: FakeConsumerChannel,
    public readonly topology = new FakeBrokerTopology(),
  ) {}

  public async openTopologyChannel(): Promise<FakeTopologyChannel> {
    const channel = new FakeTopologyChannel(this.topology);
    this.topologyChannels.push(channel);
    return channel;
  }

  public onReconnect(handler: () => Promise<void>): () => void {
    this.reconnectHandler = handler;
    return () => {
      if (this.reconnectHandler === handler) {
        this.reconnectHandler = undefined;
      }
    };
  }

  public async getConsumerChannel(): Promise<FakeConsumerChannel> {
    return this.channel;
  }

  public async health(): Promise<{ connected: boolean; consumerActive: boolean; publisherActive: boolean }> {
    return {
      connected: true,
      consumerActive: true,
      publisherActive: true,
    };
  }

  public async triggerReconnect(): Promise<void> {
    assert.ok(this.reconnectHandler, 'expected reconnect handler to be registered');
    await this.reconnectHandler();
  }
}

// How the fake broker answers a publish on the confirm channel, as amqplib reports it to the callback.
type FakeConfirm = 'ack' | 'nack' | 'close' | 'never' | 'throw';

class FakePublisherChannel extends EventEmitter {
  public publishCalls = 0;

  public assertExchangeCalls = 0;

  public readonly published: Array<{ exchange: string; routingKey: string; content: Buffer; options: Record<string, unknown> }> = [];

  public constructor(
    private readonly accepted: boolean,
    public confirm: FakeConfirm | ((routingKey: string) => FakeConfirm) = 'ack',
  ) {
    super();
  }

  public async assertExchange(): Promise<void> {
    this.assertExchangeCalls += 1;
  }

  public publish(exchange: string, routingKey: string, content: Buffer, options: Record<string, unknown>, callback?: (error: Error | null) => void): boolean {
    const confirm = typeof this.confirm === 'function' ? this.confirm(routingKey) : this.confirm;

    if (confirm === 'throw') {
      throw new Error('Channel closed');
    }

    this.publishCalls += 1;
    this.published.push({ exchange, routingKey, content, options });
    setImmediate(() => {
      if (confirm === 'ack') {
        callback?.(null);
      } else if (confirm === 'nack') {
        callback?.(new Error('message nacked'));
      } else if (confirm === 'close') {
        callback?.(new Error('channel closed'));
      }
    });
    return this.accepted;
  }
}

class FakePublisherConnectionManager {
  public readonly topologyChannels: FakeTopologyChannel[] = [];

  private readonly reconnectHandlers: Array<() => Promise<void>> = [];

  public constructor(
    private readonly channel: FakePublisherChannel,
    public readonly topology = new FakeBrokerTopology(),
  ) {}

  public onReconnect(handler: () => Promise<void>): () => void {
    this.reconnectHandlers.push(handler);
    return () => undefined;
  }

  public async triggerReconnect(): Promise<void> {
    for (const handler of this.reconnectHandlers) {
      await handler();
    }
  }

  public async openTopologyChannel(): Promise<FakeTopologyChannel> {
    const channel = new FakeTopologyChannel(this.topology);
    this.topologyChannels.push(channel);
    return channel;
  }

  public async getPublisherChannel(): Promise<FakePublisherChannel> {
    return this.channel;
  }

  public async health(): Promise<{ connected: boolean; consumerActive: boolean; publisherActive: boolean }> {
    return {
      connected: true,
      consumerActive: true,
      publisherActive: true,
    };
  }
}

test('validateEnvelope accepts all public message types', () => {
  const jobEnvelope = createExecutionJob();
  const ackMessage = createAckMessage();
  const progressMessage = createProgressMessage();
  const resultMessage = createResultMessage();
  const failureMessage = createFailureMessage();

  assert.equal(validateEnvelope(jobEnvelope).messageType, 'execution.job');
  assert.equal(validateEnvelope(ackMessage).messageType, 'execution.ack');
  assert.equal(validateEnvelope(progressMessage).messageType, 'execution.progress');
  assert.equal(validateEnvelope(resultMessage).messageType, 'execution.result');
  assert.equal(validateEnvelope(failureMessage).messageType, 'execution.failure');
});

// Every timestamp field on the wire, each with a builder that sets it on an otherwise valid message.
const timestampFields: Array<[field: string, build: (value: string) => unknown]> = [
  ['timestamp', (value) => ({ ...createExecutionJob(), timestamp: value })],
  ['payload.attempt.firstAttemptAt', (value) => {
    const job = createExecutionJob();
    return { ...job, payload: { ...job.payload, attempt: { firstAttemptAt: value } } };
  }],
  ['payload.attempt.previousAttemptAt', (value) => {
    const job = createExecutionJob();
    return { ...job, payload: { ...job.payload, attempt: { previousAttemptAt: value } } };
  }],
  ['payload.completedAt', (value) => {
    const result = createResultMessage();
    return { ...result, payload: { ...result.payload, completedAt: value } };
  }],
  ['payload.failedAt', (value) => {
    const failure = createFailureMessage();
    return { ...failure, payload: { ...failure.payload, failedAt: value } };
  }],
];

const timestampCases: Array<[value: string, accepted: boolean]> = [
  ['2026-11-12T10:00:00Z', true],
  ['2026-11-12T10:00:00+05:30', true],
  ['2026-11-12T10:00:00.123456Z', true],
  ['2026-11-12T10:00Z', false], // minute precision
  ['2026-11-12T10:00:00+0530', false], // offset without the colon
  ['2026-11-12T10:00:00+05', false], // hours-only offset
  ['2026-11-12T10:00:00', false], // no offset
  ['2026-11-12t10:00:00z', false], // lower-case t and z
  ['2026-11-12T10:00:60Z', false], // leap second
];

test('validateEnvelope accepts RFC 3339 timestamps with seconds and a Z or ±hh:mm offset, and nothing looser', () => {
  for (const [field, build] of timestampFields) {
    for (const [value, accepted] of timestampCases) {
      const message = build(value);

      if (accepted) {
        assert.doesNotThrow(() => validateEnvelope(message), `${field} ${value} should be accepted`);
      } else {
        assert.throws(() => validateEnvelope(message), /^Error: invalid execution\.[a-z]+ envelope: /, `${field} ${value} should be rejected`);
      }
    }
  }
});

test('an invalid timestamp surfaces as the library validation error, not a raw ZodError', () => {
  assert.throws(
    () => validateEnvelope({ ...createExecutionJob(), timestamp: '2026-11-12T10:00Z' }),
    (error: unknown) => {
      assert.ok(error instanceof Error);
      assert.ok(!(error instanceof ZodError), 'a ZodError escaped validateEnvelope');
      assert.equal(error.name, 'Error');
      assert.match(error.message, /^invalid execution\.job envelope: /);
      assert.match(error.message, /timestamp/);
      return true;
    },
  );
});

test('buildRoutingKey keeps job and event namespaces separate', () => {
  const jobRoutingKey = buildRoutingKey('execution.job', {
    siteId: 'site-a',
    nodeId: 'node-1',
    capabilityKind: 'tool',
    capabilityName: 'search.documents',
  });
  const resultRoutingKey = buildRoutingKey('execution.result', {
    siteId: 'site-a',
    nodeId: 'node-1',
  });

  assert.equal(jobRoutingKey, 'edge.job.site-a.node-1.tool.search-documents');
  assert.equal(resultRoutingKey, 'edge.event.result.site-a.node-1');
});

test('validateCompatibility reports minimum version mismatches', () => {
  const result = validateCompatibility(
    {
      channelVersion: '0.1.0',
      minConsumerVersion: '1.2.0',
    },
    {
      consumerVersion: '1.1.0',
    },
  );

  assert.equal(result.compatible, false);
  assert.match(result.reasons[0] ?? '', /consumer version/);
});

test('mock edge channel supports job dispatch, ack, and event publication', async () => {
  const channel = createMockEdgeChannel({
    siteId: 'site-a',
    nodeId: 'node-1',
  });
  const jobEnvelope = createExecutionJob();

  await channel.consumer.start();

  channel.consumer.onJob(async (job, delivery) => {
    await channel.publisher.publishAck({
      ...createAckMessage({
        messageId: 'ack-message-2',
        payload: {
          jobId: job.payload.jobId,
          attemptId: job.payload.attemptId,
          siteId: 'site-a',
          nodeId: 'node-1',
          accepted: true,
        },
      }),
    });

    await channel.publisher.publishProgress({
      ...createProgressMessage({
        messageId: 'progress-message-2',
        payload: {
          jobId: job.payload.jobId,
          attemptId: job.payload.attemptId,
          siteId: 'site-a',
          nodeId: 'node-1',
          state: 'running',
          percent: 80,
        },
      }),
    });

    await channel.publisher.publishResult({
      ...createResultMessage({
        messageId: 'result-message-2',
        payload: {
          jobId: job.payload.jobId,
          attemptId: job.payload.attemptId,
          siteId: 'site-a',
          nodeId: 'node-1',
          completedAt: new Date().toISOString(),
          output: {
            documents: ['a', 'b'],
          },
        },
      }),
    });

    await channel.consumer.ack(delivery);
  });

  const delivery = await channel.dispatchJob(jobEnvelope);
  const deliveries = channel.getDeliveries();
  const publishedEvents = channel.getPublishedEvents();

  assert.equal(delivery.deliveryTag, 1);
  assert.equal(deliveries[0]?.status, 'acked');
  assert.deepEqual(
    publishedEvents.map((event) => event.message.messageType),
    ['execution.ack', 'execution.progress', 'execution.result'],
  );

  await channel.close();
});

test('edge consumer re-initializes subscription after reconnect', async () => {
  const channel = new FakeConsumerChannel();
  const connectionManager = new FakeConsumerConnectionManager(channel);

  const consumer = new EdgeJobConsumer(
    {
      amqpUrl: 'amqp://test.local',
      siteId: 'site-a',
      nodeId: 'node-1',
    },
    connectionManager as never,
  );

  await consumer.start();
  assert.equal(channel.consumeCount, 1);

  await connectionManager.triggerReconnect();

  assert.equal(channel.consumeCount, 2);
  assert.deepEqual(channel.cancelledTags, ['ctag-1']);

  await consumer.stop();
});

test('publisher waits for drain when broker applies backpressure', async () => {
  const channel = new FakePublisherChannel(false);
  const connectionManager = new FakePublisherConnectionManager(channel);

  const publisher = new EdgeEventPublisher(
    {
      amqpUrl: 'amqp://test.local',
      siteId: 'site-a',
      nodeId: 'node-1',
      backpressureTimeoutMs: 200,
    },
    connectionManager as never,
  );

  const publishPromise = publisher.publishAck(createAckMessage());

  setTimeout(() => {
    channel.emit('drain');
  }, 20);

  await publishPromise;
  assert.equal(channel.publishCalls, 1);
});

test('publisher fails when drain event does not arrive before timeout', async () => {
  const channel = new FakePublisherChannel(false);
  const connectionManager = new FakePublisherConnectionManager(channel);

  const publisher = new EdgeEventPublisher(
    {
      amqpUrl: 'amqp://test.local',
      siteId: 'site-a',
      nodeId: 'node-1',
      backpressureTimeoutMs: 25,
    },
    connectionManager as never,
  );

  await assert.rejects(
    () => publisher.publishAck(createAckMessage()),
    /publisher backpressure timeout/,
  );
});

test('consumer rejects job when minConsumerVersion is set but runtimeConsumerVersion is missing', async () => {
  const channel = new FakeConsumerChannel();
  const connectionManager = new FakeConsumerConnectionManager(channel);

  const consumer = new EdgeJobConsumer(
    {
      amqpUrl: 'amqp://test.local',
      siteId: 'site-a',
      nodeId: 'node-1',
    },
    connectionManager as never,
  );

  let handled = false;
  consumer.onJob(async () => {
    handled = true;
  });

  await consumer.start();

  const jobEnvelope = createExecutionJob({
    compatibility: {
      ...createBaseEnvelope().compatibility,
      minConsumerVersion: '1.0.0',
    },
  });

  channel.emitMessage({
    fields: {
      deliveryTag: 101,
      routingKey: 'edge.job.site-a.node-1.tool.search-documents',
      redelivered: false,
    },
    content: Buffer.from(JSON.stringify(jobEnvelope), 'utf8'),
  });

  await new Promise((resolve) => setTimeout(resolve, 0));

  assert.equal(handled, false);
  assert.equal(channel.nackCount, 1);
  assert.equal(channel.lastNackDeliveryTag, 101);

  await consumer.stop();
});

test('consumer rejects job when runtimeConsumerVersion is below minConsumerVersion', async () => {
  const channel = new FakeConsumerChannel();
  const connectionManager = new FakeConsumerConnectionManager(channel);

  const consumer = new EdgeJobConsumer(
    {
      amqpUrl: 'amqp://test.local',
      siteId: 'site-a',
      nodeId: 'node-1',
      runtimeConsumerVersion: '0.9.0',
    },
    connectionManager as never,
  );

  let handled = false;
  consumer.onJob(async () => {
    handled = true;
  });

  await consumer.start();

  const jobEnvelope = createExecutionJob({
    compatibility: {
      ...createBaseEnvelope().compatibility,
      minConsumerVersion: '1.0.0',
    },
  });

  channel.emitMessage({
    fields: {
      deliveryTag: 102,
      routingKey: 'edge.job.site-a.node-1.tool.search-documents',
      redelivered: false,
    },
    content: Buffer.from(JSON.stringify(jobEnvelope), 'utf8'),
  });

  await new Promise((resolve) => setTimeout(resolve, 0));

  assert.equal(handled, false);
  assert.equal(channel.nackCount, 1);
  assert.equal(channel.lastNackDeliveryTag, 102);

  await consumer.stop();
});

test('publisher rejects event when minProducerVersion is set but runtimeProducerVersion is missing', async () => {
  const channel = new FakePublisherChannel(true);
  const connectionManager = new FakePublisherConnectionManager(channel);

  const publisher = new EdgeEventPublisher(
    {
      amqpUrl: 'amqp://test.local',
      siteId: 'site-a',
      nodeId: 'node-1',
    },
    connectionManager as never,
  );

  await assert.rejects(
    () =>
      publisher.publishAck(
        createAckMessage({
          compatibility: {
            ...createBaseEnvelope().compatibility,
            minProducerVersion: '1.0.0',
          },
        }),
      ),
    /producerVersion is required/,
  );

  assert.equal(channel.publishCalls, 0);
});

test('publisher rejects event when runtimeProducerVersion is below minProducerVersion', async () => {
  const channel = new FakePublisherChannel(true);
  const connectionManager = new FakePublisherConnectionManager(channel);

  const publisher = new EdgeEventPublisher(
    {
      amqpUrl: 'amqp://test.local',
      siteId: 'site-a',
      nodeId: 'node-1',
      runtimeProducerVersion: '0.9.0',
    },
    connectionManager as never,
  );

  await assert.rejects(
    () =>
      publisher.publishAck(
        createAckMessage({
          compatibility: {
            ...createBaseEnvelope().compatibility,
            minProducerVersion: '1.0.0',
          },
        }),
      ),
    /producer version 0.9.0 is lower than required 1.0.0/,
  );

  assert.equal(channel.publishCalls, 0);
});

// --- Topology mode -------------------------------------------------------------------------------

const passiveConfig = {
  amqpUrl: 'amqp://node-1:secret@broker.example.com:5672',
  siteId: 'site-a',
  nodeId: 'node-1',
  topologyMode: 'passive' as const,
};

const nodeQueueName = 'mcp.edge.consumer.site-a.node-1';

test('topologyMode defaults to declare, and validateConfig rejects an unknown mode', () => {
  assert.equal(resolveConfig({ amqpUrl: 'amqp://broker.example.com' }).topologyMode, 'declare');
  assert.equal(resolveConfig(passiveConfig).topologyMode, 'passive');
  assert.deepEqual(validateConfig(passiveConfig), []);

  const errors = validateConfig({ amqpUrl: 'amqp://broker.example.com', topologyMode: 'lazy' as never });
  assert.deepEqual(errors, ["topologyMode must be one of 'declare', 'passive'"]);
});

test('declare mode, the default, still declares the consumer topology and the events exchange', async () => {
  const consumerChannel = new FakeConsumerChannel();
  const consumerManager = new FakeConsumerConnectionManager(consumerChannel);
  const consumer = new EdgeJobConsumer({ ...passiveConfig, topologyMode: undefined }, consumerManager as never);

  await consumer.start();

  assert.deepEqual(consumerChannel.calls.filter((call) => declaringCalls.test(call)), [
    'assertExchange mcp.edge.jobs',
    'assertExchange mcp.edge.dlx',
    `assertQueue ${nodeQueueName}`,
    `bindQueue ${nodeQueueName} mcp.edge.jobs edge.job.site-a.node-1.*.*`,
    `bindQueue ${nodeQueueName} mcp.edge.jobs edge.job.site-a.any.*.*`,
    `bindQueue ${nodeQueueName} mcp.edge.jobs edge.job.any.node-1.*.*`,
    `bindQueue ${nodeQueueName} mcp.edge.jobs edge.job.any.any.*.*`,
  ]);
  assert.equal(consumerManager.topologyChannels.length, 0);

  const publisherChannel = new FakePublisherChannel(true);
  const publisherManager = new FakePublisherConnectionManager(publisherChannel);
  const publisher = new EdgeEventPublisher({ ...passiveConfig, topologyMode: 'declare' }, publisherManager as never);

  await publisher.publishAck(createAckMessage());
  await publisher.publishResult(createResultMessage());

  assert.equal(publisherChannel.assertExchangeCalls, 2);
  assert.equal(publisherManager.topologyChannels.length, 0);

  await consumer.stop();
});

test('passive consumer only checks its queue, and never declares a queue or exchange or binds', async () => {
  const channel = new FakeConsumerChannel();
  const connectionManager = new FakeConsumerConnectionManager(channel);
  const consumer = new EdgeJobConsumer(passiveConfig, connectionManager as never);

  await consumer.start();

  assert.deepEqual(channel.calls, ['prefetch 1', `consume ${nodeQueueName}`]);
  assert.equal(channel.bindCount, 0);
  assert.equal(connectionManager.topologyChannels.length, 1);
  assert.deepEqual(connectionManager.topologyChannels[0]?.calls, [`checkQueue ${nodeQueueName}`, 'close']);

  await consumer.stop();
});

test('passive publisher checks the events exchange once, and never declares it', async () => {
  const channel = new FakePublisherChannel(true);
  const connectionManager = new FakePublisherConnectionManager(channel);
  const publisher = new EdgeEventPublisher(passiveConfig, connectionManager as never);

  // Concurrent first publishes share one check.
  await Promise.all([publisher.publishAck(createAckMessage()), publisher.publishProgress(createProgressMessage())]);
  await publisher.publishResult(createResultMessage());
  await publisher.publishFailure(createFailureMessage());

  assert.equal(channel.publishCalls, 4);
  assert.equal(channel.assertExchangeCalls, 0);
  assert.equal(connectionManager.topologyChannels.length, 1);
  assert.deepEqual(connectionManager.topologyChannels[0]?.calls, ['checkExchange mcp.edge.events', 'close']);
});

test('verifyTopology caches a success, so start() after createEdgeChannel does not check twice', async () => {
  const channel = new FakeConsumerChannel();
  const connectionManager = new FakeConsumerConnectionManager(channel);
  const consumer = new EdgeJobConsumer(passiveConfig, connectionManager as never);

  await consumer.verifyTopology();
  await consumer.start();

  assert.equal(connectionManager.topologyChannels.length, 1);
  assert.equal(channel.consumeCount, 1);

  await consumer.stop();
});

test('passive consumer fails startup with an error naming the missing queue, and does not retry it', async () => {
  const channel = new FakeConsumerChannel();
  const connectionManager = new FakeConsumerConnectionManager(channel);
  connectionManager.topology.missing.add(`queue ${nodeQueueName}`);
  const consumer = new EdgeJobConsumer(passiveConfig, connectionManager as never);

  await assert.rejects(
    () => consumer.start(),
    (error: unknown) => {
      assert.ok(error instanceof EdgeTopologyError);
      assert.equal(error.kind, 'queue');
      assert.equal(error.entityName, nodeQueueName);
      assert.equal(error.replyCode, 404);
      assert.match(error.message, new RegExp(`queue '${nodeQueueName.replace(/\./g, '\\.')}' does not exist`));
      assert.match(error.message, /never declares topology/);
      return true;
    },
  );

  // Nothing was declared, bound or consumed, and the check channel was closed.
  assert.deepEqual(channel.calls, []);
  assert.equal(connectionManager.topologyChannels[0]?.closed, true);

  const health = await consumer.health();
  assert.equal(health.consumerActive, false);
  assert.match(String(health.details?.topologyError), /does not exist/);

  // A reconnect does not bring a consumer that never started back to check again.
  await connectionManager.triggerReconnect();
  assert.equal(connectionManager.topologyChannels.length, 1);
  assert.equal(channel.consumeCount, 0);
});

test('passive publisher rejects with an error naming the missing exchange, and checks again on the next publish', async () => {
  const channel = new FakePublisherChannel(true);
  const connectionManager = new FakePublisherConnectionManager(channel);
  connectionManager.topology.missing.add('exchange mcp.edge.events');
  const publisher = new EdgeEventPublisher(passiveConfig, connectionManager as never);

  await assert.rejects(
    () => publisher.publishAck(createAckMessage()),
    (error: unknown) =>
      error instanceof EdgeTopologyError &&
      error.kind === 'exchange' &&
      error.entityName === 'mcp.edge.events' &&
      /exchange 'mcp\.edge\.events' does not exist/.test(error.message),
  );
  assert.equal(channel.publishCalls, 0);
  assert.equal(channel.assertExchangeCalls, 0);

  // A failure is not cached: once the control plane declares the exchange, publishing works.
  connectionManager.topology.missing.clear();
  await publisher.publishAck(createAckMessage());

  assert.equal(connectionManager.topologyChannels.length, 2);
  assert.equal(channel.publishCalls, 1);
});

test('a refused passive check names the permission the broker wants', async () => {
  const channel = new FakeConsumerChannel();
  const connectionManager = new FakeConsumerConnectionManager(channel);
  connectionManager.topology.refused.add(`queue ${nodeQueueName}`);
  const consumer = new EdgeJobConsumer(passiveConfig, connectionManager as never);

  await assert.rejects(
    () => consumer.start(),
    (error: unknown) =>
      error instanceof EdgeTopologyError &&
      error.replyCode === 403 &&
      /ACCESS_REFUSED/.test(error.message) &&
      /configure, write or read/.test(error.message),
  );
  assert.deepEqual(channel.calls, []);
});

test('a passive check that fails without an AMQP reply code passes the original error through', async () => {
  const channel = new FakeConsumerChannel();
  const connectionManager = new FakeConsumerConnectionManager(channel);
  const connectionLost = new Error('Channel ended, no reply will be forthcoming');
  connectionManager.topology.nextError = connectionLost;
  const consumer = new EdgeJobConsumer(passiveConfig, connectionManager as never);

  await assert.rejects(() => consumer.start(), (error: unknown) => error === connectionLost);
  assert.equal(connectionManager.topologyChannels[0]?.closed, true);
});

test('passive consumer checks its queue again after a reconnect, and stops cleanly if it is gone', async () => {
  const channel = new FakeConsumerChannel();
  const connectionManager = new FakeConsumerConnectionManager(channel);
  const consumer = new EdgeJobConsumer(passiveConfig, connectionManager as never);

  await consumer.start();
  await connectionManager.triggerReconnect();

  assert.equal(connectionManager.topologyChannels.length, 2);
  assert.equal(channel.consumeCount, 2);
  assert.deepEqual(channel.cancelledTags, ['ctag-1']);

  // The queue disappears while the node is disconnected: the reconnect handler does not throw
  // (which would leave the reconnect loop to swallow it), does not subscribe, and reports why.
  connectionManager.topology.missing.add(`queue ${nodeQueueName}`);
  await connectionManager.triggerReconnect();

  assert.equal(connectionManager.topologyChannels.length, 3);
  assert.equal(channel.consumeCount, 2);
  assert.equal(channel.calls.filter((call) => declaringCalls.test(call)).length, 0);

  const health = await consumer.health();
  assert.equal(health.consumerActive, false);
  assert.match(String(health.details?.topologyError), new RegExp(`queue '${nodeQueueName.replace(/\./g, '\\.')}' does not exist`));

  // Stopped means stopped: later reconnects leave it alone.
  await connectionManager.triggerReconnect();
  assert.equal(connectionManager.topologyChannels.length, 3);
});

test('passive publisher checks the events exchange again after a reconnect, not on every publish', async () => {
  const channel = new FakePublisherChannel(true);
  const connectionManager = new FakePublisherConnectionManager(channel);
  const publisher = new EdgeEventPublisher(passiveConfig, connectionManager as never);

  await publisher.publishAck(createAckMessage());
  await publisher.publishAck(createAckMessage());
  assert.equal(connectionManager.topologyChannels.length, 1);

  await connectionManager.triggerReconnect();
  await publisher.publishAck(createAckMessage());
  await publisher.publishAck(createAckMessage());

  assert.equal(connectionManager.topologyChannels.length, 2);
  assert.equal(channel.publishCalls, 4);
});

// --- Node identity (userId) ----------------------------------------------------------------------

test('resolveUserId prefers an explicit userId over the amqpUrl username', () => {
  assert.equal(resolveUserId({ amqpUrl: 'amqp://shared:secret@broker.example.com', userId: 'node-1' }), 'node-1');
  assert.equal(resolveUserId({ amqpUrl: 'amqps://broker.example.com', userId: 'node-1' }), 'node-1');
});

test('resolveUserId takes the URL-decoded username from amqpUrl', () => {
  assert.equal(resolveUserId({ amqpUrl: 'amqp://edge-node-1:secret@broker.example.com:5672/edge' }), 'edge-node-1');
  assert.equal(resolveUserId({ amqpUrl: 'amqps://node-1%40site-a:p%40ss@broker.example.com' }), 'node-1@site-a');
  assert.equal(resolveUserId({ amqpUrl: 'amqp://node%2B1:secret@broker.example.com' }), 'node+1');
  // A username without a password is still the username amqplib logs in with.
  assert.equal(resolveUserId({ amqpUrl: 'amqp://node-1@broker.example.com' }), 'node-1');
  // Malformed percent-encoding is left as it is, as amqplib's unescape() leaves it.
  assert.equal(resolveUserId({ amqpUrl: 'amqp://node%ZZ:secret@broker.example.com' }), 'node%ZZ');
});

test('resolveUserId falls back to guest, amqplib\'s default, when amqpUrl has no credentials', () => {
  assert.equal(defaultAmqpUsername, 'guest');
  assert.equal(resolveUserId({ amqpUrl: 'amqp://broker.example.com:5672' }), 'guest');
  assert.equal(resolveUserId({ amqpUrl: 'amqp://localhost' }), 'guest');
});

test('validateConfig reports a userId that cannot be derived', () => {
  assert.match(validateConfig({ amqpUrl: 'amqp://:secret@broker.example.com' }).join('; '), /password but no username; set userId/);
  assert.match(validateConfig({ amqpUrl: 'not a url' }).join('; '), /not a valid URL; set userId/);
  assert.deepEqual(validateConfig({ amqpUrl: 'not a url', userId: 'node-1' }), []);
  assert.match(validateConfig({ amqpUrl: 'amqp://broker.example.com', userId: '' }).join('; '), /userId must not be empty/);

  assert.throws(
    () => new EdgeEventPublisher({ amqpUrl: 'amqp://:secret@broker.example.com' }, new FakePublisherConnectionManager(new FakePublisherChannel(true)) as never),
    /invalid edge channel config: cannot derive userId/,
  );
});

test('publisher sets the broker username as userId on every event, in both topology modes', async () => {
  for (const topologyMode of ['declare', 'passive'] as const) {
    const channel = new FakePublisherChannel(true);
    const publisher = new EdgeEventPublisher(
      {
        amqpUrl: 'amqps://node-1%40site-a:secret@broker.example.com',
        siteId: 'site-a',
        nodeId: 'node-1',
        topologyMode,
      },
      new FakePublisherConnectionManager(channel) as never,
    );

    await publisher.publishAck(createAckMessage());
    await publisher.publishProgress(createProgressMessage());
    await publisher.publishResult(createResultMessage());
    await publisher.publishFailure(createFailureMessage());

    assert.equal(channel.published.length, 4, topologyMode);
    for (const { options } of channel.published) {
      assert.equal(options.userId, 'node-1@site-a', topologyMode);
    }
  }
});

test('publisher uses an explicit userId, and guest when amqpUrl has no credentials', async () => {
  const explicitChannel = new FakePublisherChannel(true);
  const explicit = new EdgeEventPublisher(
    { amqpUrl: 'amqp://shared:secret@broker.example.com', userId: 'node-1' },
    new FakePublisherConnectionManager(explicitChannel) as never,
  );
  await explicit.publishAck(createAckMessage());
  assert.equal(explicitChannel.published[0]?.options.userId, 'node-1');

  const defaultChannel = new FakePublisherChannel(true);
  const fallback = new EdgeEventPublisher(
    { amqpUrl: 'amqp://broker.example.com' },
    new FakePublisherConnectionManager(defaultChannel) as never,
  );
  await fallback.publishAck(createAckMessage());
  assert.equal(defaultChannel.published[0]?.options.userId, 'guest');
});

test('mock edge channel records the userId the publisher would send', async () => {
  const channel = createMockEdgeChannel({ amqpUrl: 'amqp://node-1:secret@broker.example.com', siteId: 'site-a' });

  await channel.publisher.publishAck(createAckMessage());

  assert.equal(channel.getPublishedEvents()[0]?.userId, 'node-1');
  await channel.close();
});

// --- Contracts: library version ------------------------------------------------------------------

test('edgeLibraryName and edgeLibraryVersion match package.json', () => {
  const packageJson = JSON.parse(readFileSync(new URL('../../package.json', import.meta.url), 'utf8')) as { name: string; version: string };

  assert.equal(edgeLibraryName, packageJson.name);
  assert.equal(edgeLibraryVersion, packageJson.version);
});

// --- Contracts: canonical JSON and the capability digest -----------------------------------------

test('canonicalJson writes RFC 8785 form: sorted members, no whitespace, JSON.stringify strings and numbers', () => {
  assert.equal(canonicalJson({ b: 1, a: [true, null, 'x'], c: { z: '', y: -0.5 } }), '{"a":[true,null,"x"],"b":1,"c":{"y":-0.5,"z":""}}');
  // Members sort by UTF-16 code units, so upper case before lower case and "é" after "z".
  assert.equal(canonicalJson({ z: 1, é: 2, Z: 3, a: 4 }), '{"Z":3,"a":4,"z":1,"é":2}');
  assert.equal(canonicalJson({ text: 'line\nbreak "quoted" \u0007' }), '{"text":"line\\nbreak \\"quoted\\" \\u0007"}');
  assert.equal(canonicalJson({ big: 1e21, small: 1e-7, int: 10 }), '{"big":1e+21,"int":10,"small":1e-7}');
  // Members holding undefined are left out, as JSON.stringify leaves them out.
  assert.equal(canonicalJson({ kept: 1, dropped: undefined }), '{"kept":1}');

  assert.throws(() => canonicalJson({ n: Number.NaN }), TypeError);
  assert.throws(() => canonicalJson({ n: Number.POSITIVE_INFINITY }), TypeError);
  assert.throws(() => canonicalJson([1, undefined]), TypeError);
  assert.throws(() => canonicalJson({ n: 1n }), TypeError);
});

// The vector docs/PROTOCOL.md publishes, so an implementation in another language can check itself.
const digestVectorSnapshot: NodeCapabilitySnapshot = {
  runtimes: { node: '22.20.0' },
  packages: [{ releaseId: 'rel-1', packageVersion: '1.2.0', packageId: 'net-tools', distHash: `mcpod-dist-v2:${'0'.repeat(64)}` }],
  edgeLibrary: { version: '0.1.0', name: '@vibeforge-systems/amqp-mcp-edge' },
};
const digestVectorCanonical =
  '{"edgeLibrary":{"name":"@vibeforge-systems/amqp-mcp-edge","version":"0.1.0"},' +
  '"packages":[{"distHash":"mcpod-dist-v2:0000000000000000000000000000000000000000000000000000000000000000",' +
  '"packageId":"net-tools","packageVersion":"1.2.0","releaseId":"rel-1"}],"runtimes":{"node":"22.20.0"}}';
const digestVector = 'sha256:c50b81fdd0a559330746ba1d3a830ad7f6a2b20b934ec7c90c40951d746530d2';

test('computeCapabilitiesDigest is sha256 over the canonical JSON, matching the published vector', () => {
  assert.equal(canonicalJson(digestVectorSnapshot), digestVectorCanonical);
  assert.equal(`sha256:${createHash('sha256').update(digestVectorCanonical, 'utf8').digest('hex')}`, digestVector);
  assert.equal(computeCapabilitiesDigest(digestVectorSnapshot), digestVector);

  // Member order does not matter; array order and unknown members do.
  const reordered = JSON.parse(JSON.stringify({ edgeLibrary: digestVectorSnapshot.edgeLibrary, packages: digestVectorSnapshot.packages, runtimes: digestVectorSnapshot.runtimes })) as NodeCapabilitySnapshot;
  assert.equal(computeCapabilitiesDigest(reordered), digestVector);
  assert.notEqual(computeCapabilitiesDigest({ ...digestVectorSnapshot, extra: 'x' } as NodeCapabilitySnapshot), digestVector);
  const second = { packageId: 'a-first', packageVersion: '1.0.0', distHash: `mcpod-dist-v2:${'1'.repeat(64)}` };
  assert.notEqual(
    computeCapabilitiesDigest({ ...digestVectorSnapshot, packages: [...digestVectorSnapshot.packages, second] }),
    computeCapabilitiesDigest({ ...digestVectorSnapshot, packages: [second, ...digestVectorSnapshot.packages] }),
  );
});

test('createCapabilitySnapshot names this library and Node.js, and sorts packages so the digest is stable', () => {
  const later = { packageId: 'net-tools', packageVersion: '1.10.0', distHash: `mcpod-dist-v2:${'b'.repeat(64)}` };
  const earlier = { packageId: 'net-tools', packageVersion: '1.0.0', distHash: `mcpod-dist-v2:${'c'.repeat(64)}` };
  const other = { packageId: 'alpha', packageVersion: '2.0.0', releaseId: 'rel-alpha', distHash: `mcpod-dist-v2:${'d'.repeat(64)}` };

  const snapshot = createCapabilitySnapshot({ packages: [later, other, earlier], runtimes: { python: '3.12.4' } });

  assert.deepEqual(snapshot.edgeLibrary, { name: edgeLibraryName, version: edgeLibraryVersion });
  assert.deepEqual(snapshot.runtimes, { node: process.versions.node, python: '3.12.4' });
  // Sorted by packageId, then packageVersion as a string.
  assert.deepEqual(snapshot.packages.map((item) => `${item.packageId}@${item.packageVersion}`), ['alpha@2.0.0', 'net-tools@1.0.0', 'net-tools@1.10.0']);
  assert.equal(
    computeCapabilitiesDigest(createCapabilitySnapshot({ packages: [earlier, later, other], runtimes: { python: '3.12.4' } })),
    computeCapabilitiesDigest(snapshot),
  );
  // An explicit runtime wins over the detected one, and the input array is not reordered.
  assert.equal(createCapabilitySnapshot({ runtimes: { node: '24.0.0' } }).runtimes.node, '24.0.0');
  assert.deepEqual(createCapabilitySnapshot().packages, []);
});

// --- Contracts: node.hello and node.heartbeat ----------------------------------------------------

test('validateEnvelope accepts node.hello and node.heartbeat, with or without the snapshot on a heartbeat', () => {
  assert.equal(validateEnvelope(createHelloMessage()).messageType, 'node.hello');
  assert.equal(validateEnvelope(createHeartbeatMessage()).messageType, 'node.heartbeat');

  const changed = createSnapshot({ packages: [] });
  const withSnapshot = createHeartbeatMessage({
    payload: { ...createHeartbeatMessage().payload, capabilities: changed, capabilitiesDigest: computeCapabilitiesDigest(changed) },
  });
  assert.equal(validateEnvelope(withSnapshot).messageType, 'node.heartbeat');
  assert.deepEqual(edgeMessageTypes.filter((type) => type.startsWith('node.')), ['node.hello', 'node.heartbeat']);
});

test('a lifecycle message whose digest does not match the snapshot it carries is rejected', () => {
  const hello = createHelloMessage();
  const stale = { ...hello, payload: { ...hello.payload, capabilitiesDigest: computeCapabilitiesDigest(createSnapshot({ packages: [] })) } };
  assert.throws(() => validateEnvelope(stale), /^Error: invalid node\.hello envelope: .*capabilitiesDigest.*does not match capabilities/s);

  const heartbeat = createHeartbeatMessage();
  const mismatched = { ...heartbeat, payload: { ...heartbeat.payload, capabilities: createSnapshot({ packages: [] }) } };
  assert.throws(() => validateEnvelope(mismatched), /^Error: invalid node\.heartbeat envelope: .*does not match capabilities/s);

  // Unknown snapshot members are covered by the digest: adding one without recomputing fails.
  const extended = { ...hello, payload: { ...hello.payload, capabilities: { ...hello.payload.capabilities, gpu: 'none' } } };
  assert.throws(() => validateEnvelope(extended), /does not match capabilities/);
  const recomputed = { ...extended, payload: { ...extended.payload, capabilitiesDigest: computeCapabilitiesDigest(extended.payload.capabilities) } };
  assert.equal(validateEnvelope(recomputed).messageType, 'node.hello');
});

test('node lifecycle payloads reject missing identity, bad counters, bad digests and bad snapshots', () => {
  const hello = createHelloMessage();
  const helloWith = (payload: Record<string, unknown>) => ({ ...hello, payload: { ...hello.payload, ...payload } });
  const snapshotWith = (snapshot: Record<string, unknown>) => {
    const capabilities = { ...createSnapshot(), ...snapshot } as NodeCapabilitySnapshot;
    return helloWith({ capabilities, capabilitiesDigest: computeCapabilitiesDigest(capabilities) });
  };

  const rejected: Array<[label: string, message: unknown]> = [
    ['no siteId', helloWith({ siteId: undefined })],
    ['no nodeId', helloWith({ nodeId: undefined })],
    ['no sessionId', helloWith({ sessionId: '' })],
    ['sequence 0', helloWith({ sequence: 0 })],
    ['fractional sequence', helloWith({ sequence: 1.5 })],
    ['heartbeatIntervalMs 0', helloWith({ heartbeatIntervalMs: 0 })],
    ['no startedAt', helloWith({ startedAt: undefined })],
    ['minute-precision startedAt', helloWith({ startedAt: '2026-11-14T10:00Z' })],
    ['no capabilities on hello', helloWith({ capabilities: undefined })],
    ['upper-case digest', helloWith({ capabilitiesDigest: hello.payload.capabilitiesDigest.toUpperCase() })],
    ['digest without its tag', helloWith({ capabilitiesDigest: hello.payload.capabilitiesDigest.slice('sha256:'.length) })],
    ['empty runtimes', snapshotWith({ runtimes: {} })],
    ['no edgeLibrary', snapshotWith({ edgeLibrary: undefined })],
    ['dist hash with upper-case hex', snapshotWith({ packages: [{ packageId: 'p', packageVersion: '1', distHash: `mcpod-dist-v2:${'A'.repeat(64)}` }] })],
    ['dist hash without a tag', snapshotWith({ packages: [{ packageId: 'p', packageVersion: '1', distHash: 'a'.repeat(64) }] })],
    ['dist hash of 63 digits', snapshotWith({ packages: [{ packageId: 'p', packageVersion: '1', distHash: `sha256:${'a'.repeat(63)}` }] })],
    ['package without a version', snapshotWith({ packages: [{ packageId: 'p', distHash: `sha256:${'a'.repeat(64)}` }] })],
  ];

  for (const [label, message] of rejected) {
    assert.throws(() => validateEnvelope(message), /^Error: invalid node\.hello envelope: /, label);
  }

  const heartbeat = createHeartbeatMessage();
  assert.throws(() => validateEnvelope({ ...heartbeat, payload: { ...heartbeat.payload, siteId: undefined } }), /invalid node\.heartbeat envelope/);
  assert.throws(() => validateEnvelope({ ...heartbeat, payload: { ...heartbeat.payload, capabilitiesDigest: undefined } }), /invalid node\.heartbeat envelope/);

  // Additive members are accepted, as the compatibility rules require.
  assert.equal(validateEnvelope(helloWith({ futureField: { any: 'thing' } })).messageType, 'node.hello');
  // A 128-digit dist hash (a SHA-512-based format) is accepted.
  assert.equal(validateEnvelope(snapshotWith({ packages: [{ packageId: 'p', packageVersion: '1', distHash: `x-v1:${'e'.repeat(128)}` }] })).messageType, 'node.hello');
});

test('an unknown messageType raises EdgeUnsupportedMessageTypeError, so consumers can ignore it', () => {
  assert.throws(
    () => validateEnvelope({ ...createHelloMessage(), messageType: 'node.control' }),
    (error: unknown) => {
      assert.ok(error instanceof EdgeUnsupportedMessageTypeError);
      assert.ok(error instanceof Error);
      assert.equal(error.messageType, 'node.control');
      assert.equal(error.message, 'unsupported messageType: node.control');
      return true;
    },
  );
  // A known type that fails its schema is a plain validation error, not an unsupported type.
  assert.throws(
    () => validateEnvelope({ ...createHelloMessage(), payload: {} }),
    (error: unknown) => !(error instanceof EdgeUnsupportedMessageTypeError) && /^invalid node\.hello envelope: /.test((error as Error).message),
  );
});

// --- Contracts: routing and node identity --------------------------------------------------------

test('node lifecycle events route as edge.event.hello and edge.event.heartbeat, inside the node topic permission', () => {
  assert.equal(buildRoutingKey('node.hello', { siteId: 'Site A', nodeId: 'node.1' }), 'edge.event.hello.site-a.node-1');
  assert.equal(buildRoutingKey('node.heartbeat', { siteId: 'site-a', nodeId: 'node-1' }), 'edge.event.heartbeat.site-a.node-1');

  // The topic write permission docs/PROTOCOL.md gives a node already covers both.
  const topicWrite = /^edge\.event\.[a-z]+\.site-a\.node-1$/;
  for (const messageType of edgeMessageTypes.filter((type) => type !== 'execution.job')) {
    assert.match(buildRoutingKey(messageType, { siteId: 'site-a', nodeId: 'node-1' }), topicWrite, messageType);
  }
});

test('parseEventRoutingKey splits every event routing key and refuses anything else', () => {
  for (const [eventType, messageType] of Object.entries(eventRoutingTypes)) {
    const routingKey = buildRoutingKey(messageType, { siteId: 'site-a', nodeId: 'node-1' });
    assert.deepEqual(parseEventRoutingKey(routingKey), { eventType, messageType, siteId: 'site-a', nodeId: 'node-1' });
  }

  for (const routingKey of [
    'edge.job.site-a.node-1.tool.search-documents',
    'edge.event.control.site-a.node-1',
    'edge.event.hello.site-a',
    'edge.event.hello.site-a.node-1.extra',
    'edge.event.hello..node-1',
    'edge.event.constructor.site-a.node-1',
    '',
  ]) {
    assert.equal(parseEventRoutingKey(routingKey), undefined, routingKey);
  }
});

test('buildNodeKey and sanitizeRoutingSegment give the form routing keys and queue names use', () => {
  assert.equal(buildNodeKey({ siteId: 'Site A', nodeId: 'node.1' }), 'site-a.node-1');
  assert.equal(sanitizeRoutingSegment(' Edge_Node-7 '), 'edge_node-7');
  assert.equal(sanitizeRoutingSegment(undefined), 'any');
});

test('checkEventIdentity accepts the provisioned node and names each mismatch', () => {
  const provisioned = { userId: 'node-1@site-a', siteId: 'site-a', nodeId: 'node-1' };
  const hello = createHelloMessage();
  const routingKey = 'edge.event.hello.site-a.node-1';

  assert.deepEqual(checkEventIdentity({ routingKey, userId: 'node-1@site-a', envelope: hello }, provisioned), []);
  assert.deepEqual(checkEventIdentity({ routingKey: 'edge.event.result.site-a.node-1', userId: 'node-1@site-a', envelope: createResultMessage() }, provisioned), []);

  const reasons = (event: Parameters<typeof checkEventIdentity>[0]) => checkEventIdentity(event, provisioned).join(' | ');

  assert.match(reasons({ routingKey, userId: undefined, envelope: hello }), /has no user_id/);
  assert.match(reasons({ routingKey, userId: 'node-2@site-a', envelope: hello }), /user_id 'node-2@site-a' is not the provisioned user/);
  assert.match(reasons({ routingKey: 'edge.event.hello.site-a.node-2', userId: 'node-1@site-a', envelope: hello }), /routing key 'edge\.event\.hello\.site-a\.node-2' is not 'edge\.event\.hello\.site-a\.node-1'/);
  // A routing key of another event type is a mismatch too.
  assert.match(reasons({ routingKey: 'edge.event.heartbeat.site-a.node-1', userId: 'node-1@site-a', envelope: hello }), /routing key/);
  assert.match(
    reasons({ routingKey, userId: 'node-1@site-a', envelope: { ...hello, payload: { ...hello.payload, nodeId: 'node-2' } } }),
    /payload\.nodeId 'node-2' is not the provisioned node/,
  );
  assert.match(
    reasons({ routingKey, userId: 'node-1@site-a', envelope: { ...hello, payload: { ...hello.payload, siteId: 'site-b' } } }),
    /payload\.siteId 'site-b' is not the provisioned site/,
  );
  assert.match(reasons({ routingKey, userId: 'node-1@site-a', envelope: createExecutionJob() }), /execution\.job is not an event/);
});

// --- Contracts: publishing and consuming lifecycle messages --------------------------------------

test('publisher sends node.hello and node.heartbeat to the events exchange with the node routing keys and user_id', async () => {
  const channel = new FakePublisherChannel(true);
  const publisher = new EdgeEventPublisher(
    { amqpUrl: 'amqps://node-1%40site-a:secret@broker.example.com', siteId: 'site-a', nodeId: 'node-1' },
    new FakePublisherConnectionManager(channel) as never,
  );

  await publisher.publishNodeHello(createHelloMessage());
  await publisher.publishNodeHeartbeat(createHeartbeatMessage());

  assert.deepEqual(
    channel.published.map(({ exchange, routingKey, options }) => [exchange, routingKey, options.type, options.userId]),
    [
      ['mcp.edge.events', 'edge.event.hello.site-a.node-1', 'node.hello', 'node-1@site-a'],
      ['mcp.edge.events', 'edge.event.heartbeat.site-a.node-1', 'node.heartbeat', 'node-1@site-a'],
    ],
  );
  assert.equal(validateEnvelope(JSON.parse(channel.published[0]!.content.toString('utf8'))).messageType, 'node.hello');
});

test('publisher refuses a lifecycle message whose digest does not match, before publishing', async () => {
  const channel = new FakePublisherChannel(true);
  const publisher = new EdgeEventPublisher({ amqpUrl: 'amqp://node-1:secret@broker.example.com' }, new FakePublisherConnectionManager(channel) as never);
  const hello = createHelloMessage();

  await assert.rejects(
    () => publisher.publishNodeHello({ ...hello, payload: { ...hello.payload, capabilitiesDigest: `sha256:${'0'.repeat(64)}` } }),
    /invalid node\.hello envelope/,
  );
  assert.equal(channel.publishCalls, 0);
});

test('mock edge channel records node lifecycle events', async () => {
  const channel = createMockEdgeChannel({ amqpUrl: 'amqp://node-1:secret@broker.example.com', siteId: 'site-a', nodeId: 'node-1' });

  await channel.publisher.publishNodeHello(createHelloMessage());
  await channel.publisher.publishNodeHeartbeat(createHeartbeatMessage());

  assert.deepEqual(
    channel.getPublishedEvents().map((event) => [event.routingKey, event.message.messageType, event.userId]),
    [
      ['edge.event.hello.site-a.node-1', 'node.hello', 'node-1'],
      ['edge.event.heartbeat.site-a.node-1', 'node.heartbeat', 'node-1'],
    ],
  );
  await channel.close();
});

test('a lifecycle message delivered on a job queue is rejected without requeue and never reaches the handler', async () => {
  const channel = new FakeConsumerChannel();
  const consumer = new EdgeJobConsumer(
    { amqpUrl: 'amqp://test.local', siteId: 'site-a', nodeId: 'node-1', runtimeConsumerVersion: '0.1.0' },
    new FakeConsumerConnectionManager(channel) as never,
  );
  let handled = false;
  consumer.onJob(async () => {
    handled = true;
  });
  await consumer.start();

  channel.emitMessage({
    fields: { deliveryTag: 7, routingKey: 'edge.job.site-a.node-1.task.package-install', redelivered: false },
    content: Buffer.from(JSON.stringify(createHelloMessage()), 'utf8'),
  });
  await new Promise((resolve) => setTimeout(resolve, 0));

  assert.equal(handled, false);
  assert.equal(channel.nackCount, 1);
  assert.equal(channel.lastNackDeliveryTag, 7);
  await consumer.stop();
});

// --- Conventions: deploy jobs (package.install) --------------------------------------------------

test('parsePackageInstallJob returns the release, the signed URL and the expected dist hash', () => {
  const job = createPackageInstallJob();
  assert.equal(validateEnvelope(job).messageType, 'execution.job');
  assert.equal(isPackageInstallJob(job), true);
  assert.equal(isToolJob(job), false);

  const parsed = parsePackageInstallJob(job);
  assert.equal(parsed.job, job);
  assert.deepEqual(parsed.release, { packageId: 'net-tools', packageVersion: '1.2.0', releaseId: 'rel-net-tools-1.2.0' });
  assert.deepEqual(parsed.download, { url: signedDownloadUrl, expiresAt: '2026-11-15T10:00:00Z' });
  assert.equal(parsed.distHash, `mcpod-dist-v2:${'a'.repeat(64)}`);

  // expiresAt is optional, and a packageAssignment naming the same release is accepted.
  const minimal = createPackageInstallJob({ download: { url: signedDownloadUrl } });
  assert.equal(parsePackageInstallJob(minimal).download.expiresAt, undefined);
  for (const packageAssignment of [
    { packageId: 'net-tools', packageVersion: '1.2.0', releaseId: 'rel-net-tools-1.2.0' },
    { packageId: 'net-tools', packageVersion: '1.2.0' },
  ]) {
    assert.deepEqual(parsePackageInstallJob({ ...job, payload: { ...job.payload, packageAssignment } }).release, parsed.release);
  }
});

test('parsePackageInstallJob refuses every way a deploy job can be malformed, without echoing the URL', () => {
  const cases: Array<[label: string, job: ReturnType<typeof createPackageInstallJob>, issue: RegExp]> = [
    ['http URL', createPackageInstallJob({ download: { url: signedDownloadUrl.replace('https:', 'http:') } }), /input\.download\.url: must be an absolute https URL/],
    ['relative URL', createPackageInstallJob({ download: { url: '/releases/net-tools.tgz' } }), /input\.download\.url/],
    ['URL with credentials', createPackageInstallJob({ download: { url: 'https://user:pass@packages.example.com/net-tools.tgz' } }), /must not carry a username or password/],
    ['no download', createPackageInstallJob({ download: undefined }), /input\.download/],
    ['bad expiresAt', createPackageInstallJob({ download: { url: signedDownloadUrl, expiresAt: '2026-11-15T10:00Z' } }), /input\.download\.expiresAt/],
    ['no dist hash', createPackageInstallJob({ distHash: undefined }), /input\.distHash/],
    ['upper-case dist hash', createPackageInstallJob({ distHash: `mcpod-dist-v2:${'A'.repeat(64)}` }), /input\.distHash: must be an algorithm tag/],
    ['dist hash with whitespace', createPackageInstallJob({ distHash: ` mcpod-dist-v2:${'a'.repeat(64)}` }), /input\.distHash/],
    ['no releaseId', createPackageInstallJob({ release: { packageId: 'net-tools', packageVersion: '1.2.0' } }), /input\.release\.releaseId/],
    [
      'packageAssignment of another release',
      createPackageInstallJob({}, { payload: { ...createPackageInstallJob().payload, packageAssignment: { packageId: 'net-tools', packageVersion: '1.1.0' } } }),
      /packageAssignment: names another release/,
    ],
    [
      'packageAssignment with another releaseId',
      createPackageInstallJob({}, { payload: { ...createPackageInstallJob().payload, packageAssignment: { packageId: 'net-tools', packageVersion: '1.2.0', releaseId: 'rel-other' } } }),
      /packageAssignment\.releaseId: differs/,
    ],
    [
      'a tool job',
      { ...createPackageInstallJob(), payload: { ...createPackageInstallJob().payload, capability: { kind: 'tool', name: 'package.install' } } },
      /capability must be \{kind:'task', name:'package\.install'\}/,
    ],
  ];

  for (const [label, job, issue] of cases) {
    assert.throws(
      () => parsePackageInstallJob(job),
      (error: unknown) => {
        assert.ok(error instanceof EdgeConventionError, label);
        assert.equal(error.convention, 'package.install', label);
        assert.match(error.issues.join('; '), issue, label);
        assert.ok(!error.message.includes('not-a-real-signature'), `${label}: the error echoes the signed URL`);
        return true;
      },
      label,
    );
  }
});

test('packageInstallResultOutputSchema accepts the deploy result and rejects an incomplete one', () => {
  const release = { packageId: 'net-tools', packageVersion: '1.2.0', releaseId: 'rel-net-tools-1.2.0' };
  const distHash = `mcpod-dist-v2:${'a'.repeat(64)}`;

  assert.equal(packageInstallResultOutputSchema.safeParse({ release, distHash, alreadyInstalled: false }).success, true);
  assert.equal(packageInstallResultOutputSchema.safeParse({ release, distHash }).success, false);
  assert.equal(packageInstallResultOutputSchema.safeParse({ release: { packageId: 'net-tools' }, distHash, alreadyInstalled: true }).success, false);
  assert.equal(packageInstallResultOutputSchema.safeParse({ release, distHash: 'not-a-hash', alreadyInstalled: true }).success, false);
});

// --- Conventions: tool jobs ----------------------------------------------------------------------

test('parseToolJob returns the tool name, its arguments, the timeout and the idempotency key', () => {
  const job = createExecutionJob();
  const parsed = parseToolJob(job);

  assert.equal(isToolJob(job), true);
  assert.equal(isPackageInstallJob(job), false);
  assert.equal(parsed.name, 'search.documents');
  assert.equal(parsed.arguments, job.payload.input);
  assert.equal(parsed.timeoutMs, 30000);
  assert.equal(parsed.idempotencyKey, 'job-1-attempt-1');

  for (const name of ['getUser', 'DATA_EXPORT_v2', 'admin.tools.list', 'mcp_od_net_ping', 'a'.repeat(128)]) {
    assert.equal(parseToolJob({ ...job, payload: { ...job.payload, capability: { kind: 'tool', name } } }).name, name);
  }
});

test('parseToolJob refuses a non-tool capability and names that break the MCP tool-name rule', () => {
  const job = createExecutionJob();

  assert.throws(() => parseToolJob(createPackageInstallJob()), (error: unknown) => error instanceof EdgeConventionError && /capability\.kind must be 'tool'/.test(error.message));

  for (const name of ['search documents', 'tool/with/slashes', 'résumé', 'a'.repeat(129), 'comma,name']) {
    assert.throws(
      () => parseToolJob({ ...job, payload: { ...job.payload, capability: { kind: 'tool', name } } }),
      (error: unknown) => error instanceof EdgeConventionError && error.convention === 'tool' && /1 to 128 characters/.test(error.message),
      name,
    );
  }
});

test('parseToolResultOutput takes a complete MCP CallToolResult and refuses input_required', () => {
  const content = [{ type: 'text', text: 'pong' }];

  assert.deepEqual(parseToolResultOutput({ content }).content, content);
  assert.equal(parseToolResultOutput({ content, isError: true }).isError, true);
  assert.equal(parseToolResultOutput({ content: [], resultType: 'complete', structuredContent: [1, 2] }).resultType, 'complete');
  // structuredContent may be any JSON value, as MCP 2026-07-28 allows.
  assert.equal(parseToolResultOutput({ content, structuredContent: 'plain' }).structuredContent, 'plain');

  for (const [label, output] of [
    ['input_required', { resultType: 'input_required', inputRequests: {}, content }],
    ['no content', { isError: false }],
    ['a content block without a type', { content: [{ text: 'pong' }] }],
    ['isError not a boolean', { content, isError: 'yes' }],
  ] as const) {
    assert.throws(() => parseToolResultOutput(output), EdgeConventionError, label);
  }
});

test('edgeFailureCodes are distinct upper-case codes', () => {
  const codes = Object.values(edgeFailureCodes);
  assert.equal(new Set(codes).size, codes.length);
  for (const code of codes) {
    assert.match(code, /^[A-Z][A-Z_]*[A-Z]$/);
  }
});

// --- Bounded delivery attempts --------------------------------------------------------------------

const settle = () => new Promise((resolve) => setTimeout(resolve, 0));

function jobDelivery(deliveryTag: number, redelivered: boolean, job = createExecutionJob(), headers: Record<string, unknown> = {}): FakeDelivery {
  return {
    fields: { deliveryTag, routingKey: 'edge.job.site-a.node-1.tool.search-documents', redelivered },
    properties: { headers },
    content: Buffer.from(JSON.stringify(job), 'utf8'),
  };
}

class RecordingFailurePublisher {
  public readonly failures: JobFailureEvent[] = [];

  public error?: Error;

  // Set by throwingConsumer to the channel's log, so a test sees publishes and settlements in order.
  public log: string[] = [];

  public async publishFailure(message: JobFailureEvent): Promise<void> {
    if (this.error) {
      throw this.error;
    }

    // The event is checked as the real publisher would check it.
    assert.equal(validateEnvelope(message).messageType, 'execution.failure');
    this.failures.push(message);
    this.log.push(`failure ${message.payload.jobId}`);
  }
}

// A consumer whose handler throws on every delivery. Pass `null` for a consumer with no publisher.
function throwingConsumer(config: Partial<Parameters<typeof resolveConfig>[0]> = {}, publisher: RecordingFailurePublisher | null = new RecordingFailurePublisher()) {
  const log: string[] = [];
  const channel = new FakeConsumerChannel(log);
  if (publisher) {
    publisher.log = log;
  }
  const consumer = new EdgeJobConsumer(
    { amqpUrl: 'amqp://test.local', siteId: 'site-a', nodeId: 'node-1', runtimeConsumerVersion: '0.1.0', ...config },
    new FakeConsumerConnectionManager(channel) as never,
    publisher ? { failurePublisher: publisher } : {},
  );
  const contexts: JobDeliveryContext[] = [];
  consumer.onJob(async (_job, delivery) => {
    contexts.push(delivery);
    throw new Error('handler bug');
  });
  return { channel, consumer, contexts, log, publisher };
}

test('a handler that always throws is requeued once by default, then reported and dead-lettered', async () => {
  assert.equal(defaultMaxDeliveryAttempts, 2);
  assert.equal(resolveConfig({ amqpUrl: 'amqp://test.local' }).maxDeliveryAttempts, 2);
  const { channel, consumer, contexts, log, publisher } = throwingConsumer();
  await consumer.start();

  channel.emitMessage(jobDelivery(1, false));
  await settle();
  assert.deepEqual(channel.settlements, ['nack 1 requeue=true']);
  assert.equal(publisher?.failures.length, 0);

  channel.emitMessage(jobDelivery(2, true));
  await settle();
  // The failure is published before the job is rejected without requeue.
  assert.deepEqual(log, ['nack 1 requeue=true', 'failure job-1', 'nack 2 requeue=false']);
  assert.deepEqual(contexts.map((context) => context.deliveryAttempt), [1, 2]);

  const [failure] = publisher!.failures;
  assert.equal(failure?.payload.code, 'DELIVERY_ATTEMPTS_EXHAUSTED');
  assert.equal(failure?.payload.code, edgeFailureCodes.deliveryAttemptsExhausted);
  assert.equal(failure?.payload.retryable, false);
  assert.equal(failure?.payload.jobId, 'job-1');
  assert.equal(failure?.payload.attemptId, 'attempt-1');
  assert.equal(failure?.payload.siteId, 'site-a');
  assert.equal(failure?.payload.nodeId, 'node-1');
  // A code and the error's class name; the handler's message is not sent by default.
  assert.deepEqual(failure?.payload.details, { deliveryAttempt: 2, maxDeliveryAttempts: 2, handlerError: { code: 'HANDLER_ERROR', name: 'Error' } });
  assert.ok(!JSON.stringify(failure).includes('handler bug'), 'the failure event carries the handler error message');
  assert.deepEqual(failure?.producer, { service: edgeLibraryName, version: edgeLibraryVersion });
  assert.equal((await consumer.health()).details?.pendingDeliveries, 0);
  await consumer.stop();
});

test('maxDeliveryAttempts counts this consumer\'s own requeues of the same job attempt', async () => {
  const { channel, consumer, contexts, publisher } = throwingConsumer({ maxDeliveryAttempts: 3 });
  await consumer.start();

  // Each redelivery of one message: the broker's flag alone would say "at least 2" every time.
  channel.emitMessage(jobDelivery(1, false));
  await settle();
  channel.emitMessage(jobDelivery(2, true));
  await settle();
  channel.emitMessage(jobDelivery(3, true));
  await settle();

  assert.deepEqual(contexts.map((context) => context.deliveryAttempt), [1, 2, 3]);
  assert.deepEqual(channel.settlements, ['nack 1 requeue=true', 'nack 2 requeue=true', 'nack 3 requeue=false']);
  assert.equal(publisher?.failures.length, 1);
  assert.equal(publisher?.failures[0]?.payload.details?.deliveryAttempt, 3);

  // A new attempt of the job (a new message) starts again from 1.
  const retry = createExecutionJob({ messageId: 'job-message-2', payload: { ...createExecutionJob().payload, attemptId: 'attempt-2' } });
  channel.emitMessage(jobDelivery(4, false, retry));
  await settle();
  assert.equal(contexts.at(-1)?.deliveryAttempt, 1);
  assert.equal(channel.settlements.at(-1), 'nack 4 requeue=true');
  await consumer.stop();
});

test('a redelivered job with no local count is attempt 2, and x-delivery-count adds broker returns', async () => {
  const { channel, consumer, contexts, publisher } = throwingConsumer({ maxDeliveryAttempts: 4 });
  await consumer.start();

  // Redelivered after a restart or a dropped connection: this process never saw it.
  channel.emitMessage(jobDelivery(1, true));
  await settle();
  // A quorum queue counted two returns of a channel that closed with it unacked.
  const other = createExecutionJob({ messageId: 'job-message-3', payload: { ...createExecutionJob().payload, jobId: 'job-3', attemptId: 'attempt-3' } });
  channel.emitMessage(jobDelivery(2, true, other, { 'x-delivery-count': 2 }));
  await settle();
  // A header that is not a count is ignored.
  const third = createExecutionJob({ messageId: 'job-message-4', payload: { ...createExecutionJob().payload, jobId: 'job-4', attemptId: 'attempt-4' } });
  channel.emitMessage(jobDelivery(3, false, third, { 'x-delivery-count': 'many' }));
  await settle();

  assert.deepEqual(contexts.map((context) => context.deliveryAttempt), [2, 3, 1]);
  assert.deepEqual(channel.settlements, ['nack 1 requeue=true', 'nack 2 requeue=true', 'nack 3 requeue=true']);

  channel.emitMessage(jobDelivery(4, true, other, { 'x-delivery-count': 2 }));
  await settle();
  // 1 + 2 broker returns + 1 requeue by this consumer = 4, the last attempt.
  assert.equal(contexts.at(-1)?.deliveryAttempt, 4);
  assert.equal(channel.settlements.at(-1), 'nack 4 requeue=false');
  assert.equal(publisher?.failures[0]?.payload.jobId, 'job-3');
  await consumer.stop();
});

test('maxDeliveryAttempts 1 dead-letters on the first throw', async () => {
  const { channel, consumer, publisher } = throwingConsumer({ maxDeliveryAttempts: 1 });
  await consumer.start();
  channel.emitMessage(jobDelivery(1, false));
  await settle();
  assert.deepEqual(channel.settlements, ['nack 1 requeue=false']);
  assert.equal(publisher?.failures.length, 1);
  await consumer.stop();
});

test('the last attempt is dead-lettered even when the failure cannot be reported, and health says why', async () => {
  const failing = new RecordingFailurePublisher();
  failing.error = new Error('broker refused the publish');
  const cases: Array<[label: string, setup: ReturnType<typeof throwingConsumer>, reason: RegExp]> = [
    ['publish fails', throwingConsumer({ maxDeliveryAttempts: 1 }, failing), /execution\.failure for job job-1 was not published: broker refused the publish/],
    ['no publisher', throwingConsumer({ maxDeliveryAttempts: 1 }, null), /no failure publisher/],
    ['no nodeId', throwingConsumer({ maxDeliveryAttempts: 1, nodeId: undefined, nodeGroup: 'group-a' }), /no nodeId/],
  ];

  for (const [label, { channel, consumer }, reason] of cases) {
    await consumer.start();
    channel.emitMessage(jobDelivery(1, false));
    await settle();
    assert.deepEqual(channel.settlements, ['nack 1 requeue=false'], label);
    const health = await consumer.health();
    assert.equal(health.details?.failureReportErrors, 1, label);
    assert.match(String(health.details?.lastFailureReportError), reason, label);
    await consumer.stop();
  }
});

test('a handler that settles its delivery and then throws keeps its own outcome', async () => {
  const channel = new FakeConsumerChannel();
  const publisher = new RecordingFailurePublisher();
  const consumer = new EdgeJobConsumer(
    { amqpUrl: 'amqp://test.local', siteId: 'site-a', nodeId: 'node-1', runtimeConsumerVersion: '0.1.0', maxDeliveryAttempts: 1 },
    new FakeConsumerConnectionManager(channel) as never,
    { failurePublisher: publisher },
  );
  consumer.onJob(async (_job, delivery) => {
    await consumer.ack(delivery);
    throw new Error('after ack');
  });
  await consumer.start();
  channel.emitMessage(jobDelivery(1, false));
  await settle();
  assert.deepEqual(channel.settlements, ['ack 1']);
  assert.equal(publisher.failures.length, 0);
  await consumer.stop();
});

test('a handler\'s own requeue counts toward deliveryAttempt, but is not limited by the consumer', async () => {
  const channel = new FakeConsumerChannel();
  const consumer = new EdgeJobConsumer(
    { amqpUrl: 'amqp://test.local', siteId: 'site-a', nodeId: 'node-1', runtimeConsumerVersion: '0.1.0' },
    new FakeConsumerConnectionManager(channel) as never,
  );
  const attempts: Array<number | undefined> = [];
  consumer.onJob(async (_job, delivery) => {
    attempts.push(delivery.deliveryAttempt);
    await consumer.nack(delivery, { requeue: true });
  });
  await consumer.start();
  for (const [tag, redelivered] of [[1, false], [2, true], [3, true]] as const) {
    channel.emitMessage(jobDelivery(tag, redelivered));
    await settle();
  }
  assert.deepEqual(attempts, [1, 2, 3]);
  assert.deepEqual(channel.settlements, ['nack 1 requeue=true', 'nack 2 requeue=true', 'nack 3 requeue=true']);
  await consumer.stop();
});

test('validateConfig checks maxDeliveryAttempts and producer, and the failure uses a configured producer', async () => {
  assert.deepEqual(validateConfig({ amqpUrl: 'amqp://test.local', maxDeliveryAttempts: 0 }), ['maxDeliveryAttempts must be a positive integer']);
  assert.deepEqual(validateConfig({ amqpUrl: 'amqp://test.local', maxDeliveryAttempts: 1.5 }), ['maxDeliveryAttempts must be a positive integer']);
  assert.deepEqual(validateConfig({ amqpUrl: 'amqp://test.local', producer: { service: '', version: '1' } }), ['producer.service and producer.version must be non-empty strings']);
  assert.deepEqual(validateConfig({ amqpUrl: 'amqp://test.local', producer: { service: 'w', version: '1', instanceId: '' } }), ['producer.instanceId must be a non-empty string when set']);

  const producer = { service: 'edge-worker', version: '3.1.0', instanceId: 'worker-7' };
  const { channel, consumer, publisher } = throwingConsumer({ maxDeliveryAttempts: 1, producer });
  await consumer.start();
  channel.emitMessage(jobDelivery(1, false));
  await settle();
  assert.deepEqual(publisher?.failures[0]?.producer, producer);
  await consumer.stop();
});

// Runs one job whose handler throws `thrown` on its only attempt, and returns the failure's details.
async function reportedHandlerError(thrown: unknown, config: Partial<Parameters<typeof resolveConfig>[0]> = {}) {
  const channel = new FakeConsumerChannel();
  const publisher = new RecordingFailurePublisher();
  const consumer = new EdgeJobConsumer(
    { amqpUrl: 'amqp://test.local', siteId: 'site-a', nodeId: 'node-1', runtimeConsumerVersion: '0.1.0', maxDeliveryAttempts: 1, ...config },
    new FakeConsumerConnectionManager(channel) as never,
    { failurePublisher: publisher },
  );
  consumer.onJob(async () => {
    throw thrown;
  });
  await consumer.start();
  channel.emitMessage(jobDelivery(1, false));
  await settle();
  await consumer.stop();
  assert.deepEqual(channel.settlements, ['nack 1 requeue=false']);
  const [failure] = publisher.failures;
  assert.ok(failure);
  return { failure, handlerError: failure.payload.details?.handlerError as { code: string; name?: string; description?: string } };
}

test('by default the failure event carries no handler error text, only a code and a class name', async () => {
  const secret = 'connect ECONNREFUSED amqp://node-1:s3cret@broker.internal:5672 at /srv/edge/secrets.json?token=abc';

  class PackageStoreError extends Error {
    public override name = 'PackageStoreError';
  }

  const cases: Array<[label: string, thrown: unknown, expected: { code: string; name?: string }]> = [
    ['an Error', new Error(secret), { code: 'HANDLER_ERROR', name: 'Error' }],
    ['a subclass', new PackageStoreError(secret), { code: 'HANDLER_ERROR', name: 'PackageStoreError' }],
    ['a TypeError', new TypeError(secret), { code: 'HANDLER_ERROR', name: 'TypeError' }],
    // A name that is not a plain identifier could carry text of its own, so it is left out.
    ['a name with text in it', Object.assign(new Error(secret), { name: `Error: ${secret}` }), { code: 'HANDLER_ERROR' }],
    ['a thrown string', secret, { code: 'HANDLER_ERROR' }],
    ['a thrown object', { message: secret }, { code: 'HANDLER_ERROR' }],
  ];

  for (const [label, thrown, expected] of cases) {
    const { failure, handlerError } = await reportedHandlerError(thrown);
    assert.deepEqual(handlerError, expected, label);
    const wire = JSON.stringify(failure);
    for (const fragment of ['s3cret', 'broker.internal', '/srv/edge', 'token=abc', 'ECONNREFUSED']) {
      assert.ok(!wire.includes(fragment), `${label}: the failure event carries '${fragment}'`);
    }
  }
});

test('describeHandlerError opts in to a description, cut to maxHandlerErrorDescriptionLength', async () => {
  assert.equal(maxHandlerErrorDescriptionLength, 1000);

  const short = await reportedHandlerError(new Error('disk full'), { describeHandlerError: (error) => `cannot write: ${(error as Error).message}` });
  assert.deepEqual(short.handlerError, { code: 'HANDLER_ERROR', name: 'Error', description: 'cannot write: disk full' });

  const long = await reportedHandlerError(new Error('x'), { describeHandlerError: () => 'y'.repeat(5000) });
  assert.equal(long.handlerError.description, 'y'.repeat(1000));

  // The cut never leaves half of a surrogate pair.
  const emoji = await reportedHandlerError(new Error('x'), { describeHandlerError: () => `${'y'.repeat(999)}\u{1F600}tail` });
  assert.equal(emoji.handlerError.description, 'y'.repeat(999));

  // A describer that returns nothing, an empty string or a non-string adds nothing, and one that
  // throws does not stop the report.
  for (const describeHandlerError of [() => undefined, () => '', () => 42 as unknown as string, () => {
    throw new Error('describer bug');
  }]) {
    const { handlerError } = await reportedHandlerError(new Error('x'), { describeHandlerError });
    assert.deepEqual(handlerError, { code: 'HANDLER_ERROR', name: 'Error' });
  }

  assert.deepEqual(validateConfig({ amqpUrl: 'amqp://test.local', describeHandlerError: 'yes' as never }), ['describeHandlerError must be a function']);
});

test('mock edge channel requeues a throwing handler until the last attempt, then reports and dead-letters it', async () => {
  const channel = createMockEdgeChannel({ amqpUrl: 'amqp://node-1:secret@broker.example.com', siteId: 'site-a', nodeId: 'node-1' });
  channel.consumer.onJob(async () => {
    throw new Error('handler bug');
  });
  await channel.consumer.start();

  const first = await channel.dispatchJob(createExecutionJob());
  const second = await channel.dispatchJob(createExecutionJob(), { redelivered: true });

  assert.equal(first.deliveryAttempt, 1);
  assert.equal(second.deliveryAttempt, 2);
  assert.deepEqual(channel.getDeliveries().map((delivery) => [delivery.status, delivery.requeue]), [['nacked', true], ['nacked', false]]);
  const [event] = channel.getPublishedEvents();
  assert.equal(event?.routingKey, 'edge.event.failure.site-a.node-1');
  assert.ok(event?.message.messageType === 'execution.failure');
  assert.equal(event.message.payload.code, 'DELIVERY_ATTEMPTS_EXHAUSTED');
  assert.equal(event.message.payload.retryable, false);
  await channel.close();
});

// --- Publisher confirms ---------------------------------------------------------------------------

test('every publish waits for the broker confirm before it resolves', async () => {
  const channel = new FakePublisherChannel(true, 'never');
  const publisher = new EdgeEventPublisher({ amqpUrl: 'amqp://node-1:secret@broker.example.com', confirmTimeoutMs: 1000 }, new FakePublisherConnectionManager(channel) as never);
  let resolved = false;
  const publishing = publisher.publishResult(createResultMessage()).then(() => {
    resolved = true;
  });

  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(channel.publishCalls, 1);
  assert.equal(resolved, false, 'the publish resolved before the broker confirmed it');
  assert.equal((await publisher.health()).publisherActive, false);

  channel.confirm = 'ack';
  await assert.rejects(publishing, (error: unknown) => error instanceof EdgePublishError && error.reason === 'timeout');
});

test('a nacked, unconfirmed or refused publish rejects with an EdgePublishError naming the event', async () => {
  const cases: Array<[confirm: FakeConfirm, reason: EdgePublishError['reason'], text: RegExp]> = [
    ['nack', 'nacked', /the broker nacked execution\.result result-message-1; treat it as not delivered/],
    ['close', 'channel-closed', /the channel closed before the broker confirmed execution\.result result-message-1/],
    ['never', 'timeout', /did not confirm execution\.result result-message-1 within 30ms/],
    ['throw', 'channel-closed', /could not publish execution\.result result-message-1: Channel closed/],
  ];

  for (const [confirm, reason, text] of cases) {
    const channel = new FakePublisherChannel(true, confirm);
    const publisher = new EdgeEventPublisher({ amqpUrl: 'amqp://node-1:secret@broker.example.com', confirmTimeoutMs: 30 }, new FakePublisherConnectionManager(channel) as never);

    await assert.rejects(
      () => publisher.publishResult(createResultMessage()),
      (error: unknown) => {
        assert.ok(error instanceof EdgePublishError, confirm);
        assert.equal(error.reason, reason, confirm);
        assert.equal(error.messageType, 'execution.result', confirm);
        assert.equal(error.messageId, 'result-message-1', confirm);
        assert.match(error.message, text, confirm);
        return true;
      },
    );
    assert.equal((await publisher.health()).publisherActive, false, confirm);
  }

  assert.deepEqual(validateConfig({ amqpUrl: 'amqp://test.local', confirmTimeoutMs: 0 }), ['confirmTimeoutMs must be a positive integer']);
  assert.equal(resolveConfig({ amqpUrl: 'amqp://test.local' }).confirmTimeoutMs, 10000);
});

test('under backpressure a publish waits for both the drain and the confirm', async () => {
  const channel = new FakePublisherChannel(false, 'nack');
  const publisher = new EdgeEventPublisher({ amqpUrl: 'amqp://node-1:secret@broker.example.com', backpressureTimeoutMs: 200 }, new FakePublisherConnectionManager(channel) as never);
  const publishing = publisher.publishAck(createAckMessage());
  setTimeout(() => channel.emit('drain'), 20);
  // The channel drained, but the broker nacked the event: the publish still fails.
  await assert.rejects(publishing, (error: unknown) => error instanceof EdgePublishError && error.reason === 'nacked');
});

test('a worker whose result publish is nacked does not ack the job: it is requeued, then reported and dead-lettered', async () => {
  const consumerChannel = new FakeConsumerChannel();
  // The broker nacks results and accepts the failure report.
  const publisherChannel = new FakePublisherChannel(true, (routingKey) => (routingKey.startsWith('edge.event.result.') ? 'nack' : 'ack'));
  const config = { amqpUrl: 'amqp://node-1:secret@broker.example.com', siteId: 'site-a', nodeId: 'node-1', runtimeConsumerVersion: '0.1.0' };
  const publisher = new EdgeEventPublisher(config, new FakePublisherConnectionManager(publisherChannel) as never);
  const consumer = new EdgeJobConsumer(config, new FakeConsumerConnectionManager(consumerChannel) as never, { failurePublisher: publisher });
  const publishErrors: unknown[] = [];

  consumer.onJob(async (_job, delivery) => {
    try {
      await publisher.publishResult(createResultMessage());
    } catch (error) {
      publishErrors.push(error);
      throw error;
    }
    await consumer.ack(delivery);
  });
  await consumer.start();

  consumerChannel.emitMessage(jobDelivery(1, false));
  await new Promise((resolve) => setTimeout(resolve, 20));
  consumerChannel.emitMessage(jobDelivery(2, true));
  await new Promise((resolve) => setTimeout(resolve, 20));

  assert.deepEqual(consumerChannel.settlements, ['nack 1 requeue=true', 'nack 2 requeue=false']);
  assert.equal(publishErrors.length, 2);
  assert.ok(publishErrors.every((error) => error instanceof EdgePublishError && error.reason === 'nacked'));
  assert.deepEqual(publisherChannel.published.map((entry) => entry.routingKey), [
    'edge.event.result.site-a.node-1',
    'edge.event.result.site-a.node-1',
    'edge.event.failure.site-a.node-1',
  ]);
  const failure = JSON.parse(publisherChannel.published[2]!.content.toString('utf8')) as JobFailureEvent;
  assert.equal(failure.payload.code, 'DELIVERY_ATTEMPTS_EXHAUSTED');
  assert.deepEqual(failure.payload.details?.handlerError, { code: 'HANDLER_ERROR', name: 'EdgePublishError' });
  await consumer.stop();
});
