import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import test from 'node:test';

import { ZodError } from 'zod';

import {
  buildRoutingKey,
  createMockEdgeChannel,
  defaultAmqpUsername,
  EdgeEventPublisher,
  EdgeJobConsumer,
  EdgeTopologyError,
  resolveConfig,
  resolveUserId,
  validateCompatibility,
  validateConfig,
  validateEnvelope,
} from '../index.js';
import { createAckMessage, createBaseEnvelope, createExecutionJob, createFailureMessage, createProgressMessage, createResultMessage } from './fixtures.js';

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

class FakeConsumerChannel {
  public readonly calls: string[] = [];

  public consumeCount = 0;

  public bindCount = 0;

  public cancelledTags: string[] = [];

  public nackCount = 0;

  public lastNackDeliveryTag?: number;

  private consumeHandler?: (message: { fields: { deliveryTag: number; routingKey: string; redelivered: boolean }; content: Buffer } | null) => void;

  public ack(): void {
    // no-op for test double
  }

  public nack(message: { fields: { deliveryTag: number } }): void {
    this.nackCount += 1;
    this.lastNackDeliveryTag = message.fields.deliveryTag;
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
    handler: (message: { fields: { deliveryTag: number; routingKey: string; redelivered: boolean }; content: Buffer } | null) => void,
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

  public emitMessage(message: { fields: { deliveryTag: number; routingKey: string; redelivered: boolean }; content: Buffer }): void {
    assert.ok(this.consumeHandler, 'expected consume handler to be registered');
    this.consumeHandler(message);
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

class FakePublisherChannel extends EventEmitter {
  public publishCalls = 0;

  public assertExchangeCalls = 0;

  public readonly published: Array<{ exchange: string; routingKey: string; options: Record<string, unknown> }> = [];

  public constructor(private readonly accepted: boolean) {
    super();
  }

  public async assertExchange(): Promise<void> {
    this.assertExchangeCalls += 1;
  }

  public publish(exchange: string, routingKey: string, _content: Buffer, options: Record<string, unknown>): boolean {
    this.publishCalls += 1;
    this.published.push({ exchange, routingKey, options });
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
