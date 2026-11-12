import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import test, { type TestContext } from 'node:test';

import { connect } from 'amqplib';
import type { Channel, ChannelModel, ConsumeMessage } from 'amqplib';

import {
  buildConsumerQueueName,
  buildJobBindingKeys,
  buildJobRoutingKey,
  buildRoutingKey,
  createEdgeChannel,
  defaultExchangeConfig,
  EdgeChannelConnectionManager,
  EdgeJobConsumer,
  EdgeTopologyError,
  validateEnvelope,
} from '../index.js';
import {
  createAckMessage,
  createExecutionJob,
  createFailureMessage,
  createProgressMessage,
  createResultMessage,
} from './fixtures.js';

const candidateAmqpUrls = [
  process.env.AMQP_INTEGRATION_URL,
  'amqp://mcp:discovery@127.0.0.1:5672',
  'amqp://guest:guest@127.0.0.1:5672',
].filter((value, index, values): value is string => Boolean(value) && values.indexOf(value) === index);

async function connectToBroker(): Promise<{ connection: ChannelModel; amqpUrl: string }> {
  const failures: string[] = [];

  for (const amqpUrl of candidateAmqpUrls) {
    try {
      const connection = await connect(amqpUrl);
      return { connection, amqpUrl };
    } catch (error) {
      failures.push(`${amqpUrl}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  throw new Error(`unable to connect to RabbitMQ using candidate URLs: ${failures.join(' | ')}`);
}

function waitForMessage<T>(
  channel: Channel,
  queueName: string,
  timeoutMs: number,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let consumerTag: string | undefined;
    const timeout = setTimeout(() => {
      if (consumerTag) {
        void channel.cancel(consumerTag).catch(() => undefined);
      }

      reject(new Error(`timed out waiting for RabbitMQ message on ${queueName}`));
    }, timeoutMs);

    void channel.consume(
      queueName,
      (message) => {
        if (!message) {
          return;
        }

        clearTimeout(timeout);
        channel.ack(message);
        if (consumerTag) {
          void channel.cancel(consumerTag).catch(() => undefined);
        }
        resolve(JSON.parse(message.content.toString('utf8')) as T);
      },
      { noAck: false },
    ).then((result) => {
      consumerTag = result.consumerTag;
    }).catch((error) => {
      clearTimeout(timeout);
      reject(error);
    });
  });
}

interface IntegrationHarness {
  amqpUrl: string;
  runId: string;
  siteId: string;
  nodeId: string;
  jobsExchange: string;
  eventsExchange: string;
  deadLetterExchange: string;
  consumerQueueName: string;
  controlQueueName: string;
  controlConnection: ChannelModel;
  controlChannel: Channel;
}

async function createHarness(prefix: string): Promise<IntegrationHarness> {
  const runId = crypto.randomUUID().slice(0, 8);
  const siteId = `site-${runId}`;
  const nodeId = `node-${runId}`;
  const jobsExchange = `mcp.edge.jobs.${prefix}.${runId}`;
  const eventsExchange = `mcp.edge.events.${prefix}.${runId}`;
  const deadLetterExchange = `mcp.edge.dlx.${prefix}.${runId}`;
  const consumerQueueName = `mcp.edge.consumer.${siteId}.${nodeId}.${runId}`;
  const controlQueueName = `mcp.edge.control.${siteId}.${nodeId}.${runId}`;
  const { connection: controlConnection, amqpUrl } = await connectToBroker();
  // The broker closes the connection on a refused declare; without a listener that 'error' event
  // is an uncaught exception instead of a test failure.
  controlConnection.on('error', () => undefined);
  try {
    return await declareHarness(controlConnection, amqpUrl, {
      runId, siteId, nodeId, jobsExchange, eventsExchange, deadLetterExchange, consumerQueueName, controlQueueName,
    });
  } catch (error) {
    await controlConnection.close().catch(() => undefined);
    throw error;
  }
}

async function declareHarness(
  controlConnection: ChannelModel,
  amqpUrl: string,
  names: Omit<IntegrationHarness, 'amqpUrl' | 'controlConnection' | 'controlChannel'>,
): Promise<IntegrationHarness> {
  const { runId, siteId, nodeId, jobsExchange, eventsExchange, deadLetterExchange, consumerQueueName, controlQueueName } = names;
  const controlChannel = await controlConnection.createChannel();

  await controlChannel.assertExchange(jobsExchange, 'topic', { durable: true });
  await controlChannel.assertExchange(eventsExchange, 'topic', { durable: true });
  await controlChannel.assertExchange(deadLetterExchange, 'topic', { durable: true });
  // Exclusive, not durable:false + autoDelete: RabbitMQ 4.x refuses transient non-exclusive queues
  // (deprecated feature transient_nonexcl_queues). An exclusive queue is deleted with its connection.
  await controlChannel.assertQueue(controlQueueName, {
    durable: false,
    exclusive: true,
  });
  await controlChannel.bindQueue(
    controlQueueName,
    eventsExchange,
    buildRoutingKey('execution.ack', { siteId, nodeId }).replace('ack', '*'),
  );

  return {
    amqpUrl,
    runId,
    siteId,
    nodeId,
    jobsExchange,
    eventsExchange,
    deadLetterExchange,
    consumerQueueName,
    controlQueueName,
    controlConnection,
    controlChannel,
  };
}

async function destroyHarness(harness: IntegrationHarness): Promise<void> {
  await harness.controlChannel.deleteQueue(harness.controlQueueName).catch(() => undefined);
  for (const exchange of [harness.jobsExchange, harness.eventsExchange, harness.deadLetterExchange]) {
    await harness.controlChannel.deleteExchange(exchange).catch(() => undefined);
  }
  await harness.controlChannel.close().catch(() => undefined);
  await harness.controlConnection.close().catch(() => undefined);
}

test('live RabbitMQ integration publishes job lifecycle events end to end', { timeout: 20000 }, async (t) => {
  const harness = await createHarness('success');
  t.after(() => destroyHarness(harness));

  const edgeChannel = await createEdgeChannel({
    amqpUrl: harness.amqpUrl,
    siteId: harness.siteId,
    nodeId: harness.nodeId,
    runtimeConsumerVersion: '0.1.0',
    runtimeProducerVersion: '0.1.0',
    jobsExchange: harness.jobsExchange,
    eventsExchange: harness.eventsExchange,
    deadLetterExchange: harness.deadLetterExchange,
    consumerQueueName: harness.consumerQueueName,
    consumerQueueOptions: {
      durable: false,
      exclusive: true,
    },
    prefetchCount: 1,
    connectionName: `amqp-mcp-edge-integration-${harness.runId}`,
  });
  t.after(() => edgeChannel.close().catch(() => undefined));

  const ackPromise = waitForMessage(harness.controlChannel, harness.controlQueueName, 15000);
  const progressPromise = waitForMessage(harness.controlChannel, harness.controlQueueName, 15000);
  const resultPromise = waitForMessage(harness.controlChannel, harness.controlQueueName, 15000);

  edgeChannel.consumer.onJob(async (job, delivery) => {
    await edgeChannel.publisher.publishAck(
      createAckMessage({
        messageId: `ack-${harness.runId}`,
        payload: {
          jobId: job.payload.jobId,
          attemptId: job.payload.attemptId,
          siteId: harness.siteId,
          nodeId: harness.nodeId,
          accepted: true,
        },
      }),
    );

    await edgeChannel.publisher.publishProgress(
      createProgressMessage({
        messageId: `progress-${harness.runId}`,
        payload: {
          jobId: job.payload.jobId,
          attemptId: job.payload.attemptId,
          siteId: harness.siteId,
          nodeId: harness.nodeId,
          state: 'running',
          percent: 100,
          metrics: {
            documentsProcessed: 2,
          },
        },
      }),
    );

    await edgeChannel.publisher.publishResult(
      createResultMessage({
        messageId: `result-${harness.runId}`,
        payload: {
          jobId: job.payload.jobId,
          attemptId: job.payload.attemptId,
          siteId: harness.siteId,
          nodeId: harness.nodeId,
          completedAt: new Date().toISOString(),
          output: {
            documents: ['doc-a', 'doc-b'],
          },
        },
      }),
    );

    await edgeChannel.consumer.ack(delivery);
  });

  await edgeChannel.consumer.start();

  const jobEnvelope = createExecutionJob({
    messageId: `job-${harness.runId}`,
    payload: {
      jobId: `job-${harness.runId}`,
      attemptId: `attempt-${harness.runId}`,
      idempotencyKey: `job-${harness.runId}-attempt-${harness.runId}`,
      timeoutMs: 30000,
      target: {
        siteId: harness.siteId,
        nodeId: harness.nodeId,
      },
      createdBy: {
        controlPlane: 'example-control-plane',
      },
      capability: {
        kind: 'tool',
        name: 'search.documents',
      },
      input: {
        query: 'integration smoke test',
      },
    },
  });

  harness.controlChannel.publish(
    harness.jobsExchange,
    buildJobRoutingKey(jobEnvelope.payload.target, jobEnvelope.payload.capability),
    Buffer.from(JSON.stringify(jobEnvelope), 'utf8'),
    {
      contentType: 'application/json',
      type: jobEnvelope.messageType,
      messageId: jobEnvelope.messageId,
    },
  );

  const [ackMessage, progressMessage, resultMessage] = await Promise.all([
    ackPromise,
    progressPromise,
    resultPromise,
  ]);

  const ackEnvelope = validateEnvelope(ackMessage);
  const progressEnvelope = validateEnvelope(progressMessage);
  const resultEnvelope = validateEnvelope(resultMessage);

  assert.equal(ackEnvelope.messageType, 'execution.ack');
  assert.equal(progressEnvelope.messageType, 'execution.progress');
  assert.equal(resultEnvelope.messageType, 'execution.result');
  assert.equal(resultEnvelope.payload.jobId, jobEnvelope.payload.jobId);
});

test('live RabbitMQ integration requeues after nack and publishes failure before success', { timeout: 20000 }, async (t) => {
  const harness = await createHarness('requeue');
  t.after(() => destroyHarness(harness));
  const edgeChannel = await createEdgeChannel({
    amqpUrl: harness.amqpUrl,
    siteId: harness.siteId,
    nodeId: harness.nodeId,
    runtimeConsumerVersion: '0.1.0',
    runtimeProducerVersion: '0.1.0',
    jobsExchange: harness.jobsExchange,
    eventsExchange: harness.eventsExchange,
    deadLetterExchange: harness.deadLetterExchange,
    consumerQueueName: harness.consumerQueueName,
    consumerQueueOptions: {
      durable: false,
      exclusive: true,
    },
    prefetchCount: 1,
    connectionName: `amqp-mcp-edge-requeue-${harness.runId}`,
  });
  t.after(() => edgeChannel.close().catch(() => undefined));

  const failurePromise = waitForMessage(harness.controlChannel, harness.controlQueueName, 15000);
  const ackPromise = waitForMessage(harness.controlChannel, harness.controlQueueName, 15000);
  const resultPromise = waitForMessage(harness.controlChannel, harness.controlQueueName, 15000);
  const deliveries: Array<{ redelivered?: boolean; deliveryTag?: number }> = [];
  let attemptCount = 0;

  edgeChannel.consumer.onJob(async (job, delivery) => {
    attemptCount += 1;
    deliveries.push({
      deliveryTag: delivery.deliveryTag,
      redelivered: delivery.redelivered,
    });

    if (attemptCount === 1) {
      await edgeChannel.publisher.publishFailure(
        createFailureMessage({
          messageId: `failure-${harness.runId}`,
          payload: {
            jobId: job.payload.jobId,
            attemptId: job.payload.attemptId,
            siteId: harness.siteId,
            nodeId: harness.nodeId,
            failedAt: new Date().toISOString(),
            retryable: true,
            code: 'WORKER_RETRY',
            message: 'Synthetic transient failure before requeue',
            details: {
              phase: 'pre-processing',
            },
          },
        }),
      );

      await edgeChannel.consumer.nack(delivery, {
        requeue: true,
        reason: 'retry once',
      });
      return;
    }

    await edgeChannel.publisher.publishAck(
      createAckMessage({
        messageId: `ack-${harness.runId}`,
        payload: {
          jobId: job.payload.jobId,
          attemptId: job.payload.attemptId,
          siteId: harness.siteId,
          nodeId: harness.nodeId,
          accepted: true,
        },
      }),
    );

    await edgeChannel.publisher.publishResult(
      createResultMessage({
        messageId: `result-${harness.runId}`,
        payload: {
          jobId: job.payload.jobId,
          attemptId: job.payload.attemptId,
          siteId: harness.siteId,
          nodeId: harness.nodeId,
          completedAt: new Date().toISOString(),
          output: {
            recovered: true,
            attempts: attemptCount,
          },
        },
      }),
    );

    await edgeChannel.consumer.ack(delivery);
  });

  await edgeChannel.consumer.start();

  const jobEnvelope = createExecutionJob({
    messageId: `job-${harness.runId}`,
    payload: {
      jobId: `job-${harness.runId}`,
      attemptId: `attempt-${harness.runId}`,
      idempotencyKey: `job-${harness.runId}-attempt-${harness.runId}`,
      timeoutMs: 30000,
      target: {
        siteId: harness.siteId,
        nodeId: harness.nodeId,
      },
      createdBy: {
        controlPlane: 'example-control-plane',
      },
      capability: {
        kind: 'tool',
        name: 'search.documents',
      },
      input: {
        query: 'requeue smoke test',
      },
    },
  });

  harness.controlChannel.publish(
    harness.jobsExchange,
    buildJobRoutingKey(jobEnvelope.payload.target, jobEnvelope.payload.capability),
    Buffer.from(JSON.stringify(jobEnvelope), 'utf8'),
    {
      contentType: 'application/json',
      type: jobEnvelope.messageType,
      messageId: jobEnvelope.messageId,
    },
  );

  const [failureMessage, ackMessage, resultMessage] = await Promise.all([
    failurePromise,
    ackPromise,
    resultPromise,
  ]);

  const failureEnvelope = validateEnvelope(failureMessage);
  const ackEnvelope = validateEnvelope(ackMessage);
  const resultEnvelope = validateEnvelope(resultMessage);

  assert.equal(failureEnvelope.messageType, 'execution.failure');
  assert.equal(ackEnvelope.messageType, 'execution.ack');
  assert.equal(resultEnvelope.messageType, 'execution.result');
  assert.equal(deliveries.length, 2);
  assert.equal(deliveries[0]?.redelivered, false);
  assert.equal(deliveries[1]?.redelivered, true);
  assert.equal(resultEnvelope.payload.output.recovered, true);
});

// --- Shared helpers for the topology and node-identity tests -------------------------------------

// Runs cleanup steps in reverse order of registration, after the test, whatever its outcome. Each
// step gets 10 seconds, so a step stuck behind a broken connection cannot hang the run, and the
// steps after it still run.
class CleanupStack {
  private readonly steps: Array<() => Promise<unknown>> = [];

  public constructor(t: TestContext) {
    t.after(async () => {
      for (const step of this.steps.reverse()) {
        let timer: NodeJS.Timeout | undefined;
        const timeout = new Promise<void>((resolve) => {
          timer = setTimeout(resolve, 10_000);
        });
        await Promise.race([step().catch(() => undefined), timeout]);
        clearTimeout(timer);
      }
    });
  }

  public defer(step: () => Promise<unknown>): void {
    this.steps.push(step);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// Consumes `count` messages from a queue, acks them and returns them in arrival order.
function collectDeliveries(channel: Channel, queueName: string, count: number, timeoutMs: number): Promise<ConsumeMessage[]> {
  return new Promise<ConsumeMessage[]>((resolve, reject) => {
    const received: ConsumeMessage[] = [];
    let consumerTag: string | undefined;
    const finish = (error?: Error) => {
      clearTimeout(timeout);
      if (consumerTag) {
        void channel.cancel(consumerTag).catch(() => undefined);
      }
      if (error) {
        reject(error);
      } else {
        resolve(received);
      }
    };
    const timeout = setTimeout(
      () => finish(new Error(`timed out waiting for ${count} messages on ${queueName}; received ${received.length}`)),
      timeoutMs,
    );

    channel
      .consume(queueName, (message) => {
        if (!message || received.length >= count) {
          return;
        }

        channel.ack(message);
        received.push(message);
        if (received.length === count) {
          finish();
        }
      })
      .then((result) => {
        consumerTag = result.consumerTag;
        if (received.length >= count) {
          void channel.cancel(result.consumerTag).catch(() => undefined);
        }
      })
      .catch((error: Error) => finish(error));
  });
}

async function assertBrokerRefuses(
  connection: ChannelModel,
  label: string,
  operation: (channel: Channel) => Promise<unknown>,
  expected: { code: number; text: RegExp },
): Promise<void> {
  const channel = await connection.createChannel();
  // The refusal closes this channel; without a listener amqplib would close the connection too.
  channel.on('error', () => undefined);
  await assert.rejects(
    () => operation(channel),
    (error: unknown) => {
      assert.equal((error as { code?: unknown }).code, expected.code, label);
      assert.match(String((error as Error).message), expected.text, label);
      return true;
    },
    label,
  );
}

const accessRefused = { code: 403, text: /ACCESS_REFUSED/ };
const notFound = { code: 404, text: /NOT_FOUND/ };

async function withChannel<T>(connection: ChannelModel, operation: (channel: Channel) => Promise<T>): Promise<T> {
  const channel = await connection.createChannel();
  channel.on('error', () => undefined);
  try {
    return await operation(channel);
  } finally {
    await channel.close().catch(() => undefined);
  }
}

function isTopologyError(expected: { kind: 'exchange' | 'queue'; entityName: string; replyCode: number }) {
  return (error: unknown): boolean => {
    assert.ok(error instanceof EdgeTopologyError, `expected an EdgeTopologyError, got ${String(error)}`);
    assert.equal(error.kind, expected.kind);
    assert.equal(error.entityName, expected.entityName);
    assert.equal(error.replyCode, expected.replyCode);
    assert.ok(error.message.includes(`${expected.kind} '${expected.entityName}' does not exist`), error.message);
    return true;
  };
}

// --- Passive topology: what a missing entity does at startup -------------------------------------

test('passive topology: a missing queue or exchange fails startup with a clear error, and nothing is created', { timeout: 20000 }, async (t) => {
  const harness = await createHarness('passive-missing');
  const cleanup = new CleanupStack(t);
  cleanup.defer(() => destroyHarness(harness));

  const config = {
    amqpUrl: harness.amqpUrl,
    siteId: harness.siteId,
    nodeId: harness.nodeId,
    topologyMode: 'passive' as const,
    jobsExchange: harness.jobsExchange,
    eventsExchange: harness.eventsExchange,
    deadLetterExchange: harness.deadLetterExchange,
    consumerQueueName: harness.consumerQueueName,
    connectionName: `amqp-mcp-edge-passive-missing-${harness.runId}`,
  };

  // The node queue was never declared: startup names it, and does not create it.
  await assert.rejects(
    () => createEdgeChannel(config),
    isTopologyError({ kind: 'queue', entityName: harness.consumerQueueName, replyCode: 404 }),
  );
  await assertBrokerRefuses(harness.controlConnection, 'queue was not created', (channel) => channel.checkQueue(harness.consumerQueueName), notFound);

  // The queue exists (durable: the edge worker reads it over another connection), the events
  // exchange does not.
  await harness.controlChannel.assertQueue(harness.consumerQueueName, { durable: true });
  cleanup.defer(() => withChannel(harness.controlConnection, (channel) => channel.deleteQueue(harness.consumerQueueName)));
  const missingEventsExchange = `${harness.eventsExchange}.missing`;

  await assert.rejects(
    () => createEdgeChannel({ ...config, eventsExchange: missingEventsExchange }),
    isTopologyError({ kind: 'exchange', entityName: missingEventsExchange, replyCode: 404 }),
  );
  await assertBrokerRefuses(harness.controlConnection, 'exchange was not created', (channel) => channel.checkExchange(missingEventsExchange), notFound);

  // A failed check closes only its own channel: the connection stays up and nothing starts
  // reconnecting. Once the control plane declares the queue, the same consumer starts.
  const lateQueueName = `${harness.consumerQueueName}.late`;
  const connectionManager = new EdgeChannelConnectionManager({ ...config, consumerQueueName: lateQueueName });
  cleanup.defer(() => connectionManager.close());
  const consumer = new EdgeJobConsumer({ ...config, consumerQueueName: lateQueueName }, connectionManager);

  await assert.rejects(() => consumer.start(), isTopologyError({ kind: 'queue', entityName: lateQueueName, replyCode: 404 }));
  await sleep(300);

  const connectionHealth = await connectionManager.health();
  assert.equal(connectionHealth.connected, true);
  assert.equal(connectionHealth.details?.reconnecting, false);
  assert.equal(connectionHealth.details?.reconnectAttempts, 0);
  const failedHealth = await consumer.health();
  assert.equal(failedHealth.consumerActive, false);
  assert.match(String(failedHealth.details?.topologyError), /does not exist/);

  await harness.controlChannel.assertQueue(lateQueueName, { durable: true });
  cleanup.defer(() => withChannel(harness.controlConnection, (channel) => channel.deleteQueue(lateQueueName)));
  await consumer.start();

  const startedHealth = await consumer.health();
  assert.equal(startedHealth.consumerActive, true);
  assert.equal(startedHealth.details?.topologyError, undefined);
  await consumer.stop();
});

// --- Least privilege: a node user with configure '^$' ---------------------------------------------
//
// These tests create a vhost and a broker user, so they need the management HTTP API and an
// administrator account. They skip, with the reason, when it is unavailable; CI sets
// AMQP_INTEGRATION_REQUIRE_MANAGEMENT=1 so that they fail there instead.

const requireManagement = ['1', 'true'].includes(process.env.AMQP_INTEGRATION_REQUIRE_MANAGEMENT ?? '');

interface ManagementApi {
  baseUrl: string;
  username: string;
  authorization: string;
  // The administrator's AMQP URL, as the suite connected with it.
  amqpUrl: string;
}

let managementApi: Promise<ManagementApi | { unavailable: string }> | undefined;

function decodeUserinfo(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

// The management API defaults to port 15672 on the broker host, with the credentials of the AMQP
// URL the suite connected with (in CI and the docker compose file, the broker's default user).
async function discoverManagementApi(): Promise<ManagementApi | { unavailable: string }> {
  const { connection, amqpUrl } = await connectToBroker();
  await connection.close().catch(() => undefined);

  const amqp = new URL(amqpUrl);
  const base = new URL(process.env.AMQP_INTEGRATION_MANAGEMENT_URL ?? `http://${amqp.host.replace(/:\d+$/, '')}:15672`);
  const urlUser = base.username || amqp.username;
  const urlPassword = base.username ? base.password : amqp.password;
  const username = process.env.AMQP_INTEGRATION_MANAGEMENT_USERNAME ?? (urlUser ? decodeUserinfo(urlUser) : 'guest');
  const password = process.env.AMQP_INTEGRATION_MANAGEMENT_PASSWORD ?? (urlUser ? decodeUserinfo(urlPassword) : 'guest');
  base.username = '';
  base.password = '';

  const api: ManagementApi = {
    baseUrl: base.toString().replace(/\/+$/, ''),
    username,
    authorization: `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`,
    amqpUrl,
  };

  // In CI the management listener can come up after the AMQP one; wait for it there.
  const deadline = Date.now() + (requireManagement ? 20_000 : 0);

  for (;;) {
    const problem = await probeManagementApi(api);

    if (!problem) {
      return api;
    }

    if (Date.now() >= deadline) {
      return {
        unavailable:
          `RabbitMQ management API unavailable at ${api.baseUrl} (${problem}); the least-privilege tests need it ` +
          'to create a vhost and a user. Set AMQP_INTEGRATION_MANAGEMENT_URL, AMQP_INTEGRATION_MANAGEMENT_USERNAME ' +
          'and AMQP_INTEGRATION_MANAGEMENT_PASSWORD to point them at an administrator account.',
      };
    }

    await sleep(1000);
  }
}

async function probeManagementApi(api: ManagementApi): Promise<string | undefined> {
  try {
    const response = await fetch(`${api.baseUrl}/api/whoami`, {
      headers: { authorization: api.authorization },
      signal: AbortSignal.timeout(3000),
    });

    if (!response.ok) {
      return `GET /api/whoami answered ${response.status} for user '${api.username}'`;
    }

    const whoami = (await response.json()) as { name?: string; tags?: string[] | string };
    const tags = Array.isArray(whoami.tags) ? whoami.tags : String(whoami.tags ?? '').split(',');

    return tags.includes('administrator') ? undefined : `user '${api.username}' is not an administrator`;
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
}

async function managementApiOrSkip(t: TestContext): Promise<ManagementApi | undefined> {
  managementApi ??= discoverManagementApi();
  const api = await managementApi;

  if ('unavailable' in api) {
    if (requireManagement) {
      throw new Error(`${api.unavailable} AMQP_INTEGRATION_REQUIRE_MANAGEMENT is set, so this test fails instead of skipping.`);
    }

    t.skip(api.unavailable);
    return undefined;
  }

  return api;
}

async function managementRequest(api: ManagementApi, method: 'PUT' | 'DELETE', path: string, body?: unknown): Promise<void> {
  const response = await fetch(`${api.baseUrl}${path}`, {
    method,
    headers: { authorization: api.authorization, 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(10_000),
  });

  if (!response.ok && !(method === 'DELETE' && response.status === 404)) {
    throw new Error(`management API ${method} ${path} answered ${response.status}: ${await response.text()}`);
  }
}

async function managementGet<T>(api: ManagementApi, path: string): Promise<T> {
  const response = await fetch(`${api.baseUrl}${path}`, {
    headers: { authorization: api.authorization },
    signal: AbortSignal.timeout(10_000),
  });

  if (!response.ok) {
    throw new Error(`management API GET ${path} answered ${response.status}: ${await response.text()}`);
  }

  return (await response.json()) as T;
}

async function waitFor<T>(label: string, timeoutMs: number, probe: () => Promise<T | undefined>): Promise<T> {
  const deadline = Date.now() + timeoutMs;

  for (;;) {
    const value = await probe();

    if (value !== undefined) {
      return value;
    }

    if (Date.now() >= deadline) {
      throw new Error(`timed out after ${timeoutMs}ms waiting for ${label}`);
    }

    await sleep(200);
  }
}

function brokerUrl(baseAmqpUrl: string, vhost: string, credentials?: { username: string; password: string }): string {
  const url = new URL(baseAmqpUrl);
  if (credentials) {
    url.username = encodeURIComponent(credentials.username);
    url.password = encodeURIComponent(credentials.password);
  }
  url.pathname = `/${encodeURIComponent(vhost)}`;
  return url.toString();
}

interface LeastPrivilegeHarness {
  cleanup: CleanupStack;
  runId: string;
  siteId: string;
  nodeId: string;
  vhost: string;
  adminUser: string;
  nodeUser: string;
  nodeAmqpUrl: string;
  queueName: string;
  deadLetterQueueName: string;
  controlQueueName: string;
  controlConnection: ChannelModel;
  controlChannel: Channel;
}

// Builds, in a vhost of its own, what the platform provisions for one edge node: the shared
// exchanges under their production names, the node queue with its dead-letter exchange and
// bindings, a dead-letter queue, and a broker user with the least-privilege permission set from
// docs/PROTOCOL.md. Everything is removed after the test.
async function createLeastPrivilegeHarness(t: TestContext, api: ManagementApi): Promise<LeastPrivilegeHarness> {
  const cleanup = new CleanupStack(t);
  const runId = crypto.randomUUID().slice(0, 8);
  const siteId = `site-${runId}`;
  const nodeId = `node-${runId}`;
  const vhost = `amqp-mcp-edge-it-${runId}`;
  const vhostPath = encodeURIComponent(vhost);
  // The '@' must survive URL-encoding in amqpUrl and decoding into userId, or the broker refuses
  // every event the node publishes.
  const nodeUser = `${nodeId}@${siteId}`;
  const nodeUserPath = encodeURIComponent(nodeUser);
  const nodePassword = crypto.randomUUID();
  const { jobsExchange, eventsExchange, deadLetterExchange } = defaultExchangeConfig;
  const nodeConfig = { amqpUrl: 'amqp://unused', siteId, nodeId };
  const queueName = buildConsumerQueueName(nodeConfig);
  const deadLetterQueueName = `mcp.edge.dlq.${siteId}.${nodeId}`;
  const controlQueueName = `mcp.edge.control.${siteId}.${nodeId}`;

  await managementRequest(api, 'PUT', `/api/vhosts/${vhostPath}`, {});
  cleanup.defer(() => managementRequest(api, 'DELETE', `/api/vhosts/${vhostPath}`));
  await managementRequest(api, 'PUT', `/api/permissions/${vhostPath}/${encodeURIComponent(api.username)}`, {
    configure: '.*',
    write: '.*',
    read: '.*',
  });
  cleanup.defer(() => managementRequest(api, 'DELETE', `/api/permissions/${vhostPath}/${encodeURIComponent(api.username)}`));

  // The control plane's side of the broker.
  const controlConnection = await connect(brokerUrl(api.amqpUrl, vhost));
  controlConnection.on('error', () => undefined);
  cleanup.defer(() => controlConnection.close());
  const controlChannel = await controlConnection.createChannel();
  controlChannel.on('error', () => undefined);

  for (const exchange of [jobsExchange, eventsExchange, deadLetterExchange]) {
    await controlChannel.assertExchange(exchange, 'topic', { durable: true });
  }

  // Durable, not exclusive: the node reads it over its own connection, and RabbitMQ 4 refuses
  // transient non-exclusive queues.
  await controlChannel.assertQueue(queueName, {
    durable: true,
    arguments: { 'x-dead-letter-exchange': deadLetterExchange },
  });
  cleanup.defer(() => withChannel(controlConnection, (channel) => channel.deleteQueue(queueName)));
  for (const bindingKey of buildJobBindingKeys(nodeConfig)) {
    await controlChannel.bindQueue(queueName, jobsExchange, bindingKey);
  }

  await controlChannel.assertQueue(deadLetterQueueName, { durable: true });
  cleanup.defer(() => withChannel(controlConnection, (channel) => channel.deleteQueue(deadLetterQueueName)));
  await controlChannel.bindQueue(deadLetterQueueName, deadLetterExchange, '#');

  await controlChannel.assertQueue(controlQueueName, { durable: false, exclusive: true });
  cleanup.defer(() => withChannel(controlConnection, (channel) => channel.deleteQueue(controlQueueName)));
  await controlChannel.bindQueue(controlQueueName, eventsExchange, `edge.event.*.${siteId}.${nodeId}`);

  // The node's broker user.
  await managementRequest(api, 'PUT', `/api/users/${nodeUserPath}`, { password: nodePassword, tags: '' });
  cleanup.defer(() => managementRequest(api, 'DELETE', `/api/users/${nodeUserPath}`));
  await managementRequest(api, 'PUT', `/api/permissions/${vhostPath}/${nodeUserPath}`, {
    configure: '^$',
    write: `^${escapeRegExp(eventsExchange)}$`,
    read: `^${escapeRegExp(queueName)}$`,
  });
  cleanup.defer(() => managementRequest(api, 'DELETE', `/api/permissions/${vhostPath}/${nodeUserPath}`));
  await managementRequest(api, 'PUT', `/api/topic-permissions/${vhostPath}/${nodeUserPath}`, {
    exchange: eventsExchange,
    write: `^edge\\.event\\.[a-z]+\\.${escapeRegExp(siteId)}\\.${escapeRegExp(nodeId)}$`,
    read: '^$',
  });
  cleanup.defer(() => managementRequest(api, 'DELETE', `/api/topic-permissions/${vhostPath}/${nodeUserPath}`));

  return {
    cleanup,
    runId,
    siteId,
    nodeId,
    vhost,
    adminUser: api.username,
    nodeUser,
    nodeAmqpUrl: brokerUrl(api.amqpUrl, vhost, { username: nodeUser, password: nodePassword }),
    queueName,
    deadLetterQueueName,
    controlQueueName,
    controlConnection,
    controlChannel,
  };
}

test('least privilege: a passive-mode worker with configure \'^$\' runs jobs and events end to end', { timeout: 60000 }, async (t) => {
  const api = await managementApiOrSkip(t);
  if (!api) {
    return;
  }

  const harness = await createLeastPrivilegeHarness(t, api);
  const { jobsExchange } = defaultExchangeConfig;

  // Production settings: passive topology and the default exchange and queue names.
  const edgeChannel = await createEdgeChannel({
    amqpUrl: harness.nodeAmqpUrl,
    siteId: harness.siteId,
    nodeId: harness.nodeId,
    topologyMode: 'passive',
    runtimeConsumerVersion: '0.1.0',
    runtimeProducerVersion: '0.1.0',
    connectionName: `amqp-mcp-edge-least-privilege-${harness.runId}`,
  });
  harness.cleanup.defer(() => edgeChannel.close());

  const eventsReceived = collectDeliveries(harness.controlChannel, harness.controlQueueName, 3, 15000);

  edgeChannel.consumer.onJob(async (job, delivery) => {
    const common = { jobId: job.payload.jobId, attemptId: job.payload.attemptId, siteId: harness.siteId, nodeId: harness.nodeId };
    await edgeChannel.publisher.publishAck(createAckMessage({ messageId: `ack-${harness.runId}`, payload: { ...common, accepted: true } }));
    await edgeChannel.publisher.publishProgress(
      createProgressMessage({ messageId: `progress-${harness.runId}`, payload: { ...common, state: 'running', percent: 100 } }),
    );
    await edgeChannel.publisher.publishResult(
      createResultMessage({
        messageId: `result-${harness.runId}`,
        payload: { ...common, completedAt: new Date().toISOString(), output: { ok: true } },
      }),
    );
    await edgeChannel.consumer.ack(delivery);
  });

  await edgeChannel.consumer.start();

  const jobEnvelope = createExecutionJob({
    messageId: `job-${harness.runId}`,
    payload: {
      ...createExecutionJob().payload,
      jobId: `job-${harness.runId}`,
      attemptId: `attempt-${harness.runId}`,
      idempotencyKey: `job-${harness.runId}-attempt-1`,
      target: { siteId: harness.siteId, nodeId: harness.nodeId },
    },
  });
  const jobRoutingKey = buildJobRoutingKey(jobEnvelope.payload.target, jobEnvelope.payload.capability);
  harness.controlChannel.publish(jobsExchange, jobRoutingKey, Buffer.from(JSON.stringify(jobEnvelope), 'utf8'), {
    contentType: 'application/json',
    type: jobEnvelope.messageType,
    messageId: jobEnvelope.messageId,
    userId: harness.adminUser,
  });

  const events = await eventsReceived;

  assert.deepEqual(
    events.map((message) => message.fields.routingKey),
    ['ack', 'progress', 'result'].map((type) => `edge.event.${type}.${harness.siteId}.${harness.nodeId}`),
  );
  for (const message of events) {
    // Every event carries the node's broker username, which the broker has validated.
    assert.equal(message.properties.userId, harness.nodeUser);
    assert.equal(validateEnvelope(JSON.parse(message.content.toString('utf8'))).payload.jobId, jobEnvelope.payload.jobId);
  }

  // Dead-lettering needs no permission from the node: a job it rejects reaches the dead-letter
  // queue the platform bound to mcp.edge.dlx.
  const deadLettered = collectDeliveries(harness.controlChannel, harness.deadLetterQueueName, 1, 15000);
  harness.controlChannel.publish(jobsExchange, jobRoutingKey, Buffer.from('not json', 'utf8'), {
    contentType: 'application/json',
    messageId: `malformed-${harness.runId}`,
    userId: harness.adminUser,
  });

  const [deadLetter] = await deadLettered;
  assert.equal(deadLetter?.properties.messageId, `malformed-${harness.runId}`);
  assert.equal(deadLetter?.content.toString('utf8'), 'not json');
  const death = (deadLetter?.properties.headers?.['x-death'] as Array<{ queue?: string; reason?: string }> | undefined)?.[0];
  assert.equal(death?.queue, harness.queueName);
  assert.equal(death?.reason, 'rejected');
});

test('least privilege: the worker user cannot delete or redeclare the shared topology', { timeout: 60000 }, async (t) => {
  const api = await managementApiOrSkip(t);
  if (!api) {
    return;
  }

  const harness = await createLeastPrivilegeHarness(t, api);
  const { jobsExchange, eventsExchange, deadLetterExchange } = defaultExchangeConfig;
  const node = await connect(harness.nodeAmqpUrl);
  node.on('error', () => undefined);
  harness.cleanup.defer(() => node.close());

  await assertBrokerRefuses(node, `delete ${jobsExchange}`, (channel) => channel.deleteExchange(jobsExchange), accessRefused);
  await assertBrokerRefuses(node, `delete ${eventsExchange}`, (channel) => channel.deleteExchange(eventsExchange), accessRefused);
  await assertBrokerRefuses(node, `delete ${deadLetterExchange}`, (channel) => channel.deleteExchange(deadLetterExchange), accessRefused);
  await assertBrokerRefuses(node, 'delete its own queue', (channel) => channel.deleteQueue(harness.queueName), accessRefused);
  await assertBrokerRefuses(
    node,
    `declare ${eventsExchange}`,
    (channel) => channel.assertExchange(eventsExchange, 'topic', { durable: true }),
    accessRefused,
  );
  await assertBrokerRefuses(
    node,
    'bind its own queue to every job',
    (channel) => channel.bindQueue(harness.queueName, jobsExchange, 'edge.job.#'),
    accessRefused,
  );

  // Passive declares need no configure permission: the checks passive mode makes succeed.
  await withChannel(node, (channel) => channel.checkQueue(harness.queueName));
  await withChannel(node, (channel) => channel.checkExchange(eventsExchange));

  // The refusals closed only their own channels, and the shared exchanges are all still there.
  assert.equal(await withChannel(node, async () => 'connection still open'), 'connection still open');
  for (const exchange of [jobsExchange, eventsExchange, deadLetterExchange]) {
    await withChannel(harness.controlConnection, (channel) => channel.checkExchange(exchange));
  }

  // The default declare mode cannot start under this user: it needs configure.
  const declaring = await createEdgeChannel({
    amqpUrl: harness.nodeAmqpUrl,
    siteId: harness.siteId,
    nodeId: harness.nodeId,
    topologyMode: 'declare',
    reconnectEnabled: false,
  });
  harness.cleanup.defer(() => declaring.close());
  await assert.rejects(() => declaring.consumer.start(), /ACCESS_REFUSED/);
});

test('least privilege: an event with a forged user_id closes the channel with PRECONDITION_FAILED', { timeout: 60000 }, async (t) => {
  const api = await managementApiOrSkip(t);
  if (!api) {
    return;
  }

  const harness = await createLeastPrivilegeHarness(t, api);
  const { eventsExchange } = defaultExchangeConfig;
  const routingKey = buildRoutingKey('execution.ack', { siteId: harness.siteId, nodeId: harness.nodeId });
  const node = await connect(harness.nodeAmqpUrl);
  node.on('error', () => undefined);
  harness.cleanup.defer(() => node.close());

  const forgingChannel = await node.createChannel();
  const channelError = new Promise<Error & { code?: number }>((resolve, reject) => {
    forgingChannel.once('error', resolve);
    setTimeout(() => reject(new Error('the broker accepted an event with a forged user_id')), 5000);
  });
  forgingChannel.publish(eventsExchange, routingKey, Buffer.from(JSON.stringify(createAckMessage()), 'utf8'), {
    contentType: 'application/json',
    messageId: 'forged',
    userId: 'another-node',
  });

  const error = await channelError;
  assert.equal(error.code, 406);
  assert.match(error.message, /PRECONDITION_FAILED/);
  assert.match(error.message, new RegExp(`user_id property set to 'another-node' but authenticated user was '${escapeRegExp(harness.nodeUser)}'`));

  // The broker checks user_id only when it is set: an event without one is delivered. That is why
  // consumers must reject events that lack it. The forged event, published first, never arrived.
  const received = collectDeliveries(harness.controlChannel, harness.controlQueueName, 2, 10000);
  await withChannel(node, async (channel) => {
    channel.publish(eventsExchange, routingKey, Buffer.from('{}'), { messageId: 'genuine', userId: harness.nodeUser });
    channel.publish(eventsExchange, routingKey, Buffer.from('{}'), { messageId: 'anonymous' });
    // A passive check round-trips, so both publishes have reached the broker before the close.
    await channel.checkExchange(eventsExchange);
  });

  const [genuine, anonymous] = await received;
  assert.equal(genuine?.properties.messageId, 'genuine');
  assert.equal(genuine?.properties.userId, harness.nodeUser);
  assert.equal(anonymous?.properties.messageId, 'anonymous');
  assert.equal(anonymous?.properties.userId, undefined);
});


test('least privilege: after a reconnect a passive consumer whose queue is gone stops, without a reconnect loop', { timeout: 60000 }, async (t) => {
  const api = await managementApiOrSkip(t);
  if (!api) {
    return;
  }

  const harness = await createLeastPrivilegeHarness(t, api);
  const connectionName = `amqp-mcp-edge-reconnect-${harness.runId}`;
  const edgeChannel = await createEdgeChannel({
    amqpUrl: harness.nodeAmqpUrl,
    siteId: harness.siteId,
    nodeId: harness.nodeId,
    topologyMode: 'passive',
    reconnectDelayMs: 100,
    connectionName,
  });
  harness.cleanup.defer(() => edgeChannel.close());
  await edgeChannel.consumer.start();

  // The node's connection, as the broker lists it.
  const listConnections = () =>
    managementGet<Array<{ name: string; client_properties?: { connection_name?: string } }>>(
      api,
      `/api/vhosts/${encodeURIComponent(harness.vhost)}/connections`,
    ).then((connections) => connections.filter((connection) => connection.client_properties?.connection_name === connectionName));
  const [original] = await waitFor('the node connection to be listed', 15000, async () => {
    const connections = await listConnections();
    return connections.length === 1 ? connections : undefined;
  });

  // The queue disappears while the node is connected, then the broker drops the connection.
  await harness.controlChannel.deleteQueue(harness.queueName);
  await managementRequest(api, 'DELETE', `/api/connections/${encodeURIComponent(original!.name)}`);

  const stopped = await waitFor('the consumer to stop', 15000, async () => {
    const health = await edgeChannel.consumer.health();
    return health.consumerActive === false ? health : undefined;
  });
  assert.match(String(stopped.details?.topologyError), new RegExp(`queue '${escapeRegExp(harness.queueName)}' does not exist`));

  // One new connection, still up: the failed check closed only its own channel.
  await sleep(1000);
  const health = await edgeChannel.consumer.health();
  assert.equal(health.connected, true);
  assert.equal(health.consumerActive, false);
  const replacements = await waitFor('the replacement connection to be listed', 15000, async () => {
    const connections = await listConnections();
    return connections.length === 1 && connections[0]?.name !== original!.name ? connections : undefined;
  });
  assert.equal(replacements.length, 1);

  // The publisher, on the same new connection, checks the events exchange again and still works.
  const received = collectDeliveries(harness.controlChannel, harness.controlQueueName, 1, 10000);
  await edgeChannel.publisher.publishFailure(
    createFailureMessage({
      messageId: `failure-${harness.runId}`,
      payload: {
        ...createFailureMessage().payload,
        siteId: harness.siteId,
        nodeId: harness.nodeId,
        code: 'QUEUE_MISSING',
        message: 'consumer stopped: its queue is gone',
      },
    }),
  );
  const [event] = await received;
  assert.equal(event?.properties.messageId, `failure-${harness.runId}`);
  assert.equal(event?.properties.userId, harness.nodeUser);
});
