import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import test from 'node:test';

import { connect } from 'amqplib';
import type { Channel, ChannelModel } from 'amqplib';

import { buildJobRoutingKey, buildRoutingKey, createEdgeChannel, validateEnvelope } from '../index.js';
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