import type { EdgeChannelConfig } from '../config/index.js';
import { buildRoutingKey, resolveConfig, resolveUserId } from '../config/index.js';
import { validateEnvelope, type ExecutionJobEnvelope, type JobAckMessage, type JobFailureEvent, type JobProgressEvent, type JobResultEvent } from '../contracts/index.js';
import { createHealthSnapshot, type EdgeChannelHealth } from '../health/index.js';
import type { EdgeChannel } from '../connection/index.js';
import type { EdgeJobHandler, JobDeliveryContext } from '../consumer/index.js';

type EdgeEventEnvelope = JobAckMessage | JobProgressEvent | JobResultEvent | JobFailureEvent;

export interface MockPublishedEvent {
  routingKey: string;
  /** The AMQP `user_id` the real publisher would set: the node's broker username. */
  userId: string;
  message: EdgeEventEnvelope;
}

export interface MockDeliveryRecord {
  context: JobDeliveryContext;
  status: 'pending' | 'acked' | 'nacked';
  requeue?: boolean;
}

class MockEdgeJobConsumer {
  private readonly deliveries = new Map<number, MockDeliveryRecord>();

  private jobHandler: EdgeJobHandler = async () => undefined;

  private active = false;

  private nextDeliveryTag = 1;

  public onJob(handler: EdgeJobHandler): void {
    this.jobHandler = handler;
  }

  public async start(): Promise<void> {
    this.active = true;
  }

  public async stop(): Promise<void> {
    this.active = false;
    this.deliveries.clear();
  }

  public async ack(delivery: JobDeliveryContext): Promise<void> {
    const record = this.getDelivery(delivery.deliveryTag);
    record.status = 'acked';
  }

  public async nack(delivery: JobDeliveryContext, options: { requeue?: boolean; reason?: string } = {}): Promise<void> {
    const record = this.getDelivery(delivery.deliveryTag);
    record.status = 'nacked';
    record.requeue = options.requeue ?? false;
  }

  public async health(): Promise<EdgeChannelHealth> {
    return createHealthSnapshot({
      connected: true,
      consumerActive: this.active,
      details: {
        deliveries: this.deliveries.size,
      },
    });
  }

  public async dispatchJob(job: ExecutionJobEnvelope, deliveryOverrides: Partial<JobDeliveryContext> = {}): Promise<JobDeliveryContext> {
    if (!this.active) {
      throw new Error('mock consumer is not active');
    }

    const validatedEnvelope = validateEnvelope(job);

    if (validatedEnvelope.messageType !== 'execution.job') {
      throw new Error('mock dispatch requires an execution.job envelope');
    }

    const context: JobDeliveryContext = {
      deliveryTag: this.nextDeliveryTag,
      routingKey:
        deliveryOverrides.routingKey ??
        buildRoutingKey('execution.job', {
          siteId: validatedEnvelope.payload.target.siteId,
          nodeId: validatedEnvelope.payload.target.nodeId,
          nodeGroup: validatedEnvelope.payload.target.nodeGroup,
          capabilityKind: validatedEnvelope.payload.capability.kind,
          capabilityName: validatedEnvelope.payload.capability.name,
        }),
      redelivered: deliveryOverrides.redelivered ?? false,
    };

    this.nextDeliveryTag += 1;
    this.deliveries.set(context.deliveryTag!, { context, status: 'pending' });

    try {
      await this.jobHandler(validatedEnvelope, context);
    } catch {
      const record = this.getDelivery(context.deliveryTag);
      if (record.status === 'pending') {
        record.status = 'nacked';
        record.requeue = true;
      }
    }

    return context;
  }

  public getDeliveries(): MockDeliveryRecord[] {
    return Array.from(this.deliveries.values());
  }

  private getDelivery(deliveryTag?: number): MockDeliveryRecord {
    if (deliveryTag === undefined) {
      throw new Error('deliveryTag is required');
    }

    const record = this.deliveries.get(deliveryTag);

    if (!record) {
      throw new Error(`delivery ${deliveryTag} does not exist`);
    }

    return record;
  }
}

class MockEdgeEventPublisher {
  private readonly config;

  private readonly userId: string;

  private readonly publishedEvents: MockPublishedEvent[] = [];

  private active = false;

  public constructor(config: EdgeChannelConfig) {
    this.config = resolveConfig(config);
    this.userId = resolveUserId(config);
  }

  public async publishAck(message: JobAckMessage): Promise<void> {
    this.publish(message);
  }

  public async publishProgress(message: JobProgressEvent): Promise<void> {
    this.publish(message);
  }

  public async publishResult(message: JobResultEvent): Promise<void> {
    this.publish(message);
  }

  public async publishFailure(message: JobFailureEvent): Promise<void> {
    this.publish(message);
  }

  public async health(): Promise<EdgeChannelHealth> {
    return createHealthSnapshot({
      connected: true,
      publisherActive: this.active,
      details: {
        publishedEvents: this.publishedEvents.length,
      },
    });
  }

  public getPublishedEvents(): MockPublishedEvent[] {
    return [...this.publishedEvents];
  }

  private publish(message: EdgeEventEnvelope): void {
    const validatedEnvelope = validateEnvelope(message);

    if (validatedEnvelope.messageType === 'execution.job') {
      throw new Error('mock publisher only accepts execution events');
    }

    this.active = true;
    this.publishedEvents.push({
      routingKey: buildRoutingKey(validatedEnvelope.messageType, {
        siteId: validatedEnvelope.payload.siteId ?? this.config.siteId,
        nodeId: validatedEnvelope.payload.nodeId,
      }),
      userId: this.userId,
      message: validatedEnvelope,
    });
  }
}

export interface MockEdgeChannel extends EdgeChannel {
  dispatchJob(job: ExecutionJobEnvelope, deliveryOverrides?: Partial<JobDeliveryContext>): Promise<JobDeliveryContext>;
  getPublishedEvents(): MockPublishedEvent[];
  getDeliveries(): MockDeliveryRecord[];
}

export function createMockEdgeChannel(config: Partial<EdgeChannelConfig> = {}): MockEdgeChannel {
  const resolvedConfig = resolveConfig({
    amqpUrl: config.amqpUrl ?? 'amqp://mock',
    ...config,
  });

  const consumer = new MockEdgeJobConsumer();
  const publisher = new MockEdgeEventPublisher(resolvedConfig);

  return {
    consumer,
    publisher,
    async close(): Promise<void> {
      await consumer.stop();
    },
    dispatchJob(job: ExecutionJobEnvelope, deliveryOverrides?: Partial<JobDeliveryContext>): Promise<JobDeliveryContext> {
      return consumer.dispatchJob(job, deliveryOverrides);
    },
    getPublishedEvents(): MockPublishedEvent[] {
      return publisher.getPublishedEvents();
    },
    getDeliveries(): MockDeliveryRecord[] {
      return consumer.getDeliveries();
    },
  };
}