import { z } from 'zod';

export const edgeMessageTypes = [
  'execution.job',
  'execution.ack',
  'execution.progress',
  'execution.result',
  'execution.failure',
  'node.hello',
  'node.heartbeat',
] as const;

export type EdgeMessageType = (typeof edgeMessageTypes)[number];

export type MessageEnvelopeVersion = '1.0';

export interface BaseEdgeEnvelope<TType extends EdgeMessageType, TPayload> {
  specVersion: MessageEnvelopeVersion;
  messageType: TType;
  messageId: string;
  timestamp: string;
  producer: {
    service: string;
    version: string;
    instanceId?: string;
  };
  compatibility: {
    channelVersion: string;
    minConsumerVersion?: string;
    minProducerVersion?: string;
    serverFamily?: string;
    serverVersion?: string;
    packageVersion?: string;
  };
  payload: TPayload;
}

export const producerSchema = z.object({
  service: z.string().min(1),
  version: z.string().min(1),
  instanceId: z.string().min(1).optional(),
});

export const compatibilitySchema = z.object({
  channelVersion: z.string().min(1),
  minConsumerVersion: z.string().min(1).optional(),
  minProducerVersion: z.string().min(1).optional(),
  serverFamily: z.string().min(1).optional(),
  serverVersion: z.string().min(1).optional(),
  packageVersion: z.string().min(1).optional(),
});

export function createEnvelopeSchema<TType extends EdgeMessageType, TPayload extends z.ZodTypeAny>(
  messageType: TType,
  payload: TPayload,
) {
  return z
    .object({
      specVersion: z.literal('1.0'),
      messageType: z.literal(messageType),
      messageId: z.string().min(1),
      timestamp: z.string().datetime({ offset: true }),
      producer: producerSchema,
      compatibility: compatibilitySchema,
      payload,
    })
    .passthrough();
}