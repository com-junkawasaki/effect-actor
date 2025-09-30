// Message Protocol Implementation for Effect v3
// Merkle DAG Hash: message_protocol_v3_001
// Dependencies: []

import { Effect } from "effect"

/**
 * Message protocol utilities for type-safe messaging
 * Provides serialization, deserialization, and message matching
 */

/**
 * Base message interface
 * All actor messages should extend this
 */
export interface Message {
  readonly _tag: string
}

/**
 * Tagged message type helper
 * Creates nominal typing for message types
 */
export type TaggedMessage<T extends string> = Message & {
  readonly _tag: T
}

/**
 * Message matcher for pattern matching
 * Provides type-safe message handling
 */
export interface MessageMatcher<M extends Message, R> {
  readonly match: (message: Message) => message is M
  readonly handle: (message: M) => R
}

/**
 * Create a message matcher
 */
export function messageMatcher<M extends Message, R>(
  tag: M["_tag"],
  handler: (message: M) => R
): MessageMatcher<M, R> {
  return {
    match: (message: Message): message is M => message._tag === tag,
    handle: handler
  }
}

/**
 * Combine multiple message matchers
 * First match wins
 */
export function matchMessages<R>(
  message: Message,
  ...matchers: MessageMatcher<Message, R>[]
): R | undefined {
  for (const matcher of matchers) {
    if (matcher.match(message)) {
      return matcher.handle(message)
    }
  }
  return undefined
}

/**
 * System messages for actor lifecycle management
 */
export enum SystemMessageTag {
  Stop = "System.Stop",
  Restart = "System.Restart",
  GetStatus = "System.GetStatus"
}

export interface StopMessage extends TaggedMessage<typeof SystemMessageTag.Stop> {}
export interface RestartMessage extends TaggedMessage<typeof SystemMessageTag.Restart> {}
export interface GetStatusMessage extends TaggedMessage<typeof SystemMessageTag.GetStatus> {}

/**
 * Create system messages
 */
export const SystemMessages = {
  stop: (): StopMessage => ({ _tag: SystemMessageTag.Stop }),
  restart: (): RestartMessage => ({ _tag: SystemMessageTag.Restart }),
  getStatus: (): GetStatusMessage => ({ _tag: SystemMessageTag.GetStatus })
} as const

/**
 * Message serialization utilities
 * For persistence and network transport
 */
export interface MessageSerializer<M extends Message> {
  readonly serialize: (message: M) => Effect.Effect<string, Error, never>
  readonly deserialize: (data: string) => Effect.Effect<M, Error, never>
}

/**
 * JSON-based message serializer
 */
export function jsonMessageSerializer<M extends Message>(): MessageSerializer<M> {
  return {
    serialize: (message: M) =>
      Effect.try({
        try: () => JSON.stringify(message),
        catch: (error) => new Error(`Serialization failed: ${error}`)
      }),

    deserialize: (data: string) =>
      Effect.try({
        try: () => JSON.parse(data) as M,
        catch: (error) => new Error(`Deserialization failed: ${error}`)
      })
  }
}
