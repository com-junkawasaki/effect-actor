// Mailbox System Implementation for Effect v3
// Merkle DAG Hash: mailbox_v3_001
// Dependencies: ["queue"]

import { Effect, Queue } from "effect"
import type { Envelope } from "./types.ts"

/**
 * Mailbox - Message queue for actor communication
 * Built on Effect.Queue for high-performance messaging
 */
export class Mailbox<M> {
  readonly _tag = "Mailbox" as const

  constructor(
    readonly capacity: number,
    private readonly queue: Queue.Queue<Envelope<M>>
  ) {}

  /**
   * Send a message to this mailbox
   * Non-blocking operation with back-pressure
   */
  offer(envelope: Envelope<M>): Effect.Effect<boolean, Error, never> {
    return Queue.offer(this.queue, envelope).pipe(
      Effect.map(() => true),
      Effect.catchAll(() => Effect.succeed(false))
    )
  }

  /**
   * Receive a message from this mailbox
   * Blocking operation that waits for messages
   */
  take(): Effect.Effect<Envelope<M>, Error, never> {
    return Queue.take(this.queue)
  }

  /**
   * Try to receive a message without blocking
   * Returns null if no message is available
   */
  poll(): Effect.Effect<Envelope<M> | null, Error, never> {
    return Queue.poll(this.queue).pipe(
      Effect.map(option => option._tag === "Some" ? option.value : null)
    )
  }

  /**
   * Get the current size of the mailbox
   */
  size(): Effect.Effect<number, Error, never> {
    return Queue.size(this.queue)
  }

  /**
   * Check if mailbox is empty
   */
  isEmpty(): Effect.Effect<boolean, Error, never> {
    return Queue.isEmpty(this.queue)
  }

  /**
   * Check if mailbox is full
   */
  isFull(): Effect.Effect<boolean, Error, never> {
    return this.size().pipe(
      Effect.map(size => size >= this.capacity)
    )
  }

  /**
   * Shutdown the mailbox
   * Prevents further message delivery
   */
  shutdown(): Effect.Effect<void, Error, never> {
    return Queue.shutdown(this.queue)
  }

  /**
   * Check if mailbox is shutdown
   */
  isShutdown(): Effect.Effect<boolean, Error, never> {
    return Queue.isShutdown(this.queue)
  }
}

/**
 * Mailbox factory and utilities
 */
export const MailboxUtils = {
  /**
   * Create a bounded mailbox with specified capacity
   */
  bounded<M>(capacity: number): Effect.Effect<Mailbox<M>, Error, never> {
    return Queue.bounded<Envelope<M>>(capacity).pipe(
      Effect.map(queue => new Mailbox(capacity, queue))
    )
  },

  /**
   * Create an unbounded mailbox
   */
  unbounded<M>(): Effect.Effect<Mailbox<M>, Error, never> {
    return Queue.unbounded<Envelope<M>>().pipe(
      Effect.map(queue => new Mailbox(Number.MAX_SAFE_INTEGER, queue))
    )
  },

  /**
   * Create a dropping mailbox that drops oldest messages when full
   */
  dropping<M>(capacity: number): Effect.Effect<Mailbox<M>, Error, never> {
    return Queue.dropping<Envelope<M>>(capacity).pipe(
      Effect.map(queue => new Mailbox(capacity, queue))
    )
  },

  /**
   * Create a sliding mailbox that drops newest messages when full
   */
  sliding<M>(capacity: number): Effect.Effect<Mailbox<M>, Error, never> {
    return Queue.sliding<Envelope<M>>(capacity).pipe(
      Effect.map(queue => new Mailbox(capacity, queue))
    )
  }
} as const
