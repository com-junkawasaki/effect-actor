// Copyright (c) 2025 Effect Community
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Actor Runtime Implementation for Effect v3
// Merkle DAG Hash: actor_runtime_v3_001
// Dependencies: ["fiber", "supervisor"]

import { Effect, Fiber, Ref } from "effect"
import type { ActorBehavior, ActorConfig, ActorContext } from "./types"
import { ActorStatus } from "./types"
import type { ActorRef } from "./ActorRef"
import type { Mailbox } from "./Mailbox"

/**
 * Actor Runtime - Manages actor execution lifecycle
 * Uses Fiber for concurrent execution and Supervisor for monitoring
 */
export class ActorRuntime<S, M> {
  readonly _tag = "ActorRuntime" as const

  constructor(
    readonly ref: ActorRef<M>,
    readonly mailbox: Mailbox<M>,
    readonly config: ActorConfig,
    private readonly statusRef: Ref.Ref<typeof ActorStatus[keyof typeof ActorStatus]>,
    private readonly stateRef: Ref.Ref<S>,
    private readonly fiberRef: Ref.Ref<Fiber.Fiber<never, void> | null>
  ) {}

  /**
   * Start the actor runtime
   * Begins message processing loop
   */
  start(
    initialState: S,
    behavior: ActorBehavior<S, M>
  ): Effect.Effect<void, never, never> {
    const self = this
    return Effect.gen(function* () {
      // Update status to Starting
      yield* Ref.set(self.statusRef, ActorStatus.Starting)

      // Set initial state
      yield* Ref.set(self.stateRef, initialState)

      // Create actor context
      const context: ActorContext<M> = {
        self: self.ref,
        system: undefined as any, // Will be set by ActorSystem
        config: self.config
      }

      // Create message processing loop
      const processLoop = Effect.forever(
        Effect.gen(function* () {
          // Take message from mailbox
          const envelope = yield* self.mailbox.take()

          // Get current state
          const currentState = yield* Ref.get(self.stateRef)

          // Process message with behavior and timeout
          const timeoutMs = self.config.messageTimeout ?? 30000 // Default 30 seconds

          const processWithTimeout = Effect.gen(function* () {
            return yield* behavior(currentState, envelope.message, context)
          }).pipe(
            Effect.timeout(timeoutMs),
            Effect.catchTag("TimeoutException", () =>
              Effect.fail(new Error(`Message processing timeout after ${timeoutMs}ms`))
            )
          )

          const newState = yield* processWithTimeout

          // Update state
          yield* Ref.set(self.stateRef, newState)
        }).pipe(
          // Handle errors in message processing
          Effect.catchAll((error: Error) =>
            Effect.gen(function* (): Effect.Effect<undefined, never, never> {
              yield* Ref.set(self.statusRef, ActorStatus.Failed)

              // TODO: Trigger supervision strategy
              console.error("Actor error:", error)
              return undefined
            })
          )
        )
      )

      // Start processing fiber
      const fiber = yield* Effect.fork(processLoop)

      // Store fiber reference
      yield* Ref.set(self.fiberRef, fiber)

      // Update status to Running
      yield* Ref.set(self.statusRef, ActorStatus.Running)
    }).pipe(
      Effect.orDie
    )
  }

  /**
   * Stop the actor runtime
   * Gracefully shuts down message processing
   */
  stop(): Effect.Effect<void, Error, never> {
    const self = this
    return Effect.gen(function* () {
      // Update status to Stopping
      yield* Ref.set(self.statusRef, ActorStatus.Stopping)

      // Shutdown mailbox
      yield* self.mailbox.shutdown()

      // Interrupt processing fiber if running
      const fiber = yield* Ref.get(self.fiberRef)
      if (fiber) {
        yield* Fiber.interrupt(fiber)
        yield* Ref.set(self.fiberRef, null)
      }

      // Update status to Stopped
      yield* Ref.set(self.statusRef, ActorStatus.Stopped)
    }).pipe(
      Effect.orDie
    )
  }

  /**
   * Get current actor status
   */
  getStatus(): Effect.Effect<typeof ActorStatus[keyof typeof ActorStatus], Error, never> {
    return Ref.get(this.statusRef)
  }

  /**
   * Get current actor state
   */
  getState(): Effect.Effect<S, Error, never> {
    return Ref.get(this.stateRef)
  }

  /**
   * Check if actor is running
   */
  isRunning(): Effect.Effect<boolean, Error, never> {
    return this.getStatus().pipe(
      Effect.map(status => status === ActorStatus.Running)
    )
  }
}

/**
 * Actor Runtime factory and utilities
 */
export const ActorRuntimeUtils = {
  /**
   * Create a new actor runtime
   */
  make<S, M>(
    ref: ActorRef<M>,
    mailbox: Mailbox<M>,
    config: ActorConfig = {}
  ): Effect.Effect<ActorRuntime<S, M>, never, never> {
    return Effect.gen(function* () {
      const statusRef = yield* Ref.make<typeof ActorStatus[keyof typeof ActorStatus]>(ActorStatus.Stopped)
      const stateRef = yield* Ref.make<S>(undefined as any) // Will be set on start
      const fiberRef = yield* Ref.make<Fiber.Fiber<never, void> | null>(null)

      return new ActorRuntime(
        ref,
        mailbox,
        config,
        statusRef,
        stateRef,
        fiberRef
      )
    }).pipe(
      Effect.orDie
    )
  }
} as const
