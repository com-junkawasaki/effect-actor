// Copyright 2025 Effect Community
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Actor Runtime Implementation for Effect v3
// Merkle DAG Hash: actor_runtime_v3_001
// Dependencies: ["fiber", "supervisor"]

import { Effect, Fiber, Ref } from "effect"
import type { ActorBehavior, ActorConfig, ActorContext } from "./types.ts"
import { ActorStatus } from "./types.ts"
import type { ActorRef } from "./ActorRef.ts"
import type { Mailbox } from "./Mailbox.ts"

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
  ): Effect.Effect<void, Error, never> {
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

          // Process message with behavior
          const newState = yield* behavior(currentState, envelope.message, context)

          // Update state
          yield* Ref.set(self.stateRef, newState)
        }).pipe(
          // Handle errors in message processing
          Effect.catchAll(error =>
            Effect.gen(function* () {
              yield* Ref.set(self.statusRef, ActorStatus.Failed)
              // TODO: Trigger supervision strategy
              return yield* Effect.fail(error)
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
    })
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
    })
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
  ): Effect.Effect<ActorRuntime<S, M>, Error, never> {
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
    })
  }
} as const
