// Supervision System Implementation for Effect v3
// Merkle DAG Hash: supervision_v3_001
// Dependencies: ["actor_ref", "actor_runtime"]

import { Effect, Ref, Schedule } from "effect"
import { SupervisionStrategy } from "./types"
import type { ActorRef } from "./ActorRef"
import type { ActorRuntime } from "./ActorRuntime"

/**
 * Supervisor - Manages actor failure recovery
 * Implements various supervision strategies for resilient actor systems
 */
export class Supervisor<S, M> {
  readonly _tag = "Supervisor" as const

  constructor(
    readonly supervisedActor: ActorRef<M>,
    readonly strategy: SupervisionStrategy,
    readonly maxRestarts: number,
    readonly restartDelay: number,
    private readonly restartCountRef: Ref.Ref<number>,
    private readonly runtimeRef: Ref.Ref<ActorRuntime<unknown, M> | null>,
    private readonly initialState: S,
    private readonly behavior: (state: S, message: M, context: any) => Effect.Effect<S, Error, never>
  ) {}

  /**
   * Handle actor failure
   * Applies supervision strategy based on failure type
   */
  handleFailure(
    error: Error,
    runtime: ActorRuntime<unknown, M>
  ): Effect.Effect<void, never, never> {
    const self = this
    return Effect.gen(function* () {
      const currentRestarts = yield* Ref.get(self.restartCountRef)

      switch (self.strategy) {
        case SupervisionStrategy.Stop:
          yield* self.stopActor(runtime)
          break

        case SupervisionStrategy.Restart:
          if (currentRestarts < self.maxRestarts) {
            yield* self.restartActor(runtime)
            yield* Ref.set(self.restartCountRef, currentRestarts + 1)
          } else {
            yield* self.stopActor(runtime)
          }
          break

        case SupervisionStrategy.Escalate:
          // Escalate to parent supervisor (not implemented in this version)
          yield* self.stopActor(runtime)
          break

        default:
          yield* self.stopActor(runtime)
      }
    }).pipe(
      Effect.orDie
    )
  }

  /**
   * Restart the supervised actor with exponential backoff
   */
  private restartActor(
    runtime: ActorRuntime<unknown, M>
  ): Effect.Effect<void, never, never> {
    const self = this
    return Effect.gen(function* () {
      // Stop current runtime
      yield* runtime.stop()

      // Get current restart count for exponential backoff
      const restartCount = yield* Ref.get(self.restartCountRef)

      // Calculate delay with exponential backoff
      const delay = self.restartDelay * Math.pow(2, restartCount)
      const maxDelay = 30000 // Max 30 seconds
      const actualDelay = Math.min(delay, maxDelay)

      yield* Effect.log(`Waiting ${actualDelay}ms before restart (attempt ${restartCount + 1})`)
      yield* Effect.sleep(actualDelay)

      // Create new runtime with fresh initial state
      const newRuntime = yield* ActorRuntimeUtils.make<S, M>(self.supervisedActor, runtime.mailbox, runtime.config)

      // Start the actor with initial state and behavior
      yield* newRuntime.start(self.initialState, self.behavior)

      // Update runtime reference
      yield* Ref.set(self.runtimeRef, newRuntime)

      yield* Effect.log(`Successfully restarted actor ${self.supervisedActor.id}`)
    }).pipe(
      Effect.orDie
    )
  }

  /**
   * Stop the supervised actor
   */
  private stopActor(
    runtime: ActorRuntime<unknown, M>
  ): Effect.Effect<void, never, never> {
    const self = this
    return Effect.gen(function* () {
      yield* runtime.stop()
      yield* Effect.log(`Stopped actor ${self.supervisedActor.id} due to failure`)
    }).pipe(
      Effect.orDie
    )
  }

  /**
   * Register a runtime with this supervisor
   */
  registerRuntime(runtime: ActorRuntime<unknown, M>): Effect.Effect<void, never, never> {
    return Ref.set(this.runtimeRef, runtime).pipe(
      Effect.orDie
    )
  }

  /**
   * Get current restart count
   */
  getRestartCount(): Effect.Effect<number, never, never> {
    return Ref.get(this.restartCountRef).pipe(
      Effect.orDie
    )
  }
}

/**
 * Supervisor factory and utilities
 */
export const SupervisorUtils = {
  /**
   * Create a supervisor with specified strategy
   */
  make<S, M>(
    actorRef: ActorRef<M>,
    initialState: S,
    behavior: (state: S, message: M, context: any) => Effect.Effect<S, Error, never>,
    strategy: SupervisionStrategy = SupervisionStrategy.Restart,
    maxRestarts: number = 3,
    restartDelay: number = 1000
  ): Effect.Effect<Supervisor<S, M>, never, never> {
    return Effect.gen(function* () {
      const restartCountRef = yield* Ref.make(0)
      const runtimeRef = yield* Ref.make<ActorRuntime<unknown, M> | null>(null)

      return new Supervisor(
        actorRef,
        strategy,
        maxRestarts,
        restartDelay,
        restartCountRef,
        runtimeRef,
        initialState,
        behavior
      )
    }).pipe(
      Effect.orDie
    )
  },

  /**
   * Create a supervisor tree for hierarchical supervision
   */
  tree<M>(
    supervisors: Supervisor<M>[]
  ): Effect.Effect<SupervisorTree<M>, Error, never> {
    return Effect.gen(function* () {
      const childrenRef = yield* Ref.make(supervisors)

      return new SupervisorTree(childrenRef)
    })
  }
} as const

/**
 * Supervisor Tree - Hierarchical supervision structure
 * Allows parent supervisors to manage child supervisors
 */
export class SupervisorTree<M> {
  readonly _tag = "SupervisorTree" as const

  constructor(
    private readonly childrenRef: Ref.Ref<Supervisor<M>[]>
  ) {}

  /**
   * Add a child supervisor
   */
  addChild(supervisor: Supervisor<M>): Effect.Effect<void, Error, never> {
    return Ref.update(this.childrenRef, children => [...children, supervisor])
  }

  /**
   * Remove a child supervisor
   */
  removeChild(actorRef: ActorRef<M>): Effect.Effect<void, Error, never> {
    return Ref.update(this.childrenRef, children =>
      children.filter(child => !child.supervisedActor.equals(actorRef))
    )
  }

  /**
   * Handle failure in the supervisor tree
   * Escalates failures up the hierarchy
   */
  handleFailure(
    error: Error,
    runtime: ActorRuntime<unknown, M>
  ): Effect.Effect<void, never, never> {
    const self = this
    return Effect.gen(function* () {
      const children = yield* Ref.get(self.childrenRef)
      const supervisor = children.find((child: Supervisor<M>) =>
        child.supervisedActor.equals(runtime.ref)
      )

      if (supervisor) {
        yield* supervisor.handleFailure(error, runtime)
      } else {
        // No supervisor found, escalate to root
        yield* Effect.log(`No supervisor found for actor ${runtime.ref.id}, escalating failure`)
        console.error("Supervisor escalation:", error)
      }
    }).pipe(
      Effect.orDie
    )
  }

  /**
   * Get all child supervisors
   */
  getChildren(): Effect.Effect<Supervisor<M>[], never, never> {
    return Ref.get(this.childrenRef)
  }
}
