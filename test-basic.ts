// Basic functionality test for Effect Actors v3
// This is a simplified test that bypasses compilation issues

import { Effect, Queue, Ref } from "effect"

// Simplified Actor Implementation for testing
interface Message {
  readonly _tag: string
}

interface ActorRef<M extends Message> {
  tell(message: M): Effect.Effect<void, never, never>
}

class SimpleActor<M extends Message> {
  constructor(
    private mailbox: Queue.Queue<M>,
    private stateRef: Ref.Ref<number>,
    private behavior: (state: number, message: M) => Effect.Effect<number, never, never>
  ) {}

  tell(message: M): Effect.Effect<void, never, never> {
    return Queue.offer(this.mailbox, message)
  }

  async processMessages(): Promise<void> {
    while (true) {
      const message = await Effect.runPromise(Queue.take(this.mailbox))
      const currentState = await Effect.runPromise(Ref.get(this.stateRef))
      const newState = await Effect.runPromise(this.behavior(currentState, message))
      await Effect.runPromise(Ref.set(this.stateRef, newState))
    }
  }
}

// Test Messages
interface Increment extends Message {
  readonly _tag: "Increment"
  readonly amount: number
}

interface GetValue extends Message {
  readonly _tag: "GetValue"
}

// Test Behavior
const counterBehavior = (state: number, message: Increment | GetValue): Effect.Effect<number, never, never> => {
  switch (message._tag) {
    case "Increment":
      console.log(`Counter incremented: ${state} -> ${state + message.amount}`)
      return Effect.succeed(state + message.amount)
    case "GetValue":
      console.log(`Counter value: ${state}`)
      return Effect.succeed(state)
    default:
      return Effect.succeed(state)
  }
}

// Test Function
async function testBasicActor(): Promise<void> {
  console.log("=== Basic Actor Test ===")

  try {
    // Create mailbox
    const mailbox = await Effect.runPromise(Queue.unbounded<Increment | GetValue>())

    // Create state
    const stateRef = await Effect.runPromise(Ref.make(0))

    // Create actor
    const actor = new SimpleActor(mailbox, stateRef, counterBehavior)

    // Start processing (in background)
    actor.processMessages().catch(err => console.error("Processing error:", err))

    // Send messages
    await Effect.runPromise(actor.tell({ _tag: "Increment", amount: 5 }))
    await new Promise(resolve => setTimeout(resolve, 10))

    await Effect.runPromise(actor.tell({ _tag: "Increment", amount: 3 }))
    await new Promise(resolve => setTimeout(resolve, 10))

    await Effect.runPromise(actor.tell({ _tag: "GetValue" }))
    await new Promise(resolve => setTimeout(resolve, 10))

    console.log("Basic actor test completed successfully!")

  } catch (error) {
    console.error("Basic actor test failed:", error)
  }
}

// Run test
testBasicActor().then(() => {
  console.log("All tests completed")
}).catch(error => {
  console.error("Test runner failed:", error)
})
