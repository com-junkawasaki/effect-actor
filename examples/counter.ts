// Counter Actor Example
// Demonstrates basic actor usage with state management

import { Effect } from "effect"
import {
  ActorSystemUtils,
  type ActorBehavior,
  type Message,
  messageMatcher,
  matchMessages
} from "../src/index.ts"

// Counter messages
interface Increment extends Message {
  readonly _tag: "Increment"
  readonly amount: number
}

interface Decrement extends Message {
  readonly _tag: "Decrement"
  readonly amount: number
}

interface GetValue extends Message {
  readonly _tag: "GetValue"
}

type CounterMessage = Increment | Decrement | GetValue

// Create message constructors
const CounterMessages = {
  increment: (amount: number = 1): Increment => ({
    _tag: "Increment",
    amount
  }),

  decrement: (amount: number = 1): Decrement => ({
    _tag: "Decrement",
    amount
  }),

  getValue: (): GetValue => ({
    _tag: "GetValue"
  })
}

// Counter actor behavior
const counterBehavior: ActorBehavior<number, CounterMessage> = (state, message, context) => {
  return Effect.gen(function* () {
    switch (message._tag) {
      case "Increment":
        const newStateInc = state + message.amount
        console.log(`Counter incremented: ${state} -> ${newStateInc}`)
        return newStateInc

      case "Decrement":
        const newStateDec = state - message.amount
        console.log(`Counter decremented: ${state} -> ${newStateDec}`)
        return newStateDec

      case "GetValue":
        console.log(`Counter value requested: ${state}`)
        // In a real app, you might send response back to sender
        return state

      default:
        return state
    }
  })
}

// Alternative implementation using message matchers
const counterBehaviorWithMatchers: ActorBehavior<number, CounterMessage> = (state, message, context) => {
  const incrementMatcher = messageMatcher<Increment, Effect.Effect<number, Error, never>>(
    "Increment",
    (msg) => {
      const newState = state + msg.amount
      console.log(`Counter incremented: ${state} -> ${newState}`)
      return Effect.succeed(newState)
    }
  )

  const decrementMatcher = messageMatcher<Decrement, Effect.Effect<number, Error, never>>(
    "Decrement",
    (msg) => {
      const newState = state - msg.amount
      console.log(`Counter decremented: ${state} -> ${newState}`)
      return Effect.succeed(newState)
    }
  )

  const getValueMatcher = messageMatcher<GetValue, Effect.Effect<number, Error, never>>(
    "GetValue",
    () => {
      console.log(`Counter value requested: ${state}`)
      return Effect.succeed(state)
    }
  )

  return matchMessages(message, incrementMatcher, decrementMatcher, getValueMatcher) ?? Effect.succeed(state)
}

// Example usage
const runCounterExample = Effect.gen(function* () {
  console.log("=== Counter Actor Example ===")

  // Create actor system
  const system = yield* ActorSystemUtils.make("counter-system")

  // Create counter actor
  const counter = yield* system.make(
    "counter-1",
    0, // initial state
    counterBehavior,
    { mailboxCapacity: 100 }
  )

  // Send some messages
  yield* counter.tell(CounterMessages.increment(5))
  yield* counter.tell(CounterMessages.increment(3))
  yield* counter.tell(CounterMessages.getValue())
  yield* counter.tell(CounterMessages.decrement(2))
  yield* counter.tell(CounterMessages.getValue())

  // Wait a bit for messages to be processed
  yield* Effect.sleep(100)

  // Get system stats
  const stats = yield* system.getStats()
  console.log(`System stats: ${JSON.stringify(stats, null, 2)}`)

  // Shutdown system
  yield* system.shutdown()
  console.log("Counter example completed")
})

// Run the example
Effect.runPromise(runCounterExample).then(() => {
  console.log("Counter example finished successfully")
}).catch(error => {
  console.error("Counter example failed:", error)
})
