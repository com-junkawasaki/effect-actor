"use strict";
// Counter Actor Example
// Demonstrates basic actor usage with state management
Object.defineProperty(exports, "__esModule", { value: true });
const effect_1 = require("effect");
const index_ts_1 = require("../src/index.ts");
// Create message constructors
const CounterMessages = {
    increment: (amount = 1) => ({
        _tag: "Increment",
        amount
    }),
    decrement: (amount = 1) => ({
        _tag: "Decrement",
        amount
    }),
    getValue: () => ({
        _tag: "GetValue"
    })
};
// Counter actor behavior
const counterBehavior = (state, message, context) => {
    return effect_1.Effect.gen(function* () {
        switch (message._tag) {
            case "Increment":
                const newStateInc = state + message.amount;
                console.log(`Counter incremented: ${state} -> ${newStateInc}`);
                return newStateInc;
            case "Decrement":
                const newStateDec = state - message.amount;
                console.log(`Counter decremented: ${state} -> ${newStateDec}`);
                return newStateDec;
            case "GetValue":
                console.log(`Counter value requested: ${state}`);
                // In a real app, you might send response back to sender
                return state;
            default:
                return state;
        }
    });
};
// Alternative implementation using message matchers
const counterBehaviorWithMatchers = (state, message, context) => {
    const incrementMatcher = (0, index_ts_1.messageMatcher)("Increment", (msg) => {
        const newState = state + msg.amount;
        console.log(`Counter incremented: ${state} -> ${newState}`);
        return effect_1.Effect.succeed(newState);
    });
    const decrementMatcher = (0, index_ts_1.messageMatcher)("Decrement", (msg) => {
        const newState = state - msg.amount;
        console.log(`Counter decremented: ${state} -> ${newState}`);
        return effect_1.Effect.succeed(newState);
    });
    const getValueMatcher = (0, index_ts_1.messageMatcher)("GetValue", () => {
        console.log(`Counter value requested: ${state}`);
        return effect_1.Effect.succeed(state);
    });
    return (0, index_ts_1.matchMessages)(message, incrementMatcher, decrementMatcher, getValueMatcher) ?? effect_1.Effect.succeed(state);
};
// Example usage
const runCounterExample = effect_1.Effect.gen(function* () {
    console.log("=== Counter Actor Example ===");
    // Create actor system
    const system = yield* index_ts_1.ActorSystemUtils.make("counter-system");
    // Create counter actor
    const counter = yield* system.make("counter-1", 0, // initial state
    counterBehavior, { mailboxCapacity: 100 });
    // Send some messages
    yield* counter.tell(CounterMessages.increment(5));
    yield* counter.tell(CounterMessages.increment(3));
    yield* counter.tell(CounterMessages.getValue());
    yield* counter.tell(CounterMessages.decrement(2));
    yield* counter.tell(CounterMessages.getValue());
    // Wait a bit for messages to be processed
    yield* effect_1.Effect.sleep(100);
    // Get system stats
    const stats = yield* system.getStats();
    console.log(`System stats: ${JSON.stringify(stats, null, 2)}`);
    // Shutdown system
    yield* system.shutdown();
    console.log("Counter example completed");
});
// Run the example
effect_1.Effect.runPromise(runCounterExample).then(() => {
    console.log("Counter example finished successfully");
}).catch(error => {
    console.error("Counter example failed:", error);
});
//# sourceMappingURL=counter.js.map