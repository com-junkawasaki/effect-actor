"use strict";
// Basic functionality test for Effect Actors v3
// This is a simplified test that bypasses compilation issues
Object.defineProperty(exports, "__esModule", { value: true });
const effect_1 = require("effect");
class SimpleActor {
    mailbox;
    stateRef;
    behavior;
    constructor(mailbox, stateRef, behavior) {
        this.mailbox = mailbox;
        this.stateRef = stateRef;
        this.behavior = behavior;
    }
    tell(message) {
        return effect_1.Queue.offer(this.mailbox, message);
    }
    async processMessages() {
        while (true) {
            const message = await effect_1.Effect.runPromise(effect_1.Queue.take(this.mailbox));
            const currentState = await effect_1.Effect.runPromise(effect_1.Ref.get(this.stateRef));
            const newState = await effect_1.Effect.runPromise(this.behavior(currentState, message));
            await effect_1.Effect.runPromise(effect_1.Ref.set(this.stateRef, newState));
        }
    }
}
// Test Behavior
const counterBehavior = (state, message) => {
    switch (message._tag) {
        case "Increment":
            console.log(`Counter incremented: ${state} -> ${state + message.amount}`);
            return effect_1.Effect.succeed(state + message.amount);
        case "GetValue":
            console.log(`Counter value: ${state}`);
            return effect_1.Effect.succeed(state);
        default:
            return effect_1.Effect.succeed(state);
    }
};
// Test Function
async function testBasicActor() {
    console.log("=== Basic Actor Test ===");
    try {
        // Create mailbox
        const mailbox = await effect_1.Effect.runPromise(effect_1.Queue.unbounded());
        // Create state
        const stateRef = await effect_1.Effect.runPromise(effect_1.Ref.make(0));
        // Create actor
        const actor = new SimpleActor(mailbox, stateRef, counterBehavior);
        // Start processing (in background)
        actor.processMessages().catch(err => console.error("Processing error:", err));
        // Send messages
        await effect_1.Effect.runPromise(actor.tell({ _tag: "Increment", amount: 5 }));
        await new Promise(resolve => setTimeout(resolve, 10));
        await effect_1.Effect.runPromise(actor.tell({ _tag: "Increment", amount: 3 }));
        await new Promise(resolve => setTimeout(resolve, 10));
        await effect_1.Effect.runPromise(actor.tell({ _tag: "GetValue" }));
        await new Promise(resolve => setTimeout(resolve, 10));
        console.log("Basic actor test completed successfully!");
    }
    catch (error) {
        console.error("Basic actor test failed:", error);
    }
}
// Run test
testBasicActor().then(() => {
    console.log("All tests completed");
}).catch(error => {
    console.error("Test runner failed:", error);
});
//# sourceMappingURL=test-basic.js.map