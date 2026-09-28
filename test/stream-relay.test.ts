import { describe, expect, test } from "bun:test"
import { StreamRelay, type StreamMessage, type StreamRelayOptions } from "../src/core.js"

/** Two relays joined by an asynchronous channel, as two sides of one boundary. */
function pair(options: StreamRelayOptions = {}) {

    const sent: StreamMessage[] = []
    let left: StreamRelay
    let right: StreamRelay

    left = new StreamRelay(message => { sent.push(message); queueMicrotask(() => right.receive(message)) }, options)
    right = new StreamRelay(message => { sent.push(message); queueMicrotask(() => left.receive(message)) }, options)

    return { left, right, sent }
}

/** A byte source that records how many chunks were read from it. */
function source(chunks: number, size: number) {

    const state = { read: 0, canceled: null as unknown }

    const stream = new ReadableStream<Uint8Array>({

        pull(controller) {

            if (state.read === chunks) return controller.close()

            controller.enqueue(new Uint8Array(size).fill(state.read % 256))
            state.read++
        },

        cancel(reason) { state.canceled = reason }

    }, { highWaterMark: 0 })

    return { stream, state }
}

async function bytes(stream: ReadableStream<unknown>) {

    const parts: Uint8Array[] = []

    for await (const chunk of stream as ReadableStream<Uint8Array>) parts.push(chunk)

    const total = new Uint8Array(parts.reduce((sum, part) => sum + part.byteLength, 0))
    let offset = 0

    for (const part of parts) { total.set(part, offset); offset += part.byteLength }

    return total
}

const settle = () => new Promise(resolve => setTimeout(resolve, 20))

describe("StreamRelay", () => {

    test("carries a byte stream, split into chunks no larger than the limit", async () => {

        const { left, right, sent } = pair({ chunkBytes: 1024 })
        const { stream } = source(5, 3000)

        const received = await bytes(right.import(left.export(stream)))

        expect(received.byteLength).toBe(15_000)
        expect(received[0]).toBe(0)
        expect(received[14_999]).toBe(4)

        const chunks = sent.filter(message => message[0] === "chunk") as ["chunk", string, Uint8Array][]

        expect(chunks.every(([, , chunk]) => chunk.byteLength <= 1024)).toBe(true)
        expect(sent.at(-1)?.[0]).toBe("end")
    })

    test("carries chunks of any value unchanged", async () => {

        const { left, right } = pair()
        const values = [{ line: 1 }, "two", [3]]

        const stream = new ReadableStream({ start(controller) { for (const value of values) controller.enqueue(value); controller.close() } })

        const received: unknown[] = []

        for await (const chunk of right.import(left.export(stream))) received.push(chunk)

        expect(received).toEqual(values)
    })

    test("reads nothing until the receiver pulls, and no further than its window", async () => {

        const { left, right } = pair({ window: 4 })
        const { stream, state } = source(100, 10)

        const id = left.export(stream)

        await settle()
        expect(state.read).toBe(0)

        const reader = right.import(id).getReader()

        await settle()
        expect(state.read).toBe(4)

        await reader.read()
        await settle()
        expect(state.read).toBe(5)

        await reader.cancel()
    })

    test("canceling the received stream cancels its source", async () => {

        const { left, right } = pair()
        const { stream, state } = source(100, 10)

        const reader = right.import(left.export(stream)).getReader()

        await reader.read()
        await reader.cancel(new Error("Enough"))
        await settle()

        expect(state.canceled).toBeInstanceOf(Error)
        expect((state.canceled as Error).message).toBe("Enough")
    })

    test("a failing source fails the received stream with its message", async () => {

        const { left, right } = pair()
        const stream = new ReadableStream({ pull() { throw new Error("Disk failed") } })

        await expect(bytes(right.import(left.export(stream)))).rejects.toThrow("Disk failed")
    })

    test("closing the relay fails what it receives and cancels what it sends", async () => {

        const { left, right } = pair()
        const { stream, state } = source(100, 10)
        const incoming = right.import(left.export(new ReadableStream({ pull() { } }))).getReader()

        right.export(stream)
        right.close(new Error("Disconnected"))
        await settle()

        await expect(incoming.read()).rejects.toThrow("Disconnected")
        expect((state.canceled as Error).message).toBe("Disconnected")
        expect(() => right.export(new ReadableStream())).toThrow("Disconnected")
    })

    test("a stream nobody pulls is canceled after the idle time, and its reference fails", async () => {

        const { left, right } = pair({ idle: 10 })
        const { stream, state } = source(1, 10)

        const id = left.export(stream)

        await new Promise(resolve => setTimeout(resolve, 40))

        expect((state.canceled as Error).message).toBe("The stream was not read in time")
        await expect(bytes(right.import(id))).rejects.toThrow("The stream is no longer available")
    })

    test("refuses chunks for a stream it is not reading", async () => {

        const { right, sent } = pair()

        right.receive(["chunk", "unknown", new Uint8Array(1)])

        expect(sent).toEqual([["cancel", "unknown", "The stream is not being read"]])
    })

    test("recognizes only well-formed relay messages", () => {

        expect(StreamRelay.isMessage(["pull", "id", 2])).toBe(true)
        expect(StreamRelay.isMessage(["pull", "id", 0])).toBe(false)
        expect(StreamRelay.isMessage(["end", "id"])).toBe(true)
        expect(StreamRelay.isMessage(["cancel", "id", 1])).toBe(false)
        expect(StreamRelay.isMessage(["publish", "id"])).toBe(false)
    })
})
