# The Link Core

Composable bidirectional event routing without a transport or application model.
Core provides the primitives shared by every The Link adapter: tunnels, links,
synchronized properties, codecs, and method decorators.

## Install

```sh
npm install @the-link/core
```

## Tunnel

A `Tunnel` publishes named events and aggregates the values returned by their
handlers.

```ts
import { Tunnel } from "@the-link/core"

const tunnel = new Tunnel()

tunnel.subscribe("sum", (left: number, right: number) => left + right)

const total = await tunnel.publishFirst<number>("sum", 20, 22)

console.log(total) // 42
```

Every publication passes through four ordered phases:

1. Interceptors transform or reject the payload.
2. Subscribers handle the exact event.
3. Forwarders route matching event prefixes.
4. Finalizers transform the aggregated results.

Registration methods return cleanup functions, so routing can be composed and
removed without retaining separate listener bookkeeping.

```ts
const unsubscribe = tunnel.subscribe("status", console.log)

await tunnel.publish("status", "ready")
unsubscribe()
```

`subscribeOnce()`, `waitFor()`, and `waitFirst()` cover one-time and awaitable
events. `forwardTo()` connects a tunnel or forwarding function and can rewrite
the matching event prefix.

## TheLink

`TheLink` is the transport-neutral unit built from two tunnels:

- `$outbound` carries events toward an adapter or another routing layer.
- `$inbound` carries events received from an adapter or another routing layer.

Links compose through `subscribeTo()`, `publishTo()`, and `connectTo()`. Prefix
arguments create namespaces while the returned cleanup function removes the
relationship.

```ts
import { TheLink } from "@the-link/core"

const application = new TheLink()
const account = new TheLink()

const disconnect = application.connectTo(account, "", "account:")

disconnect()
```

Core does not decide what an event means or which transport carries it. The
Client, Server, Process, and Tab packages adapt this same contract to concrete
communication boundaries.

## Properties

`Property` synchronizes one provider-owned value through a link. A public
provider accepts update requests; a private provider rejects them. Consumers
bind to the provider's generated key.

```ts
import { Property, TheLink } from "@the-link/core"

const providerLink = new TheLink()
const consumerLink = new TheLink()

providerLink.$outbound.forwardTo(consumerLink.$inbound)
consumerLink.$outbound.forwardTo(providerLink.$inbound)

const theme = Property.public(providerLink, "light")
const snapshot = theme.toJSON()

const remoteTheme = Property.consumer(consumerLink, snapshot.key, snapshot.value)

remoteTheme.tunnel.subscribe("change", value => console.log(value))
await theme.update("dark")
```

The links must already be connected through the routing or transport topology.
Each property exposes its current `value`, public `key`, local `change` tunnel,
and an optional inbound interceptor.

## Decorators

The ECMAScript decorators register methods against a link during construction:

```ts
import { TheLink } from "@the-link/core"
import { Subscribe } from "@the-link/core/decorators"

class ApplicationLink extends TheLink {
    @Subscribe("greet")
    async greet(name: string) {
        return `Hello ${name}`
    }
}
```

The available decorators are `Intercept`, `Subscribe`, `Forward`, `Finalize`,
`Publish`, and `Connect`. Projects that still compile TypeScript's legacy
decorator form can import the compatible implementations from
`@the-link/core/decorators/legacy`.

## Entry points

| Import | Contract |
| --- | --- |
| `@the-link/core` | links, tunnels, properties, codecs, and their types |
| `@the-link/core/decorators` | ECMAScript decorators |
| `@the-link/core/decorators/legacy` | legacy TypeScript decorators |

The default codec serializes JSON as UTF-8 bytes. A transport using a custom
codec must configure the same serializer and deserializer at both ends.

## Development

```sh
bun install --frozen-lockfile
bun run verify
```

## License

MIT
