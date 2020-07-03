View full documentation at https://docs.satellite.earth/

## Usage

``` js
const World = require('@satellite-earth/world');
const { Earth } = require('@satellite-earth/core');

// API to interface with contract
const earth = new Earth();

// Connect using HttpProvider (e.g. Infura)
await earth.connect({
	httpProviderUrl: PROVIDER_URL,
	httpBasicAuthParams: { password: PASSWORD }
});

// Create world instance with your configuration
const world = new World(earth, {

	// Block number at which the first epoch begins
	genesis: 10190026,

	// Alias name of person who may sign epochs
	signer: 'alice',

	// This function should return Ethereum block meta
	// data from a local cache instead of having to sync
	// directly from the blockchain on each restart.
	getBlock: async (number) => {

		// . . .

		return block;
	},

	// This function should return a torrent's data.
	// It's used by the world instance to initialize
	// past epochs and/or to load initial state data.
	getTorrentData: async (torrent) => {

		// . . .

		return data;
	},

	// Handle newly created epochs
	releaseEpoch: async (epoch) => {

	}
});
```

**Constructor**

1. `Earth` - Earth API instance
2. `Object` - Configuration
	- `confirm` - `Number` - Number of blocks "window" to allow for inclusion of new signals, i.e. the world's lag behind the latest block (default `12`). It's recommended that in production you do not set a lower value that this.
	- `signer` - `String` - (required) Alias name of the person who has the right to sign this world's epochs
	- `genesis` - `Number` - (required) Block number when the first epoch begins
	- `getBlock` - `Function` - Async function called called with block number should return block meta daa
	- `getTorrentData` - `Function` - (required) Async function called with torrent model should return torrent data
	- `releaseEpoch` - `Function` - (required) Async function should handle releasing epoch when new epoch is created
	- `onBuffer` - `Function` - Called with signal that was received while world instance was in a non-listening state (e.g. during advance, or pending epoch release)
	- `onReceive` - `Function` - Async function called with signal received
	- `onIgnore` - `Function` - Async function called with `{ signal, error }` when the world instance declines to receive a signal
	- `onAdvance` - `Function` - Async function called with signals `{ included, rejected }` after the world advances to a new block position
	- `onDrop` - `Function` - Returns array of signals when they are dropped from current epoch

## Properties

### signals

``` js
const { received, buffered, dropped } = world.signals;
```

`Object` - Signals of the current epoch

- `received` - `Array` - Signals pending inclusion in current epoch
- `buffered` - `Array` - Signals buffered and pending reception
- `dropped` - `Object` - Map of signals dropped by world signer

---

### history

``` js
world.history
```

`Array` - Meta data of previous world epochs

---

### signer

``` js
world.signer // 'alice'
```

`String` - Alias name of the person who has the right to sign this world's epochs

---

### genesis

``` js
world.genesis // 10190026
```

`Number` - Block number when the first epoch begins

---

### position

``` js
world.position // 10375893
```

`Number` - Block number to which the world has advanced

---

### listening

``` js
world.listening // true
```

`Boolean` - If world is currently receiving new signals

---

### confirm

``` js
world.confirm // 12
```

`Number` - Number of blocks "window" to allow for inclusion of new signals, i.e. the world's lag behind the latest block (default `12`).

---

### initialized

``` js
world.initialized // true
```

`Boolean` - If world is currently initialized (the first call to `advance` initializes the world)

## Methods

### listen

``` js
world.listen(true);
```

Manually set world listening state. Any signals held in the `buffered` array are automatically passed to `receive` when listening is set to `true`.

**Parameters**

1. `Boolean` - Listening state

**Returns**

---

### contact

``` js
world.contact();
```

Get meta data to reconstruct the world up until the present (this is the data sent to the [Client](/client)).

**Parameters**

1. `Object` - Options
- `since` - `Number` - Minimum block number for signals to be sent. By default, clients cache signals they have received from a world on their local device. This option is provided to save bandwidth, allowing clients to request only those recent signals of the current epoch that the world has collected since last contact.

**Returns**

`Object` - World meta data: `history` (meta data for past epochs) and `current` including (`epoch`, `initial`, `signals`, `dropped`, and `position`)

---

### receive

``` js
world.receive(signal);
```

[description]

**Parameters**

1. `Signal|Object` - Instance of `Signal` or signal payload

---

### drop

``` js
await world.drop([ '04344e0a19509945a8383dac5f0e8ada9da6640c', '3b58748191ff2bbf98cdfbb63710babe030bc122' ])
```

Remove signals from the current epoch.

**Parameters**

1. `Array` - Signal `uuid`s to remove

**Returns**

`Promise` returns `Boolean` - If drop succeeded (drop will fail if world instance is not in a `listening` state)

---

### build

``` js
await world.build(history, async (currentEpoch) => {

	// . . .

	return currentSignals;
})
```

Recontruct the world from past epochs. Normally called on server restart to repopulate the world's data from an external database.

**Parameters**

1. `Array` - Past epoch meta data in world `history`
2. `Function` - Async function called with current epoch model should return signals of the current epoch

**Returns**

`Promise`

---

### advance

``` js
await world.advance();
```

Advance the world's block number position, i.e. include any signals held in `signals.received` with timestamps less than or equal to that value.

**Parameters**

1. `Number` - (optional) Block number to which world should advance (default 12 blocks behind latest block)

**Returns**

`Promise`

---

### stage

``` js
const epoch = await world.stage(10290314);
```

Get finalized current epoch and pause the world's listening state in preparation for release.

**Parameters**

1. `Number` - Final `omega` (ending) block number for the current epoch

**Returns**

`Promise` returns `Epoch`

---

### release

``` js
await world.release('2280542a7dd337a1c8a92125dd399e482689bd22b5cf85fa1d6d09930bac8b7f4e95902fe4a72e29ee79583a169d89b06b51049d6c02c04268ec790d061c2f5b1c')
```

Release the current staged epoch and create the next.

**Parameters**

1. `String` - World signer's signature

**Returns**

`Promise`
