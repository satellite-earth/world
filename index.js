
const Epoch = require('@satellite-earth/epoch');
const Signal = require('@satellite-earth/signal');

class World {

	constructor (earth, config) {

		if (!earth) {
			throw Error('Must provide @earth/core instance as first argument');
		}

		if (typeof config.genesis === 'undefined') {
			throw Error('Must provide \'genesis\' block number for world');
		}

		if (typeof config.signer === 'undefined') {
			throw Error('Must provide \'signer\' (utf8 alias of epoch signer)');
		}

		if (typeof config.getTorrentData === 'undefined') {
			throw Error('Must provide async function \'getTorrentData\'');
		}

		if (typeof config.releaseEpoch === 'undefined') {
			throw Error('Must provide async function \'releaseEpoch\'');
		}

		if (config.genesis < earth.deployed) {
			throw Error(`\'genesis\' block number must be no less than ${earth.deployed}`);
		}

		// Earth API instance provides high level api for
		// signing and verifying data against Ethereum
		this.earth = earth;

		// Each signal is signed by its author
		this.signals = {

			// Signals are buffered if the listener is suspended
			// and will be passed to receive when listen resumes
			buffered: [],

			// Received signals awaiting inclusion. Calling advance
			// moves signals with matching block hashes into epoch
			received: [],

			// Uuids of signals that were removed after being included,
			// mapped to the block number at which each was dropped
			dropped: {}
		};

		// Epochs are torrents that contain signals. This array
		// holds the models for the epochs comprising the world
		this.history = [];

		// Alias name of person who can sign new epochs
		this.signer = config.signer;

		// Called just before signal is added to 'buffered'
		this.onBuffer = config.onBuffer;

		// Called just before signal is added to 'received'
		this.onReceive = config.onReceive;

		// Called when receive function does not accept signal
		this.onIgnore = config.onIgnore;

		// Called just after world advances to new block position
		this.onAdvance = config.onAdvance;

		// Called when an error occurs during state update
		this.onReject = config.onReject;

		// Called when signals are dropped from epoch
		this.onDrop = config.onDrop;

		// Optional function to return new block. See docs:
		this.getBlock = config.getBlock;

		// Return torrent data for loading epochs/states
		this.getTorrentData = config.getTorrentData;

		// Required to handle newly released epochs
		this.releaseEpoch = config.releaseEpoch;

		// Number of confirmations before including block
		this.confirm = config.confirm || 12;

		// Minimum block number of first epoch
		this.genesis = config.genesis;

		// Current block number, null until calling advance
		this.position = null;

		// World cannot receive signals until calling listen
		this.listening = false;
	}

	// Each defined event handler is called with the
	// world instance itself as the second parameter
	event (name, args) {
		try {
			if (this[name]) { this[name](args, this); }
		} catch (err) {
			console.log(`Error in event ${name}`, err);
		}
	}

	// Set whether world should listen for signals. Signals
	// received while listening=false will be stored in the
	// signals.buffered array and are passed back to receive
	// when listening is set to true. It's useful to be able
	// to temporarily pause signal reception for certain ops
	// like synchronizing the world clock and/or directory.
	listen (listening) {

		// Set receiver state
		this.listening = listening;

		// If receiver reactivating
		if (this.listening) {

			// Pass each buffered signal to receive
			for (let signal of this.signals.buffered) {
				this.receive(signal);
			}

			// Clear buffered array
			this.signals.buffered = [];
		}
	}

	// Return meta data that the client will need to
	// synchronize its state with this world's state
	contact (options = {}) {

		// Meta data for current epoch
		const epoch = {
			name: this.signer,
			alpha: String(this.epoch.alpha),
			number: String(this.epoch.number)
		};

		// Genesis epochs do not have an ancestor
		if (this.epoch.number > 0) {
			epoch.ancestor = this.epoch.ancestor;
		}

		// Meta data for initial states
		const initial = this.epoch.initial.map(state => {
			return state._signed_;
		});

		// Signals tentatively included in current epoch
		const signals = this.epoch.signals.filter(signal => {
			return !options.since || signal.blockNumber >= options.since;
		}).map(signal => {
			return signal.payload;
		});

		// Uuids of dropped signals since last contact
		const dropped = Object.keys(this.signals.dropped).filter(uuid => {
			return !options.since || this.signals.dropped[uuid] >= options.since;
		});

		// Return meta data sufficient to reconstruct the
		// entire state of the world up until the present
		return {
			current: { epoch, initial, signals, dropped, position: this.position },
			history: this.history
		};
	}

	// Adds a signal to pool awaiting verification after
	// checking that signal contains proper params
	receive (data) {

		let signal;
		let ok;

		try {

			// Instantiate new signal model if necessary
			signal = data instanceof Signal ? data : new Signal(data);

			// If world is not listening (such as when syncing the clock/directory)
			// buffer signal so it will be passed back to receive on call to listen
			if (!this.listening) {
				this.event('onBuffer', data);
				this.signals.buffered.push(signal);
				return;
			}

			// Check signed epoch uuid matches uuid of previous epoch
			if (signal.epoch !== this.epoch.ancestor) {
				throw Error('Epoch does not match');
			}

			// Check that block has not already been included
			const block = this.earth.clock.readHash(signal.block);
			if (block && block.number <= this.position) {
				throw Error('Block has already been included');
			}

			// Loop backward to compare recently received signals first
			for (let z = this.signals.received.length - 1; z >= 0; z--) {
				if (signal.uuid === this.signals.received[z].uuid) {
					throw Error('Duplicate signal');
				}
			}

			ok = true;

		} catch (error) {
			this.event('onIgnore', { signal, error });
		}

		if (ok) { // Prevalidation ok

			// Remove any existing location params
			signal.clearLocation();

			// Add this world's domain param
			signal.addParams({ world: this.signer });

			// If signal was previously dropped (this can
			// happen when reloading signals on restart)
			if (signal.dropped) {

				// Repopulate its entry on the dropped record
				this.signals.dropped[signal.uuid] = signal.dropped;

			} else { // Otherwise, proceed

				// Put the message in the received pool
				// to be verified as the world advances
				this.signals.received.push(signal);
				this.event('onReceive', signal);
			}
		}
	}

	// Remove specific already-included signals from current epoch
	async drop (uuids) {

		// If currently advancing, immediately
		// return falsey value so caller knows
		// that drop could not be executed.
		if (!this.listening) {
			console.log('In prog, skipped drop');
			return false;
		}

		// Buffer signals while processing
		this.listen(false);

		// Remove signals and add to dropped record
		const dropped = await this.epoch.drop(uuids, this.getTorrentData);
		
		// Keep a record of the uuid of each signal
		// and the block number when it was dropped
		for (let signal of dropped) {
			this.signals.dropped[signal.uuid] = this.position;
		}

		// Fire event for external env
		this.event('onDrop', dropped);

		// Resume reception
		this.listen(true);

		// Indicate success
		return true;
	}

	// Recontruct the world from past epochs
	async build (history, getCurrent) {

		// Get epoch models sorted oldest to most recent
		this.history = history ? history.map(item => {
			return item instanceof Epoch ? item.payload : item;
		}).sort((a, b) => {
			return a.number - b.number;
		}) : [];

		if (this.history.length > 0) { // If historical epochs provided

			// Iterate through historical epochs to build world
			for (let i = 0; i < this.history.length; i++) {

				const epoch = new Epoch(this.history[i]);

				// Download signal data and load into epoch.
				// Epoch contains the logic for decoding the
				// data and applying each signal to the state.
				// If not first epoch, initialize states with
				// corresponding final states of previous epoch.
				const data = await this.getTorrentData(epoch);
				const body = data instanceof Buffer ? data : Buffer.from(data);
				await epoch.data(body, (state) => {
					return Buffer.from(this.epoch.state[state.name].compressed);
				});

				this.epoch = epoch;
			}

			// Initialize the most recent epoch
			this.epoch = await this.epoch.next({ name: this.signer });

		} else { // Current epoch defaults to genesis

			this.epoch = new Epoch({
				name: this.signer,
				alpha: this.genesis,
				number: 0
			});
		}

		// Apply current epoch signals, if provided
		if (getCurrent) {
			const current = await getCurrent(this.epoch);		
			for (let signal of current) {
				this.receive(signal);
			}
		}

		// Prepare to receive new signals
		this.listen(true);
	}

	// Detect recent blocks and apply confirmed signals to state
	async advance (to) {

		let directoryUpdates = [];
		let clockUpdates = {};
		let toBlock;
		
		if (!this.listening) {
			console.log('In prog, skipped advance');
			return;
		}

		try {

			// Set 'toBlock' as provided value, or set automatically
			// based on the latest block number minus confirmations
			if (typeof to !== 'undefined') {
				toBlock = to;
			} else {
				const latest = await this.earth.web3.eth.getBlockNumber();
				toBlock = latest - this.confirm;
			}			

		} catch (err) {
			console.log('Network error, skipped advance');
			return;
		}

		// Only sync clock if new blocks have
		// been created since last advance
		if (this.earth.clock.initialized) {
			if ((toBlock <= this.position) || (toBlock === this.earth.clock.max.number)) {
				console.log('No new blocks, skipped advance');
				return;
			}
		}

		// Stop listening for signals while advancing in time, signals
		// received will be placed into the buffered array temporarily
		this.listen(false);

		try {

			// Synchronize the clock with latest blocks
			clockUpdates = await this.earth.synchronizeClock({
				startBlock: this.epoch.alpha,
				getBlock: this.getBlock,
				toBlock
			});

		} catch (err) {
			console.log('Failed to synchronize clock, skipped advance');
			this.listen(true); // Reset listener
			return;
		}

		try {

			// Synchronize the directory to latest confirmed block
			directoryUpdates = await this.earth.synchronizeDirectory(toBlock);

		} catch (err) {
			console.log('Failed to synchronize directory, skipped advance');
			this.listen(true); // Reset listener
			return;
		}

		// Signals that have reached min
		// number of block confirmations
		const confirmed = [];

		// Signals waiting for confirmation
		const pending = [];

		// Signals verified and included in the epoch
		const included = [];

		// Signals that failed verification/inclusion
		const rejected = [];

		// Locate each signal against clock time
		// and split into two arrays accordingly
		for (let signal of this.signals.received) {
			signal.locateSync(this.earth);
			if (signal.located && signal.blockNumber <= toBlock) {
				confirmed.push(signal);
			} else {
				pending.push(signal);
			}
		}

		// Keep pending signals for next advance
		this.signals.received = pending;

		// Sort confirmed signals temporal ascending with
		// Signal's native deterministic comparator. It's
		// critical that the order in which signals are
		// included is unambigious so that all observers
		// can agree on the final value of all states in
		// the epoch after iterating across the signals.
		confirmed.sort((a, b) => { return a.compare(b); });

		// Loop through each signal to be applied
		for (let signal of confirmed) {

			try { // Try to modify the state with signal data

				// Verify authorship, integrity, and context
				signal.verifySync(this.earth, this.earth.clock.readHash(signal.block).number);

				// Include signal in current epoch
				this.epoch.include(signal);

				// Add to included array
				included.push(signal);

			} catch (error) {
				this.event('onReject', { signal, error });
				rejected.push(signal);
			}
		}

		// Remember the latest block number included
		this.position = toBlock;

		// Fire event with new position, included/rejected signals,
		// new synchronized block data and new directory logs
		this.event('onAdvance', {
			position: toBlock,
			included,
			rejected,
			clockUpdates,
			directoryUpdates
		});

		// Resume listening
		this.listen(true);
		console.log(`World advanced to ${toBlock}`);
	}

	// Finalize the current epoch and pause
	// the world in preparation for release
	async stage (omega) {

		this.listen(false);

		return await this.epoch.finalize(omega);
	}

	// Provide the signature to release the finalized
	// epoch. This allows the world signer to create
	// the signature elsewhere (like in the browser)
	async release (signature) {

		this.epoch.authorAlias = this.signer;
		this.epoch.signature = signature;

		// Sanity check - verify epoch signed by world signer
		this.epoch.verifySync(this.earth, this.epoch.omega);

		// Identify and record timestamp of the omega block
		const { timestamp } = this.earth.clock.readNumber(this.epoch.omega);
		this.epoch.addParams({ released: timestamp });

		// Handle the newly released epoch. Wait for this
		// call to succeed before initializing next epoch.
		// This function is provided to the world when it
		// is created and is expected to handle saving the
		// epoch meta, epoch data, and raw data for states.
		await this.releaseEpoch(this.epoch);

		// Push the epoch into history
		this.history.push(this.epoch.payload);

		// Clear any remaining signal data
		this.signals.buffered = [];
		this.signals.received = [];
		this.signals.dropped = {};

		// Initialize succeeding epoch
		this.epoch = await this.epoch.next({ name: this.signer });

		// Resume listening
		this.listen(true);
	}

	get initialized () {
		return this.position !== null;
	}
}

module.exports = World;
