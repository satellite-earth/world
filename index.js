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

		// Called when historical epoch is loaded
		this.onBuild = config.onBuild;

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

		// Assemble current epoch
		const current = { epoch, position: this.position };

		if (options.initial) {
			current.initial = initial;
		}

		// Signals tentatively included in current epoch
		current.signals = options.signals ? this.epoch.signals.filter(signal => {
			return !options.since || signal.blockNumber >= options.since;
		}).map(signal => {
			return signal.payload;
		}) : [];

		// Uuids of dropped signals since last contact
		current.dropped = options.signals ? Object.keys(this.signals.dropped).filter(uuid => {
			return !options.since || this.signals.dropped[uuid] >= options.since;
		}) : [];
		
		// Include nameserver endpoints
		const ns = Object.keys(this.earth.nameserver);
		if (ns.length > 0) {
			current.ns = {};
			for (let name of ns) {
				current.ns[name] = this.earth.nameserver[name].endpoint;
			}
		}

		const response = { current };

		// Include previous epochs if requested
		if (options.history) {
			response.history = this.history;
		}

		return response;
	}

	// Adds a signal to pool awaiting verification after
	// checking that signal contains proper params
	receive (data) {

		let signal;

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
				throw Error('Epoch context inconsistent');
			}

			if (typeof signal.blockNumber === 'undefined') {
				throw Error('Missing \'blockNumber\' param');
			}

			// Check that block has not already been included
			if (signal.blockNumber <= this.position) {
				throw Error('Block has already been included');
			}

			// Loop backward to compare recently received signals first
			for (let z = this.signals.received.length - 1; z >= 0; z--) {
				if (signal.uuid === this.signals.received[z].uuid) {
					throw Error('Duplicate signal');
				}
			}

		} catch (error) {
			this.event('onIgnore', { signal, error });
			return;
		}

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
			return item instanceof Epoch ? item : new Epoch(item);
		}).sort((a, b) => {
			return parseInt(a._signed_.number) - parseInt(b._signed_.number);
		}) : [];

		if (this.history.length > 0) { // If historical epochs provided

			// Iterate through historical epochs to build world
			for (let i = 0; i < this.history.length; i++) {

				const epoch = new Epoch(this.history[i].payload);

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

				// Fire event with inflated epoch
				this.event('onBuild', epoch);

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

	// Detect recent blocks and apply signals to state
	async advance (toBlock = this.position) {

		//let toBlock = to;
		
		if (!this.listening) {
			console.log('Synchronization in progress, skipped advance');
			return;
		}

		// Only sync clock if new blocks have been created since last advance
		if (toBlock <= this.position && this.earth.clock.initialized) {
			console.log('No new blocks, skipped advance');
			return;
		}

		// Stop listening for signals while advancing in time, signals
		// received will be placed into the buffered array temporarily
		this.listen(false);

		// Top level name records from Ethereum
		let directoryUpdates = [];

		// Block hash data for verifying signal timestamps
		let clockUpdates = {};

		// Signals with confirmed timestamp
		const detected = [];

		// Signals that failed verification
		const rejected = [];

		// Signals that passed verification
		const included = [];

		// Signals waiting for detection
		let pending = [];

		// Sync clock with new blocks as necessary
		if (!this.earth.clock.initialized || toBlock > this.position) {

			// Get block numbers whose hashes need to be synced
			const subset = this.signals.received.map(signal => {
				return signal.blockNumber;
			}).filter(n => {
				return n <= toBlock;
			});

			try {

				// Synchronize the clock with latest blocks
				clockUpdates = await this.earth.synchronizeClock({
					startBlock: this.epoch.alpha,
					getBlock: this.getBlock,
					toBlock,
					subset
				});

			} catch (err) {
				console.log('Failed to synchronize clock, skipped advance', err);
				this.listen(true); // Reset listener
				return;
			}
		}

		// Sync top level directory as necessary
		if (!this.earth.directory.initialized || toBlock > this.earth.directory.blockNumber ) {

			try {

				// Synchronize the directory to latest confirmed block
				directoryUpdates = await this.earth.synchronizeDirectory(toBlock);

			} catch (err) {
				console.log('Failed to synchronize directory, skipped advance', err);
				this.listen(true); // Reset listener
				return;
			}
		}

		// Loop across received signals
		for (let signal of this.signals.received) {

			// If signal falls within range
			if (signal.blockNumber <= toBlock) {

				try {

					// Compare signed blockhash to value
					// from Ethereum to verify timestamp
					signal.locateSync(this.earth.clock);
					detected.push(signal);

				} catch (error) {
					this.event('onReject', { signal, error });
					rejected.push(signal);
				}

			} else {
				pending.push(signal);
			}
		}

		// For those signals with a verified timestamp
		if (detected.length > 0) {

			try {

				// Synchronize with nameserver(s) to get the latest namespace
				// records for each sender. If any nameserver responds with an
				// error, this could indicate that it has not yet synced to the
				// required block position so signals are sent back to pending.
				await this.earth.synchronizeNamespace(detected, toBlock);

			} catch (err) {
				console.log('Failed to synchronize namespace, skipped advance');
				pending = pending.concat(detected);
				this.listen(true); // Reset listener
				return;
			}			
		}

		// Keep pending signals for next advance
		this.signals.received = pending;

		// Sort detected signals temporal ascending with
		// Signal's native deterministic comparator. It's
		// critical that the order in which signals are
		// included is unambigious so that all observers
		// can agree on the final value of all states in
		// the epoch after iterating across the signals.
		detected.sort((a, b) => { return a.compare(b); });

		// Loop through each signal to be applied
		for (let signal of detected) {

			try { // Try to modify the state with signal data

				// Verify authorship, integrity, and context
				signal.verifySync(this.earth);

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
			included,
			rejected,
			clockUpdates,
			directoryUpdates,
			position: toBlock
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

		// Identify and record timestamp of the omega block
		this.epoch.addParams({
			released: this.earth.clock.readNumber(this.epoch.omega).timestamp,
			blockNumber: this.epoch.omega
		});

		// Sanity check - verify epoch signed by world signer
		this.epoch.verifySync(this.earth);

		// Handle the newly released epoch. Wait for this
		// call to succeed before initializing next epoch.
		// This function is provided to the world when it
		// is created and is expected to handle saving the
		// epoch meta, epoch data, and raw data for states.
		await this.releaseEpoch(this.epoch);

		// Push the epoch into history
		this.history.push(new Epoch(this.epoch.payload));

		// Clear any remaining signal data
		this.signals.buffered = [];
		this.signals.received = [];
		this.signals.dropped = {};

		// Initialize succeeding epoch
		this.epoch = await this.epoch.next({ name: this.signer });

		// Resume listening for signals
		this.listen(true);
	}

	get initialized () {
		return this.position !== null;
	}
}

module.exports = World;
