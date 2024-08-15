/*
	Roots DB

	Copyright (c) 2014 - 2021 Cédric Ronvel

	The MIT License (MIT)

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
*/

"use strict" ;



const Promise = require( 'seventh' ) ;
const events = require( 'events' ) ;
const url = require( 'url' ) ;
const mongodb = require( 'mongodb' ) ;
const MongoClient = mongodb.MongoClient ;
const tree = require( 'tree-kit' ) ;
const ErrorStatus = require( 'error-status' ) ;

const log = require( 'logfella' ).global.use( 'roots-db:mongodb' ) ;



function MongoDriver( collection ) {
	this.collection = collection ;

	var pathname ;
	var parts = collection.config.pathname.split( '/' ) ;
	//console.log( 'parts: ' , parts ) ;

	if ( parts.length === 3 ) {
		// Get the collection's name
		this.upstreamCollection = parts.pop() ;
		pathname = parts.join( '/' ) ;
	}

	var urlObject = new url.URL( collection.config ) ;
	urlObject.pathname = pathname ;
	urlObject.searchParams.set( 'compressors' , 'zstd' ) ;	// Force zstd
	this.url = url.format( urlObject ) ;

	if ( this.url.includes( '%' ) ) {
		// Problems has been reported with password containing invalid URL-parts characters
		log.warning( "The Roots-DB's mongodb driver connection string contains percent-encoded parts that may cause incompatibilities with the underlying mongodb module: %s" , this.url ) ;
	}

	this.client = null ;
	this.db = null ;
	this.mongoCollection = null ;
	this.raw = null ;	// RAW acces to the driver, for this driver this is always driver.mongoCollection
	this.checkFrozen = collection.freezable ;
	//this.ready = false ;

	this.connectPromise = Promise.dormant( resolve => resolve( this._autoConnect() ) ) ;

	//console.log( 'url: ' , this.url ) ;
	//console.log( 'upstreamCollection: ' , this.upstreamCollection ) ;

	this.getIndexes = this.commonGateway( 'getIndexes' ) ;
	this.dropIndex = this.commonGateway( 'dropIndex' ) ;
	this.buildIndex = this.commonGateway( 'buildIndex' ) ;

	this.get = this.commonGateway( 'get' ) ;
	this.getUnique = this.commonGateway( 'getUnique' ) ;
	this.create = this.commonGateway( 'create' ) ;
	this.overwrite = this.commonGateway( 'overwrite' ) ;
	this.update = this.commonGateway( 'update' ) ;
	this.patch = this.commonGateway( 'patch' ) ;
	this.delete = this.commonGateway( 'delete' ) ;
	this.multiGet = this.commonGateway( 'multiGet' ) ;
	this.collect = this.commonGateway( 'collect' ) ;
	this.find = this.commonGateway( 'find' ) ;
	this.findIdList = this.commonGateway( 'findIdList' ) ;
	this.findEach = this.commonGateway( 'findEach' ) ;
	this.getNextCounterFor = this.commonGateway( 'getNextCounterFor' ) ;

	this.freeze = this.commonGateway( 'freeze' ) ;
	this.unfreeze = this.commonGateway( 'unfreeze' ) ;

	this.clear = this.commonGateway( 'clear' ) ;

	this.lock = this.commonGateway( 'lock' ) ;
	this.unlock = this.commonGateway( 'unlock' ) ;
	this.lockingQuery = this.commonGateway( 'lockingQuery' ) ;
	this.releaseLocks = this.commonGateway( 'releaseLocks' ) ;
	this.lockingFind = this.commonGateway( 'lockingFind' ) ;
}



module.exports = MongoDriver ;
MongoDriver.prototype = Object.create( events.prototype ) ;
MongoDriver.prototype.constructor = MongoDriver ;



MongoDriver.prototype.type = 'mongodb' ;
MongoDriver.prototype.idKey = '_id' ;
MongoDriver.prototype.idConstructor = mongodb.ObjectId ;
MongoDriver.prototype.idPrototype = mongodb.ObjectId.prototype ;



// Driver preferences
MongoDriver.prototype.immutablePrototypes = [ mongodb.ObjectId.prototype ] ;
//MongoDriver.prototype.pathSeparator = '.' ;



/*
	This is a decorator that factorize a lot of common work,
	like checking connection, patching errors, and so on...
*/
MongoDriver.prototype.commonGateway = function( method ) {
	return ( ... args ) => {

		//console.log( "commonGateway for" , method ) ;
		return this.connectPromise.then(
			() => MongoDriver[ method ].call( this , ... args )
		).catch( error => {
			// Rewrite errors
			switch ( error.code ) {
				case 26 :
					// NS not found: the collection does not exist on the server
					// Create it and retry!
					// NOTE THAT THIS IS SURPRISINGLY SLOW!!!
					//console.log( "About to create collection:" , this.upstreamCollection ) ;
					return this.db.createCollection( this.upstreamCollection ).then( () => this[ method ]( ... args ) ) ;

				case 11000 : {
					// Duplicate key, forward a more meaningful error, extracting data from the error message
					let forwardError = ErrorStatus.conflict( { message: "Duplicate key" , code: "duplicateKey" } ) ;
					//forwardError.fromError = error ;
					forwardError.key = error.keyValue ;

					let match = error.errmsg.match( /collection: *([^ .]+)\.([^ .]+) index: *([^ ]+)/ ) ;

					if ( match ) {
						forwardError.db = match[ 1 ] ;
						forwardError.collection = match[ 2 ] ;
						forwardError.indexName = match[ 3 ] ;
					}
					//log.hdebug( "Duplicate key error: %E\nInspection: %Y" , error , error ) ;

					throw forwardError ;
				}

				case 17280 : {
					// key too large to index
					let forwardError = ErrorStatus.badRequest( { message: "Key too large to index (up to 1024 bytes supported)" , code: "keyTooLargeToIndex" } ) ;
					//log.hdebug( "key too large to index error: %E\nInspection: %J" , error , error ) ;
					throw forwardError ;
				}

				default :
					throw error ;
			}
		} ) ;
	} ;
} ;



// Return a promise for connection
MongoDriver.prototype._autoConnect = function() {
	//log.debug( 'entering _autoConnect() for %s...' , this.url ) ;

	// Connect to the upstream db
	return MongoClient.connect( this.url ).then( client => {
		//console.log( '_autoConnect() succeeded!' ) ;
		this.client = client ;
		this.db = this.client.db() ;
		this.raw = this.mongoCollection = this.db.collection( this.upstreamCollection ) ;
	} ) ;
} ;



// Only used by unit test...
MongoDriver.prototype.rawInit = function() { return this.connectPromise ; } ;
//MongoDriver.prototype.rawId = function( rawDocument ) { return rawDocument._id.toString() ; }
MongoDriver.prototype.createId = function( from ) { return new mongodb.ObjectId( from ) ; } ;



MongoDriver.prototype.checkId = function( rawDocument , enforce ) {
	var id ;

	if ( rawDocument._id ) {
		id = rawDocument._id ;
	}

	if ( typeof id === 'string' ) {
		// Let it crash or fix it?
		try {
			id = new mongodb.ObjectId( id ) ;
		}
		catch ( error ) { id = new mongodb.ObjectId() ; }
	}
	else if ( enforce && ! ( id instanceof mongodb.ObjectId ) ) {
		id = new mongodb.ObjectId() ;
	}

	// Lock the id property?
	//if ( id ) { Object.defineProperty( rawDocument , '_id' , { value: id , enumerable: true } ) ; }
	// /!\ No, don't lock it, doormen needs write access on anything enumerable
	if ( id ) { rawDocument._id = id ; }

	return id ;
} ;



// Useful???
// Clone a raw document safely
MongoDriver.prototype.clone = function( rawDocument ) {
	var k ,
		exported = Array.isArray( rawDocument ) ? [] : {} ;

	for ( k in rawDocument ) {
		if ( rawDocument[ k ] && typeof rawDocument[ k ] === 'object' ) {
			if ( rawDocument[ k ] instanceof mongodb.ObjectId ) { exported[ k ] = rawDocument[ k ] ; }
			else { exported[ k ] = this.clone( rawDocument[ k ] ) ; }
		}
		else {
			exported[ k ] = rawDocument[ k ] ;
		}
	}

	return exported ;
} ;





/* Requests */



// Get *USER* indexes (not returning indexes on _id)
MongoDriver.getIndexes = function() {
	//console.log( mongodb.Collection.prototype ) ;

	return this.mongoCollection.getIndexes().then( rawIndexes => {

		//console.log( 'DRIVER: rawIndexes' , rawIndexes ) ;

		var input , output , key1 , key2 , name , indexes = {} ;

		for ( key1 in rawIndexes ) {
			name = '' ;
			output = {} ;
			input = rawIndexes[ key1 ] ;

			if ( input.name === '_id_' ) { continue ; }

			for ( key2 in input ) {
				switch ( key2 ) {
					case 'v' :
					case 'ns' :
					case 'background' :
						break ;
					case 'name' :
						name = input.name ;
						break ;
					case 'key' :
						output.properties = input.key ;
						break ;
					case 'unique' :
						output[ key2 ] = input[ key2 ] ;
						break ;
					case 'collation' :
						output[ key2 ] = input[ key2 ] ;
						break ;
					default :
						if ( ! output.driver ) { output.driver = {} ; }
						output.driver[ key2 ] = input[ key2 ] ;
				}
			}

			// Check hash consistency?
			/*
			var indexHash = hash.fingerprint( output ) ;
			console.log( 'indexHash:' , indexHash ) ;
			console.log( 'indexHash === name ?' , indexHash === name ) ;
			//*/

			output.name = name ;
			indexes[ name ] = output ;
		}

		//console.log( 'DRIVER: indexes' , indexes ) ;

		return indexes ;
	} ) ;
} ;



// Drop an index of a collection
MongoDriver.dropIndex = function( indexName ) {
	//log.debug( "DRIVER: dropIndex(): %s" , indexName ) ;
	return this.mongoCollection.dropIndex( indexName ) ;
} ;



// Index/re-index a collection
MongoDriver.buildIndex = function( index ) {
	var options = Object.assign(
		{
			name: index.name ,
			unique: !! index.unique ,
			background: true
		} ,
		index.driver
	) ;

	// That boring driver reject null/undefined values, so it should be defined outside of Object.assign()
	if ( index.collation ) { options.collation = index.collation ; }

	if ( index.partial ) {
		options.partialFilterExpression = {} ;
		for ( let property in index.properties ) {
			options.partialFilterExpression[ property ] = {
				$exists: true
				// Does not work, and there is no reliable way to reject null out of the indexes :S
				// Great, tons of flexibilities except for the very basic ultra-common use-case...
				// Update: it's 2024, and it's still not possible -_-'
				// , $ne: null
			} ;
		}
	}

	//log.debug( "DRIVER: createIndex(): %Y\n%Y" , index.properties , options ) ;
	return this.mongoCollection.createIndex( index.properties , options ) ;
} ;



// Get a document by ID
MongoDriver.get = function( id ) {
	if ( typeof id === 'string' ) { id = new mongodb.ObjectId( id ) ; }
	return this.mongoCollection.findOne( { _id: id } ) ;
} ;



// Get a document by a unique fingerprint
MongoDriver.getUnique = function( fingerprint ) {
	if ( typeof fingerprint._id === 'string' ) { fingerprint._id = new mongodb.ObjectId( fingerprint._id ) ; }
	return this.mongoCollection.findOne( fingerprint ) ;
} ;



// Create (insert) a new document
MongoDriver.create = function( rawDocument , lockId = null ) {
	if ( typeof rawDocument._id === 'string' ) { rawDocument._id = new mongodb.ObjectId( rawDocument._id ) ; }

	if ( lockId !== null ) {
		rawDocument._lockedBy = lockId ;
		rawDocument._lockedAt = new Date() ;
	}

	return this.mongoCollection.insertOne( rawDocument ) ;
} ;



// Overwrite a document: create if it does not exist or full update if it exists
MongoDriver.overwrite = function( rawDocument ) {
	if ( typeof rawDocument._id === 'string' ) { rawDocument._id = new mongodb.ObjectId( rawDocument._id ) ; }

	var filter = { _id: rawDocument._id } ;
	if ( this.checkFrozen ) { filter._frozen = { $ne: true } ; }

	return this.mongoCollection.replaceOne( filter , rawDocument , { upsert: true } ) ;
} ;



// Full update of a document
MongoDriver.update = function( id , rawDocument ) {
	if ( typeof id === 'string' ) { id = new mongodb.ObjectId( id ) ; }

	if ( rawDocument._id && rawDocument._id !== id ) { throw new Error( "Raw Document ID and query ID mismatch" ) ; }

	var filter = { _id: id } ;
	if ( this.checkFrozen ) { filter._frozen = { $ne: true } ; }

	return this.mongoCollection.replaceOne( filter , rawDocument ) ;
} ;



// Partial update (patch) of a document
MongoDriver.patch = function( id , rawPatch ) {
	if ( ! rawPatch.set && ! rawPatch.unset ) { return Promise.resolved ; }

	if ( typeof id === 'string' ) { id = new mongodb.ObjectId( id ) ; }

	var filter = { _id: id } ;
	if ( this.checkFrozen ) { filter._frozen = { $ne: true } ; }

	var dbPatch = {} ;

	if ( rawPatch.set ) {
		dbPatch.$set = rawPatch.set ;
		delete dbPatch.$set._id ;	// Should not be updated
	}

	if ( rawPatch.unset ) {
		dbPatch.$unset = rawPatch.unset ;
		delete dbPatch.$unset._id ;	// Should not be deleted
	}

	return this.mongoCollection.updateOne( filter , dbPatch ) ;
} ;



// Delete a document
MongoDriver.delete = function( id ) {
	if ( typeof id === 'string' ) { id = new mongodb.ObjectId( id ) ; }

	var filter = { _id: id } ;
	if ( this.checkFrozen ) { filter._frozen = { $ne: true } ; }

	return this.mongoCollection.deleteOne( filter ) ;
} ;



// Get a batch of documents given an array of ID
MongoDriver.multiGet = function( ids ) {
	var i , length = ids.length ;

	// First, check all ids
	for ( i = 0 ; i < length ; i ++ ) {
		if ( typeof ids[ i ] === 'string' ) { ids[ i ] = new mongodb.ObjectId( ids[ i ] ) ; }
	}

	return this.mongoCollection.find( { _id: { $in: ids } } ).toArray() ;
} ;



/*
	Get a batch of documents given some fingerprint
	Options supported:
	- skip
	- limit
*/
MongoDriver.collect = function( fingerprint , options ) {
	//console.log( 'driver collect fingerprint' , fingerprint ) ;
	if ( typeof fingerprint._id === 'string' ) { fingerprint._id = new mongodb.ObjectId( fingerprint._id ) ; }

	var cursor = this.mongoCollection.find( fingerprint ) ;
	if ( options ) { cursor = cursorOptions( cursor , options ) ; }

	return cursor.toArray() ;
} ;



// Get a batch of documents given a query object
MongoDriver.find = function( queryObject , options ) {
	// In others driver, the queryObject should be processed to construct a complex query.
	// But since roots-DB queries ARE MongoDB object's queries, there is nothing to do here.

	//console.log( 'driver find queryObject' , queryObject ) ;
	if ( typeof queryObject._id === 'string' ) { queryObject._id = new mongodb.ObjectId( queryObject._id ) ; }

	var cursor = this.mongoCollection.find( queryObject ) ;
	if ( options ) { cursor = cursorOptions( cursor , options ) ; }

	return cursor.toArray() ;
} ;



// Get a batch of documents' ID given a query object
// options.partial: return an array of objects having a _id property (a partial document)
MongoDriver.findIdList = function( queryObject , options ) {
	// In others driver, the queryObject should be processed to construct a complex query.
	// But since roots-DB queries ARE MongoDB object's queries, there is nothing to do here.

	//console.log( 'driver find queryObject' , queryObject ) ;
	if ( typeof queryObject._id === 'string' ) { queryObject._id = new mongodb.ObjectId( queryObject._id ) ; }

	var cursor = this.mongoCollection.find( queryObject ).project( { _id: 1 } ) ;
	if ( options ) { cursor = cursorOptions( cursor , options ) ; }

	if ( options.partial ) {
		return cursor.toArray() ;
	}

	return cursor.toArray().then( array => array.map( element => element._id ) ) ;

} ;



// Like find(), but do not return a batch
// For instance it is serialized, later we will have a 'concurrency' option
MongoDriver.findEach = async function( queryObject , options , iterator ) {
	var rawDocument ;

	//console.log( 'driver find queryObject' , queryObject ) ;
	if ( typeof queryObject._id === 'string' ) { queryObject._id = new mongodb.ObjectId( queryObject._id ) ; }

	var cursor = this.mongoCollection.find( queryObject ) ;
	if ( options ) { cursor = cursorOptions( cursor , options ) ; }

	while ( ( rawDocument = await cursor.next() ) ) {
		await iterator( rawDocument ) ;
	}

	return ;
} ;



// Get and increment a counter
MongoDriver.getNextCounterFor = async function( name ) {
	var result = await this.mongoCollection.findOneAndUpdate( { name } , { $inc: { counter: 1 } } , { upsert: true } ) ;

	// It returns the value before incrementation, so we have to add 1
	return result ? result.counter + 1 : 1 ;
} ;



// Freeze a document: make it not updatable
MongoDriver.freeze = function( id ) {
	if ( ! this.checkFrozen ) { throw new Error( "This collection is not freezable" ) ; }
	if ( typeof id === 'string' ) { id = new mongodb.ObjectId( id ) ; }
	return this.mongoCollection.updateOne( { _id: id } , { $set: { _frozen: true } } ) ;
} ;



// Unfreeze a document: make it updatable again
MongoDriver.unfreeze = function( id ) {
	if ( ! this.checkFrozen ) { throw new Error( "This collection is not freezable" ) ; }
	if ( typeof id === 'string' ) { id = new mongodb.ObjectId( id ) ; }
	return this.mongoCollection.updateOne( { _id: id } , { $set: { _frozen: false } } ) ;
} ;



// Collection-wide



// Delete all documents of the collection
MongoDriver.clear = function() {
	var filter = {} ;
	if ( this.checkFrozen ) { filter._frozen = { $ne: true } ; }

	return this.mongoCollection.deleteMany( filter ) ;
} ;



// Misc



function cursorOptions( cursor , options ) {
	// 'sort' MUST be *BEFORE* limit', because those operations are made in that order
	if ( options.sort ) {
		if ( options.collation ) {
			cursor = cursor.collation( options.collation ) ;
		}
		cursor = cursor.sort( options.sort ) ;
	}

	if ( options.skip ) {
		cursor = cursor.skip( options.skip ) ;
	}

	if ( options.limit ) {
		cursor = cursor.limit( options.limit ) ;
	}

	return cursor ;
}





/* Higher level methods */

/*
	Lock variants

	single doc get:
		- lock, it should affect exactly one doc
		- get the locked doc

	multi doc get 1:
		- lock
		- count how many doc are matching
		- number of lock and count should match

	multi doc get 2:
		- query for all docs
		- lock them all
		- the number of lock should match the number of doc
*/

MongoDriver.lock = function( id , lockTimeout ) {
	var lockId = new mongodb.ObjectId() ,
		now = new Date() ,
		timeoutBefore = new Date( now.getTime() - lockTimeout ) ;

	if ( typeof id === 'string' ) { id = new mongodb.ObjectId( id ) ; }

	var lockQueryObject = {
		_id: id ,
		$or: [
			{ _lockedBy: null } ,
			{ _lockedAt: { $lt: timeoutBefore } }
		]
	} ;

	var update = {
		$set: {
			_lockedBy: lockId ,
			_lockedAt: now
		}
	} ;

	return this.mongoCollection.updateMany( lockQueryObject , update ).then( result => result.matchedCount ? lockId : null ) ;
} ;



// Unlock a document by its ID, provided the correct lockId
MongoDriver.unlock = function( id , lockId ) {
	//log.error( 'releasing %I' , lockId ) ;
	return this.mongoCollection.updateMany(
		{ _id: id , _lockedBy: lockId } ,
		{ $set: { _lockedBy: null , _lockedAt: null } }
	).then( result => !! result.matchedCount ) ;
} ;



// Internal only?
// Lock documents and return a unique lockId, to be able to retrieve them
MongoDriver.lockingQuery = async function( queryObject , lockTimeout ) {
	var lockMatchCount , queryMatchCount ,
		lockId = new mongodb.ObjectId() ,
		now = new Date() ,
		timeoutBefore = new Date( now.getTime() - lockTimeout ) ;

	if ( typeof queryObject._id === 'string' ) { queryObject._id = new mongodb.ObjectId( queryObject._id ) ; }

	var lockQueryObject = Object.assign( {} , queryObject ) ;

	var lockPart = [
		{ _lockedBy: null } ,
		{ _lockedAt: { $lt: timeoutBefore } }
	] ;

	if ( ! lockQueryObject.$or ) {
		lockQueryObject.$or = lockPart ;
	}
	else {
		if ( ! lockQueryObject.$and ) { lockQueryObject.$and = [] ; }

		lockQueryObject.$and.push( { $or: lockPart } ) ;
		lockQueryObject.$and.push( { $or: lockQueryObject.$or } ) ;
		delete lockQueryObject.$or ;
	}

	var update = {
		$set: {
			_lockedBy: lockId ,
			_lockedAt: now
		}
	} ;

	lockMatchCount = ( await this.mongoCollection.updateMany( lockQueryObject , update ) ).matchedCount ;
	return lockMatchCount ? lockId : null ;
} ;



// Release a lock, given a lockId
MongoDriver.releaseLocks = function( lockId ) {
	return this.mongoCollection.updateMany(
		{ _lockedBy: lockId } ,
		{ $set: { _lockedBy: null , _lockedAt: null } }
	).then( result => result.matchedCount ) ;
} ;



// Lock, retrieve a documents from the query and pass a release function.
// When the 'other' argument (boolean) is set, things that are part of the query but that didn't lock are passed as the 2nd argument to actionFn.
MongoDriver.lockingFind = async function( queryObject , lockTimeout , other , actionFn ) {
	var rawBatch , otherRawBatch , allRawBatch ;
	var lockId = await this.lockingQuery( queryObject , lockTimeout ) ;

	if ( other ) {
		if ( lockId ) {
			allRawBatch = await this.mongoCollection.find( queryObject ).toArray() ;

			rawBatch = [] ;
			otherRawBatch = [] ;

			allRawBatch.forEach( raw => {
				if ( lockId.equals( raw._lockedBy ) ) {
					rawBatch.push( raw ) ;
				}
				else {
					otherRawBatch.push( raw ) ;
				}
			} ) ;
		}
		else {
			rawBatch = [] ;
			otherRawBatch = [] ;
		}
	}
	else {
		if ( lockId ) {
			rawBatch = await this.mongoCollection.find( { _lockedBy: lockId } ).toArray() ;
		}
		else {
			rawBatch = [] ;
		}
	}

	if ( ! actionFn ) {
		return { lockId , rawBatch , otherRawBatch } ;
	}

	await actionFn( lockId , rawBatch , otherRawBatch ) ;
	await this.releaseLocks( lockId ) ;
} ;



/* Polyfills & mongo shell consistencies */



if ( ! mongodb.Collection.prototype.getIndexes ) {
	mongodb.Collection.prototype.getIndexes = mongodb.Collection.prototype.indexes ;
}

