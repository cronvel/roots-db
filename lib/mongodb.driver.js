/*
	Roots DB

	Copyright (c) 2014 - 2019 Cédric Ronvel

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

	this.url = url.format( Object.assign( {} , collection.config , { pathname: pathname } ) ) ;

	this.client = null ;
	this.db = null ;
	this.mongoCollection = null ;
	this.raw = null ;	// RAW acces to the driver, for this driver this is always driver.mongoCollection
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

	this.lock = this.commonGateway( 'lock' ) ;
	this.unlock = this.commonGateway( 'unlock' ) ;
	this.partialQueryLock = this.commonGateway( 'partialQueryLock' ) ;
	this.releaseLock = this.commonGateway( 'releaseLock' ) ;
	this.lockedPartialFind = this.commonGateway( 'lockedPartialFind' ) ;
}



module.exports = MongoDriver ;
MongoDriver.prototype = Object.create( events.prototype ) ;
MongoDriver.prototype.constructor = MongoDriver ;



MongoDriver.prototype.type = 'mongodb' ;
MongoDriver.prototype.idKey = '_id' ;
MongoDriver.prototype.idConstructor = mongodb.ObjectID ;
MongoDriver.prototype.idPrototype = mongodb.ObjectID.prototype ;



// Driver preferences
MongoDriver.prototype.immutables = [ mongodb.ObjectID.prototype ] ;
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
			var forwardError , match ;

			// Rewrite errors
			switch ( error.code ) {
				case 26 :
					// NS not found: the collection does not exist on the server
					// Create it and retry!
					// NOTE THAT THIS IS SURPRISINGLY SLOW!!!
					//console.log( "About to create collection:" , this.upstreamCollection ) ;
					return this.db.createCollection( this.upstreamCollection ).then( () => this[ method ]( ... args ) ) ;

				case 11000 :
					// Duplicate key, forward a more meaningful error, extracting data from the error message
					forwardError = ErrorStatus.conflict( { message: "Duplicate key" , code: "duplicateKey" } ) ;
					match = error.errmsg.match( /collection: *([^ .]+)\.([^ .]+) index: *([^ ]+) * dup key: *(.*)/ ) ;

					if ( match ) {
						forwardError.db = match[ 1 ] ;
						forwardError.collection = match[ 2 ] ;
						forwardError.indexName = match[ 3 ] ;
						forwardError.key = match[ 4 ] ;
					}
					//log.hdebug( "Duplicate key error: %E\nInspection: %Y" , error , error ) ;

					throw forwardError ;

				case 17280 :
					// key too large to index
					forwardError = ErrorStatus.badRequest( { message: "Key too large to index (up to 1024 bytes supported)" , code: "keyTooLargeToIndex" } ) ;
					//log.hdebug( "key too large to index error: %E\nInspection: %J" , error , error ) ;
					throw forwardError ;

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
	return MongoClient.connect( this.url , { useNewUrlParser: true } ).then( client => {
		//console.log( '_autoConnect() succeeded!' ) ;
		this.client = client ;
		this.db = this.client.db() ;
		this.raw = this.mongoCollection = this.db.collection( this.upstreamCollection ) ;
	} ) ;
} ;



// Only used by unit test...
MongoDriver.prototype.rawInit = function() { return this.connectPromise ; } ;
//MongoDriver.prototype.rawId = function( rawDocument ) { return rawDocument._id.toString() ; }
MongoDriver.prototype.createId = function( from ) { return new mongodb.ObjectID( from ) ; } ;



MongoDriver.prototype.checkId = function( rawDocument , enforce ) {
	var id ;

	if ( rawDocument._id ) {
		id = rawDocument._id ;
	}

	if ( typeof id === 'string' ) {
		// Let it crash or fix it?
		try {
			id = new mongodb.ObjectID( id ) ;
		}
		catch ( error ) { id = new mongodb.ObjectID() ; }
	}
	else if ( enforce && ! ( id instanceof mongodb.ObjectID ) ) {
		id = new mongodb.ObjectID() ;
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
			if ( rawDocument[ k ] instanceof mongodb.ObjectID ) { exported[ k ] = rawDocument[ k ] ; }
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

	if ( index.partial ) {
		options.partialFilterExpression = {} ;
		for ( let property in index.properties ) {
			options.partialFilterExpression[ property ] = {
				$exists: true
				// Does not work, and there is no reliable way to reject null out of the indexes :S
				// Great, tons of flexibilities except for the very basic ultra-common use-case...
				// , $ne: null
			} ;
		}
	}

	//log.debug( "DRIVER: createIndex(): %Y\n%Y" , index.properties , options ) ;
	return this.mongoCollection.createIndex( index.properties , options ) ;
} ;



// Get a document by ID
MongoDriver.get = function( id ) {
	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }
	return this.mongoCollection.findOne( { _id: id } ) ;
} ;



// Get a document by a unique fingerprint
MongoDriver.getUnique = function( fingerprint ) {
	if ( typeof fingerprint._id === 'string' ) { fingerprint._id = mongodb.ObjectID( fingerprint._id ) ; }
	return this.mongoCollection.findOne( fingerprint ) ;
} ;



// Create (insert) a new document
MongoDriver.create = function( rawDocument ) {
	if ( typeof rawDocument._id === 'string' ) { rawDocument._id = mongodb.ObjectID( rawDocument._id ) ; }
	return this.mongoCollection.insertOne( rawDocument ) ;
} ;



// Overwrite a document: create if it does not exist or full update if it exists
MongoDriver.overwrite = function( rawDocument ) {
	if ( typeof rawDocument._id === 'string' ) { rawDocument._id = mongodb.ObjectID( rawDocument._id ) ; }

	// save(): Shorthand for insert/update is save - if _id value set, the record is updated if it exists or inserted
	// if it does not; if the _id value is not set, then the record is inserted as a new one.
	// this.mongoCollection.save( rawDocument , callback ) ;

	return this.mongoCollection.replaceOne( { _id: rawDocument._id } , rawDocument , { upsert: true } ) ;
} ;



// Full update of a document
MongoDriver.update = function( id , rawDocument ) {
	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }

	if ( rawDocument._id && rawDocument._id !== id ) { throw new Error( "Raw Document ID and query ID mismatch" ) ; }

	return this.mongoCollection.replaceOne( { _id: id } , rawDocument ) ;
} ;



// Partial update (patch) of a document
MongoDriver.patch = function( id , rawPatch ) {
	if ( ! rawPatch.set && ! rawPatch.unset ) { return Promise.resolved ; }

	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }

	var dbPatch = {} ;

	if ( rawPatch.set ) {
		dbPatch.$set = rawPatch.set ;
		delete dbPatch.$set._id ;	// Should not be updated
	}

	if ( rawPatch.unset ) {
		dbPatch.$unset = rawPatch.unset ;
		delete dbPatch.$unset._id ;	// Should not be deleted
	}

	return this.mongoCollection.updateOne( { _id: id } , dbPatch ) ;
} ;



// Delete a document
MongoDriver.delete = function( id ) {
	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }
	return this.mongoCollection.deleteOne( { _id: id } ) ;
} ;



// Get a batch of documents given an array of ID
MongoDriver.multiGet = function( ids ) {
	var i , length = ids.length ;

	// First, check all ids
	for ( i = 0 ; i < length ; i ++ ) {
		if ( typeof ids[ i ] === 'string' ) { ids[ i ] = mongodb.ObjectID( ids[ i ] ) ; }
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
	if ( typeof fingerprint._id === 'string' ) { fingerprint._id = mongodb.ObjectID( fingerprint._id ) ; }

	var cursor = this.mongoCollection.find( fingerprint ) ;
	if ( options ) { cursor = cursorOptions( cursor , options ) ; }

	return cursor.toArray() ;
} ;



// Get a batch of documents given a query object
MongoDriver.find = function find( queryObject , options ) {
	// In others driver, the queryObject should be processed to construct a complex query.
	// But since roots-DB queries ARE MongoDB object's queries, there is nothing to do here.

	//console.log( 'driver find queryObject' , queryObject ) ;
	if ( typeof queryObject._id === 'string' ) { queryObject._id = mongodb.ObjectID( queryObject._id ) ; }

	var cursor = this.mongoCollection.find( queryObject ) ;
	if ( options ) { cursor = cursorOptions( cursor , options ) ; }

	return cursor.toArray() ;
} ;



function cursorOptions( cursor , options ) {
	// 'sort' MUST be *BEFORE* limit', because those operations are made in that order
	if ( options.sort ) {
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
	var lockId = new mongodb.ObjectID() ,
		now = new Date() ,
		timeoutBefore = new Date( now.getTime() - lockTimeout ) ;

	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }

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

	return this.mongoCollection.updateMany( lockQueryObject , update ).then( result => {
		return result.matchedCount ? lockId : null ;
	} ) ;
} ;



// Unlock a document by its ID, provided the correct lockId
MongoDriver.unlock = function( id , lockId ) {
	//log.error( 'releasing %I' , lockId ) ;
	return this.mongoCollection.updateMany(
		{ _id: id , _lockedBy: lockId } ,
		{ $set: { _lockedBy: null , _lockedAt: null } }
	).then( result => !! result.matchedCount ) ;
} ;



// Lock documents and return a unique lockId, to be able to retrieve them
MongoDriver.partialQueryLock = async function( queryObject , lockTimeout /*, allowPartialLock */ ) {
	var lockMatchCount , queryMatchCount ,
		lockId = new mongodb.ObjectID() ,
		now = new Date() ,
		timeoutBefore = new Date( now.getTime() - lockTimeout ) ;

	if ( typeof queryObject._id === 'string' ) { queryObject._id = mongodb.ObjectID( queryObject._id ) ; }

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

	// A somewhat complete query lock could be achieved like this (but it does not works very well)
	/*
	if ( allowPartialLock ) {
		lockMatchCount = ( await this.mongoCollection.updateMany( lockQueryObject , update ) ).matchedCount ;
		return lockMatchCount ? lockId : null ;
	}

	[ lockMatchCount , queryMatchCount ] = await Promise.all( [
		this.mongoCollection.updateMany( lockQueryObject , update ).then( r => r.matchedCount ) ,
		this.mongoCollection.count( queryObject )
	] ) ;

	return lockMatchCount >= queryMatchCount ? lockId : null ;
	*/
} ;



// Release a lock, given a lockId
MongoDriver.releaseLock = function( lockId ) {
	//log.error( 'releasing %I' , lockId ) ;
	return this.mongoCollection.updateMany(
		{ _lockedBy: lockId } ,
		{ $set: { _lockedBy: null , _lockedAt: null } }
	).then( result => result.matchedCount ) ;
} ;



/*
MongoDriver.lockedGet = async function( id , actionFn , lockTimeout ) {
	var lockId = this.lockById( id ) ;
} ;
*/



// Lock, retrieve a documents from the query and pass a release function
MongoDriver.lockedPartialFind = async function( queryObject , lockTimeout , actionFn ) {
	var lockId = await this.partialQueryLock( queryObject , lockTimeout ) ;
	if ( ! lockId ) { return false ; }
	var rawBatch = await this.mongoCollection.find( { _lockedBy: lockId } ).toArray() ;
	await actionFn( rawBatch ) ;
	await this.releaseLock( lockId ) ;
	return true ;
} ;



/* Polyfills & mongo shell consistencies */



if ( ! mongodb.Collection.prototype.getIndexes ) {
	mongodb.Collection.prototype.getIndexes = mongodb.Collection.prototype.indexes ;
}


