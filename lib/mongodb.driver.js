/*
	Roots DB

	Copyright (c) 2014 - 2018 Cédric Ronvel

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



// Load modules
var Promise = require( 'seventh' ) ;
var events = require( 'events' ) ;
var url = require( 'url' ) ;
var mongodb = require( 'mongodb' ) ;
var MongoClient = mongodb.MongoClient ;
var tree = require( 'tree-kit' ) ;
var ErrorStatus = require( 'error-status' ) ;

var log = require( 'logfella' ).global.use( 'roots-db:mongodb' ) ;



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

	this.url = url.format( tree.extend( null , {} , collection.config , { pathname: pathname } ) ) ;

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
	this.lockById = this.commonGateway( 'lockById' ) ;
	this.unlock = this.commonGateway( 'unlock' ) ;
	this.releaseLock = this.commonGateway( 'releaseLock' ) ;
	this.lockRetrieveRelease = this.commonGateway( 'lockRetrieveRelease' ) ;
	this.lockRetrieveReleaseById = this.commonGateway( 'lockRetrieveReleaseById' ) ;
}



module.exports = MongoDriver ;
MongoDriver.prototype = Object.create( events.prototype ) ;
MongoDriver.prototype.constructor = MongoDriver ;



MongoDriver.prototype.type = 'mongodb' ;
MongoDriver.prototype.idKey = '_id' ;



// Driver preferences
MongoDriver.prototype.immutables = [ mongodb.ObjectID.prototype ] ;
//MongoDriver.prototype.pathSeparator = '.' ;



/*
	This is a decorator that factorize a lot of common work,
	like checking connection, patching errors, and so on...
*/
MongoDriver.prototype.commonGateway = function( method ) {
	return ( ... args ) => {

		return this.connectPromise.then(
			() => MongoDriver[ method ].call( this , ... args )
		).catch( error => {
			// Rewrite errors
			switch ( error.code ) {
				case 11000 :
					// Duplicate key
					throw ErrorStatus.conflict( { message: "Duplicate key" , code: "duplicateKey" } ) ;
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

	if ( '$id' in rawDocument ) {
		id = rawDocument.$id ;
		delete rawDocument.$id ;
	}
	else if ( rawDocument._id ) {
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
			else { exported[ k ] = clone( rawDocument[ k ] ) ; }
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
	//console.log( "DRIVER: dropIndex():" , indexName ) ;
	return this.mongoCollection.dropIndex( indexName ) ;
} ;



// Index/re-index a collection
MongoDriver.buildIndex = function( index ) {
	var options = Object.assign( {
		name: index.name ,
		unique: !! index.unique ,
		background: true
	} ,
	index.driver
	) ;

	//log.debug( "DRIVER: ensureIndex(): %Y\n%Y" , index.properties , options ) ;

	return this.mongoCollection.ensureIndex( index.properties , options ) ;
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
	// Should not be updated
	delete rawPatch._id ;

	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }
	return this.mongoCollection.updateOne( { _id: id } , { $set: rawPatch } ) ;
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



// Get a batch of documents given some fingerprint
MongoDriver.collect = function( fingerprint ) {
	//console.log( 'driver collect fingerprint' , fingerprint ) ;
	if ( typeof fingerprint._id === 'string' ) { fingerprint._id = mongodb.ObjectID( fingerprint._id ) ; }
	return this.mongoCollection.find( fingerprint ).toArray() ;
} ;



// Get a batch of documents given a query object
MongoDriver.find = function( queryObject ) {
	// In others driver, the queryObject should be processed to construct a complex query.
	// But since roots-DB queries ARE MongoDB object's queries, there is nothing to do here.

	//console.log( 'driver find queryObject' , queryObject ) ;
	if ( typeof queryObject._id === 'string' ) { queryObject._id = mongodb.ObjectID( queryObject._id ) ; }
	return this.mongoCollection.find( queryObject ).toArray() ;
} ;





/* Higher level methods */



MongoDriver.lockById = function( id , lockTimeout ) {
	var lockId = new mongodb.ObjectID() ;
	var now = new Date() ;
	var timeout = new Date( now.getTime() - lockTimeout ) ;
	//var backInTime = new Date( now.getTime() + lockTimeout ) ;

	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }

	var lockQueryObject = {
		_id: id ,
		$or: [
			{ _lockedBy: null } ,
			{ _lockedAt: { $lt: timeout } }
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



// Lock documents and return a unique lockId, to be able to retrieve them
MongoDriver.lock = async function( queryObject , lockTimeout ) {
	var lockId = new mongodb.ObjectID() ;
	var now = new Date() ;
	var timeout = new Date( now.getTime() - lockTimeout ) ;
	//var backInTime = new Date( now.getTime() + lockTimeout ) ;
	
	if ( typeof queryObject._id === 'string' ) { queryObject._id = mongodb.ObjectID( queryObject._id ) ; }

	var lockQueryObject = Object.assign( {} , queryObject ) ;

	var lockPart = [
		{ _lockedBy: null } ,
		{ _lockedAt: { $lt: timeout } }
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

	var lockMatchCount = ( await this.mongoCollection.updateMany( lockQueryObject , update ) ).matchedCount ;
	var queryMatchCount = await this.mongoCollection.count( queryObject ) ;
	log.error( "lockMatchCount: %Y , queryMatchCount: %Y" , lockMatchCount , queryMatchCount ) ;

	return lockMatchCount >= queryMatchCount ? lockId : null ;
} ;



// Unlock a document by its ID, provided the correct lockId
MongoDriver.unlock = function( id , lockId ) {
	//log.error( 'releasing %I' , lockId ) ;
	return this.mongoCollection.updateMany(
		{ _id: id , _lockedBy: lockId } ,
		{ $set: { _lockedBy: null , _lockedAt: null } }
	).then( result => !! result.matchedCount ) ;
} ;



// Release a lock, given a lockId
MongoDriver.releaseLock = function( lockId ) {
	//log.error( 'releasing %I' , lockId ) ;
	return this.mongoCollection.updateMany(
		{ _lockedBy: lockId } ,
		{ $set: { _lockedBy: null , _lockedAt: null } }
	).then( result => result.matchedCount ) ;
} ;



/* Transaction-like lock function */



MongoDriver.lockedGet = async function( id , actionFn ) {
	var lockId = this.lockById( id ) ;
	
} ;



// DEPRECATED, use .lockedGet(), .lockedGetUnique(), .lockedCollect(), .lockedFind()
// Lock, retrieve a documents from the query and pass a release function
MongoDriver.lockRetrieveRelease = function( queryObject , lockTimeout ) {
	var lockId ;

	return this.lock( queryObject , lockTimeout ).then( lockId_ => {
		if ( ! lockId_ ) { return null ; }
		lockId = lockId_ ;
		return this.mongoCollection.find( { _lockedBy: lockId } ).toArray() ;
	} )
		.then(
			batch => {
				return {
					batch: batch || [] ,
					release: () => this.releaseLock( lockId )
				} ;
			} ,
			error => {
				return this.releaseLock( lockId ).then( () => { throw error ; } ) ;
			}
		) ;
} ;



// DEPRECATED, use .lockedGet(), .lockedGetUnique(), .lockedCollect(), .lockedFind()
MongoDriver.lockRetrieveReleaseById = function( id , lockTimeout ) {
	return MongoDriver.lockRetrieveRelease.call( this , { _id: id } , lockTimeout ) ;
} ;








/* Polyfills & mongo shell consistencies */



if ( ! mongodb.Collection.prototype.getIndexes ) {
	mongodb.Collection.prototype.getIndexes = mongodb.Collection.prototype.indexes ;
}


