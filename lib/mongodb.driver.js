/*
	Roots DB
	
	Copyright (c) 2014 - 2016 Cédric Ronvel
	
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
var mongoClient = mongodb.MongoClient ;
var tree = require( 'tree-kit' ) ;
var ErrorStatus = require( 'error-status' ) ;
var log = require( 'logfella' ).global.use( 'roots-db:mongodb' ) ;

function noop() {}
function noopCallback( callback ) { callback() ; }



function MongoDriver( collection )
{
	Object.defineProperties( this , {
		collection: { value: collection } ,
		type: { value: 'mongodb' } ,
		idKey: { value: '_id' }
	} ) ;
	
	var pathname ;
	var parts = collection.config.pathname.split( '/' ) ;
	//console.log( 'parts: ' , parts ) ;
	
	if ( parts.length === 3 )
	{
		// Get the collection's name
		this.upstreamCollection = parts.pop() ;
		pathname = parts.join( '/' ) ;
	}
	
	this.url = url.format( tree.extend( null , {} , collection.config , { pathname: pathname } ) ) ;
	
	this.db = null ;
	this.mongoCollection = null ;
	this.raw = null ;	// RAW acces to the driver, for this driver this is always driver.mongoCollection
	this.ready = false ;
	
	// /!\ Should be deleted
	this.pendingConnect = false ;
	this.pendingConnectPromVer = false ;
	this.readyPromVer = false ;
	this.connectPromise = Promise.dormant( resolve => resolve( this._autoConnect() ) ) ;
	
	//console.log( 'url: ' , this.url ) ;
	//console.log( 'upstreamCollection: ' , this.upstreamCollection ) ;
	
	this.getIndexes = wrapper.bind( this , MongoDriver.getIndexes ) ;
	this.dropIndex = wrapper.bind( this , MongoDriver.dropIndex ) ;
	this.buildIndex = wrapper.bind( this , MongoDriver.buildIndex ) ;
	
	this.get = wrapper.bind( this , MongoDriver.get ) ;
	this.getUnique = wrapper.bind( this , MongoDriver.getUnique ) ;
	this.create = wrapper.bind( this , MongoDriver.create ) ;
	this.overwrite = wrapper.bind( this , MongoDriver.overwrite ) ;
	this.update = wrapper.bind( this , MongoDriver.update ) ;
	this.patch = wrapper.bind( this , MongoDriver.patch ) ;
	this.delete = wrapper.bind( this , MongoDriver.delete ) ;
	this.multiGet = wrapper.bind( this , MongoDriver.multiGet ) ;
	this.collect = wrapper.bind( this , MongoDriver.collect ) ;
	this.find = wrapper.bind( this , MongoDriver.find ) ;
	
	this.lock = wrapper.bind( this , MongoDriver.lock ) ;
	this.releaseLock = wrapper.bind( this , MongoDriver.releaseLock ) ;
	this.lockRetrieveRelease = wrapper.bind( this , MongoDriver.lockRetrieveRelease ) ;
	
	// Promise version
	
	this.getIndexesPromVer = this.commonGateway( 'getIndexesPromVer' ) ;
	this.dropIndexPromVer = this.commonGateway( 'dropIndexPromVer' ) ;
	this.buildIndexPromVer = this.commonGateway( 'buildIndexPromVer' ) ;
	
	this.getPromVer = this.commonGateway( 'getPromVer' ) ;
	this.getUniquePromVer = this.commonGateway( 'getUniquePromVer' ) ;
	this.createPromVer = this.commonGateway( 'createPromVer' ) ;
	this.overwritePromVer = this.commonGateway( 'overwritePromVer' ) ;
	this.updatePromVer = this.commonGateway( 'updatePromVer' ) ;
	this.patchPromVer = this.commonGateway( 'patchPromVer' ) ;
	this.deletePromVer = this.commonGateway( 'deletePromVer' ) ;
	this.multiGetPromVer = this.commonGateway( 'multiGetPromVer' ) ;
	this.collectPromVer = this.commonGateway( 'collectPromVer' ) ;
	this.findPromVer = this.commonGateway( 'findPromVer' ) ;
	
	this.lockPromVer = this.commonGateway( 'lockPromVer' ) ;
	this.releaseLockPromVer = this.commonGateway( 'releaseLockPromVer' ) ;
	this.lockRetrieveReleasePromVer = this.commonGateway( 'lockRetrieveReleasePromVer' ) ;
}



module.exports = MongoDriver ;
MongoDriver.prototype = Object.create( events.prototype ) ;
MongoDriver.prototype.constructor = MongoDriver ;



// Driver preferences
MongoDriver.prototype.objectFilter = { blacklist: [ mongodb.ObjectID.prototype ] } ;
//MongoDriver.prototype.pathSeparator = '.' ;



/*
	This is a decorator that factorize a lot of common work,
	like checking connection, patching errors, and so on...
*/
MongoDriver.prototype.commonGateway = function commonGateway( method )
{
	return ( ... args ) => {
		
		return this.connectPromise.then(
			() => MongoDriver[ method ].call( this , ... args )
		).catch( error => {
			// Rewrite errors
			switch ( error.code )
			{
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
MongoDriver.prototype._autoConnect = function _autoConnect()
{
	//console.log( 'entering _autoConnect() for' , this.url , '...' ) ;
	
	// Connect to the upstream db
	return mongoClient.connect( this.url ).then( db => {
		//console.log( '_autoConnect() succeeded!' ) ;
		this.db = db ;
		this.raw = this.mongoCollection = db.collection( this.upstreamCollection ) ;
	} ) ;
} ;



// Only used by unit test...
MongoDriver.prototype.rawInit = function rawInit() { return this.connectPromise ; } ;

//MongoDriver.prototype.rawId = function rawId( rawDocument ) { return rawDocument._id.toString() ; }

MongoDriver.prototype.createId = function createId( from ) { return new mongodb.ObjectID( from ) ; } ;



MongoDriver.prototype.checkId = function checkId( rawDocument , enforce )
{
	var id ;
	
	if ( '$id' in rawDocument )
	{
		id = rawDocument.$id ;
		delete rawDocument.$id ;
	}
	else if ( rawDocument._id )
	{
		id = rawDocument._id ;
	}
	
	if ( typeof id === 'string' )
	{
		// Let it crash or fix it?
		try {
			id = new mongodb.ObjectID( id ) ;
		} catch ( error ) { id = new mongodb.ObjectID() ; }
	}
	else if ( enforce && ! ( id instanceof mongodb.ObjectID ) )
	{
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
MongoDriver.prototype.clone = function clone( rawDocument )
{
	var k ,
		exported = Array.isArray( rawDocument ) ? [] : {} ;
	
	for ( k in rawDocument )
	{
		if ( rawDocument[ k ] && typeof rawDocument[ k ] === 'object' )
		{
			if ( rawDocument[ k ] instanceof mongodb.ObjectID ) { exported[ k ] = rawDocument[ k ] ; }
			else { exported[ k ] = clone( rawDocument[ k ] ) ; }
		}
		else
		{
			exported[ k ] = rawDocument[ k ] ;
		}
	}
	
	return exported ;
} ;





			/* Requests */



// Get *USER* indexes (not returning indexes on _id)
MongoDriver.getIndexesPromVer = function getIndexes()
{
	//console.log( mongodb.Collection.prototype ) ;
	
	return this.mongoCollection.getIndexes().then( rawIndexes => {
		
		//console.log( 'DRIVER: rawIndexes' , rawIndexes ) ;
		
		var input , output , key1 , key2 , name , indexes = {} ;
		
		for ( key1 in rawIndexes )
		{
			name = '' ;
			output = {} ;
			input = rawIndexes[ key1 ] ;
			
			if ( input.name === '_id_' ) { continue ; }
			
			for ( key2 in input )
			{
				switch ( key2 )
				{
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
MongoDriver.dropIndexPromVer = function dropIndex( indexName )
{
	//console.log( "DRIVER: dropIndex():" , indexName ) ;
	return this.mongoCollection.dropIndex( indexName ) ;
} ;



// Index/re-index a collection
MongoDriver.buildIndexPromVer = function buildIndex( index )
{
	var options = Object.assign( {
			name: index.name ,
			unique: !! index.unique ,
			background: true
		} ,
		index.driver
	) ;
	
	//console.log( "DRIVER: ensureIndex():" , index.properties , options ) ;
	
	return this.mongoCollection.ensureIndex( index.properties , options ) ;
} ;



// Get a document by ID
MongoDriver.getPromVer = function get( id )
{
	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }
	return this.mongoCollection.findOne( { _id : id } ) ;
} ;



// Get a document by a unique fingerprint
MongoDriver.getUniquePromVer = function getUnique( fingerprint )
{
	if ( typeof fingerprint._id === 'string' ) { fingerprint._id = mongodb.ObjectID( fingerprint._id ) ; }
	return this.mongoCollection.findOne( fingerprint ) ;
} ;



// Create (insert) a new document
MongoDriver.createPromVer = function create( rawDocument )
{
	if ( typeof rawDocument._id === 'string' ) { rawDocument._id = mongodb.ObjectID( rawDocument._id ) ; }
	return this.mongoCollection.insertOne( rawDocument ) ;
} ;



// Overwrite a document: create if it does not exist or full update if it exists
MongoDriver.overwritePromVer = function overwrite( rawDocument )
{
	if ( typeof rawDocument._id === 'string' ) { rawDocument._id = mongodb.ObjectID( rawDocument._id ) ; }
	
	// save(): Shorthand for insert/update is save - if _id value set, the record is updated if it exists or inserted
	// if it does not; if the _id value is not set, then the record is inserted as a new one.
	// this.mongoCollection.save( rawDocument , callback ) ;
	
	return this.mongoCollection.updateOne( { _id: rawDocument._id } , rawDocument , { upsert: true } ) ;
} ;



// Full update of a document
MongoDriver.updatePromVer = function update( id , rawDocument )
{
	// Should not be updated
	delete rawDocument._id ;
	
	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }
	return this.mongoCollection.updateOne( { _id : id } , rawDocument ) ;
} ;



// Partial update (patch) of a document
MongoDriver.patchPromVer = function patch( id , rawDocument )
{
	// Should not be updated
	delete rawDocument._id ;
	
	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }
	return this.mongoCollection.updateOne( { _id : id } , { $set: rawDocument } ) ;
} ;



// Delete a document
MongoDriver.deletePromVer = function delete_( id )
{
	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }
	return this.mongoCollection.deleteOne( { _id: id } ) ;
} ;



// Get a batch of documents given an array of ID
MongoDriver.multiGetPromVer = function multiGet( ids )
{
	var i , length = ids.length ;
	
	// First, check all ids
	for ( i = 0 ; i < length ; i ++ )
	{
		if ( typeof ids[ i ] === 'string' ) { ids[ i ] = mongodb.ObjectID( ids[ i ] ) ; }
	}
	
	return this.mongoCollection.find( { _id: { $in: ids } } ).toArray() ;
} ;



// Get a batch of documents given some fingerprint
MongoDriver.collectPromVer = function collect( fingerprint )
{
	//console.log( 'driver collect fingerprint' , fingerprint ) ;
	if ( typeof fingerprint._id === 'string' ) { fingerprint._id = mongodb.ObjectID( fingerprint._id ) ; }
	return this.mongoCollection.find( fingerprint ).toArray() ;
} ;



// Get a batch of documents given a query object
MongoDriver.findPromVer = function find( queryObject )
{
	// In others driver, the queryObject should be processed to construct a complex query.
	// But since roots-DB queries ARE MongoDB object's queries, there is nothing to do here.
	
	//console.log( 'driver find queryObject' , queryObject ) ;
	if ( typeof queryObject._id === 'string' ) { queryObject._id = mongodb.ObjectID( queryObject._id ) ; }
	return this.mongoCollection.find( queryObject ).toArray() ;
} ;





			/* Higher level methods */



// Lock documents and return a unique lockId, to be able to retrieve them
MongoDriver.lockPromVer = function lock( queryObject , lockTimeout )
{
	var lockId = new mongodb.ObjectID() ;
	var now = new Date() ;
	var timeout = new Date( now.getTime() - lockTimeout ) ;
	//var backInTime = new Date( now.getTime() + lockTimeout ) ;
	
	if ( typeof queryObject._id === 'string' ) { queryObject._id = mongodb.ObjectID( queryObject._id ) ; }
	
	var lockQuery = [
		{ _lockedBy: null } ,
		{ _lockedAt: { $lt: timeout } }
	] ;
	
	if ( ! queryObject.$or )
	{
		queryObject.$or = lockQuery ;
	}
	else
	{
		if ( ! queryObject.$and ) { queryObject.$and = [] ; }
		
		queryObject.$and.push( { $or: lockQuery } ) ;
		queryObject.$and.push( { $or: queryObject.$or } ) ;
		delete queryObject.$or ;
	}
	
	var update = {
		$set: {
			_lockedBy: lockId ,
			_lockedAt: now
		}
	} ;
	
	//console.log( '\ndriver lock queryObject' , JSON.stringify( queryObject ) , '\n' ) ;
	
	return this.mongoCollection.updateMany( queryObject , update ).then( result => {
		//console.log( result ) ;
		return result.matchedCount ? lockId : null ;
	} ) ;
} ;



// Release a lock, given a lockId
MongoDriver.releaseLockPromVer = function releaseLock( lockId )
{
	//log.error( 'releasing %I' , lockId ) ;
	return this.mongoCollection.updateMany(
		{ _lockedBy: lockId } ,
		{ $set: { _lockedBy: null , _lockedAt: null } }
	).then( result => result.matchedCount ) ;
} ;



// Lock, retrieve a documents from the query and pass a release function
MongoDriver.lockRetrieveReleasePromVer = function lockRetrieveRelease( queryObject , lockTimeout )
{
	var lockId ;
	
	this.lockPromVer( queryObject , lockTimeout ).then( lockId_ => {
		if ( ! lockId_ ) { return null ; }
		lockId = lockId_ ;
		return this.mongoCollection.find( { _lockedBy: lockId } ).toArray() ;
	} ).then( batch => {
		if ( ! batch ) { return null ; }
		return {
			batch: batch ,
			release: this.releaseLock.bind( this , lockId )
		} ;
	} ) ;
} ;





			/* Polyfills & mongo shell consistencies */



if ( ! mongodb.Collection.prototype.getIndexes )
{
	mongodb.Collection.prototype.getIndexes = mongodb.Collection.prototype.indexes ;
}










// Future trash







// This is a small wrapper for specific function, to avoid having to boilerplate the same things again and again...
function wrapper( method )
{
	var //self = this ,
		callback , args = Array.prototype.slice.call( arguments ) ;
	
	if ( args.length > 1 )
	{
		// Replace the callback by the callback wrapper
		callback = args[ args.length - 1 ] = callbackWrapper.bind( this , args[ args.length - 1 ] ) ;
	}
	
	if ( ! this.ready )
	{
		args.unshift( this ) ;
		return this.connect( Function.prototype.bind.apply( wrapper , args ) , callback ) ;
	}
	
	// Call the specific method
	method.apply( this , Array.prototype.slice.call( args , 1 ) ) ;
}



// The main goal of the callback is mainly to rewrite errors
function callbackWrapper( callback , error )
{
	var args = Array.prototype.slice.call( arguments ) ;
	
	if ( ! error )
	{
		callback.apply( undefined , Array.prototype.slice.call( args , 1 ) ) ;
		return ;
	}
	
	switch ( error.code )
	{
		case 11000 :
			// Duplicate key
			args[ 1 ] = ErrorStatus.conflict( { message: "Duplicate key" , code: "duplicateKey" } ) ;
			break ;
	}
	
	callback.apply( undefined , Array.prototype.slice.call( args , 1 ) ) ;
}



// This accept 2 callbacks, it looks not very common, but it helps dealing with lazy-connection:
// less code in the query function, since they can pass themselves as the onSuccess and caller callback as onError,
// so no need to check connect() response
MongoDriver.prototype.connect = function connect( onSuccess , onError )
{
	//console.log( 'entering connect() for' , this.url , '...' ) ;
	if ( this.ready ) { onSuccess() ; return ; }
	
	var self = this ;
	
	// Prevent from concurrent connection
	if ( this.pendingConnect )
	{
		//console.log( 'there is a pending connection for this url...' ) ;
		
		this.once( 'connect' , function( error ) {
			if ( error )
			{
				//console.log( 'pending connect() failed...' ) ;
				if ( typeof onError === 'function' ) { onError( error ) ; }
			}
			else
			{
				//console.log( 'pending connect() succeeded!' ) ;
				if ( typeof onSuccess === 'function' ) { onSuccess() ; }
			}
		} ) ;
		
		return ;
	}
	
	this.pendingConnect = true ;
	
	// Connect to the upstream db
	mongoClient.connect( this.url , function( error , db ) {
	
		// No more pending...
		self.pendingConnect = false ;
		
		if ( error )
		{
			//console.log( 'connect() failed...' ) ;
			if ( typeof onError === 'function' ) { onError( error ) ; }
		}
		else
		{
			//console.log( 'connect() succeeded!' ) ;
			self.db = db ;
			self.raw = self.mongoCollection = db.collection( self.upstreamCollection ) ;
			self.ready = true ;
			if ( typeof onSuccess === 'function' ) { onSuccess() ; }
		}
		
		self.emit( 'connect' , error ) ;
	} ) ;
} ;


// Get a document by ID
MongoDriver.get = function get( id , callback )
{
	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }
	this.mongoCollection.findOne( { _id : id } , callback ) ;
} ;


// Get a document by a unique fingerprint
MongoDriver.getUnique = function getUnique( fingerprint , callback )
{
	if ( typeof fingerprint._id === 'string' ) { fingerprint._id = mongodb.ObjectID( fingerprint._id ) ; }
	this.mongoCollection.findOne( fingerprint , callback ) ;
} ;


// Create (insert) a new document
MongoDriver.create = function create( rawDocument , callback )
{
	if ( typeof rawDocument._id === 'string' ) { rawDocument._id = mongodb.ObjectID( rawDocument._id ) ; }
	this.mongoCollection.insertOne( rawDocument , callback ) ;
} ;


// Overwrite a document: create if it does not exist or full update if it exists
MongoDriver.overwrite = function overwrite( rawDocument , callback )
{
	if ( typeof rawDocument._id === 'string' ) { rawDocument._id = mongodb.ObjectID( rawDocument._id ) ; }
	
	// save(): Shorthand for insert/update is save - if _id value set, the record is updated if it exists or inserted
	// if it does not; if the _id value is not set, then the record is inserted as a new one.
	// this.mongoCollection.save( rawDocument , callback ) ;
	
	this.mongoCollection.updateOne( { _id: rawDocument._id } , rawDocument , { upsert: true } , callback ) ;
} ;


// Full update of a document
MongoDriver.update = function update( id , rawDocument , callback )
{
	// Should not be updated
	delete rawDocument._id ;
	
	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }
	this.mongoCollection.updateOne( { _id : id } , rawDocument , callback ) ;
} ;


// Partial update (patch) of a document
MongoDriver.patch = function patch( id , rawDocument , callback )
{
	// Should not be updated
	delete rawDocument._id ;
	
	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }
	this.mongoCollection.updateOne( { _id : id } , { $set: rawDocument } , callback ) ;
} ;



// Delete a document
MongoDriver.delete = function delete_( id , callback )
{
	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }
	this.mongoCollection.deleteOne( { _id: id } , callback ) ;
} ;

// Get a batch of documents given an array of ID
MongoDriver.multiGet = function multiGet( ids , callback )
{
	var i , length = ids.length ;
	
	// First, check all ids
	for ( i = 0 ; i < length ; i ++ )
	{
		if ( typeof ids[ i ] === 'string' ) { ids[ i ] = mongodb.ObjectID( ids[ i ] ) ; }
	}
	
	this.mongoCollection.find( { _id: { $in: ids } } ).toArray( callback ) ;
} ;


// Get a batch of documents given some fingerprint
MongoDriver.collect = function collect( fingerprint , callback )
{
	//console.log( 'driver collect fingerprint' , fingerprint ) ;
	if ( typeof fingerprint._id === 'string' ) { fingerprint._id = mongodb.ObjectID( fingerprint._id ) ; }
	this.mongoCollection.find( fingerprint ).toArray( callback ) ;
} ;



// Get a batch of documents given a query object
MongoDriver.find = function find( queryObject , callback )
{
	// In others driver, the queryObject should be processed to construct a complex query.
	// But since roots-DB queries ARE MongoDB object's queries, there is nothing to do here.
	
	//console.log( 'driver find queryObject' , queryObject ) ;
	if ( typeof queryObject._id === 'string' ) { queryObject._id = mongodb.ObjectID( queryObject._id ) ; }
	this.mongoCollection.find( queryObject ).toArray( callback ) ;
} ;


// Get *USER* indexes (not returning indexes on _id)
MongoDriver.getIndexes = function getIndexes( callback )
{
	//console.log( mongodb.Collection.prototype ) ;
	
	this.mongoCollection.getIndexes( function( error , rawIndexes ) {
		if ( error ) { callback( error ) ; return ; }
		
		//console.log( 'DRIVER: rawIndexes' , rawIndexes ) ;
		
		var input , output , key1 , key2 , name , indexes = {} ;
		
		for ( key1 in rawIndexes )
		{
			name = '' ;
			output = {} ;
			input = rawIndexes[ key1 ] ;
			
			if ( input.name === '_id_' ) { continue ; }
			
			for ( key2 in input )
			{
				switch ( key2 )
				{
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
		
		callback( undefined , indexes ) ;
	} ) ;
} ;



// Drop an index of a collection
MongoDriver.dropIndex = function dropIndex( indexName , callback )
{
	//console.log( "DRIVER: dropIndex():" , indexName ) ;
	
	this.mongoCollection.dropIndex( indexName , callback ) ;
} ;



// Index/re-index a collection
MongoDriver.buildIndex = function buildIndex( index , callback )
{
	var options = Object.assign( {
			name: index.name ,
			unique: !! index.unique ,
			background: true
		} ,
		index.driver
	) ;
	
	//console.log( "DRIVER: ensureIndex():" , index.properties , options ) ;
	
	this.mongoCollection.ensureIndex( index.properties , options , callback ) ;
} ;

// Lock documents and return a unique lockId, to be able to retrieve them
MongoDriver.lock = function lock( queryObject , lockTimeout , callback )
{
	var lockId = new mongodb.ObjectID() ;
	var now = new Date() ;
	var timeout = new Date( now.getTime() - lockTimeout ) ;
	//var backInTime = new Date( now.getTime() + lockTimeout ) ;
	
	if ( typeof queryObject._id === 'string' ) { queryObject._id = mongodb.ObjectID( queryObject._id ) ; }
	
	var lockQuery = [
		{ _lockedBy: null } ,
		{ _lockedAt: { $lt: timeout } }
	] ;
	
	if ( ! queryObject.$or )
	{
		queryObject.$or = lockQuery ;
	}
	else
	{
		if ( ! queryObject.$and ) { queryObject.$and = [] ; }
		
		queryObject.$and.push( { $or: lockQuery } ) ;
		queryObject.$and.push( { $or: queryObject.$or } ) ;
		delete queryObject.$or ;
	}
	
	var update = {
		$set: {
			_lockedBy: lockId ,
			_lockedAt: now
		}
	} ;
	
	//console.log( '\ndriver lock queryObject' , JSON.stringify( queryObject ) , '\n' ) ;
	
	this.mongoCollection.updateMany( queryObject , update , function( error , result ) {
		if ( error ) { callback( error ) ; return ; }
		//console.log( result ) ;
		callback( undefined , result.matchedCount , lockId ) ;
	} ) ;
} ;


// Lock, retrieve a documents from the query and pass a release function
MongoDriver.lockRetrieveRelease = function lockRetrieveRelease( queryObject , lockTimeout , callback )
{
	var self = this ;
	
	this.lock( queryObject , lockTimeout , function( error , lockedCount , lockId ) {
		
		if ( error ) { callback( error , undefined , noopCallback ) ; return ; }
		if ( ! lockedCount ) { callback( undefined , [] , noopCallback ) ; return ; }
		
		self.mongoCollection.find( { _lockedBy: lockId } ).toArray( function( error , batch ) {
			callback( error , batch , self.releaseLock.bind( self , lockId ) ) ;
			/*
			var releaseFn = self.releaseLock.bind( self , lockId ) ;
			if ( error ) { callback( error , undefined , releaseFn ) ; return ; }
			callback( undefined , batch , releaseFn ) ;
			*/
		} ) ;
	} ) ;
} ;




// Release a lock, given a lockId
MongoDriver.releaseLock = function releaseLock( lockId , callback )
{
	//log.error( 'releasing %I' , lockId ) ;
	this.mongoCollection.updateMany(
		{ _lockedBy: lockId } ,
		{ $set: { _lockedBy: null , _lockedAt: null } } ,
		function( error , result ) {
			if ( error ) { callback( error ) ; return ; }
			//log.error( 'released: %I' , result.matchedCount ) ;
			callback( undefined , result.matchedCount ) ;
		}
	) ;
} ;



