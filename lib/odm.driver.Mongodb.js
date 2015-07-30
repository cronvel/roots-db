/*
	The Cedric's Swiss Knife (CSK) - CSK Object-Document Mapping

	Copyright (c) 2015 Cédric Ronvel 
	
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

/*
	WARNING: This is the mongodb driver for the 1.4.x version.
	See: http://mongodb.github.io/node-mongodb-native/2.0/meta/changes-from-1.0/
		... for the changes needed for the 2.0.x
*/

// Load modules
var events = require( 'events' ) ;
var url = require( 'url' ) ;
var util = require( 'util' ) ;
var mongodb = require( 'mongodb' ) ;
var mongoClient = mongodb.MongoClient ;
var tree = require( 'tree-kit' ) ;



function Mongodb( collection )
{
	var driver = Object.create( Mongodb.prototype , {
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
		driver.upstreamCollection = parts.pop() ;
		pathname = parts.join( '/' ) ;
	}
	
	driver.url = url.format( tree.extend( null , {} , collection.config , { pathname: pathname } ) ) ;
	
	driver.db = null ;
	driver.mongoCollection = null ;
	driver.raw = null ;	// RAW acces to the driver, for this driver this is always driver.mongoCollection
	driver.ready = false ;
	driver.pendingConnect = false ;
	
	//console.log( 'url: ' , driver.url ) ;
	//console.log( 'upstreamCollection: ' , driver.upstreamCollection ) ;
	
	return driver ;
}

//util.inherits( Mongodb , odm.driver.Common ) ;
util.inherits( Mongodb , events.EventEmitter ) ;

Mongodb.prototype.constructor = Mongodb ;

module.exports = Mongodb ;



// Driver preferences
Mongodb.prototype.objectFilter = { blacklist: [ mongodb.ObjectID.prototype ] } ;
Mongodb.prototype.pathSeparator = '.' ;



// This accept 2 callbacks, it looks not very common, but it helps dealing with lazy-connection:
// less code in the query function, since they can pass themselves as the onSuccess and caller callback as onError,
// so no need to check connect() response
Mongodb.prototype.connect = function connect( onSuccess , onError )
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



Mongodb.prototype.rawInit = function rawInit( callback )
{
	var self = this ;
	
	if ( this.raw ) { callback( undefined , this.raw ) ; return ; }
	
	this.connect(
		function() { callback( undefined , self.raw ) ; } ,
		function( error ) { callback( error ) ; }
	) ;
} ;



//Mongodb.prototype.rawId = function rawId( rawDocument ) { return rawDocument._id.toString() ; }



Mongodb.prototype.createId = function createId( rawDocument , id )
{
	if ( id || ! rawDocument || typeof rawDocument !== 'object' || rawDocument._id === undefined )
	{
		if ( typeof id === 'string' )
		{
			id = new mongodb.ObjectID( id ) ;
		}
		else if ( ! id || ! ( id instanceof mongodb.ObjectID ) )
		{
			id = new mongodb.ObjectID() ;
		}
		
		if ( rawDocument && typeof rawDocument === 'object' ) { rawDocument._id = id ; }
		return id ;
	}
	
	return rawDocument._id ;
} ;



// Get *USER* indexes (not returning indexes on _id)
Mongodb.prototype.getIndexes = function getIndexes( callback )
{
	// If not ready, try to connect
	if ( ! this.ready ) { return this.connect( Mongodb.prototype.getIndexes.bind( this , callback ) , callback ) ; }
	
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
Mongodb.prototype.dropIndex = function dropIndex( indexName , callback )
{
	// If not ready, try to connect
	if ( ! this.ready ) { return this.connect( Mongodb.prototype.dropIndex.bind( this , indexName , callback ) , callback ) ; }
	
	//console.log( "DRIVER: dropIndex():" , indexName ) ;
	
	this.mongoCollection.dropIndex( indexName , callback ) ;
} ;



// Index/re-index a collection
Mongodb.prototype.buildIndex = function buildIndex( index , callback )
{
	// If not ready, try to connect
	if ( ! this.ready ) { return this.connect( Mongodb.prototype.buildIndex.bind( this , index , callback ) , callback ) ; }
	
	var options = tree.extend( null , {
			name: index.name ,
			unique: ( index.unique ? true : false ) ,
			background: true
		} ,
		index.driver
	) ;
	
	//console.log( "DRIVER: ensureIndex():" , index.properties , options ) ;
	
	this.mongoCollection.ensureIndex( index.properties , options , callback ) ;
} ;



// Get a document by ID
Mongodb.prototype.get = function get( id , callback )
{
	// If not ready, try to connect
	if ( ! this.ready ) { return this.connect( Mongodb.prototype.get.bind( this , id , callback ) , callback ) ; }
	
	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }
	this.mongoCollection.findOne( { _id : id } , callback ) ;
} ;



// Get a document by a unique fingerprint
Mongodb.prototype.getUnique = function getUnique( fingerprint , callback )
{
	// If not ready, try to connect
	if ( ! this.ready ) { return this.connect( Mongodb.prototype.getUnique.bind( this , fingerprint , callback ) , callback ) ; }
	
	if ( typeof fingerprint._id === 'string' ) { fingerprint._id = mongodb.ObjectID( fingerprint._id ) ; }
	this.mongoCollection.findOne( fingerprint , callback ) ;
} ;



// Create (insert) a new document
Mongodb.prototype.create = function create( rawDocument , callback )
{
	// If not ready, try to connect
	if ( ! this.ready ) { return this.connect( Mongodb.prototype.create.bind( this , rawDocument , callback ) , callback ) ; }
	
	if ( typeof rawDocument._id === 'string' ) { console.log( rawDocument._id ) ; rawDocument._id = mongodb.ObjectID( rawDocument._id ) ; }
	this.mongoCollection.insert( rawDocument , callback ) ;
} ;



// Overwrite a document: create if it does not exist or full update if it exists
Mongodb.prototype.overwrite = function overwrite( rawDocument , callback )
{
	// If not ready, try to connect
	if ( ! this.ready ) { return this.connect( Mongodb.prototype.overwrite.bind( this , rawDocument , callback ) , callback ) ; }
	
	if ( typeof rawDocument._id === 'string' ) { rawDocument._id = mongodb.ObjectID( rawDocument._id ) ; }
	
	// save(): Shorthand for insert/update is save - if _id value set, the record is updated if it exists or inserted
	// if it does not; if the _id value is not set, then the record is inserted as a new one.
	this.mongoCollection.save( rawDocument , callback ) ;
} ;



// Full update of a document
Mongodb.prototype.update = function update( id , rawDocument , callback )
{
	// If not ready, try to connect
	if ( ! this.ready ) { return this.connect( Mongodb.prototype.update.bind( this , id , rawDocument , callback ) , callback ) ; }
	
	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }
	this.mongoCollection.update( { _id : id } , rawDocument , callback ) ;
} ;



// Partial update (patch) of a document
Mongodb.prototype.patch = function patch( id , rawDocument , callback )
{
	// If not ready, try to connect
	if ( ! this.ready ) { return this.connect( Mongodb.prototype.patch.bind( this , id , rawDocument , callback ) , callback ) ; }
	
	if ( ! ( id instanceof mongodb.ObjectID ) ) { id = mongodb.ObjectID( id ) ; }
	if ( typeof rawDocument._id === 'string' ) { rawDocument._id = mongodb.ObjectID( rawDocument._id ) ; }
	this.mongoCollection.update( { _id : id } , { $set: rawDocument } , callback ) ;
} ;



// Delete a document
Mongodb.prototype.delete = function delete_( id , callback )
{
	// If not ready, try to connect
	if ( ! this.ready ) { return this.connect( Mongodb.prototype.delete.bind( this , id , callback ) , callback ) ; }
	
	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }
	this.mongoCollection.remove( { _id: id } , { justOne: true } , callback ) ;
} ;



// Get a batch of documents given some fingerprint
Mongodb.prototype.collect = function collect( fingerprint , callback )
{
	// If not ready, try to connect
	if ( ! this.ready ) { return this.connect( Mongodb.prototype.collect.bind( this , fingerprint , callback ) , callback ) ; }
	
	//console.log( 'driver collect fingerprint' , fingerprint ) ;
	if ( typeof fingerprint._id === 'string' ) { fingerprint._id = mongodb.ObjectID( fingerprint._id ) ; }
	this.mongoCollection.find( fingerprint ).toArray( callback ) ;
} ;



// Get a batch of documents given a query object
Mongodb.prototype.find = function find( queryObject , callback )
{
	// If not ready, try to connect
	if ( ! this.ready ) { return this.connect( Mongodb.prototype.collect.bind( this , queryObject , callback ) , callback ) ; }
	
	// In others driver, the queryObject should be processed to construct a complex query.
	// But since ODM's kit queries ARE MongoDB object's queries, there is nothing to do here.
	
	//console.log( 'driver find queryObject' , queryObject ) ;
	if ( typeof queryObject._id === 'string' ) { queryObject._id = mongodb.ObjectID( queryObject._id ) ; }
	this.mongoCollection.find( queryObject ).toArray( callback ) ;
} ;





			/* Polyfills & mongo shell consistencies */



if ( ! mongodb.Collection.prototype.getIndexes )
{
	mongodb.Collection.prototype.getIndexes = mongodb.Collection.prototype.indexes ;
}
