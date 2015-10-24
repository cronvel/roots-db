/*
	The Cedric's Swiss Knife (CSK) - CSK RootsDB

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
var ErrorStatus = require( 'error-status' ) ;



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
	
	driver.getIndexes = wrapper.bind( driver , Mongodb.getIndexes ) ;
	driver.dropIndex = wrapper.bind( driver , Mongodb.dropIndex ) ;
	driver.buildIndex = wrapper.bind( driver , Mongodb.buildIndex ) ;
	
	driver.get = wrapper.bind( driver , Mongodb.get ) ;
	driver.getUnique = wrapper.bind( driver , Mongodb.getUnique ) ;
	driver.create = wrapper.bind( driver , Mongodb.create ) ;
	driver.overwrite = wrapper.bind( driver , Mongodb.overwrite ) ;
	driver.update = wrapper.bind( driver , Mongodb.update ) ;
	driver.patch = wrapper.bind( driver , Mongodb.patch ) ;
	driver.delete = wrapper.bind( driver , Mongodb.delete ) ;
	driver.multiGet = wrapper.bind( driver , Mongodb.multiGet ) ;
	driver.collect = wrapper.bind( driver , Mongodb.collect ) ;
	driver.find = wrapper.bind( driver , Mongodb.find ) ;
	
	return driver ;
}

//util.inherits( Mongodb , odm.driver.Common ) ;
util.inherits( Mongodb , events.EventEmitter ) ;

Mongodb.prototype.constructor = Mongodb ;

module.exports = Mongodb ;



// Driver preferences
Mongodb.prototype.objectFilter = { blacklist: [ mongodb.ObjectID.prototype ] } ;
//Mongodb.prototype.pathSeparator = '.' ;



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



Mongodb.prototype.createId = function createId() { return new mongodb.ObjectID() ; } ;



Mongodb.prototype.checkId = function checkId( rawDocument , enforce )
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
	
	// Lock the id property
	if ( id ) { Object.defineProperty( rawDocument , '_id' , { value: id , enumerable: true } ) ; }
	
	return id ;
} ;



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



// This is a small wrapper for specific function, to avoid having to boilerplate the same things again and again...
function wrapper( method )
{
	var self = this , callback , args = Array.prototype.slice.call( arguments ) ;
	
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





			/* Requests */



// Get *USER* indexes (not returning indexes on _id)
Mongodb.getIndexes = function getIndexes( callback )
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
Mongodb.dropIndex = function dropIndex( indexName , callback )
{
	//console.log( "DRIVER: dropIndex():" , indexName ) ;
	
	this.mongoCollection.dropIndex( indexName , callback ) ;
} ;



// Index/re-index a collection
Mongodb.buildIndex = function buildIndex( index , callback )
{
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
Mongodb.get = function get( id , callback )
{
	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }
	this.mongoCollection.findOne( { _id : id } , callback ) ;
} ;



// Get a document by a unique fingerprint
Mongodb.getUnique = function getUnique( fingerprint , callback )
{
	if ( typeof fingerprint._id === 'string' ) { fingerprint._id = mongodb.ObjectID( fingerprint._id ) ; }
	this.mongoCollection.findOne( fingerprint , callback ) ;
} ;



// Create (insert) a new document
Mongodb.create = function create( rawDocument , callback )
{
	if ( typeof rawDocument._id === 'string' ) { rawDocument._id = mongodb.ObjectID( rawDocument._id ) ; }
	this.mongoCollection.insert( rawDocument , callback ) ;
} ;



// Overwrite a document: create if it does not exist or full update if it exists
Mongodb.overwrite = function overwrite( rawDocument , callback )
{
	if ( typeof rawDocument._id === 'string' ) { rawDocument._id = mongodb.ObjectID( rawDocument._id ) ; }
	
	// save(): Shorthand for insert/update is save - if _id value set, the record is updated if it exists or inserted
	// if it does not; if the _id value is not set, then the record is inserted as a new one.
	this.mongoCollection.save( rawDocument , callback ) ;
} ;



// Full update of a document
Mongodb.update = function update( id , rawDocument , callback )
{
	// Should not be updated
	delete rawDocument._id ;
	
	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }
	this.mongoCollection.update( { _id : id } , rawDocument , callback ) ;
} ;



// Partial update (patch) of a document
Mongodb.patch = function patch( id , rawDocument , callback )
{
	// Should not be updated
	delete rawDocument._id ;
	
	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }
	this.mongoCollection.update( { _id : id } , { $set: rawDocument } , callback ) ;
} ;



// Delete a document
Mongodb.delete = function delete_( id , callback )
{
	if ( typeof id === 'string' ) { id = mongodb.ObjectID( id ) ; }
	this.mongoCollection.remove( { _id: id } , { justOne: true } , callback ) ;
} ;



// Get a batch of documents given an array of ID
Mongodb.multiGet = function multiGet( ids , callback )
{
	var i , length = ids.length ;
	
	// First, check all ids
	for ( i = 0 ; i < length ; i ++ )
	{
		if ( typeof ids[ i ] === 'string' ) { ids[ i ] = mongodb.ObjectID( ids[ i ] ) ; }
	}
	
	this.mongoCollection.find( { _id: { "$in": ids } } ).toArray( callback ) ;
} ;



// Get a batch of documents given some fingerprint
Mongodb.collect = function collect( fingerprint , callback )
{
	//console.log( 'driver collect fingerprint' , fingerprint ) ;
	if ( typeof fingerprint._id === 'string' ) { fingerprint._id = mongodb.ObjectID( fingerprint._id ) ; }
	this.mongoCollection.find( fingerprint ).toArray( callback ) ;
} ;



// Get a batch of documents given a query object
Mongodb.find = function find( queryObject , callback )
{
	// In others driver, the queryObject should be processed to construct a complex query.
	// But since roots-DB queries ARE MongoDB object's queries, there is nothing to do here.
	
	//console.log( 'driver find queryObject' , queryObject ) ;
	if ( typeof queryObject._id === 'string' ) { queryObject._id = mongodb.ObjectID( queryObject._id ) ; }
	this.mongoCollection.find( queryObject ).toArray( callback ) ;
} ;





			/* Polyfills & mongo shell consistencies */



if ( ! mongodb.Collection.prototype.getIndexes )
{
	mongodb.Collection.prototype.getIndexes = mongodb.Collection.prototype.indexes ;
}
