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
	In progress:
	- Embedded
		
	TODO:
	- memproxy with fingerprint
	- Fingerprint hash
	- === HOOKS ===
	- set on backlink
*/

// Load modules
var url = require( 'url' ) ;
var hash = require( 'hash-kit' ) ;
var tree = require( 'tree-kit' ) ;
var async = require( 'async-kit' ) ;
var doormen = require( 'doormen' ) ;
var ErrorStatus = require( 'error-status' ) ;



var odm = {} ;
module.exports = odm ;





			/* Constants */



Object.defineProperties( odm , {
	NONE: { value: 0 } ,
	UPSTREAM: { value: 1 } ,
	MEMPROXY: { value: 2 } ,
	INTERNAL: { value: 3 }
} ) ;





			/* World */



odm.World = function World()
{
	var world = Object.create( odm.World.prototype , {
		collections: { value: {} , enumerable: true }
	} ) ;
	
	return world ;
} ;

odm.World.prototype.constructor = odm.World ;



odm.World.prototype.createCollection = function worldCreateCollection( name , schema )
{
	return ( this.collections[ name ] = odm.Collection( this , name , schema ) ) ;
} ;





			/* Collection */



// WIP...

var collectionHookSchema = {
	type: 'array',
	sanitize: 'toArray',
	of: { type: 'function' }
} ;

var collectionSchema = {
	type: 'strictObject',
	extraProperties: true,
	properties: {
		hooks: {
			type: 'strictObject',
			default: {
				beforeCreateDocument: [],
				afterCreateDocument: []
			},
			extraProperties: true,
			properties: {
				beforeCreateDocument: collectionHookSchema,
				afterCreateDocument: collectionHookSchema
			}
		}
	}
} ;

odm.Collection = function Collection( world , name , schema )
{
	doormen( collectionSchema , schema ) ;
	
	var collection = Object.create( odm.Collection.prototype , {
		world: { value: world } ,
		name: { value: name , enumerable: true } ,
		driver: { value: undefined , writable: true }
	} ) ;
	
	var key , element , indexName ;
	
	if ( typeof schema.url !== 'string' ) { throw new Error( '[odm] schema.url should be a string' ) ; }
	collection.url = schema.url ;
	collection.config = url.parse( collection.url , true ) ;
	collection.config.driver = collection.config.protocol.split( ':' )[ 0 ] ;
	collection.config.driver = collection.config.driver.charAt( 0 ).toUpperCase() + collection.config.driver.slice( 1 ) ;
	
	// Create the validator schema
	collection.dataSchema = doormen.purifySchema( schema ) ;
	if ( ! collection.dataSchema.properties ) { collection.dataSchema.properties = {} ; }
	
	// Temp? or not?
	if ( ! collection.dataSchema.properties._id ) { collection.dataSchema.properties._id = { optional: true , type: 'mongoId' } ; }
	
	collection.validate = doormen.bind( doormen , collection.dataSchema ) ;
	
	
	// TODO: Check schema
	
	collection.properties = schema.properties || {} ;
	collection.meta = schema.meta || {} ;
	collection.documentBase = {} ;
	collection.suspectedBase = {} ;
	collection.memProxyRawDocuments = {} ;
	collection.useMemProxy = !! schema.useMemProxy ;
	
	// Already checked
	collection.hooks = schema.hooks ;
	
	
	// Indexes
	collection.indexes = {} ;
	collection.uniques = [] ;
	
	if ( Array.isArray( schema.indexes ) )
	{
		for ( key in schema.indexes )
		{
			element = schema.indexes[ key ] ;
			if ( ! element || typeof element !== 'object' || ! element.properties || typeof element.properties !== 'object' ) { continue ; }
			
			if ( element.unique ) { collection.uniques.push( Object.keys( element.properties ) ) ; }
			
			indexName = hash.fingerprint( element ) ;
			collection.indexes[ indexName ] = tree.extend( null , { name: indexName } , element ) ;
		}
	}
	
	
	// Properties
	for ( key in collection.properties )
	{
		Object.defineProperty( collection.documentBase , key , {
			value: collection.properties[ key ].default ,
			writable: true ,	// needed, or derivative object cannot set it
			enumerable: true
		} ) ;
		
		Object.defineProperty( collection.suspectedBase , key , {
			value: undefined ,
			writable: true ,	// needed, or derivative object cannot set it
			enumerable: true
		} ) ;
	}
	
	
	// Meta
	for ( key in collection.meta )
	{
		element = collection.meta[ key ] ;
		
		switch ( element.type )
		{
			case 'link' :
				collection.addMetaLink( key , element.property , element.collection ) ;
				break ;
				
			case 'backlink' :
				collection.addMetaBacklink( key , element.property , element.collection ) ;
				break ;
				
			default :
				throw new Error( '[odm] unknown meta type: ' + element.type ) ;
		}
	}
	
	// Init the driver
	collection.initDriver() ;
	collection.uniques.unshift( [ collection.driver.idKey ] ) ;
	
	
	collection.deepInherit =
		tree.extend.bind( null , { inherit: true , deep: true , deepFilter: collection.driver.objectFilter } ) ;

	return collection ;
} ;

odm.Collection.prototype.constructor = odm.Collection ;



odm.Collection.prototype.addMetaLink = function collectionAddMetaLink( key , property , collectionName )
{
	var self = this ;
	// Be careful: this.world.collections does not contains this collection at this time: we are still in the constructor!
	
	var getter = function getter()
	{
		var witness , document = this[''] , linkCollection = self.world.collections[ collectionName ] ;
		
		if ( ! document.suspected || this[ property ] !== undefined )
		{
			if ( ! ( document.meta[ property ] instanceof odm.Document ) || this[ property ] != document.meta[ property ].id )	// jshint ignore:line
			{
				//console.log( '### Link get ###' ) ;
				document.meta[ property ] = linkCollection.get( this[ property ] ) ;
			}
		}
		else
		{
			// Here we have no ID -- erf, idea :) -- about what we will get
			if (
				! ( document.meta[ property ] instanceof odm.Document ) ||
				! ( witness = document.meta[ property ].witness ) ||
				witness.property !== property ||
				witness.document !== document ||
				witness.type !== 'link'
			)
			{
				//console.log( '### Link describe suspect ###' ) ;
				document.meta[ property ] = odm.Document( linkCollection , null , {
					suspected: true ,
					//useMemProxy: linkCollection.useMemProxy ,
					witness: {
						document: document ,
						property: property ,
						type: 'link'
					}
				} ) ;
			}
		}
		
		return document.meta[ property ] ;
	} ;
	
	
	var setter = function setter( linkDocument )
	{
		//var linkCollection = self.world.collections[ collectionName ] ;
		
		// Throw error or not?
		if ( ! ( linkDocument instanceof odm.Document ) || linkDocument.collection.name !== collectionName ) { return ; }
		
		var document = this[''] ;
		
		this[ property ] = linkDocument.id ;
		document.meta[ property ] = linkDocument ;
	} ;
	
	
	Object.defineProperty( this.documentBase , key , {
		configurable: true ,
		get: getter ,
		set: setter
	} ) ;
	
	Object.defineProperty( this.suspectedBase , key , {
		configurable: true ,
		get: getter ,
		set: setter
	} ) ;
} ;



odm.Collection.prototype.addMetaBacklink = function collectionAddMetaBacklink( key , property , collectionName )
{
	var self = this ;
	// Be careful: this.world.collections does not contains this collection at this time: we are still in the constructor!
	
	var getter = function getter()
	{
		var witness , fingerprint = {} , document = this[''] , backlinkCollection = self.world.collections[ collectionName ] ;
		
		if ( ! document.suspected || document.id )
		{
			fingerprint[ property ] = document.id ;
			fingerprint = self.createFingerprint( fingerprint , { from: odm.INTERNAL } ) ;
			
			if ( ! ( document.meta[ property ] instanceof odm.Batch ) || document.meta[ property ].fingerprint.$ != fingerprint.$ )	// jshint ignore:line
			{
				//console.log( '### Backlink collect ###' , fingerprint ) ;
				document.meta[ property ] = backlinkCollection.collect( fingerprint ) ;
			}
		}
		else
		{
			// Here we have no ID -- erf, idea :) -- about what we will get
			if (
				! ( document.meta[ property ] instanceof odm.Batch ) ||
				! ( witness = document.meta[ property ].witness ) ||
				witness.property !== property ||
				witness.document !== document ||
				witness.type !== 'backlink'
			)
			{
				//console.log( '### Backlink describe suspect ###' ) ;
				document.meta[ property ] = odm.Batch( backlinkCollection , null , {
					suspected: true ,
					//useMemProxy: backlinkCollection.useMemProxy ,
					witness: {
						document: document ,
						property: property ,
						type: 'backlink'
					}
				} ) ;
			}
		}
		
		return document.meta[ property ] ;
	} ;
	
	
	var setter = function setter()	// linkDocument )
	{
		throw new Error( 'Not done ATM!' ) ;
		
		//var backlinkCollection = self.world.collections[ collectionName ] ;
		/*
		// Throw error or not?
		if ( ! ( linkDocument instanceof odm.Document ) || linkDocument.collection.name !== collectionName ) { return ; }
		
		var document = this[''] ;
		
		this[ property ] = linkDocument.id ;
		document.meta[ property ] = linkDocument ;
		*/
	} ;
	
	
	Object.defineProperty( this.documentBase , key , {
		configurable: true ,
		get: getter ,
		set: setter
	} ) ;
	
	Object.defineProperty( this.suspectedBase , key , {
		configurable: true ,
		get: getter ,
		set: setter
	} ) ;
} ;



odm.Collection.prototype.initDriver = function collectionInitDriver()
{
	// already connected? nothing to do!
	if ( this.driver ) { return ; }
	
	if ( ! odm.driver[ this.config.driver ] )
	{
		try {
			// First try drivers shipped with odm
			odm.driver[ this.config.driver ] = require( './odm.driver.' + this.config.driver ) ;
		}
		catch ( error ) {
			// Then try drivers in node_modules
			try {
				odm.driver[ this.config.driver ] = require( 'odm.driver.' + this.config.driver ) ;
			}
			catch ( error ) {
				throw new Error( '[odm] Cannot load driver: ' + this.config.driver ) ;
			}
		}
	}
	
	this.driver = odm.driver[ this.config.driver ]( this ) ;
} ;



odm.Collection.prototype.createDocument = function collectionCreateDocument( properties , options )
{
	var i , document ;
	
	// Do not move hooks into odm.Document()!!!
	// It should not be triggered by internal usage
	
	//console.log( "before hook of odm.Document()" , properties ) ;
	
	if ( typeof properties === 'object' )
	{
		for ( i = 0 ; i < this.hooks.beforeCreateDocument.length ; i ++ )
		{
			properties = this.hooks.beforeCreateDocument[ i ]( properties ) ;
		}
	}
	
	//console.log( "before odm.Document()" , properties ) ;
	document = odm.Document( this , properties , options ) ;
	//console.log( "after odm.Document()" , document.$ ) ;
	
	for ( i = 0 ; i < this.hooks.afterCreateDocument.length ; i ++ )
	{
		this.hooks.afterCreateDocument[ i ]( document ) ;
	}
	
	return document ;
} ;



odm.Collection.prototype.createId = function collectionCreateId( rawDocument , id )
{
	return this.driver.createId( rawDocument , id ) ;
} ;



odm.Collection.prototype.createFingerprint = function collectionCreateFingerprint( rawFingerprint , options )
{
	return odm.Fingerprint( this , rawFingerprint , options ) ;
} ;



// Index/re-index a collection
odm.Collection.prototype.buildIndexes = function collectionBuildIndexes( options , callback )
{
	if ( ! options || typeof options !== 'object' )
	{
		if ( typeof options === 'function' ) { callback = options ; }
		options = {} ;
	}
	
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	var self = this ;
	
	//console.log( "this.indexes" , this.indexes ) ;
	
	async.waterfall( [
	
		// Firstly, get indexes
		function( jobCallback ) {
			//console.log( 'Entering stage 1' ) ;
			self.driver.getIndexes( jobCallback ) ;
		} ,
		
		// Secondly, drop obsolete indexes & prepare missing indexes
		function( upstreamIndexes , jobCallback ) {
			//console.log( 'Entering stage 2 -- upstreamIndexes' , upstreamIndexes ) ;
			
			var key , index , obsoleteIndexes = [] , missingIndexes = [] ;
			
			for ( key in upstreamIndexes )
			{
				if ( ! self.indexes[ key ] ) { obsoleteIndexes.push( key ) ; }
			}
			
			for ( key in self.indexes )
			{
				if ( ! upstreamIndexes[ key ] ) { missingIndexes.push( key ) ; }
			}
			
			if ( ! obsoleteIndexes.length ) { jobCallback( undefined , missingIndexes ) ; }
			
			async.foreach( obsoleteIndexes , function( indexName , foreachCallback ) {
				//console.log( "Drop index:" , indexName ) ;
				self.driver.dropIndex( indexName , foreachCallback ) ;
			} )
			.parallel()	// Parallel mode is ok here
			.exec( function( error ) {
				if ( error ) { jobCallback( error ) ; }
				jobCallback( undefined , missingIndexes ) ;
			} ) ;
		} ,
		
		// Finally, create missing indexes
		function( missingIndexes , jobCallback ) {
			//console.log( 'Entering stage 3' ) ;
			
			if ( ! missingIndexes.length ) { jobCallback( undefined ) ; }
			
			// No parallel mode here: building indexes can be really intensive for the DB,
			// also some DB will block anything else, so it's not relevant to parallelize here
			async.foreach( missingIndexes , function( indexName , foreachCallback ) {
				//console.log( "Create index:" , indexName , self.indexes[ indexName ] ) ;
				self.driver.buildIndex( self.indexes[ indexName ] , foreachCallback ) ;
			} )
			.exec( jobCallback ) ;
		}
	] ) 
	.exec( callback ) ;
} ;



/*
	There are 2 modes:
		* with callback, this is the standard async mode, the upstream is checked and the result is provided to the callback
		* without, this return synchronously immediately, providing either a document from the memProxy or a 'suspect' document
	
	options:
		raw: no mapping, just raw driver output
		useMemProxy: if false it disable memProxy storage, so objects can be loaded multiple times from the same DB source,
			leading to concurrencies trouble and worse performance
	
	callback( error , document|rawDocument )
*/
odm.Collection.prototype.get = function collectionGet( id , options , callback )
{
	var useMemProxy , rawDocument , document , self = this ;
	
	// Managing function's arguments
	if ( typeof id !== 'string' && ! id.toString ) { throw new Error( '[odm] provided id cannot be converted to a string' ) ; }
	if ( arguments.length === 2 && typeof options === 'function' ) { callback = options ; options = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	if ( typeof callback !== 'function' ) { callback = null ; }
	
	useMemProxy = options.useMemProxy === undefined ? this.useMemProxy : options.useMemProxy ;
	
	// Try to get the document out of the memProxy storage
	if ( useMemProxy && ( rawDocument = this.memProxyGet( id , options ) ) )
	{
		if ( options.raw )
		{
			// Sync mode:
			if ( ! callback ) { return rawDocument ; }
			
			// Async mode:
			callback( undefined , rawDocument ) ;
			return ;
		}
		
		document = odm.Document( self , rawDocument , { from: odm.MEMPROXY, useMemProxy: useMemProxy } ) ;
		
		// Sync mode:
		if ( ! callback ) { return document ; }
		
		// Async mode:
		callback(
			undefined ,
			odm.Document( self , rawDocument , { from: odm.MEMPROXY, useMemProxy: useMemProxy } )
		) ;
		return ;
	}
	
	// Sync mode: return a 'suspect' document
	if ( ! callback ) { return odm.Document( self , {} , { suspected: true, id: id, useMemProxy: useMemProxy } ) ; }
	
	// Async mode: get it from upstream
	this.driver.get( id , function( error , rawDocument ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		if ( ! rawDocument )
		{
			if ( useMemProxy ) { self.memProxyUnset( id ) ; }
			callback( ErrorStatus.notFound( { message: 'Document not found' } ) ) ;
			return ;
		}
		
		if ( useMemProxy ) { self.memProxySet( id , rawDocument ) ; }
		
		if ( options.raw ) { callback( undefined , rawDocument ) ; return ; }
		
		callback(
			undefined ,
			odm.Document( self , rawDocument , { from: odm.UPSTREAM, useMemProxy: useMemProxy } )
		) ;
	} ) ;
} ;



// Get a document by a unique Fingerprint
odm.Collection.prototype.getUnique = function collectionGetUnique( fingerprint , options , callback )
{
	var useMemProxy , self = this ;
	
	// Managing function's arguments
	if ( ! fingerprint || typeof fingerprint !== 'object' ) { throw new Error( "[odm] fingerprint should be an object" ) ; }
	if ( arguments.length === 2 && typeof options === 'function' ) { callback = options ; options = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	if ( typeof callback !== 'function' ) { callback = null ; }
	
	// Check if we have a unique fingerprint
	if ( ! ( fingerprint instanceof odm.Fingerprint ) ) { fingerprint = this.createFingerprint( fingerprint ) ; }
	
	if ( ! fingerprint.unique )
	{
		var error = ErrorStatus.badRequest( { message: 'This is not a unique fingerprint' } ) ;
		if ( callback ) { callback( error ) ; return ; }
		return error ;
	}
	
	useMemProxy = options.useMemProxy === undefined ? this.useMemProxy : options.useMemProxy ;
	
	// === TODO: memProxy ===
	// Needed: unique fingerprint indexes
	
	// Sync mode: return a 'suspect' document
	if ( ! callback ) { return odm.Document( self , {} , { suspected: true, fingerprint: fingerprint, useMemProxy: useMemProxy } ) ; }
	
	this.driver.getUnique( fingerprint.$ , function( error , rawDocument ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		if ( ! rawDocument )
		{
			//if ( useMemProxy ) { self.memProxyUnset( id ) ; }
			callback( ErrorStatus.notFound( { message: 'Document not found' } ) ) ;
			return ;
		}
		
		//if ( useMemProxy ) { self.memProxySet( id , rawDocument ) ; }
		
		if ( options.raw ) { callback( undefined , rawDocument ) ; return ; }
		
		callback(
			undefined ,
			odm.Document( self , rawDocument , { from: odm.UPSTREAM, useMemProxy: useMemProxy } )
		) ;
	} ) ;
} ;



// Get a set of document
odm.Collection.prototype.collect = function collectionCollect( fingerprint , options , callback )
{
	var useMemProxy , self = this ;
	
	// Managing function's arguments
	if ( ! fingerprint || typeof fingerprint !== 'object' ) { throw new Error( "[odm] fingerprint should be an object" ) ; }
	if ( arguments.length === 2 && typeof options === 'function' ) { callback = options ; options = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	if ( typeof callback !== 'function' ) { callback = null ; }
	
	// Create fingerprint if needed
	if ( ! ( fingerprint instanceof odm.Fingerprint ) ) { fingerprint = this.createFingerprint( fingerprint ) ; }
	
	useMemProxy = options.useMemProxy === undefined ? this.useMemProxy : options.useMemProxy ;
	
	// === TODO: memProxy ===
	// Needed: fingerprint indexes
	
	// Sync mode: return a 'suspect' batch
	if ( ! callback ) { return odm.Batch( self , fingerprint , { suspected: true, useMemProxy: useMemProxy } ) ; }
	
	this.driver.collect( fingerprint.$ , function( error , rawBatch ) {
		
		if ( error ) { callback( error ) ; return ; }
		if ( ! rawBatch ) { callback( ErrorStatus.notFound( { message: 'Batch not found' } ) ) ; return ; }	// should never happen?
		
		var i , batch , length = rawBatch.length , idKey = self.driver.idKey ;
		
		if ( options.raw )
		{
			if ( useMemProxy )
			{
				for ( i = 0 ; i < length ; i ++ ) { self.memProxySet( rawBatch[ i ][ idKey ] , rawBatch[ i ] ) ; }
			}
			
			callback( error , rawBatch ) ;
			return ;
		}
		
		batch = odm.Batch( self , fingerprint ) ;
		
		for ( i = 0 ; i < length ; i ++ )
		{
			if ( useMemProxy ) { self.memProxySet( rawBatch[ i ][ idKey ] , rawBatch[ i ] ) ; }
			batch.add( odm.Document( self , rawBatch[ i ] , { from: odm.UPSTREAM, useMemProxy: useMemProxy } ) ) ;
		}
		
		callback( undefined , batch ) ;
	} ) ;
} ;



// Get a set of document
odm.Collection.prototype.find = function collectionFind( queryObject , options , callback )
{
	// No memProxy support: it will be dropped
	
	var self = this ;
	
	// Managing function's arguments
	if ( ! queryObject || typeof queryObject !== 'object' ) { throw new Error( "[odm] queryObject should be an object" ) ; }
	if ( arguments.length === 2 && typeof options === 'function' ) { callback = options ; options = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	if ( typeof callback !== 'function' ) { throw new Error( "[odm] missing callback" ) ; }
	
	this.driver.find( queryObject , function( error , rawBatch ) {
		
		if ( error ) { callback( error ) ; return ; }
		if ( ! rawBatch ) { callback( ErrorStatus.notFound( { message: 'Batch not found' } ) ) ; return ; }	// should never happen?
		
		var i , batch , length = rawBatch.length , idKey = self.driver.idKey ;
		
		if ( options.raw )
		{
			callback( error , rawBatch ) ;
			return ;
		}
		
		batch = odm.Batch( self ) ;
		
		for ( i = 0 ; i < length ; i ++ )
		{
			batch.add( odm.Document( self , rawBatch[ i ] , { from: odm.UPSTREAM, useMemProxy: false } ) ) ;
		}
		
		callback( undefined , batch ) ;
	} ) ;
} ;



/*
	Useful or useless?
*/
/*
odm.Collection.prototype.describeSuspect = function collectionDescribeSuspect( fingerprint , witness , options )
{
	var useMemProxy , self = this ;
	
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	
	useMemProxy = options.useMemProxy === undefined ? this.useMemProxy : options.useMemProxy ;
	
	return odm.Document( self , fingerprint , { suspected: true, witness: witness, useMemProxy: useMemProxy } ) ;
} ;
*/





			/* MemProxy Store */



// Get a clone of a raw document from the memProxy
odm.Collection.prototype.memProxyGet = function collectionMemProxyGet( id )
{
	var idString ;
	
	if ( typeof id === 'string' ) { idString = id ; }
	else if ( id.toString ) { idString = id.toString() ; }
	else { throw new Error( '[odm] provided id cannot be converted to a string' ) ; }
	
	if ( ! this.memProxyRawDocuments[ idString ] )
	{
		//console.log( '### Cannot found' , idString , 'in the memProxy ###' ) ;
		return null ;
	}
	
	//console.log( '### Getting' , idString , 'out of the memProxy ###' ) ;
	return tree.extend( { deep: true, proto: true } , null , this.memProxyRawDocuments[ idString ] ) ;
} ;



// Set a raw document (clone) into the memProxy
odm.Collection.prototype.memProxySet = function collectionMemProxySet( id , rawDocument )
{
	var idString ;
	
	if ( typeof id === 'string' ) { idString = id ; }
	else if ( id.toString ) { idString = id.toString() ; }
	else { throw new Error( '[odm] provided id cannot be converted to a string' ) ; }
	
	//console.log( '### Setting' , idString , ' in the memProxy ###' ) ;
	this.memProxyRawDocuments[ idString ] = tree.extend( { deep: true, proto: true } , null , rawDocument ) ;
} ;



odm.Collection.prototype.memProxyPatch = function collectionMemProxyPatch( id , rawDocument )
{
	var idString ;
	
	if ( typeof id === 'string' ) { idString = id ; }
	else if ( id.toString ) { idString = id.toString() ; }
	else { throw new Error( '[odm] provided id cannot be converted to a string' ) ; }
	
	if ( ! this.memProxyRawDocuments[ idString ] )
	{
		//console.log( '### Cannot found (patch)' , idString , 'in the memProxy ###' ) ;
		return false ;
	}
	
	//console.log( '### Patching' , idString , ' in the memProxy ###' ) ;
	tree.extend( { deep: true, proto: true, skipRoot: true } , this.memProxyRawDocuments[ idString ] , rawDocument ) ;
	return true ;
} ;



// Unset a raw document
odm.Collection.prototype.memProxyUnset = function collectionMemProxyUnset( id )
{
	var idString ;
	
	if ( typeof id === 'string' ) { idString = id ; }
	else if ( id.toString ) { idString = id.toString() ; }
	else { throw new Error( '[odm] provided id cannot be converted to a string' ) ; }
	
	//console.log( '### Unsetting' , idString , ' from the memProxy ###' ) ;
	delete this.memProxyRawDocuments[ idString ] ;
} ;



odm.Collection.prototype.memProxyReset = function collectionMemProxyReset()
{
	//console.log( '### Resetting the memProxy ###' ) ;
	this.memProxyRawDocuments = {} ;
} ;





			/* Document */



// ( collection , [properties] , [options] )
odm.Document = function Document( collection , properties , options )
{
	if ( ! ( collection instanceof odm.Collection ) ) { throw new TypeError( '[odm] Argument #0 of odm.Document() should be an instance of odm.Collection' ) ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	
	var deepInherit = collection.deepInherit ;
	
	var document = Object.create( odm.Document.prototype , {
		world: { value: collection.world } ,
		collection: { value: collection } ,
		meta: { value: {} } ,
		suspected: { writable: true, value: false } ,
		loaded: { writable: true, value: false } ,
		saved: { writable: true, value: false } ,
		deleted: { writable: true, value: false } ,
		upstreamExists: { writable: true, value: false } ,
		useMemProxy: { writable: true, value: false }
	} ) ;
	
	document.useMemProxy = options.useMemProxy === undefined ? collection.useMemProxy : options.useMemProxy ;
	
	// Suspect is set when the object is in a state where it may exist upstream but should be loaded first
	if ( options.suspected )
	{
		document.suspected = true ;
		document.upstream = deepInherit( null , collection.suspectedBase ) ;
		document.$ = deepInherit( null , document.upstream ) ;
		document.id = null ;
		document.fingerprint = null ;
		document.witness = null ;
		
		Object.defineProperty( document.$ , '' , { value: document } ) ;	// link to the parent
		
		if ( options.fingerprint && typeof options.fingerprint === 'object' )
		{
			var fingerprint ;
			
			// Check if we have a unique fingerprint
			if ( options.fingerprint instanceof odm.Fingerprint ) { fingerprint = options.fingerprint ; }
			else { fingerprint = this.createFingerprint( options.fingerprint ) ; }
			
			if ( fingerprint.unique )
			{
				tree.extend( { own: true } , document.upstream , fingerprint.sparseDocument ) ;
				//console.log( '<<<<<<<<<< document.upstream:' , document.upstream ) ;
				document.fingerprint = fingerprint ;
			}
		}
		
		if ( options.witness && typeof options.witness === 'object' && Object.keys( options.witness ).length )
		{
			document.witness = options.witness ;
		}
		
		if ( options.id )
		{
			if ( typeof options.id !== 'string' && ! options.id.toString ) { throw new Error( '[odm] provided id cannot be converted to a string' ) ; }
			Object.defineProperty( document , 'id' , { value: collection.driver.createId( document.$ , options.id ) , enumerable: true } ) ;
		}
		
		if ( ! document.id && ! document.fingerprint && ! document.witness )
		{
			throw new Error( '[odm] cannot instanciate a suspect without id, fingerprint or witness' ) ;
		}
		
		return document ;
	}
	
	// Hydrating the document as fast as possible
	if ( typeof properties === 'object' )
	{
		switch ( options.from )
		{
			case odm.UPSTREAM :
			case odm.MEMPROXY :
				// it is safe to get directly 'properties' from upstream or memProxy: every object is already unique (cloned)
				document.upstream = deepInherit( properties , collection.documentBase ) ;
				document.$ = deepInherit( null , document.upstream ) ;
				Object.defineProperty( document.$ , '' , { value: document } ) ;	// link to the parent
				
				document.loaded = true ;
				document.upstreamExists = true ;
				break ;
				
			//case odm.NONE :
			default :
				document.upstream = deepInherit( null , collection.documentBase ) ;
				document.$ = deepInherit( null , document.upstream ) ;
				
				// It is probably unsafe to reference directly userland properties
				// Here we clone 'property' and fix prototype accordingly,
				// also we have to trust userland prototype
				
				//tree.extend( { deep: true, proto: true, skipRoot: true } , document.$ , properties ) ;
				
//------------------------------------------------------------------------------ WIP -----------------------------------------
				
				//console.log( "bob: " , collection.validate( properties ) ) ;
				//*
				
				try {
					tree.extend(
						{ deep: true, proto: true, skipRoot: true, deepFilter: collection.driver.objectFilter } ,
						document.$ ,
						collection.validate( properties )
					) ;
				}
				catch ( error ) {
					error.validatorMessage = error.message ;
					error.message = '[odm] validator error: ' + error.message ;
					//console.log( "error!!!!!!" , properties ) ;
					//console.log( "error!!!!!!" , collection.dataSchema ) ;
					throw error ;
				}
				//*/
				
				Object.defineProperty( document.$ , '' , { value: document } ) ;	// link to the parent
		}
	}
	else
	{
		document.upstream = deepInherit( null , collection.documentBase ) ;
		document.$ = deepInherit( null , document.upstream ) ;
		Object.defineProperty( document.$ , '' , { value: document } ) ;	// link to the parent
	}
	
	var id = collection.driver.createId( document.$ , options.id ) ;
	Object.defineProperty( document , 'id' , { value: id , enumerable: true } ) ;
	
	return document ;
} ;

odm.Document.prototype.constructor = odm.Document ;



// Return a one-value state
odm.Document.prototype.state = function documentState()
{
	if ( this.deleted ) { return 'deleted' ; }
	
	if ( this.suspected )
	{
		if ( this.upstreamExists ) { return 'existing-suspect' ; }
		return 'suspected' ;
	}
	
	if ( this.upstreamExists )
	{
		if ( ( this.saved || this.loaded ) && Object.keys( this.$ ).length === 0 ) { return 'synced' ; }
		return 'exists' ;
	}
	
	return 'app-side' ;
} ;



// Reveal a suspected Document
odm.Document.prototype.reveal = function documentReveal( options , callback )
{
	if ( ! options || typeof options !== 'object' )
	{
		if ( typeof options === 'function' ) { callback = options ; }
		options = {} ;
	}
	
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	if ( ! this.suspected || ( options.idOnly && this.id ) ) { callback( undefined , this ) ; return ; }
	
	if ( this.id ) { this.revealById( options , callback ) ; return ; }
	
	if ( this.fingerprint ) { this.revealByFingerprint( options , callback ) ; return ; }
	
	if ( this.witness ) { this.revealByWitness( options , callback ) ; return ; }
} ;



odm.Document.prototype.revealById = function documentRevealById( options , callback )
{
	var idString , rawDocument , deepInherit = this.collection.deepInherit , self = this ;
	
	if ( typeof this.id === 'string' ) { idString = this.id ; }
	else if ( this.id.toString ) { idString = this.id.toString() ; }
	else { throw new Error( '[odm] provided id cannot be converted to a string' ) ; }
	
	// Try to get the document out of the memProxy storage
	if ( this.useMemProxy && ( rawDocument = this.collection.memProxyGet( this.id , options ) ) )
	{
		this.upstream = deepInherit( rawDocument , this.collection.documentBase ) ;
		deepInherit( this.$ , this.upstream ) ;
		delete this.$[ this.collection.driver.idKey ] ;
		callback( undefined , self ) ;
		return ;
	}
	
	this.collection.driver.get( this.id , function( error , rawDocument ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		if ( ! rawDocument )
		{
			self.suspected = false ;
			self.deleted = true ;
			self.upstreamExists = false ;
			if ( self.useMemProxy ) { self.collection.memProxyUnset( self.id ) ; }
			callback( ErrorStatus.notFound( { message: 'Document not found' } ) ) ;
			return ;
		}
		
		self.suspected = false ;
		self.loaded = true ;
		self.upstreamExists = true ;
		
		self.upstream = deepInherit( rawDocument , self.collection.documentBase ) ;
		deepInherit( self.$ , self.upstream ) ;
		delete self.$[ self.collection.driver.idKey ] ;
		
		if ( self.useMemProxy ) { self.collection.memProxySet( self.id , rawDocument ) ; }
		
		callback( undefined , self ) ;
	} ) ;
} ;



odm.Document.prototype.revealByFingerprint = function documentRevealByFingerprint( options , callback )
{
	var deepInherit = this.collection.deepInherit , self = this ;
	
	if ( ! ( this.fingerprint instanceof odm.Fingerprint ) ) { throw new Error( '[odm] no fingerprint for this suspect' ) ; }
	
	/* proxy does not support fingerprint ATM
	// Try to get the document out of the memProxy storage
	var idString , rawDocument ;
	if ( this.useMemProxy && ( rawDocument = this.collection.memProxyGet( this.id , options ) ) )
	{
		this.upstream = deepInherit( rawDocument , this.collection.documentBase ) ;
		deepInherit( this.$ , this.upstream ) ;
		delete this.$[ this.collection.driver.idKey ] ;
		callback( undefined , self ) ;
		return ;
	}
	*/
	
	this.collection.driver.getUnique( this.fingerprint.$ , function( error , rawDocument ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		if ( ! rawDocument )
		{
			self.suspected = false ;
			self.deleted = true ;
			self.upstreamExists = false ;
			//if ( self.useMemProxy ) { self.collection.memProxyUnset( self.id ) ; }
			callback( ErrorStatus.notFound( { message: 'Document not found' } ) ) ;
			return ;
		}
		
		self.suspected = false ;
		self.loaded = true ;
		self.upstreamExists = true ;
		
		self.upstream = deepInherit( rawDocument , self.collection.documentBase ) ;
		deepInherit( self.$ , self.upstream ) ;
		delete self.$[ self.collection.driver.idKey ] ;
		
		//if ( self.useMemProxy ) { self.collection.memProxySet( self.id , rawDocument ) ; }
		
		callback( undefined , self ) ;
	} ) ;
} ;



odm.Document.prototype.revealByWitness = function documentRevealByWitness( options , callback )
{
	var self = this ;
	
	if ( ! this.witness || typeof this.witness !== 'object' ) { throw new Error( '[odm] no witness for this suspect' ) ; }
	
	switch ( this.witness.type )
	{
		case 'link' :
			if ( this.witness.document.suspected )
			{
				// Do not transmit options.idOnly
				this.witness.document.reveal( {} , function( error ) {
					if ( error ) { callback( error ) ; return ; }
					self.revealByWitness( options , callback ) ;
				} ) ;
				
				return ;
			}
			
			if ( ! this.witness.document.$[ this.witness.property ] )
			{
				this.deleted = true ;
				callback( ErrorStatus.notFound( { message: 'Document not found' } ) ) ;
				return ;
			}
			
			this.id = this.witness.document.$[ this.witness.property ] ;
			
			if ( options.idOnly ) { callback( undefined , this ) ; return ; }
			
			this.revealById( options , callback ) ;
			
			break ;
		
		// those type cannot exist for a document:
		//case 'backlink' :
		default :
			throw new Error( '[odm] Cannot reveal batch with this type of witness: ' + this.witness.type ) ;
	}
} ;



odm.Document.prototype.export = function documentExport()
{
	return tree.extend( { deep: true , deepFilter: this.collection.driver.objectFilter } , {} , this.$ ) ;
} ;



// callback( error )
odm.Document.prototype.save = function documentSave( options , callback )
{
	var self = this , collection = this.collection , rawDocument , method , deepInherit = collection.deepInherit ;
	
	if ( ! options || typeof options !== 'object' )
	{
		if ( typeof options === 'function' ) { callback = options ; }
		options = {} ;
	}
	
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	if ( this.suspected ) { throw new Error( '[odm] cannot save a suspected document - it is on the TODO LIST already' ) ; }
	if ( this.deleted ) { throw new Error( 'Current document is deleted' ) ; }
	
	if ( this.upstreamExists )
	{
		if ( options.fullSave )
		{
			method = 'update' ;
			rawDocument = tree.extend(
				{ own: true, deep: true, deepFilter: collection.driver.objectFilter } ,
				{} ,
				this.upstream ,
				this.$
			) ;
		}
		else
		{
			method = 'patch' ;
			rawDocument = tree.extend(
				{ own: true, deep: true, flat: this.collection.driver.pathSeparator, deepFilter: collection.driver.objectFilter } ,
				{} ,
				this.$
			) ;
		}
		
		// Full save (update) or simple patch
		this.collection.driver[ method ]( this.id , rawDocument , function( error ) {
			
			if ( error ) { callback( error ) ; return ; }
			
			// merge $ back into upstream, then create a fresh new $
			tree.extend( { own: true, move: true } , self.upstream , self.$ ) ;
			
			if ( self.useMemProxy )
			{
				if ( options.fullSave ) { self.collection.memProxySet( self.id , self.upstream ) ; }
				else { self.collection.memProxyPatch( self.id , self.$ ) ; }
			}
			
			self.saved = true ;
			callback() ;
		} ) ;
	}
	else if ( options.overwrite )
	{
		// create (insert) needed
		this.collection.driver.overwrite( tree.extend( { own: true, deep: true, deepFilter: collection.driver.objectFilter } , {} , this.$ ) , function( error )
		{
			if ( error ) { callback( error ) ; return ; }
			
			// now that it exists in DB, $ become DB and a fresh $ should be created
			self.upstream = self.$ ;
			self.$ = deepInherit( null , self.upstream ) ;
			Object.defineProperty( self.$ , '' , { value: self } ) ;	// link to the parent
			
			if ( self.useMemProxy ) { self.collection.memProxySet( self.id , self.upstream ) ; }
			
			// mark this document as saved and trigger the callback
			self.saved = true ;
			self.upstreamExists = true ;
			callback() ;
		} ) ;
	}
	else
	{
		// create (insert) needed
		this.collection.driver.create( tree.extend( { own: true, deep: true, deepFilter: collection.driver.objectFilter } , {} , this.$ ) , function( error )
		{
			if ( error ) { callback( error ) ; return ; }
			
			// now that it exists in DB, $ become DB and a fresh $ should be created
			self.upstream = self.$ ;
			self.$ = deepInherit( null , self.upstream ) ;
			Object.defineProperty( self.$ , '' , { value: self } ) ;	// link to the parent
			
			if ( self.useMemProxy ) { self.collection.memProxySet( self.id , self.upstream ) ; }
			
			// mark this document as saved and trigger the callback
			self.saved = true ;
			self.upstreamExists = true ;
			callback() ;
		} ) ;
	}
} ;



odm.Document.prototype.delete = function documentDelete( options , callback )
{
	var self = this ;
	
	if ( ! options || typeof options !== 'object' )
	{
		if ( typeof options === 'function' ) { callback = options ; }
		options = {} ;
	}
	
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	if ( this.suspected ) { throw new Error( '[odm] cannot delete a suspected document - it is on the TODO LIST already' ) ; }
	if ( this.deleted ) { throw new Error( 'Current document is already deleted' ) ; }
	
	this.collection.driver.delete( this.id , function( error )
	{
		if ( error ) { callback( error ) ; return ; }
		
		self.deleted = true ;
		self.upstreamExists = false ;
		if ( self.useMemProxy ) { self.collection.memProxyUnset( self.id ) ; }
		
		callback() ;
	} ) ;
} ;





			/* Batch */



/*
	FEATURE TODO:
		* set common properties with batch.$
*/
odm.Batch = function Batch( collection , fingerprint , options )
{
	if ( ! ( collection instanceof odm.Collection ) ) { throw new TypeError( '[odm] Argument #0 of odm.Batch() should be an instance of odm.Collection' ) ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	
	var batch = Object.create( odm.Batch.prototype , {
		world: { value: collection.world } ,
		collection: { value: collection } ,
		documents: { value: [] } ,
		suspected: { writable: true, value: false }
	} ) ;
	
	batch.fingerprint = null ;
	
	if ( fingerprint && typeof fingerprint === 'object' )
	{
		if ( fingerprint instanceof odm.Fingerprint ) { batch.fingerprint = fingerprint ; }
		else { batch.fingerprint = collection.createFingerprint( fingerprint ) ; }
	}
	
	// Suspect is set when the object is in a state where it may exist upstream but should be loaded first
	if ( options.suspected )
	{
		batch.suspected = true ;
		
		if ( options.witness && typeof options.witness === 'object' && Object.keys( options.witness ).length )
		{
			batch.witness = options.witness ;
		}
		else
		{
			batch.witness = null ;
		}
		
		if ( ! batch.fingerprint && ! batch.witness )
		{
			throw new Error( '[odm] cannot instanciate a suspect without fingerprint or witness' ) ;
		}
		
		return batch ;
	}
	
	return batch ;
} ;



/*
	Should handle hooks, e.g. if the batch is related to a multilink, it should update the parent multilink property.
*/
odm.Batch.prototype.add = function batchAdd( document )
{
	if ( ! ( document instanceof odm.Document ) ) { throw new TypeError( '[odm] Argument #0 of odm.Batch.prototype.add() should be an instance of odm.Document' ) ; }
	this.documents.push( document ) ;
} ;



// Reveal a suspected Batch
odm.Batch.prototype.reveal = function batchReveal( options , callback )
{
	if ( ! options || typeof options !== 'object' )
	{
		if ( typeof options === 'function' ) { callback = options ; }
		options = {} ;
	}
	
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	if ( ! this.suspected )
	{
		callback( undefined , this ) ;
		return ;
	}
	
	if ( this.fingerprint ) { this.revealByFingerprint( options , callback ) ; return ; }
	
	if ( this.witness ) { this.revealByWitness( options , callback ) ; return ; }
} ;



odm.Batch.prototype.revealByFingerprint = function batchRevealByFingerprint( options , callback )
{
	var self = this ;
	
	this.collection.driver.collect( this.fingerprint.$ , function( error , rawBatch ) {
		
		if ( error ) { callback( error ) ; return ; }
		if ( ! rawBatch ) { callback( ErrorStatus.notFound( { message: 'Batch not found' } ) ) ; return ; }	// should never happen?
		
		var i , length = rawBatch.length , idKey = self.collection.driver.idKey ;
		
		for ( i = 0 ; i < length ; i ++ )
		{
			if ( self.useMemProxy ) { self.collection.memProxySet( rawBatch[ i ][ idKey ] , rawBatch[ i ] ) ; }
			self.add( odm.Document( self.collection , rawBatch[ i ] , { from: odm.UPSTREAM, useMemProxy: self.useMemProxy } ) ) ;
		}
		
		self.suspected = false ;
		
		callback( undefined , self ) ;
	} ) ;
	
} ;



odm.Batch.prototype.revealByWitness = function batchRevealByWitness( options , callback )
{
	var self = this ;
	
	if ( ! this.witness || typeof this.witness !== 'object' ) { throw new Error( '[odm] no witness for this suspect' ) ; }
	
	switch ( this.witness.type )
	{
		case 'backlink' :
			if ( this.witness.document.suspected && ! this.witness.document.id )
			{
				// Do not transmit random options...
				this.witness.document.reveal( { idOnly: true } , function( error ) {
					if ( error ) { callback( error ) ; return ; }
					self.revealByWitness( options , callback ) ;
				} ) ;
				
				return ;
			}
			
			this.fingerprint = {} ;
			this.fingerprint[ this.witness.property ] = this.witness.document.id ;
			this.fingerprint = this.collection.createFingerprint( this.fingerprint , { from: odm.INTERNAL } ) ;
			this.revealByFingerprint( options , callback ) ;
			break ;
		
		// those type cannot exist for a batch:
		//case 'link' :
		default :
			throw new Error( '[odm] Cannot reveal batch with this type of witness: ' + this.witness.type ) ;
	}
} ;



odm.Batch.prototype.export = function batchExport()
{
	var i , raw = [] , length = this.documents.length ;
	
	for ( i = 0 ; i < length ; i ++ )
	{
		raw[ i ] = tree.extend( { deep: true , deepFilter: this.collection.driver.objectFilter } , {} , this.documents[ i ].$ ) ;
	}
	
	return raw ;
} ;








			/* Fingerprint */



/*
	The goal of this class is to figure out weither a fingerprint is unique or not, thus detecting if a query
	should return a Batch or a Document.
	Some other data may be calculated at that time.
	For instance, it does not much.
	
	options:
		* fromSparseDocument: a sparse document is given, so it should be converted to the 'flat' format
*/
odm.Fingerprint = function Fingerprint( collection , rawFingerprint , options )
{
	if ( ! rawFingerprint || typeof rawFingerprint !== 'object' ) { rawFingerprint = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	
	var fingerprint = Object.create( odm.Fingerprint.prototype , {
		//world: { value: collection.world } ,
		collection: { value: collection }
	} ) ;
	
	if ( ! options.fromSparseDocument )
	{
		Object.defineProperties( fingerprint , {
			$: {
				enumerable: true ,
				value: options.from !== odm.INTERNAL ?
					tree.extend( { own: true, deep: true, deepFilter: collection.driver.objectFilter } , {} , rawFingerprint ) :
					rawFingerprint
			} ,
			sparseDocument: {
				configurable: true ,
				enumerable: true ,
				get: odm.Fingerprint.prototype.unflatten.bind( fingerprint )
			}
		} ) ;
	}
	else
	{
		Object.defineProperties( fingerprint , {
			sparseDocument: {
				enumerable: true ,
				value: options.from !== odm.INTERNAL ?
					tree.extend( { own: true, deep: true, deepFilter: collection.driver.objectFilter } , {} , rawFingerprint ) :
					rawFingerprint
			} ,
			$: {
				configurable: true ,
				enumerable: true ,
				get: odm.Fingerprint.prototype.flatten.bind( fingerprint )
			}
		} ) ;
	}
	
	// Lazyloading
	Object.defineProperties( fingerprint , {
		unique: {
			configurable: true ,
			enumerable: true ,
			get: odm.Fingerprint.prototype.uniquenessCheck.bind( fingerprint )
		}
	} ) ;
	
	return fingerprint ;
} ;

odm.Fingerprint.prototype.constructor = odm.Fingerprint ;



odm.Fingerprint.prototype.uniquenessCheck = function uniquenessCheck()
{
	var i , j , index , match , uniques = this.collection.uniques ;
	
	for ( i = 0 ; i < uniques.length ; i ++ )
	{
		index = uniques[ i ] ;
		match = 0 ;
		for ( j = 0 ; j < index.length ; j ++ )
		{
			if ( index[ j ] in this.$ ) { match ++ ; }
		}
		
		if ( match === index.length )
		{
			Object.defineProperty( this , 'unique' , { value: true , enumerable: true } ) ;
			return true ;
		}
	}
	
	Object.defineProperty( this , 'unique' , { value: false , enumerable: true } ) ;
	return false ;
} ;



odm.Fingerprint.prototype.flatten = function flatten()
{
	var flat = tree.extend(
		{
			flat: this.collection.driver.pathSeparator ,
			deepFilter: this.collection.driver.objectFilter
		} ,
		null ,
		this.sparseDocument
	) ;
	
	Object.defineProperty( this , '$' , { value: flat , enumerable: true } ) ;
	return flat ;
} ;



odm.Fingerprint.prototype.unflatten = function unflatten()
{
	var sparseDocument = tree.extend(
		{
			unflat: this.collection.driver.pathSeparator ,
			deepFilter: this.collection.driver.objectFilter
		} ,
		null ,
		this.$
	) ;
	
	Object.defineProperty( this , 'sparseDocument' , { value: sparseDocument , enumerable: true } ) ;
	return sparseDocument ;
} ;





			/* Utilities */



function noop() {}



// bulk( method , arrayOfObjects , [arg1] , [arg2] , [...] , callback )
// An utility for bulk action
odm.bulk = function bulk()
{
	if (
		arguments.length < 3 ||
		typeof arguments[ 0 ] !== 'string' ||
		! arguments[ 1 ] || typeof arguments[ 1 ] !== 'object' ||
		typeof arguments[ arguments.length - 1 ] !== 'function'
	)
	{
		throw new Error( "[odm] bulk() usage is: bulk( method , arrayOfObjects , [arg1] , [arg2] , [...] , callback )" ) ;
	}
	
	var bulkMethod = arguments[ 0 ] ;
	var objectArray = arguments[ 1 ] ;
	var bulkCallback = arguments[ arguments.length - 1 ] ;
	var bulkArgs = Array.prototype.slice.call( arguments , 2 , -1 ) ;
	
	async.foreach( objectArray , function( object , foreachCallback ) {
		object[ bulkMethod ].apply( object , bulkArgs.concat( foreachCallback ) ) ;
	} )
	.parallel()
	.exec( bulkCallback ) ;
} ;





			/* Common driver */



odm.driver = {} ;

odm.driver.Common = function Common() // ( collection , callback )
{
	throw new Error( "[odm] Cannot create an odm Common driver object directly" ) ;
} ;

odm.driver.Common.prototype.constructor = odm.driver.Common ;


