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



// Load modules
var rootsDb = require( './rootsDb.js' ) ;

var async = require( 'async-kit' ) ;
var doormen = require( 'doormen' ) ;
var hash = require( 'hash-kit' ) ;
var tree = require( 'tree-kit' ) ;
var ErrorStatus = require( 'error-status' ) ;

var url = require( 'url' ) ;




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



function Collection( world , name , schema )
{
	doormen( collectionSchema , schema ) ;
	
	var collection = Object.create( Collection.prototype , {
		world: { value: world } ,
		name: { value: name , enumerable: true } ,
		driver: { value: undefined , writable: true }
	} ) ;
	
	var key , element , indexName ;
	
	if ( typeof schema.url !== 'string' ) { throw new Error( '[roots-db] schema.url should be a string' ) ; }
	collection.url = schema.url ;
	collection.config = url.parse( collection.url , true ) ;
	collection.config.driver = collection.config.protocol.split( ':' )[ 0 ] ;
	if ( ! schema.properties ) { schema.properties = {} ; }
	
	// Create the validator schema
	//collection.documentSchema = doormen.purifySchema( schema ) ;
	collection.documentSchema = schema ;
	
	// Temp? or not?
	if ( ! collection.documentSchema.properties._id ) { collection.documentSchema.properties._id = { optional: true , type: 'objectId' } ; }
	
	collection.validate = doormen.bind( doormen , collection.documentSchema ) ;
	
	// Attachment URL for files
	if ( typeof schema.attachmentUrl === 'string' )
	{
		collection.attachmentUrl = schema.attachmentUrl ;
		if ( collection.attachmentUrl[ collection.attachmentUrl.length - 1 ] !== '/' ) { collection.attachmentUrl += '/' ; }
	}
	else
	{
		collection.attachmentUrl = null ;
	}
	
	
	// TODO: Check schema
	
	collection.meta = schema.meta || {} ;
	collection.suspectedBase = {} ;
	
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
			
			if ( ! element || typeof element !== 'object' || ! element.properties || typeof element.properties !== 'object' )
			{
				continue ;
			}
			
			if ( element.unique ) { collection.uniques.push( Object.keys( element.properties ) ) ; }
			
			indexName = hash.fingerprint( element ) ;
			collection.indexes[ indexName ] = tree.extend( null , { name: indexName } , element ) ;
		}
	}
	
	
	/*
	// Properties
	for ( key in collection.properties )
	{
		Object.defineProperty( collection.suspectedBase , key , {
			value: undefined ,
			writable: true ,	// needed, or derivative object cannot set it
			enumerable: true
		} ) ;
	}
	*/
	
	// Meta
	/*
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
				throw new Error( '[roots-db] unknown meta type: ' + element.type ) ;
		}
	}
	*/
	
	
	// Init the driver
	collection.initDriver() ;
	collection.uniques.unshift( [ collection.driver.idKey ] ) ;
	
	
	return collection ;
}

Collection.prototype.constructor = Collection ;
module.exports = Collection ;



function noop() {}



Collection.prototype.initDriver = function collectionInitDriver()
{
	// already connected? nothing to do!
	if ( this.driver ) { return ; }
	
	if ( ! rootsDb.driver[ this.config.driver ] )
	{
		try {
			// First try drivers shipped with rootsDb
			rootsDb.driver[ this.config.driver ] = require( './' + this.config.driver + '.driver.js' ) ;
		}
		catch ( error ) {
			// Then try drivers in node_modules
			try {
				rootsDb.driver[ this.config.driver ] = require( 'roots-db-' + this.config.driver ) ;
			}
			catch ( error ) {
				throw new Error( '[roots-db] Cannot load driver: ' + this.config.driver ) ;
			}
		}
	}
	
	this.driver = rootsDb.driver[ this.config.driver ]( this ) ;
} ;



Collection.prototype.createId = function collectionCreateId()
{
	return this.driver.createId() ;
} ;



Collection.prototype.createFingerprint = function collectionCreateFingerprint( rawFingerprint , options )
{
	if ( ! rawFingerprint || typeof rawFingerprint !== 'object' ) { rawFingerprint = {} ; }
	
	var wrapper = rootsDb.FingerprintWrapper( this , rawFingerprint , options ) ;
	
	return wrapper.fingerprint ;
} ;



// Index/re-index a collection
Collection.prototype.buildIndexes = function collectionBuildIndexes( options , callback )
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
			
			var key , obsoleteIndexes = [] , missingIndexes = [] ;
			
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



			/* Document-oriented method */



Collection.prototype.createDocument = function collectionCreateDocument( rawDocument , options )
{
	var i ;
	
	if ( ! rawDocument || typeof rawDocument !== 'object' ) { rawDocument = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	
	for ( i = 0 ; i < this.hooks.beforeCreateDocument.length ; i ++ )
	{
		this.hooks.beforeCreateDocument[ i ]( rawDocument ) ;
	}
	
	rootsDb.DocumentWrapper( this , rawDocument , options ) ;
	
	for ( i = 0 ; i < this.hooks.afterCreateDocument.length ; i ++ )
	{
		this.hooks.afterCreateDocument[ i ]( rawDocument ) ;
	}
	
	return rawDocument ;
} ;



Collection.prototype.createBatch = function collectionCreateBatch( rawBatch , options )
{
	var i ;
	
	if ( ! Array.isArray( rawBatch ) ) { rawBatch = [] ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	
	/*
	for ( i = 0 ; i < this.hooks.beforeCreateBatch.length ; i ++ )
	{
		this.hooks.beforeCreateBatch[ i ]( rawBatch ) ;
	}
	*/
	
	rootsDb.BatchWrapper( this , rawBatch , options ) ;
	
	/*
	for ( i = 0 ; i < this.hooks.afterCreateBatch.length ; i ++ )
	{
		this.hooks.afterCreateBatch[ i ]( rawBatch ) ;
	}
	*/
	
	return rawBatch ;
} ;



/*
	There are 2 modes:
		* with callback, this is the standard async mode, the upstream is checked and the result is provided to the callback
		* without, this return synchronously immediately, providing or a 'suspect' document
	
	options:
		raw: no mapping, just raw driver output
	
	callback( error , document|rawDocument )
*/
Collection.prototype.get = function collectionGet( id , options , callback )
{
	var self = this ;
	
	// Managing function's arguments
	if ( ! id ) { throw new Error( '[roots-db] collectionGet(): argument #0 should be an ID' ) ; }
	if ( arguments.length === 2 && typeof options === 'function' ) { callback = options ; options = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	if ( typeof callback !== 'function' ) { callback = null ; }
	
	// Sync mode: return a 'suspect' document
	//if ( ! callback ) { return rootsDb.DocumentWrapper( self , {} , { suspected: true, id: id } ) ; }
	
	// Async mode: get it from upstream
	this.driver.get( id , function( error , rawDocument ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		if ( ! rawDocument )
		{
			callback( ErrorStatus.notFound( { message: 'Document not found' } ) ) ;
			return ;
		}
		
		if ( ! options.raw )
		{
			rootsDb.DocumentWrapper( self , rawDocument , { fromUpstream: true , skipValidation: true } ) ;
		}
		
		self.postDocumentRetrieve( rawDocument , options , callback ) ;
		//callback( undefined , rawDocument ) ;
	} ) ;
} ;



// Get a document by a unique Fingerprint
Collection.prototype.getUnique = function collectionGetUnique( fingerprint , options , callback )
{
	var self = this ;
	
	// Managing function's arguments
	if ( ! fingerprint || typeof fingerprint !== 'object' ) { throw new Error( "[roots-db] fingerprint should be an object" ) ; }
	if ( arguments.length === 2 && typeof options === 'function' ) { callback = options ; options = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	if ( typeof callback !== 'function' ) { callback = null ; }
	
	// Check if we have a unique fingerprint
	if ( ! ( fingerprint.$ instanceof rootsDb.FingerprintWrapper ) ) { this.createFingerprint( fingerprint ) ; }
	
	if ( ! fingerprint.$.unique )
	{
		var error = ErrorStatus.badRequest( { message: 'This is not a unique fingerprint' } ) ;
		if ( callback ) { callback( error ) ; return ; }
		return error ;
	}
	
	// Sync mode: return a 'suspect' document
	//if ( ! callback ) { return rootsDb.DocumentWrapper( self , {} , { suspected: true, fingerprint: fingerprint } ) ; }
	
	this.driver.getUnique( fingerprint.$.fingerprint , function( error , rawDocument ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		if ( ! rawDocument )
		{
			callback( ErrorStatus.notFound( { message: 'Document not found' } ) ) ;
			return ;
		}
		
		if ( ! options.raw )
		{
			rootsDb.DocumentWrapper( self , rawDocument , { fromUpstream: true , skipValidation: true } ) ;
		}
		
		self.postDocumentRetrieve( rawDocument , options , callback ) ;
		//callback( undefined , rawDocument ) ;
	} ) ;
} ;



// Get a set of document
Collection.prototype.collect = function collectionCollect( fingerprint , options , callback )
{
	var self = this ;
	
	// Managing function's arguments
	if ( ! fingerprint || typeof fingerprint !== 'object' ) { throw new Error( "[roots-db] fingerprint should be an object" ) ; }
	if ( arguments.length === 2 && typeof options === 'function' ) { callback = options ; options = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	if ( typeof callback !== 'function' ) { callback = null ; }
	
	// Create fingerprint if needed
	//if ( ! ( fingerprint instanceof rootsDb.FingerprintWrapper ) ) { fingerprint = this.createFingerprint( fingerprint ) ; }
	
	// Sync mode: return a 'suspect' batch
	//if ( ! callback ) { return rootsDb.BatchWrapper( self , fingerprint , { suspected: true } ) ; }
	
	this.driver.collect( fingerprint , function( error , rawBatch ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		if ( ! rawBatch )
		{
			// should never happen?
			callback( ErrorStatus.notFound( { message: 'Batch not found' } ) ) ;
			return ;
		}
		
		if ( ! options.raw )
		{
			rootsDb.BatchWrapper( self , rawBatch , { fromUpstream: true , skipValidation: true } ) ;
		}
		
		self.postBatchRetrieve( rawBatch , options , callback ) ;
		//callback( undefined , rawBatch ) ;
	} ) ;
} ;



// Get a set of document
Collection.prototype.find = function collectionFind( queryObject , options , callback )
{
	var self = this ;
	
	// Managing function's arguments
	if ( ! queryObject || typeof queryObject !== 'object' ) { throw new Error( "[odm] queryObject should be an object" ) ; }
	if ( arguments.length === 2 && typeof options === 'function' ) { callback = options ; options = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	if ( typeof callback !== 'function' ) { throw new Error( "[odm] missing callback" ) ; }
	
	this.driver.find( queryObject , function( error , rawBatch ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		if ( ! rawBatch )
		{
			// should never happen?
			callback( ErrorStatus.notFound( { message: 'Batch not found' } ) ) ;
			return ;
		}
		
		if ( ! options.raw )
		{
			rootsDb.BatchWrapper( self , rawBatch , { fromUpstream: true , skipValidation: true } ) ;
		}
		
		self.postBatchRetrieve( rawBatch , options , callback ) ;
		//callback( undefined , rawBatch ) ;
	} ) ;
} ;



Collection.prototype.postDocumentRetrieve = function postDocumentRetrieve( document , options , callback )
{
	if ( ! options.populate ) { callback( undefined , document ) ; return ; }
	
	document.$.populate( options.populate , {} , function( error ) {
		// Ignore error here?
		callback( undefined , document ) ;
	} ) ;
} ;



Collection.prototype.postBatchRetrieve = function postBatchRetrieve( batch , options , callback )
{
	var self = this ;
	
	if ( ! options.populate ) { callback( undefined , batch ) ; return ; }
	
	console.error( "[roots-db] postBatchRetrieve(): populate are really-really-really not optimized ATM." ) ;
	
	async.foreach( batch , function( document , foreachCallback ) {
		self.postDocumentRetrieve( document , options , foreachCallback ) ;
	} )
	.exec( function( error ) {
		// Ignore error here?
		callback( undefined , batch ) ;
	} ) ;
} ;
















// Should re-implement those features later...




/*
Collection.prototype.addMetaLink = function collectionAddMetaLink( key , property , collectionName )
{
	var self = this ;
	// Be careful: this.world.collections does not contains this collection at this time: we are still in the constructor!
	
	var getter = function getter()
	{
		var witness , document = this[''] , linkCollection = self.world.collections[ collectionName ] ;
		
		if ( ! document.suspected || this[ property ] !== undefined )
		{
			if ( ! ( document.meta[ property ] instanceof rootsDb.DocumentWrapper ) || this[ property ] != document.meta[ property ].id )	// jshint ignore:line
			{
				//console.log( '### Link get ###' ) ;
				document.meta[ property ] = linkCollection.get( this[ property ] ) ;
			}
		}
		else
		{
			// Here we have no ID -- erf, idea :) -- about what we will get
			if (
				! ( document.meta[ property ] instanceof rootsDb.DocumentWrapper ) ||
				! ( witness = document.meta[ property ].witness ) ||
				witness.property !== property ||
				witness.document !== document ||
				witness.type !== 'link'
			)
			{
				//console.log( '### Link describe suspect ###' ) ;
				document.meta[ property ] = rootsDb.DocumentWrapper( linkCollection , null , {
					suspected: true ,
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
		if ( ! ( linkDocument instanceof rootsDb.DocumentWrapper ) || linkDocument.collection.name !== collectionName ) { return ; }
		
		var document = this[''] ;
		
		this[ property ] = linkDocument.id ;
		document.meta[ property ] = linkDocument ;
	} ;
	
	
	Object.defineProperty( this.suspectedBase , key , {
		configurable: true ,
		get: getter ,
		set: setter
	} ) ;
} ;



Collection.prototype.addMetaBacklink = function collectionAddMetaBacklink( key , property , collectionName )
{
	var self = this ;
	// Be careful: this.world.collections does not contains this collection at this time: we are still in the constructor!
	
	var getter = function getter()
	{
		var witness , fingerprint = {} , document = this[''] , backlinkCollection = self.world.collections[ collectionName ] ;
		
		if ( ! document.suspected || document.id )
		{
			fingerprint[ property ] = document.id ;
			fingerprint = self.createFingerprint( fingerprint ) ;
			
			if ( ! ( document.meta[ property ] instanceof rootsDb.BatchWrapper ) || document.meta[ property ].fingerprint.$ != fingerprint.$ )	// jshint ignore:line
			{
				//console.log( '### Backlink collect ###' , fingerprint ) ;
				document.meta[ property ] = backlinkCollection.collect( fingerprint ) ;
			}
		}
		else
		{
			// Here we have no ID -- erf, idea :) -- about what we will get
			if (
				! ( document.meta[ property ] instanceof rootsDb.BatchWrapper ) ||
				! ( witness = document.meta[ property ].witness ) ||
				witness.property !== property ||
				witness.document !== document ||
				witness.type !== 'backlink'
			)
			{
				//console.log( '### Backlink describe suspect ###' ) ;
				document.meta[ property ] = rootsDb.BatchWrapper( backlinkCollection , null , {
					suspected: true ,
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
		if ( ! ( linkDocument instanceof rootsDb.DocumentWrapper ) || linkDocument.collection.name !== collectionName ) { return ; }
		
		var document = this[''] ;
		
		this[ property ] = linkDocument.id ;
		document.meta[ property ] = linkDocument ;
		* /
	} ;
	
	Object.defineProperty( this.suspectedBase , key , {
		configurable: true ,
		get: getter ,
		set: setter
	} ) ;
} ;
*/


