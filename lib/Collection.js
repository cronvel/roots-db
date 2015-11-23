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

var log = require( 'logfella' ).global.use( 'roots-db' ) ;

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
	collection.skipValidation = !! schema.skipValidation ;
	collection.patchDrivenValidation = schema.patchDrivenValidation === undefined ? true : schema.patchDrivenValidation ;
	
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
				log.info( "Collection '%s': DROP obsolete index '%s'" , self.name , indexName ) ;
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
				log.info( "Collection '%s': CREATE index '%s' -> %J" , self.name , indexName , self.indexes[ indexName ] ) ;
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
	//var i ;
	
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
	* id: the id of the document to retrieve
	* options:
		* raw: no mapping, just raw driver output
	* callback: function( error , document|rawDocument )
*/
Collection.prototype.get = function collectionGet( id , options , callback )
{
	var self = this , document ;
	
	// Managing function's arguments
	if ( ! id ) { throw new Error( '[roots-db] collectionGet(): argument #0 should be an ID' ) ; }
	if ( arguments.length === 2 && typeof options === 'function' ) { callback = options ; options = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	if ( typeof callback !== 'function' ) { callback = null ; }
	
	
	// Check the cache
	if ( options.cache && ( document = options.cache.get( this.name , id , options.noReference ) ) )
	{
		log.debug( 'Collection.get(): Cache hit!' ) ;
		self.postDocumentRetrieve( document , options , callback ) ;
		return ;
	}
	
	
	this.driver.get( id , function( error , rawDocument ) {
		
		if ( error ) { callback( error ) ; return ; }
		
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
	
	// /!\ Should be able to check the cache for a fingerprint too /!\
	
	
	this.driver.getUnique( fingerprint.$.fingerprint , function( error , rawDocument ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		self.postDocumentRetrieve( rawDocument , options , callback ) ;
		//callback( undefined , rawDocument ) ;
	} ) ;
} ;



Collection.prototype.postDocumentRetrieve = function postDocumentRetrieve( document , options , callback )
{
	if ( ! document )
	{
		callback( ErrorStatus.notFound( { message: 'Document not found' } ) ) ;
		return ;
	}
	
	if ( ! options.raw && ! document.$ )
	{
		rootsDb.DocumentWrapper( this , document , { fromUpstream: true , skipValidation: true } ) ;
	}
	
	if ( options.cache )
	{
		options.cache.add( this.name , document , options.noReference ) ;
	}
	
	// Check if we are done here...
	if ( ! options.populate ) { callback( undefined , document ) ; return ; }
	
	
	// Additionnal stuff, like population...
	
	if ( ! options.cache )
	{
		// We MUST use the cache NOW!
		options.cache = rootsDb.MemoryModel( this.world ) ;
		options.cache.add( this.name , document , options.noReference ) ;
	}
	
	document.$.populate( options.populate , {} , function( error ) {
		// Ignore error here?
		callback( undefined , document ) ;
	} ) ;
} ;



/*
	* ids: an array of ids of the document to retrieve
	* options:
		* raw: no mapping, just raw driver output
	* callback: function( error , document|rawDocument )
*/
Collection.prototype.multiGet = function collectionMultiGet( ids , options , callback )
{
	var self = this , cachedBatch , notFoundArray ;
	
	// Managing function's arguments
	if ( ! Array.isArray( ids ) ) { throw new Error( '[roots-db] collectionGet(): argument #0 should be an array of ID' ) ; }
	if ( arguments.length === 2 && typeof options === 'function' ) { callback = options ; options = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	if ( typeof callback !== 'function' ) { callback = null ; }
	
	
	// Check the cache
	if ( options.cache )
	{
		notFoundArray = [] ;
		cachedBatch = options.cache.multiGet( this.name , ids , notFoundArray , options.noReference ) ;
		
		if ( ! notFoundArray.length )
		{
			log.debug( 'Collection.multiGet(): Complete cache hit!' ) ;
			self.postBatchRetrieve( cachedBatch , options , callback ) ;
			return ;
		}
		
		log.debug( 'Collection.multiGet(): Partial cache hit!' ) ;
		ids = notFoundArray ;
	}
	
	this.driver.multiGet( ids , function( error , rawBatch ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		if ( cachedBatch ) { rawBatch = rawBatch.concat( cachedBatch ) ; }
		
		self.postBatchRetrieve( rawBatch , options , callback ) ;
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
	
	// /!\ Should be able to check the cache for a fingerprint too /!\
	
	this.driver.collect( fingerprint , function( error , rawBatch ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		self.postBatchRetrieve( rawBatch , options , callback ) ;
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
	
	// /!\ Should be able to perform a find in the cache /!\
	
	this.driver.find( queryObject , function( error , rawBatch ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		self.postBatchRetrieve( rawBatch , options , callback ) ;
		//callback( undefined , rawBatch ) ;
	} ) ;
} ;



Collection.prototype.postBatchRetrieve = function postBatchRetrieve( batch , options , callback )
{
	var self = this , i , iMax ;
	
	if ( ! batch )
	{
		// should never happen?
		callback( ErrorStatus.notFound( { message: 'Batch not found' } ) ) ;
		return ;
	}
	
	if ( ! options.raw )
	{
		rootsDb.BatchWrapper( self , batch , { fromUpstream: true , skipValidation: true } ) ;
	}
	
	// Check if we are done here...
	if ( ! options.populate || ! batch.length ) { callback( undefined , batch ) ; return ; }
	
	
	// Additionnal stuff, like population...
	
	if ( ! options.cache )
	{
		// We MUST use the cache NOW!
		options.cache = rootsDb.MemoryModel( this.world ) ;
	}
	
	iMax = batch.length ;
	
	for ( i = 0 ; i < iMax ; i ++ )
	{
		options.cache.add( this.name , batch[ i ] , options.noReference ) ;
		batch[ i ].$.preparePopulate( options.populate , options ) ;
	}
	
	self.world.populate( options , function( error ) {
		// Ignore error here?
		callback( undefined , batch ) ;
	} ) ;
} ;

