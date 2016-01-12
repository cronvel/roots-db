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
var DocumentWrapper = require( './DocumentWrapper.js' ) ;
var BatchWrapper = require( './BatchWrapper.js' ) ;

var async = require( 'async-kit' ) ;
var doormen = require( 'doormen' ) ;
var hash = require( 'hash-kit' ) ;
var tree = require( 'tree-kit' ) ;
var ErrorStatus = require( 'error-status' ) ;

var log = require( 'logfella' ).global.use( 'roots-db' ) ;

var url = require( 'url' ) ;

function noop() {}



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
		DocumentWrapper: {
			type: 'function' ,
			default: DocumentWrapper
		} ,
		BatchWrapper: {
			type: 'function' ,
			default: BatchWrapper
		} ,
		canLock: {
			type: 'boolean' ,
			default: false
		} ,
		lockTimeout: {
			type: 'number' ,
			default: 1000	// 1 whole second by default
		} ,
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



var autoSanitizers = {
	link: 'toLink' ,
	multiLink: 'toMultiLink' ,
	backLink: 'toBackLink'
} ;



function Collection() { throw new Error( "Use Collection.create() instead" ) ; }
module.exports = Collection ;



Collection.create = function collectionCreate( world , name , schema )
{
	var collection = Object.create( Collection.prototype ) ;
	collection.create( world , name , schema ) ;
	return collection ;
} ;



Collection.prototype.create = function collectionCreate( world , name , schema )
{
	doormen( collectionSchema , schema ) ;
	
	Object.defineProperties( this , {
		world: { value: world } ,
		name: { value: name , enumerable: true } ,
		driver: { value: undefined , writable: true }
	} ) ;
	
	var key , element , indexName , autoSanitizer ;
	
	if ( typeof schema.url !== 'string' ) { throw new Error( '[roots-db] schema.url should be a string' ) ; }
	
	this.DocumentWrapper = schema.DocumentWrapper ;
	this.BatchWrapper = schema.BatchWrapper ;
	
	this.url = schema.url ;
	this.config = url.parse( this.url , true ) ;
	this.config.driver = this.config.protocol.split( ':' )[ 0 ] ;
	
	if ( ! schema.properties ) { schema.properties = {} ; }
	
	// Collection featuring locks
	this.canLock = schema.canLock ;
	this.lockTimeout = schema.lockTimeout ;
	
	if ( this.canLock )
	{
		schema.properties._lockedBy = {
			type: 'objectId' ,
			default: null ,
			//tier: 4
		} ;
		
		schema.properties._lockedAt = {
			type: Date ,
			sanitize: 'toDate' ,
			default: null ,
			//tier: 4
		} ;
		
		schema.indexes.push( { properties: { _lockedBy: 1 } } ) ;
		schema.indexes.push( { properties: { _lockedBy: 1 , _lockedAt: 1 } } ) ;
	}
	
	// Create the validator schema
	//this.documentSchema = doormen.purifySchema( schema ) ;
	this.documentSchema = schema ;
	
	// Temp? or not?
	if ( ! this.documentSchema.properties._id )
	{
		this.documentSchema.properties._id = { optional: true , type: 'objectId' } ;
	}
	
	this.validate = doormen.bind( doormen , this.documentSchema ) ;
	this.skipValidation = !! schema.skipValidation ;
	this.patchDrivenValidation = schema.patchDrivenValidation === undefined ? true : schema.patchDrivenValidation ;
	
	// Attachment URL for files
	if ( typeof schema.attachmentUrl === 'string' )
	{
		this.attachmentUrl = schema.attachmentUrl ;
		if ( this.attachmentUrl[ this.attachmentUrl.length - 1 ] !== '/' ) { this.attachmentUrl += '/' ; }
	}
	else
	{
		this.attachmentUrl = null ;
	}
	
	
	// TODO: Check schema
	
	this.meta = schema.meta || {} ;
	this.suspectedBase = {} ;
	
	// Already checked
	this.hooks = schema.hooks ;
	
	
	// Indexes
	this.indexes = {} ;
	this.uniques = [] ;
	
	if ( Array.isArray( schema.indexes ) )
	{
		for ( key in schema.indexes )
		{
			element = schema.indexes[ key ] ;
			
			if ( ! element || typeof element !== 'object' || ! element.properties || typeof element.properties !== 'object' )
			{
				continue ;
			}
			
			if ( element.unique ) { this.uniques.push( Object.keys( element.properties ) ) ; }
			
			indexName = hash.fingerprint( element ) ;
			this.indexes[ indexName ] = tree.extend( null , { name: indexName } , element ) ;
		}
	}
	
	
	// Properties check
	for ( key in this.documentSchema.properties )
	{
		element = this.documentSchema.properties[ key ] ;
		autoSanitizer = autoSanitizers[ element.type ] ;
		
		// Auto sanitizer
		if ( autoSanitizer )
		{
			if ( ! element.sanitize )
			{
				element.sanitize = [ autoSanitizer ] ;
			}
			else if ( Array.isArray( element.sanitize ) )
			{
				if ( element.sanitize.indexOf( autoSanitizer ) === -1 )
				{
					element.sanitize.unshift( autoSanitizer ) ;
				}
			}
			else if ( element.sanitize !== autoSanitizer )
			{
				element.sanitize = [ autoSanitizer , element.sanitize ] ;
			}
		}
	}
	
	
	// Init the driver
	this.initDriver() ;
	this.uniques.unshift( [ this.driver.idKey ] ) ;
} ;





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
	
	this.driver = rootsDb.driver[ this.config.driver ].create( this ) ;
} ;



Collection.prototype.createId = function collectionCreateId()
{
	return this.driver.createId() ;
} ;



Collection.prototype.createFingerprint = function collectionCreateFingerprint( rawFingerprint , options )
{
	if ( ! rawFingerprint || typeof rawFingerprint !== 'object' ) { rawFingerprint = {} ; }
	
	var wrapper = rootsDb.FingerprintWrapper.create( this , rawFingerprint , options ) ;
	
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
	
	this.DocumentWrapper.create( this , rawDocument , options ) ;
	
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
	
	this.BatchWrapper.create( this , rawBatch , options ) ;
	
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
	
	// Manage the memory option
	if ( options.memory ) { options.cache = options.memory ; options.noReference = false ; }
	
	// Check the cache
	if ( options.cache && ( document = options.cache.get( this.name , id , options.noReference ) ) )
	{
		log.debug( 'Collection.get(): Cache hit!' ) ;
		self.postDocumentRetrieve( document , true , options , callback ) ;
		return ;
	}
	
	
	this.driver.get( id , function( error , rawDocument ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		self.postDocumentRetrieve( rawDocument , false , options , callback ) ;
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
	
	
	// Manage the memory option
	if ( options.memory ) { options.cache = options.memory ; options.noReference = false ; }
	
	// /!\ Should be able to check the cache for a fingerprint too /!\
	
	
	this.driver.getUnique( fingerprint.$.fingerprint , function( error , rawDocument ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		self.postDocumentRetrieve( rawDocument , false , options , callback ) ;
		//callback( undefined , rawDocument ) ;
	} ) ;
} ;



Collection.prototype.postDocumentRetrieve = function postDocumentRetrieve( document , fromCache , options , callback )
{
	if ( ! document )
	{
		callback( ErrorStatus.notFound( { message: 'Document not found' } ) ) ;
		return ;
	}
	
	if ( options.deepPopulate && options.deepPopulate[ this.name ] )
	{
		this.populate = options.deepPopulate[ this.name ] ;
	}
	
	if ( ! options.cache && options.populate )
	{
		// We MUST use the cache NOW!
		options.cache = this.world.createMemoryModel( { lazy: true } ) ;
	}
	
	if ( options.cache && ! fromCache )
	{
		document = options.cache.add( this.name , document , options.noReference ) ;
	}
	
	if ( ! options.raw && ! document.$ )
	{
		this.DocumentWrapper.create( this , document , { fromUpstream: true , skipValidation: true } ) ;
	}
	
	// Check if we are done here...
	if ( ! options.populate ) { callback( undefined , document ) ; return ; }
	
	
	// Additionnal stuff, like population...
	
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
	var self = this , cachedBatch = null , notFoundArray ;
	
	// Managing function's arguments
	if ( ! Array.isArray( ids ) ) { throw new Error( '[roots-db] collectionGet(): argument #0 should be an array of ID' ) ; }
	if ( arguments.length === 2 && typeof options === 'function' ) { callback = options ; options = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	if ( typeof callback !== 'function' ) { callback = null ; }
	
	
	// Manage the memory option
	if ( options.memory ) { options.cache = options.memory ; options.noReference = false ; }
	
	// Check the cache
	if ( options.cache )
	{
		notFoundArray = [] ;
		cachedBatch = options.cache.multiGet( this.name , ids , notFoundArray , options.noReference ) ;
		
		if ( ! notFoundArray.length )
		{
			log.debug( 'Collection.multiGet(): Complete cache hit!' ) ;
			self.postBatchRetrieve( [] , cachedBatch , options , callback ) ;
			return ;
		}
		else if ( notFoundArray.length < ids.length )
		{
			log.debug( 'Collection.multiGet(): Partial cache hit!' ) ;
			ids = notFoundArray ;
		}
	}
	
	this.driver.multiGet( ids , function( error , rawBatch ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		self.postBatchRetrieve( rawBatch , cachedBatch , options , callback ) ;
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
	
	
	// Manage the memory option
	if ( options.memory ) { options.cache = options.memory ; options.noReference = false ; }
	
	// /!\ Should be able to check the cache for a fingerprint too /!\
	
	this.driver.collect( fingerprint , function( error , rawBatch ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		self.postBatchRetrieve( rawBatch , null , options , callback ) ;
	} ) ;
} ;



// Get a set of document
Collection.prototype.find = function collectionFind( queryObject , options , callback )
{
	var self = this ;
	
	// Managing function's arguments
	if ( ! queryObject || typeof queryObject !== 'object' ) { throw new Error( "[roots-db] queryObject should be an object" ) ; }
	if ( arguments.length === 2 && typeof options === 'function' ) { callback = options ; options = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	if ( typeof callback !== 'function' ) { throw new Error( "[roots-db] missing callback" ) ; }
	
	
	// Manage the memory option
	if ( options.memory ) { options.cache = options.memory ; options.noReference = false ; }
	
	// /!\ Should be able to perform a find in the cache /!\
	
	this.driver.find( queryObject , function( error , rawBatch ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		self.postBatchRetrieve( rawBatch , null , options , callback ) ;
		//callback( undefined , rawBatch ) ;
	} ) ;
} ;



// Get a set of document
Collection.prototype.lockRetrieveRelease = function collectionLockRetrieveRelease( queryObject , options , callback )
{
	var self = this ;
	
	if ( ! this.canLock ) { throw new Error( "[roots-db] Cannot lock document on this collection" ) ; }
	
	// Managing function's arguments
	if ( ! queryObject || typeof queryObject !== 'object' ) { throw new Error( "[roots-db] queryObject should be an object" ) ; }
	if ( arguments.length === 2 && typeof options === 'function' ) { callback = options ; options = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	if ( typeof callback !== 'function' ) { throw new Error( "[roots-db] missing callback" ) ; }
	
	// /!\ Should be able to perform a find in the cache /!\
	
	this.driver.lockRetrieveRelease( queryObject , options.lockTimeout || this.lockTimeout , function( error , rawBatch , releaseFn ) {
		
		if ( error ) { callback( error , undefined , releaseFn ) ; return ; }
		
		self.postBatchRetrieve( rawBatch , null , options , function( error , batch ) {
			callback( error , batch , releaseFn ) ;
		} ) ;
	} ) ;
} ;



Collection.prototype.postBatchRetrieve = function postBatchRetrieve( batch , cachedBatch , options , callback )
{
	var self = this , i , iMax , j ;
	
	if ( ! batch )
	{
		// should never happen?
		callback( ErrorStatus.notFound( { message: 'Batch not found' } ) ) ;
		return ;
	}
	
	if ( options.deepPopulate && options.deepPopulate[ this.name ] )
	{
		this.populate = options.deepPopulate[ this.name ] ;
	}
	
	if ( ! options.cache && options.populate )
	{
		// We MUST use the cache NOW!
		options.cache = this.world.createMemoryModel( { lazy: true } ) ;
	}
	
	if ( options.cache && batch.length )
	{
		for ( i = 0 , iMax = batch.length ; i < iMax ; i ++ )
		{
			batch[ i ] = options.cache.add( this.name , batch[ i ] , options.noReference ) ;
		}
	}
	
	if ( cachedBatch && cachedBatch.length )
	{
		// concat in-place
		for ( i = 0 , iMax = cachedBatch.length , j = batch.length ; i < iMax ; i ++ , j ++ )
		{
			batch[ j ] = cachedBatch[ i ] ;
		}
	}
	
	if ( ! options.raw )
	{
		this.BatchWrapper.create( self , batch , { fromUpstream: true , skipValidation: true  } ) ;
	}
	
	// Check if we are done here...
	if ( ! options.populate || ! batch.length ) { callback( undefined , batch ) ; return ; }
	
	
	// Additionnal stuff, like population...
	
	iMax = batch.length ;
	
	for ( i = 0 ; i < iMax ; i ++ )
	{
		//batch[ i ] = options.cache.add( this.name , batch[ i ] , options.noReference ) ;
		batch[ i ].$.preparePopulate( options.populate , options ) ;
	}
	
	self.world.populate( options , function( error ) {
		// Ignore error here?
		callback( undefined , batch ) ;
	} ) ;
} ;

