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
var rootsDb = require( './rootsDb.js' ) ;
var DocumentWrapper = require( './DocumentWrapper.js' ) ;
var Document = require( './Document.js' ) ;
var BatchWrapper = require( './BatchWrapper.js' ) ;

var Promise = require( 'seventh' ) ;

var doormen = require( 'doormen' ) ;
var hash = require( 'hash-kit' ) ;
var tree = require( 'tree-kit' ) ;
var ErrorStatus = require( 'error-status' ) ;

var log = require( 'logfella' ).global.use( 'roots-db' ) ;

var url = require( 'url' ) ;
var fs = require( 'fs' ) ;
var fsKit = require( 'fs-kit' ) ;

function noop() {}



const IMMUTABLES = [] ;



function Collection( world , name , schema ) {
	// Now validation is done in World#createCollection()
	//doormen( Collection.schema , schema ) ;

	Object.defineProperties( this , {
		world: { value: world } ,
		name: { value: name , enumerable: true } ,
		driver: { value: undefined , writable: true }
	} ) ;
	
	this.immutables = null ;

	var i , key , element , indexName , autoSanitizer ;

	if ( typeof schema.url !== 'string' ) { throw new Error( '[roots-db] schema.url should be a string' ) ; }

	this.DocumentWrapper = schema.DocumentWrapper ;
	this.Document = schema.Document ;
	this.BatchWrapper = schema.BatchWrapper ;

	this.url = schema.url ;
	this.config = url.parse( this.url , true ) ;
	this.config.driver = this.config.protocol.split( ':' )[ 0 ] ;

	if ( ! schema.properties ) { schema.properties = {} ; }

	// Collection featuring locks
	this.canLock = schema.canLock ;
	this.lockTimeout = schema.lockTimeout ;

	if ( this.canLock ) {
		schema.properties._lockedBy = {
			type: 'objectId' ,
			system: true ,
			default: null ,
			tier: 4
		} ;

		schema.properties._lockedAt = {
			type: Date ,
			system: true ,
			sanitize: 'toDate' ,
			default: null ,
			tier: 4
		} ;

		// Do not add it if it already exists
		//schema.indexes.push( { properties: { _lockedBy: 1 } } ) ;
		//schema.indexes.push( { properties: { _lockedBy: 1 , _lockedAt: 1 } } ) ;
		Collection.ensureIndex( schema.indexes , { properties: { _lockedBy: 1 } } ) ;
		Collection.ensureIndex( schema.indexes , { properties: { _lockedBy: 1 , _lockedAt: 1 } } ) ;
	}

	// Create the validator schema
	//this.documentSchema = doormen.purifySchema( schema ) ;
	this.documentSchema = schema ;

	// Temp? or not?
	if ( ! this.documentSchema.properties._id ) {
		this.documentSchema.properties._id = {
			optional: true ,
			system: true ,
			type: 'objectId' ,
			tier: 1
		} ;
	}

	this.validate = doormen.bind( doormen , this.documentSchema ) ;
	this.validatePatch = doormen.patch.bind( doormen , this.documentSchema ) ;
	this.validateAndUpdatePatch = function( document , patch ) { doormen( { patch: patch } , this.documentSchema , document ) ; } ;

	this.skipValidation = !! schema.skipValidation ;
	this.patchDrivenValidation = schema.patchDrivenValidation === undefined ? true : schema.patchDrivenValidation ;

	// Attachment URL for files
	if ( typeof schema.attachmentUrl === 'string' ) {
		this.attachmentUrl = schema.attachmentUrl ;
		if ( this.attachmentUrl[ this.attachmentUrl.length - 1 ] !== '/' ) { this.attachmentUrl += '/' ; }

		// Should crash now if the path is not accessible, so the app will not run with bad perm
		// that can crash anytime behind your back.
		try {
			fsKit.ensurePathSync( this.attachmentUrl ) ;
			fs.accessSync( this.attachmentUrl , fs.R_OK | fs.W_OK | fs.X_OK ) ;
		}
		catch ( error ) {
			throw new Error(
				"RootsDb: cannot access the filesystem path '" + this.attachmentUrl +
				"' where the collection '" + name + "' want to read and write attachments, " + error
			) ;
		}
	}
	else {
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


	// Tier object masks (see RestQuery convention)...
	this.tierPropertyMasks = [] ;
	for ( i = 0 ; i <= Collection.MaxTier ; i ++ ) { this.tierPropertyMasks[ i ] = {} ; }


	if ( Array.isArray( schema.indexes ) ) {
		for ( key in schema.indexes ) {
			element = schema.indexes[ key ] ;

			if ( ! element || typeof element !== 'object' || ! element.properties || typeof element.properties !== 'object' ) {
				continue ;
			}

			if ( element.unique ) { this.uniques.push( Object.keys( element.properties ) ) ; }

			indexName = hash.fingerprint( element ) ;
			this.indexes[ indexName ] = tree.extend( null , { name: indexName } , element ) ;
		}
	}


	// Properties check
	for ( key in this.documentSchema.properties ) {
		element = this.documentSchema.properties[ key ] ;
		autoSanitizer = autoSanitizers[ element.type ] ;

		if ( ! element.tier ) {
			// Tier 3 = Content, in the RestQuery tier convention
			element.tier = 3 ;
		}

		// Auto sanitizer
		if ( autoSanitizer ) {
			if ( ! element.sanitize ) {
				element.sanitize = [ autoSanitizer ] ;
			}
			else if ( Array.isArray( element.sanitize ) ) {
				if ( element.sanitize.indexOf( autoSanitizer ) === -1 ) {
					element.sanitize.unshift( autoSanitizer ) ;
				}
			}
			else if ( element.sanitize !== autoSanitizer ) {
				element.sanitize = [ autoSanitizer , element.sanitize ] ;
			}
		}
	}

	this.computeTierMasks() ;

	// Init the driver
	this.initDriver() ;
	this.uniques.unshift( [ this.driver.idKey ] ) ;
} ;



module.exports = Collection ;



// Max tier value (see the RestQuery tier convention)
Collection.MaxTier = 5 ;



// WIP...

var autoSanitizers = {
	link: 'toLink' ,
	multiLink: 'toMultiLink' ,
	backLink: 'toBackLink'
} ;

var collectionHookSchema = {
	type: 'array' ,
	sanitize: 'toArray' ,
	of: { type: 'function' }
} ;

Collection.schema = {
	type: 'strictObject' ,
	extraProperties: true ,
	properties: {
		Collection: {
			type: 'function' ,
			default: Collection
		} ,
		DocumentWrapper: {
			type: 'function' ,
			default: DocumentWrapper
		} ,
		Document: {
			type: 'function' ,
			default: Document
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
			type: 'strictObject' ,
			default: {
				beforeCreateDocument: [] ,
				afterCreateDocument: []
			} ,
			extraProperties: true ,
			properties: {
				beforeCreateDocument: collectionHookSchema ,
				afterCreateDocument: collectionHookSchema
			}
		}
	}
} ;



// Backward compat
Collection.create = ( ... args ) => new Collection( ... args ) ;



Collection.ensureIndex = function ensureIndex( indexes , index ) {
	// /!\ This is only a rough test, this is no way close to a perfect test,
	// however it is sufficient to avoid repeated index insertion
	var i = 0 , iMax = indexes.length , stringified = JSON.stringify( index ) ;

	for ( ; i < iMax ; i ++ ) {
		if ( stringified === JSON.stringify( indexes[ i ] ) ) { return ; }
	}

	indexes.push( index ) ;
} ;



// Tier masks only works for top-level properties for instance
Collection.prototype.computeTierMasks = function collectionComputeTierMasks() {
	var i , key , element ;

	for ( key in this.documentSchema.properties ) {
		element = this.documentSchema.properties[ key ] ;

		for ( i = element.tier ; i <= Collection.MaxTier ; i ++ ) {
			this.tierPropertyMasks[ i ][ key ] = true ;
		}
	}
} ;



Collection.prototype.initDriver = function collectionInitDriver() {
	// already connected? nothing to do!
	if ( this.driver ) { return ; }

	if ( ! rootsDb.driver[ this.config.driver ] ) {
		try {
			// First try drivers shipped with rootsDb
			rootsDb.driver[ this.config.driver ] = require( './' + this.config.driver + '.driver.js' ) ;
		}
		catch ( error ) {
			// Then try drivers in node_modules
			try {
				rootsDb.driver[ this.config.driver ] = require( 'roots-db-' + this.config.driver ) ;
			}
			catch ( error_ ) {
				throw new Error( '[roots-db] Cannot load driver: ' + this.config.driver ) ;
			}
		}
	}

	this.driver = new rootsDb.driver[ this.config.driver ]( this ) ;
	
	this.immutables = new Set( [ ... IMMUTABLES , ... this.driver.immutables ] ) ;
} ;



Collection.prototype.createId = function collectionCreateId() {
	return this.driver.createId() ;
} ;



Collection.prototype.createFingerprint = function collectionCreateFingerprint( rawFingerprint , options ) {
	if ( ! rawFingerprint || typeof rawFingerprint !== 'object' ) { rawFingerprint = {} ; }

	var wrapper = rootsDb.FingerprintWrapper.create( this , rawFingerprint , options ) ;

	return wrapper.fingerprint ;
} ;



// Index/re-index a collection
Collection.prototype.buildIndexes = function collectionBuildIndexes() {
	// Firstly, get indexes
	return this.driver.getIndexes()
		.then( upstreamIndexes => {
			// Secondly, drop obsolete indexes & prepare missing indexes

			//console.log( 'Entering stage 2 -- upstreamIndexes' , upstreamIndexes ) ;

			var key , obsoleteIndexes = [] , missingIndexes = [] ;

			for ( key in upstreamIndexes ) {
				if ( ! this.indexes[ key ] ) { obsoleteIndexes.push( key ) ; }
			}

			for ( key in this.indexes ) {
				if ( ! upstreamIndexes[ key ] ) { missingIndexes.push( key ) ; }
			}

			if ( ! obsoleteIndexes.length ) { return missingIndexes ; }

			return Promise.all( obsoleteIndexes , indexName => this.driver.dropIndex( indexName ) ) ;
		} )
		// Finally, create missing indexes
		.then( missingIndexes => {
		//console.log( 'Entering stage 3' , missingIndexes ) ;

			if ( ! missingIndexes.length ) { return ; }

			return Promise.forEach( missingIndexes , indexName => this.driver.buildIndex( this.indexes[ indexName ] ) ) ;
		} ) ;
		//.callback( callback ) ;
} ;



/* Document-oriented method */



Collection.prototype.createDocument = function( rawDocument , options = {} ) {
	if ( ! rawDocument || typeof rawDocument !== 'object' ) { rawDocument = {} ; }

	this.hooks.beforeCreateDocument.forEach( hook => hook( rawDocument ) ) ;

	var doc = new this.Document( this , rawDocument , options ) ;

	this.hooks.afterCreateDocument.forEach( hook => hook( doc.proxy ) ) ;

	return doc.proxy ;
} ;



Collection.prototype.createBatch = function collectionCreateBatch( rawBatch , options ) {
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
*/
// Promise ready
Collection.prototype.get = function( id , options = {} ) {
	var document ;

	if ( ! id ) { throw new Error( '[roots-db] collectionGet(): argument #0 should be an ID' ) ; }

	// Manage the memory option
	if ( options.memory ) { options.cache = options.memory ; options.noReference = false ; }

	// Check the cache
	if ( options.cache && ( document = options.cache.get( this.name , id , options.noReference ) ) ) {
		log.debug( 'Collection.get(): Cache hit!' ) ;
		return this.postDocumentRetrieve( document , true , options ) ;
	}

	return this.driver.get( id ).then( rawDocument =>
		this.postDocumentRetrieve( rawDocument , false , options )
	) ;
} ;



// Get a document by a unique Fingerprint
// Promise ready
Collection.prototype.getUnique = function( fingerprint , options = {} ) {
	// Managing function's arguments
	if ( ! fingerprint || typeof fingerprint !== 'object' ) { throw new Error( "[roots-db] fingerprint should be an object" ) ; }

	// Check if we have a unique fingerprint
	if ( ! ( fingerprint.$ instanceof rootsDb.FingerprintWrapper ) ) { this.createFingerprint( fingerprint ) ; }

	if ( ! fingerprint.$.unique ) {
		throw ErrorStatus.badRequest( { message: 'This is not a unique fingerprint' } ) ;
	}


	// Manage the memory option
	if ( options.memory ) { options.cache = options.memory ; options.noReference = false ; }

	// /!\ Should be able to check the cache for a fingerprint too /!\


	return this.driver.getUnique( fingerprint.$.fingerprint ).then( rawDocument =>
		this.postDocumentRetrieve( rawDocument , false , options )
	) ;
} ;



// Promise ready
// /!\ Should be checked!!!
Collection.prototype.postDocumentRetrieve = function postDocumentRetrieve( document , fromCache , options ) {
	if ( ! document ) {
		return Promise.reject( ErrorStatus.notFound( { message: 'Document not found' } ) ) ;
	}

	if ( options.deepPopulate && options.deepPopulate[ this.name ] ) {
		options.populate = options.deepPopulate[ this.name ] ;
	}

	if ( ! options.cache && options.populate ) {
		// We MUST use the cache NOW!
		options.cache = this.world.createMemoryModel( { lazy: true } ) ;
	}

	if ( options.cache && ! fromCache ) {
		document = options.cache.add( this.name , document , options.noReference ) ;
	}

	if ( ! options.raw && ! document.$ ) {
		// /!\ Is it relevant to get the .proxy part?
		document = ( new this.Document( this , document , { fromUpstream: true , skipValidation: true } ) ).proxy ;
	}

	// Check if we are done here...
	if ( ! options.populate ) { return Promise.resolve( document ) ; }

	// Additionnal stuff, like population...
	return new Promise( ( resolve , reject ) => {
		document.$.populate( options.populate , options , ( error ) => {
			// Ignore error here?
			if ( error ) { console.error( error ) ; }
			resolve( document ) ;
		} ) ;
	} ) ;
} ;



/*
	* ids: an array of ids of the document to retrieve
	* options:
		* raw: no mapping, just raw driver output
	* callback: function( error , document|rawDocument )
*/
Collection.prototype.multiGet = function collectionMultiGet( ids , options , callback ) {
	var cachedBatch = null , notFoundArray ;

	// Managing function's arguments
	if ( ! Array.isArray( ids ) ) { throw new Error( '[roots-db] collectionGet(): argument #0 should be an array of ID' ) ; }
	if ( arguments.length === 2 && typeof options === 'function' ) { callback = options ; options = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	if ( typeof callback !== 'function' ) { callback = null ; }


	// If the array of IDs is empty, just return an empty batch now!
	if ( ! ids.length ) {
		cachedBatch = [] ;

		if ( ! options.raw ) {
			this.BatchWrapper.create( this , cachedBatch , { fromUpstream: true , skipValidation: true  } ) ;
		}

		callback( undefined , cachedBatch ) ;
		return ;
	}


	// Manage the memory option
	if ( options.memory ) { options.cache = options.memory ; options.noReference = false ; }

	// Check the cache
	if ( options.cache ) {
		notFoundArray = [] ;
		cachedBatch = options.cache.multiGet( this.name , ids , notFoundArray , options.noReference ) ;

		if ( ! notFoundArray.length ) {
			log.debug( 'Collection.multiGet(): Complete cache hit!' ) ;
			this.postBatchRetrieve( [] , cachedBatch , options ).callback( callback ) ;
			return ;
		}
		else if ( notFoundArray.length < ids.length ) {
			log.debug( 'Collection.multiGet(): Partial cache hit!' ) ;
			ids = notFoundArray ;
		}
	}

	this.driver.multiGet( ids ).then( rawBatch =>
		this.postBatchRetrieve( rawBatch , cachedBatch , options )
	)
		.callback( callback ) ;
} ;



// Get a set of document
Collection.prototype.collect = function collectionCollect( fingerprint , options , callback ) {
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

	this.driver.collect( fingerprint ).then( rawBatch =>
		this.postBatchRetrieve( rawBatch , null , options )
	)
		.callback( callback ) ;
} ;



// Get a set of document
Collection.prototype.find = function collectionFind( queryObject , options , callback ) {
	// Managing function's arguments
	if ( ! queryObject || typeof queryObject !== 'object' ) { throw new Error( "[roots-db] queryObject should be an object" ) ; }
	if ( arguments.length === 2 && typeof options === 'function' ) { callback = options ; options = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	if ( typeof callback !== 'function' ) { throw new Error( "[roots-db] missing callback" ) ; }


	// Manage the memory option
	if ( options.memory ) { options.cache = options.memory ; options.noReference = false ; }

	// /!\ Should be able to perform a find in the cache /!\

	this.driver.find( queryObject ).then( rawBatch =>
		this.postBatchRetrieve( rawBatch , null , options )
	)
		.callback( callback ) ;
} ;



// Get a set of document
Collection.prototype.lockRetrieveRelease = function collectionLockRetrieveRelease( queryObject , options , callback ) {
	var result ;

	if ( ! this.canLock ) { throw new Error( "[roots-db] Cannot lock document on this collection" ) ; }

	// Managing function's arguments
	if ( ! queryObject || typeof queryObject !== 'object' ) { throw new Error( "[roots-db] queryObject should be an object" ) ; }
	if ( arguments.length === 2 && typeof options === 'function' ) { callback = options ; options = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	if ( typeof callback !== 'function' ) { throw new Error( "[roots-db] missing callback" ) ; }

	// /!\ Should be able to perform a find in the cache /!\

	this.driver.lockRetrieveRelease( queryObject , options.lockTimeout || this.lockTimeout ).then( result_ => {

		result = result_ ;
		return this.postBatchRetrieve( result.batch , null , options ) ;
	} )
		.done(
			batch => { callback( undefined , batch , result.release ) ; } ,
			error => { callback( error ) ; }
		) ;
} ;



Collection.prototype.postBatchRetrieve = function postBatchRetrieve( batch , cachedBatch , options ) {
	var i , iMax , j ;

	if ( ! batch ) {
		// should never happen?
		return Promise.reject( ErrorStatus.notFound( { message: 'Batch not found' } ) ) ;
	}

	if ( options.deepPopulate && options.deepPopulate[ this.name ] ) {
		options.populate = options.deepPopulate[ this.name ] ;
	}

	if ( ! options.cache && options.populate ) {
		// We MUST use the cache NOW!
		options.cache = this.world.createMemoryModel( { lazy: true } ) ;
	}

	if ( options.cache && batch.length ) {
		for ( i = 0 , iMax = batch.length ; i < iMax ; i ++ ) {
			batch[ i ] = options.cache.add( this.name , batch[ i ] , options.noReference ) ;
		}
	}

	if ( cachedBatch && cachedBatch.length ) {
		// concat in-place
		for ( i = 0 , iMax = cachedBatch.length , j = batch.length ; i < iMax ; i ++ , j ++ ) {
			batch[ j ] = cachedBatch[ i ] ;
		}
	}

	if ( ! options.raw ) {
		this.BatchWrapper.create( this , batch , { fromUpstream: true , skipValidation: true  } ) ;
	}

	// Check if we are done here...
	if ( ! options.populate || ! batch.length ) { return Promise.resolve( batch ) ; }


	// Additionnal stuff, like population...

	iMax = batch.length ;

	for ( i = 0 ; i < iMax ; i ++ ) {
		//batch[ i ] = options.cache.add( this.name , batch[ i ] , options.noReference ) ;
		batch[ i ].$.preparePopulate( options.populate , options ) ;
	}

	return new Promise( ( resolve , reject ) => {
		this.world.populate( options , ( error ) => {
			// Ignore error here?
			if ( error ) { console.error( error ) ; }
			resolve( batch ) ;
		} ) ;
	} ) ;
} ;



Collection.prototype.postBatchRetrieve__ = function postBatchRetrieve( batch , cachedBatch , options , callback ) {
	var i , iMax , j ;

	if ( ! batch ) {
		// should never happen?
		callback( ErrorStatus.notFound( { message: 'Batch not found' } ) ) ;
		return ;
	}

	if ( options.deepPopulate && options.deepPopulate[ this.name ] ) {
		options.populate = options.deepPopulate[ this.name ] ;
	}

	if ( ! options.cache && options.populate ) {
		// We MUST use the cache NOW!
		options.cache = this.world.createMemoryModel( { lazy: true } ) ;
	}

	if ( options.cache && batch.length ) {
		for ( i = 0 , iMax = batch.length ; i < iMax ; i ++ ) {
			batch[ i ] = options.cache.add( this.name , batch[ i ] , options.noReference ) ;
		}
	}

	if ( cachedBatch && cachedBatch.length ) {
		// concat in-place
		for ( i = 0 , iMax = cachedBatch.length , j = batch.length ; i < iMax ; i ++ , j ++ ) {
			batch[ j ] = cachedBatch[ i ] ;
		}
	}

	if ( ! options.raw ) {
		this.BatchWrapper.create( this , batch , { fromUpstream: true , skipValidation: true  } ) ;
	}

	// Check if we are done here...
	if ( ! options.populate || ! batch.length ) { callback( undefined , batch ) ; return ; }


	// Additionnal stuff, like population...

	iMax = batch.length ;

	for ( i = 0 ; i < iMax ; i ++ ) {
		//batch[ i ] = options.cache.add( this.name , batch[ i ] , options.noReference ) ;
		batch[ i ].$.preparePopulate( options.populate , options ) ;
	}

	this.world.populate( options , ( error ) => {
		// Ignore error here?
		if ( error ) { console.error( error ) ; }
		callback( undefined , batch ) ;
	} ) ;
} ;

