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
var Document = require( './Document.js' ) ;
var Batch = require( './Batch.js' ) ;
var Population = require( './Population.js' ) ;

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



const IMMUTABLES = [ Date.prototype ] ;



function Collection( world , name , schema ) {
	// Validate the schema
	doormen( Collection.schema , schema ) ;

	Object.defineProperties( this , {
		world: { value: world } ,
		name: { value: name , enumerable: true } ,
		driver: { value: undefined , writable: true }
	} ) ;

	this.immutables = null ;

	var i , key , element , indexName , autoSanitizer ;

	if ( typeof schema.url !== 'string' ) { throw new Error( '[roots-db] schema.url should be a string' ) ; }

	this.Document = schema.Document ;
	this.Batch = schema.Batch ;

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


	// Properties check (default tier-level, auto-sanitizer, etc)
	this.checkSchemaProperties( this.documentSchema.properties ) ;

	this.computeTierMasks() ;

	// Init the driver
	this.initDriver() ;
	this.uniques.unshift( [ this.driver.idKey ] ) ;
}

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
		Document: {
			type: 'function' ,
			default: Document
		} ,
		Batch: {
			type: 'function' ,
			default: Batch
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



Collection.ensureIndex = function( indexes , index ) {
	// /!\ This is only a rough test, this is no way close to a perfect test,
	// however it is sufficient to avoid repeated index insertion
	var i = 0 , iMax = indexes.length , stringified = JSON.stringify( index ) ;

	for ( ; i < iMax ; i ++ ) {
		if ( stringified === JSON.stringify( indexes[ i ] ) ) { return ; }
	}

	indexes.push( index ) ;
} ;



// This recursively check if tier-level and auto-sanitizer are set in the schema
Collection.prototype.checkSchemaProperties = function( schemaProperties ) {
	var key , element , autoSanitizer ;

	for ( key in schemaProperties ) {
		element = schemaProperties[ key ] ;
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

		if ( element.properties && typeof element.properties === 'object' ) {
			this.checkSchemaProperties( element.properties ) ;
		}
	}
} ;



// Tier masks only works for top-level properties for instance
Collection.prototype.computeTierMasks = function() {
	var i , key , element ;

	for ( key in this.documentSchema.properties ) {
		element = this.documentSchema.properties[ key ] ;

		for ( i = element.tier ; i <= Collection.MaxTier ; i ++ ) {
			this.tierPropertyMasks[ i ][ key ] = true ;
		}
	}
} ;



Collection.prototype.initDriver = function() {
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



Collection.prototype.createId = function( from ) { return this.driver.createId( from ) ; } ;
Collection.prototype.checkId = function( rawDocument , enforce ) { return this.driver.checkId( rawDocument , enforce ) ; } ;

// Get the ID out of a raw document
Collection.prototype.getId = function( rawDocument ) { return rawDocument[ this.driver.idKey ] ; } ;

// Set the ID on a raw document
Collection.prototype.setId = function( rawDocument , id ) {
	if ( rawDocument._ && ( rawDocument._ instanceof Document ) ) {
		throw new TypeError( 'It accepts raw document only, not Document intances' ) ;
	}

	id = this.driver.createId( id ) ;
	rawDocument[ this.driver.idKey ] = id ;
	return id ;
} ;



Collection.prototype.createFingerprint = function( rawFingerprint = {} , isPartial = false ) {
	return new rootsDb.Fingerprint( this , rawFingerprint , isPartial ) ;
} ;



// Index/re-index a collection
Collection.prototype.buildIndexes = function() {
	// Firstly, get indexes
	return this.driver.getIndexes()
		.then( upstreamIndexes => {
			// Secondly, drop obsolete indexes & prepare missing indexes

			//console.log( 'Entering stage 2 -- upstreamIndexes for' , this.name , ':' , upstreamIndexes ) ;

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



Collection.prototype.createDocument = function( rawDocument , options ) {
	return ( new this.Document( this , rawDocument , options ) ).proxy ;
} ;



Collection.prototype.createBatch = function( rawBatch , options ) {
	return new this.Batch( this , rawBatch , options ) ;
} ;



/*
	* id: the id of the document to retrieve
	* options:
		* raw: no mapping, just raw driver output
*/
// Promise ready
Collection.prototype.get = function( id , options = {} ) {
	var rawDocument ;

	if ( ! id ) { throw new Error( '[roots-db] collectionGet(): argument #0 should be an ID' ) ; }

	// Check the cache
	if ( options.cache && ( rawDocument = options.cache.getRaw( this.name , id , options.noReference ) ) ) {
		log.debug( "Collection.get(): Cache hit (raw)!" ) ;
		return this.postDocumentRetrieve( rawDocument , options.cache , options ) ;
	}

	return this.driver.get( id ).then( rawDocument_ =>
		this.postDocumentRetrieve( rawDocument_ , null , options )
	) ;
} ;



// Get a document by a unique Fingerprint
// Promise ready
Collection.prototype.getUnique = function( fingerprint , options = {} ) {
	if ( ! fingerprint || typeof fingerprint !== 'object' ) { throw new Error( "[roots-db] fingerprint should be an object" ) ; }

	// Check if we have a unique fingerprint
	if ( ! ( fingerprint instanceof rootsDb.Fingerprint ) ) { fingerprint = this.createFingerprint( fingerprint ) ; }

	if ( ! fingerprint.unique ) {
		throw ErrorStatus.badRequest( { message: 'This is not a unique fingerprint' } ) ;
	}

	// /!\ Should be able to check the cache for a fingerprint too /!\

	return this.driver.getUnique( fingerprint.def ).then( rawDocument =>
		this.postDocumentRetrieve( rawDocument , false , options )
	) ;
} ;



/*
	* ids: an array of ids of the document to retrieve
	* options:
		* raw: no mapping, just raw driver output
*/
// Promise ready
Collection.prototype.multiGet = function( ids , options = {} ) {
	var cachedBatch = null , notFoundArray ;

	if ( ! Array.isArray( ids ) ) { throw new Error( '[roots-db] collectionGet(): argument #0 should be an array of ID' ) ; }

	// If the array of IDs is empty, just return an empty batch now!
	if ( ! ids.length ) {
		cachedBatch = [] ;
		if ( options.raw ) { return Promise.resolve( [] ) ; }
		return Promise.resolve( new this.Batch( this , [] , { fromUpstream: true , skipValidation: true  } ) ) ;
	}

	// Check the cache
	if ( options.cache ) {
		notFoundArray = [] ;
		cachedBatch = options.cache.multiGetRaw( this.name , ids , notFoundArray , options.noReference ) ;

		if ( ! notFoundArray.length ) {
			log.debug( 'Collection.multiGet(): Complete cache hit!' ) ;
			return this.postBatchRetrieve( [] , cachedBatch , options.cache , options ) ;
		}

		if ( notFoundArray.length < ids.length ) {
			log.debug( 'Collection.multiGet(): Partial cache hit!' ) ;
			ids = notFoundArray ;

			return this.driver.multiGet( ids ).then( rawBatch =>
				this.postBatchRetrieve( rawBatch , cachedBatch , options.cache , options )
			) ;
		}
	}

	return this.driver.multiGet( ids ).then( rawBatch =>
		this.postBatchRetrieve( rawBatch , null , null , options )
	) ;
} ;



// Get a set of document
// Promise ready
Collection.prototype.collect = function( fingerprint , options = {} ) {
	if ( ! fingerprint || typeof fingerprint !== 'object' ) { throw new Error( "[roots-db] fingerprint should be an object" ) ; }

	// Create fingerprint if needed
	//if ( ! ( fingerprint instanceof rootsDb.Fingerprint ) ) { fingerprint = this.createFingerprint( fingerprint ) ; }

	// /!\ Should be able to check the cache for a fingerprint too /!\
	return this.driver.collect( fingerprint , options ).then( rawBatch =>
		this.postBatchRetrieve( rawBatch , null , null , options )
	) ;
} ;



// Get a set of document
// Promise ready
Collection.prototype.find = function( queryObject , options = {} ) {
	if ( ! queryObject || typeof queryObject !== 'object' ) { throw new Error( "[roots-db] queryObject should be an object" ) ; }

	// /!\ Should be able to perform a find in the cache /!\
	return this.driver.find( queryObject , options ).then( rawBatch =>
		this.postBatchRetrieve( rawBatch , null , null , options )
	) ;
} ;



// Get a set of document
Collection.prototype.lockedPartialFind = function( queryObject , options , actionFn ) {
	if ( ! this.canLock ) { throw new Error( "[roots-db] Cannot lock document on this collection" ) ; }

	// Managing function's arguments
	if ( ! queryObject || typeof queryObject !== 'object' ) { throw new Error( "[roots-db] queryObject should be an object" ) ; }
	if ( arguments.length === 2 && typeof options === 'function' ) { actionFn = options ; options = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	if ( typeof actionFn !== 'function' ) { throw new Error( "[roots-db] missing actionFn" ) ; }

	// /!\ Should be able to perform a find in the cache /!\

	return this.driver.lockedPartialFind( queryObject , options.lockTimeout || this.lockTimeout , rawBatch =>
		this.postBatchRetrieve( rawBatch , null , null , options ).then( batch => actionFn( batch ) )
	) ;
} ;



// Promise ready
Collection.prototype.postDocumentRetrieve = async function( rawDocument , fromCache , options ) {
	var documentProxy ;

	if ( ! rawDocument ) {
		throw ErrorStatus.notFound( { message: 'Document not found' } ) ;
	}

	var populatePaths = options.populate || ( options.deepPopulate && options.deepPopulate[ this.name ] ) ;

	if ( options.cache && ! fromCache ) {
		rawDocument = options.cache.addRaw( this.name , rawDocument , options.noReference ) ;
	}

	if ( ! options.raw ) {
		if ( fromCache ) {
			documentProxy = fromCache.getProxyFromRaw( this.name , rawDocument ) ;
		}
		else {
			documentProxy = ( new this.Document( this , rawDocument , { fromUpstream: true , skipValidation: true } ) ).proxy ;
		}
	}

	// Check if we are done here...
	if ( ! populatePaths ) {
		return options.raw ? rawDocument : documentProxy ;
	}

	// Populate part

	// /!\ TMP /!\
	if ( options.raw ) {
		throw new Error( "Option 'raw' + 'populate' is not supported yet" ) ;
	}

	var population = new Population( this.world , options ) ;

	// Population...
	await documentProxy._.populate( populatePaths , options , population ) ;

	return documentProxy ;
} ;



Collection.prototype.postBatchRetrieve = async function( rawBatch , cachedBatch , fromCache , options ) {
	var batch ;

	// should never happen?
	//if ( ! rawBatch ) { return Promise.reject( ErrorStatus.notFound( { message: 'Batch not found' } ) ) ; }

	var populatePaths = options.populate || ( options.deepPopulate && options.deepPopulate[ this.name ] ) ;

	if ( options.cache && rawBatch.length ) {
		rawBatch.forEach( ( rawDocument , index ) => {
			rawBatch[ index ] = options.cache.addRaw( this.name , rawDocument , options.noReference ) ;

			if ( ! options.raw && ! options.noReference ) {
				// Replace by Document
				rawBatch[ index ] = options.cache.getProxyFromRaw( this.name , rawDocument ) ;
			}
		} ) ;
	}

	// Concat both array
	if ( cachedBatch && cachedBatch.length ) {
		if ( fromCache ) {
			// Replace by Document
			cachedBatch.forEach( ( rawDocument , index ) => {
				cachedBatch[ index ] = options.cache.getProxyFromRaw( this.name , rawDocument ) ;
			} ) ;
		}

		rawBatch.push( ... cachedBatch ) ;
	}

	if ( ! options.raw ) {
		batch = new this.Batch( this , rawBatch , { fromUpstream: true , skipValidation: true } ) ;
	}

	// Check if we are done here...
	if ( ! populatePaths || ! rawBatch.length ) {
		return options.raw ? rawBatch : batch ;
	}

	// Populate part

	// /!\ TMP /!\
	if ( options.raw ) {
		throw new Error( "Option 'raw' + 'populate' is not supported yet" ) ;
	}

	var population = new Population( this.world , options ) ;

	// Here we do not call .populate() for each document, instead we only prepare population,
	// so all population of the same kind are done at once by a single call to .world.populate().
	batch.forEach( documentProxy => documentProxy._.preparePopulate( populatePaths , population , options ) ) ;
	await this.world.populate( population , options ) ;

	return batch ;
} ;

