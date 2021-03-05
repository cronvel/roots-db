/*
	Roots DB

	Copyright (c) 2014 - 2020 Cédric Ronvel

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



const rootsDb = require( './rootsDb.js' ) ;
const Document = require( './Document.js' ) ;
const Batch = require( './Batch.js' ) ;
const Population = require( './Population.js' ) ;

const Promise = require( 'seventh' ) ;

const doormen = require( 'doormen' ) ;
const hash = require( 'hash-kit' ) ;
const tree = require( 'tree-kit' ) ;
const ErrorStatus = require( 'error-status' ) ;

const log = require( 'logfella' ).global.use( 'roots-db' ) ;

const url = require( 'url' ) ;
const fs = require( 'fs' ) ;
const fsKit = require( 'fs-kit' ) ;

function noop() {}



const IMMUTABLES = [ Date.prototype ] ;



function Collection( world , name , schema ) {
	// Validate the schema
	doormen( Collection.schemaSchema , schema ) ;

	Object.defineProperties( this , {
		world: { value: world } ,
		name: { value: name , enumerable: true } ,
		driver: { value: undefined , writable: true } ,
		attachmentDriver: { value: undefined , writable: true }
	} ) ;

	//this.isInit = false ;
	this.immutables = null ;

	// Create the validator schema
	//this.documentSchema = doormen.purifySchema( schema ) ;
	this.documentSchema = schema ;

	var i , key , innerKey ;

	if ( typeof schema.url !== 'string' ) { throw new Error( '[roots-db] schema.url should be a string' ) ; }

	this.Document = schema.Document ;
	this.Batch = schema.Batch ;

	this.url = schema.url ;
	this.config = url.parse( this.url , true ) ;
	this.config.driver = this.config.protocol.split( ':' )[ 0 ] ;
	this.driver = null ;

	// Attachment URL for files
	this.attachmentUrl = schema.attachmentUrl || null ;
	this.attachmentConfig = null ;
	this.attachmentDriver = null ;

	if ( typeof this.attachmentUrl === 'string' ) {
		this.attachmentConfig = url.parse( this.attachmentUrl , true ) ;
		this.attachmentConfig.driver = this.attachmentConfig.protocol ?
			this.attachmentConfig.protocol.split( ':' )[ 0 ] :
			'file' ;
	}

	if ( ! schema.properties ) { schema.properties = {} ; }

	// Collection featuring locks
	this.canLock = schema.canLock ;
	this.lockTimeout = schema.lockTimeout ;

	// Default collation for sorts?
	//this.collation = schema.collation ;

	// The timeout for Document#refresh(): it only reload if the ( NOW - syncTime ) delta is greater than this value
	// Schema validator set it to 1 second by default
	this.refreshTimeout = schema.refreshTimeout ;

	if ( this.canLock ) {
		schema.properties._lockedBy = {
			type: 'objectId' ,
			system: true ,
			default: null ,
			tags: [ 'system' ]
		} ;

		schema.properties._lockedAt = {
			type: Date ,
			system: true ,
			sanitize: 'toDate' ,
			default: null ,
			tags: [ 'system' ]
		} ;

		// Do not add it if it already exists
		this._addIndex( { properties: { _lockedBy: 1 } } ) ;
		this._addIndex( { properties: { _lockedBy: 1 , _lockedAt: 1 } } ) ;
	}

	// Collection featuring versioning
	this.versioning = schema.versioning ;

	if ( this.versioning ) {
		Object.assign( schema.properties , rootsDb.VersionCollection.versioningSchemaPropertiesOveride ) ;
	}

	this.validate = ( ... args ) => doormen( this.documentSchema , ... args ) ;
	this.validatePatch = ( ... args ) => doormen.patch( this.documentSchema , ... args ) ;
	this.validateAndUpdatePatch = ( rawDocument , patch ) => doormen( { patch: patch } , this.documentSchema , rawDocument ) ;

	this.validateOnCreate = schema.validateOnCreate !== undefined ? !! schema.validateOnCreate : true ;
	this.validateOnSave = schema.validateOnSave !== undefined ? !! schema.validateOnSave : true ;
	this.validateOnCommit = schema.validateOnCommit !== undefined ? !! schema.validateOnCommit : true ;
	this.validateOnPatch = !! schema.validateOnPatch ;
	this.validateOnSet = !! schema.validateOnSet ;	// /!\ Not coded ATM!!!


	// TODO: Check schema

	this.meta = schema.meta || {} ;

	// Already checked
	this.hooks = schema.hooks ;


	// Indexes
	this.indexes = {} ;
	this.uniques = [] ;
	this.hasTextIndex = false ;
	this.indexedProperties = {} ;
	this.indexedLinks = {} ;	// Used for back-links/back-multi-links


	if ( Array.isArray( schema.indexes ) ) {
		for ( key in schema.indexes ) {
			let index = schema.indexes[ key ] ,
				hasProperty = false ;

			// Roots DB indexing version, only change the fingerprint, thus the index's name
			// Not used ATM, since it's still v1 compatible
			//index.v = 2 ;

			if ( ! index || typeof index !== 'object' ) {
				log.error( "Bad index: %Y" , index ) ;
				continue ;
			}

			/* Now it's done by the schema's schema
			index.properties = index.properties && typeof index.properties === 'object' ? index.properties : {} ;
			index.links = index.links && typeof index.links === 'object' ? index.links : null ;
			index.unique = !! index.unique ;
			index.partial = !! index.partial ;
			index.collation = index.collation && typeof index.collation === 'object' ? index.collation : null ;
			index.driver = index.driver && typeof index.driver === 'object' ? index.driver : null ;
			*/

			if ( index.links ) {
				let linkIdPath , linkCollectionPath , subSchema ;

				for ( innerKey in index.links ) {
					hasProperty = true ;
					linkIdPath = innerKey + '._id' ;
					subSchema = doormen.path( schema , innerKey ) ;
					//log.hdebug( "subSchema: %Y" , subSchema ) ;
					index.properties[ linkIdPath ] = index.links[ innerKey ] ;
					this.indexedProperties[ linkIdPath ] = this.indexedLinks[ innerKey ] = true ;

					if ( subSchema.anyCollection ) {
						// Also add _collection to the properties of the index
						linkCollectionPath = innerKey + '._collection' ;
						index.properties[ linkCollectionPath ] = index.links[ innerKey ] ;
						this.indexedProperties[ linkCollectionPath ] = true ;
					}
				}
			}

			for ( innerKey in index.properties ) {
				hasProperty = true ;
				if ( index.properties[ innerKey ] === 'text' ) { this.hasTextIndex = true ; }
				else { this.indexedProperties[ innerKey ] = true ; }
			}

			if ( ! hasProperty ) {
				log.error( "Bad index, no property: %Y" , index ) ;
				continue ;
			}

			if ( index.unique ) { this.uniques.push( Object.keys( index.properties ) ) ; }
			index.name = hash.fingerprint( index ) ;
			this.indexes[ index.name ] = index ;		//tree.extend( null , { name: indexName } , element ) ;
		}
	}


	// Properties check (default tags, auto-sanitizer, etc)
	this.checkSchemaProperties( this.documentSchema.properties ) ;

	// Init the driver
	this.initDriver() ;
	this.initAttachmentDriver() ;

	// Add the ID data, should come after .initDriver()
	if ( ! this.documentSchema.properties[ this.driver.idKey ] ) {
		this.documentSchema.properties[ this.driver.idKey ] = {
			type: 'objectId' ,
			sanitize: 'toObjectId' ,
			optional: true ,
			system: true ,
			tags: [ 'id' ]
		} ;
	}

	this.indexedProperties[ this.driver.idKey ] = true ;
	this.uniques.unshift( [ this.driver.idKey ] ) ;
}

module.exports = Collection ;



// WIP...

// Type override
const typeOverrides = {
	link: {
		opaque: true ,
		sanitize: [ 'toLink' ]
	} ,
	requiredLink: {
		opaque: true ,
		sanitize: [ 'toLink' ]
	} ,
	multiLink: {
		opaque: true ,
		sanitize: [ 'toMultiLink' ] ,
		default: [] ,
		//of: { type: 'requiredLink' } ,
		of: { type: 'link' , sanitize: [ 'toLink' ] } ,
		constraints: [
			{
				enforce: 'unique' , path: '_id' , convert: 'toString' , noEmpty: true , resolve: true
			}
		]
	} ,
	backLink: {
		// Opaque? or forbid any patch here?
		//opaque: true ,
		sanitize: [ 'toBackLink' ]
	}
} ;

const collectionHookSchema = {
	type: 'array' ,
	sanitize: [ 'toArray' ] ,
	of: { type: 'function' }
} ;

const collectionIndexSchema = {
	type: 'strictObject' ,
	properties: {
		properties: { type: 'strictObject' , default: {} } ,
		links: { type: 'strictObject' , optional: true } ,
		unique: { type: 'boolean' , default: false } ,
		partial: { type: 'boolean' , default: false } ,
		collation: { type: 'strictObject' , optional: true } ,
		driver: { type: 'strictObject' , optional: true }
	}
} ;

Collection.schemaSchema = {
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
		versioning: {
			type: 'boolean' ,
			default: false
		} ,
		canLock: {
			type: 'boolean' ,
			default: false
		} ,
		lockTimeout: {
			type: 'number' ,
			default: 1000	// 1 whole second by default
		} ,
		refreshTimeout: {
			type: 'number' ,
			default: 1000	// 1 whole second by default
		} ,
		//collation: { type: 'strictObject' , optional: true } ,
		indexes: {
			type: 'array' ,
			default: [] ,
			of: collectionIndexSchema
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



// There is nothing to init anymore.
//Collection.prototype.init = async function() { if ( this.isInit ) { return ; } this.isInit = true ; } ;
Collection.prototype.init = function() {} ;



// Internal, add an index during instanciation
Collection.prototype._addIndex = function( index ) {
	// Validate the schema
	doormen( collectionIndexSchema , index ) ;
	this.documentSchema.indexes.push( index ) ;
} ;



// This recursively check if tags and auto-sanitizer are set in the schema
Collection.prototype.checkSchemaProperties = function( schemaProperties ) {
	for ( let key in schemaProperties ) {
		this.checkSchemaProperty( schemaProperties[ key ] ) ;
	}
} ;



// This recursively check if tags and auto-sanitizer are set in the schema
Collection.prototype.checkSchemaProperty = function( element ) {
	//var autoSanitizer = autoSanitizers[ element.type ] ;

	if ( ! element.tags || ! element.tags.length ) {
		// 'content' is the default data tag
		element.tags = [ 'content' ] ;
	}

	// Auto sanitizer
	/*
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
	*/

	// Override
	if ( typeOverrides[ element.type ] ) {
		Object.assign( element , typeOverrides[ element.type ] ) ;

		// Use-case: userland may force a minLength for things like multiLink, that have override for 'default'
		if ( element.minLength && Array.isArray( element.default ) && element.default.length < element.minLength ) {
			delete element.default ;
		}
	}

	if ( ! element.inputHint ) {
		element.inputHint = this.autoInputHint( element ) ;
	}

	if ( element.of && typeof element.of === 'object' ) {
		this.checkSchemaProperty( element.of ) ;
	}

	if ( element.properties && typeof element.properties === 'object' ) {
		for ( let key in element.properties ) {
			this.checkSchemaProperty( element.properties[ key ] ) ;
		}
	}
} ;



Collection.prototype.autoInputHint = function( element ) {
	switch ( element.type ) {
		case 'strictObject' :
		case 'object' :
		case 'array' :
			return 'embedded' ;
		case 'date' :
			if ( element.in ) { return 'list' ; }
			// Date picker
			return 'date' ;
		case 'boolean' :
			return 'checkbox' ;
		case 'link' :
		case 'multiLink' :
		case 'backLink' :
			return 'embedded' ;
		case 'attachment' :
			return 'file' ;
	}

	if ( element.in ) { return 'list' ; }

	return 'text' ;
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
				rootsDb.driver[ this.config.driver ] = require( 'roots-db-driver-' + this.config.driver ) ;
			}
			catch ( error_ ) {
				throw new Error( '[roots-db] Cannot load driver: ' + this.config.driver ) ;
			}
		}
	}

	this.driver = new rootsDb.driver[ this.config.driver ]( this ) ;

	this.immutables = new Set( [ ... IMMUTABLES , ... this.driver.immutables ] ) ;
} ;



Collection.prototype.initAttachmentDriver = function() {
	// no attachment or already connected? nothing to do!
	if ( ! this.attachmentConfig || this.attachmentDriver ) { return ; }
	//console.log( "attachment driver:" , this.attachmentConfig.driver ) ;

	if ( ! rootsDb.attachmentDriver[ this.attachmentConfig.driver ] ) {
		try {
			// First try attachmentDrivers shipped with rootsDb
			//console.log( "attachment driver:" , './' + this.attachmentConfig.driver + '.attachmentDriver.js' ) ;
			rootsDb.attachmentDriver[ this.attachmentConfig.driver ] = require( './' + this.attachmentConfig.driver + '.attachmentDriver.js' ) ;
		}
		catch ( error ) {
			// Then try attachmentDrivers in node_modules
			try {
				rootsDb.attachmentDriver[ this.attachmentConfig.driver ] = require( 'roots-db-attachment-driver-' + this.attachmentConfig.driver ) ;
			}
			catch ( error_ ) {
				throw new Error( '[roots-db] Cannot load attachmentDriver: ' + this.attachmentConfig.driver ) ;
			}
		}
	}

	this.attachmentDriver = new rootsDb.attachmentDriver[ this.attachmentConfig.driver ]( this ) ;
} ;



Collection.prototype.createId = function( from ) { return this.driver.createId( from ) ; } ;
Collection.prototype.checkId = function( rawDocument , enforce ) { return this.driver.checkId( rawDocument , enforce ) ; } ;

// Get the ID out of a raw document
Collection.prototype.getId = function( rawDocument ) { return rawDocument[ this.driver.idKey ] ; } ;

// Set the ID on a raw document
Collection.prototype.setId = function( rawDocument , id ) {
	if ( rawDocument._ && ( rawDocument._ instanceof Document ) ) {
		throw new TypeError( '.setId() only accepts raw document, not Document intances' ) ;
	}

	id = this.driver.createId( id ) ;
	rawDocument[ this.driver.idKey ] = id ;
	return id ;
} ;

// Delete the ID on a raw document
Collection.prototype.deleteId = function( rawDocument ) {
	if ( rawDocument._ && ( rawDocument._ instanceof Document ) ) {
		throw new TypeError( '.deleteId() only accepts raw document, not Document intances' ) ;
	}

	return ( delete rawDocument[ this.driver.idKey ] ) ;
} ;



Collection.prototype.createFingerprint = function( rawFingerprint = {} , isPartial = false ) {
	return new rootsDb.Fingerprint( this , rawFingerprint , isPartial ) ;
} ;



// Index/re-index a collection
Collection.prototype.buildIndexes = function() {
	var obsoleteIndexes = [] , missingIndexes = [] ;

	// Firstly, get indexes
	return this.driver.getIndexes()
		.then( upstreamIndexes => {
			// Secondly, drop obsolete indexes & prepare missing indexes
			var key ;

			for ( key in upstreamIndexes ) {
				if ( ! this.indexes[ key ] ) { obsoleteIndexes.push( key ) ; }
			}

			for ( key in this.indexes ) {
				if ( ! upstreamIndexes[ key ] ) { missingIndexes.push( key ) ; }
			}

			if ( ! obsoleteIndexes.length ) { return missingIndexes ; }

			return Promise.map( obsoleteIndexes , indexName => this.driver.dropIndex( indexName ) ) ;
		} )
		// Finally, create missing indexes
		.then( () => {
			if ( ! missingIndexes.length ) { return ; }
			return Promise.forEach( missingIndexes , indexName => this.driver.buildIndex( this.indexes[ indexName ] ) ) ;
		} ) ;
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

	// Use default collation when possible
	//if ( this.collation && options.collation === undefined ) { options.collation = this.collation ; }

	// /!\ Should be able to check the cache for a fingerprint too /!\
	return this.driver.collect( fingerprint , options ).then( rawBatch =>
		this.postBatchRetrieve( rawBatch , null , null , options )
	) ;
} ;



// Get a set of document
// Promise ready
Collection.prototype.find = function( queryObject , options = {} ) {
	if ( ! queryObject || typeof queryObject !== 'object' ) { throw new Error( "[roots-db] queryObject should be an object" ) ; }

	// Use default collation when possible
	//if ( this.collation && options.collation === undefined ) { options.collation = this.collation ; }

	// /!\ Should be able to perform a find in the cache /!\
	return this.driver.find( queryObject , options ).then( rawBatch =>
		this.postBatchRetrieve( rawBatch , null , null , options )
	) ;
} ;



// Like find, but do not return a batch, call the callback with each document
// .findEach( queryObject , [options] , iterator )
// Promise ready
Collection.prototype.findEach = async function( queryObject , options , iterator ) {
	if ( ! queryObject || typeof queryObject !== 'object' ) { throw new Error( "[roots-db] queryObject should be an object" ) ; }

	if ( typeof options === 'function' ) { iterator = options ; options = {} ; }
	else if ( ! options ) { options = {} ; }

	// Use default collation when possible
	//if ( this.collation && options.collation === undefined ) { options.collation = this.collation ; }

	var error , errors = [] ;

	// /!\ Should be able to perform a find in the cache /!\
	await this.driver.findEach( queryObject , options , rawDocument =>
		this.postDocumentRetrieve( rawDocument , false , options ).then( iterator )
			.catch( error_ => {
			// Survive errors and report at the end?
				log.error( ".findEach(): %E" , error_ ) ;
				errors.push( error_ ) ;
			} )
	) ;

	if ( errors.length ) {
		error = new Error( ".findEach() have " + errors.length + " error(s)" ) ;
		error.errors = errors ;
		throw error ;
	}
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
			documentProxy = ( new this.Document(
				this , rawDocument , {
					fromUpstream: true ,
					skipValidation: true ,
					tagMask: options.tagMask ,
					populateTagMask: options.populateTagMask ,
					enumerateMasking: options.enumerateMasking
				}
			) ).proxy ;
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
		batch = new this.Batch( this , rawBatch , {
			fromUpstream: true ,
			skipValidation: true ,
			tagMask: options.tagMask ,
			populateTagMask: options.populateTagMask ,
			enumerateMasking: options.enumerateMasking
		} ) ;
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

