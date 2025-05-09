/*
	Roots DB

	Copyright (c) 2014 - 2021 Cédric Ronvel

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
const misc = require( './misc.js' ) ;

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



const IMMUTABLE_PROTOTYPES = [ Date.prototype ] ;



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
	this.immutablePrototypes = null ;

	// Create the validator schema
	//this.documentSchema = doormen.purifySchema( schema ) ;
	this.documentSchema = schema ;

	if ( typeof schema.url !== 'string' ) { throw new Error( '[roots-db] schema.url should be a string' ) ; }

	this.Document = schema.Document ;
	this.Batch = schema.Batch ;

	this.connectionUrl = schema.url ;
	this.url = misc.connectionlessUrl( this.connectionUrl ) ;
	this.config = new url.URL( this.connectionUrl ) ;
	this.config.driver = this.config.protocol.split( ':' )[ 0 ].split( '+' )[ 0 ] ;
	this.driver = null ;

	// Attachment URL for files
	this.attachmentUrl = schema.attachmentUrl || null ;
	this.attachmentConfig = null ;
	this.attachmentDriver = null ;
	this.attachmentHashType = schema.attachmentHashType || null ;

	// In the storage and in publicURL the file will be created with its extension (files on the server will be more easy to open)
	this.attachmentAppendExtension = !! schema.attachmentAppendExtension ;

	// If defined, serve as the base URL for Attachment's publicUrl (e.g.: resource's CDN URL)
	this.attachmentPublicBaseUrl = null ;
	if ( schema.attachmentPublicBaseUrl ) {
		this.attachmentPublicBaseUrl = schema.attachmentPublicBaseUrl ;
		if ( this.attachmentPublicBaseUrl[ this.attachmentPublicBaseUrl.length - 1 ] !== '/' ) {
			this.attachmentPublicBaseUrl += '/' ;
		}
	}

	if ( typeof this.attachmentUrl === 'string' ) {
		this.attachmentConfig = new url.URL( this.attachmentUrl ) ;
		this.attachmentConfig.driver = this.attachmentConfig.protocol ?
			this.attachmentConfig.protocol.split( ':' )[ 0 ] :
			'file' ;
	}

	// Fake data generator
	this.fakeDataGeneratorConfig =
		! schema.fakeDataGenerator ? null :
		typeof schema.fakeDataGenerator === 'string' ? { type: schema.fakeDataGenerator } :
		typeof schema.fakeDataGenerator === 'object' ? schema.fakeDataGenerator :
		null ;
	this.fakeDataGenerator = null ;

	if ( ! schema.properties ) { schema.properties = {} ; }

	// Collection featuring locks
	this.lockable = schema.lockable ;
	this.lockTimeout = schema.lockTimeout ;

	// Default collation for sorts?
	//this.collation = schema.collation ;

	// The timeout for Document#refresh(): it only reload if the ( NOW - syncTime ) delta is greater than this value
	// Schema validator set it to 1 second by default
	this.refreshTimeout = schema.refreshTimeout ;

	if ( this.lockable ) {
		schema.properties._lockedBy = {
			type: 'objectId' ,
			system: true ,
			rootsDbInternal: true ,
			default: null ,
			tags: [ 'system' ]
		} ;

		schema.properties._lockedAt = {
			type: 'date' ,
			system: true ,
			rootsDbInternal: true ,
			sanitize: 'toDate' ,
			default: null ,
			tags: [ 'system' ]
		} ;

		// Do not add it if it already exists
		this._addIndex( { properties: { _lockedBy: 1 } } ) ;
		this._addIndex( { properties: { _lockedBy: 1 , _lockedAt: 1 } } ) ;
	}


	// Collection featuring freeze (make the document read-only)
	this.freezable = schema.freezable ;
	if ( this.freezable ) {
		schema.properties._frozen = {
			type: 'boolean' ,
			system: true ,
			rootsDbInternal: true ,
			default: false ,
			tags: [ 'systemContent' ]
		} ;
	}


	schema.properties._import = {
		type: 'object' ,
		optional: true ,
		system: true ,
		tags: [ 'system' ] ,
		extraProperties: true ,
		properties: {
			_importId: {
				// The ID of the whole import operation, that can span accross multiple documents and collections
				type: 'string' ,
				system: true ,
				tags: [ 'system' ]
			} ,
			_foreignId: {
				// The ID of the document on the foreign DB, that can be used to restore links
				type: 'string' ,
				optional: true ,
				system: true ,
				tags: [ 'system' ]
			}
		}
	} ;

	// Collection featuring versioning
	this.versioning = schema.versioning ;

	if ( this.versioning ) {
		Object.assign( schema.properties , rootsDb.VersionsCollection.versioningSchemaPropertiesOveride ) ;
	}

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
	this.defaultSortCollations = {} ;	// For each property-list, the default collation to use, if any...
	this.hasTextIndex = false ;

	// Used by RestQuery to detect indexed/unindexed queries, it contains all the keys of indexed regular properties
	// and indexed links, and also extra keys for multiLinks: <multiLink>.*
	this.indexedProperties = new Set() ;


	if ( Array.isArray( schema.indexes ) ) {
		for ( let key of Object.keys( schema.indexes ) ) {
			let index = schema.indexes[ key ] ,
				hasProperty = false ,
				badProperty = false ;

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
				for ( let innerKey of Object.keys( index.links ) ) {
					let subSchema ;

					try {
						subSchema = doormen.subSchema( schema , innerKey ) ;
					}
					catch ( error ) {
						log.error( "Cannot index property '" + innerKey + "' of collection '" + this.name + "' (as link) which is not part of the schema (schema error: " + error.message + ")" ) ;
						badProperty = true ;
						continue ;
					}

					if ( subSchema.type === 'link' ) {
						hasProperty = true ;
						index.properties[ innerKey + '._id' ] = index.links[ innerKey ] ;
						this.indexedProperties.add( innerKey ) ;
						this.indexedProperties.add( innerKey + '._id' ) ;

						if ( subSchema.anyCollection ) {
							index.properties[ innerKey + '._collection' ] = index.links[ innerKey ] ;
							this.indexedProperties.add( innerKey + '._collection' ) ;
						}
					}
					else if ( subSchema.type === 'multiLink' ) {
						hasProperty = true ;
						index.properties[ innerKey + '.*._id' ] = index.links[ innerKey ] ;
						this.indexedProperties.add( innerKey ) ;
						this.indexedProperties.add( innerKey + '.*' ) ;
						this.indexedProperties.add( innerKey + '.*._id' ) ;

						if ( subSchema.anyCollection ) {
							index.properties[ innerKey + '.*._collection' ] = index.links[ innerKey ] ;
							this.indexedProperties.add( innerKey + '.*._collection' ) ;
						}
					}
					else {
						log.error( "Cannot index property '" + innerKey + "' of collection '" + this.name + "' as link because it's a regular property" ) ;
						badProperty = true ;
					}
				}
			}

			for ( let innerKey of Object.keys( index.properties ) ) {
				let subSchema ;

				try {
					subSchema = doormen.subSchema( schema , innerKey ) ;
				}
				catch ( error ) {
					log.error( "Cannot index property '" + innerKey + "' of collection '" + this.name + "' which is not part of the schema (schema error: " + error.message + ")" ) ;
					badProperty = true ;
					continue ;
				}

				if ( subSchema.type === 'link' || subSchema.type === 'multiLink' || subSchema.type === 'backLink' ) {
					log.error( "Cannot index property '" + innerKey + "' of collection '" + this.name + "' as a regular property because it's a " + subSchema.type ) ;
					badProperty = true ;
					continue ;
				}

				hasProperty = true ;

				if ( index.properties[ innerKey ] === 'text' ) { this.hasTextIndex = true ; }
				else { this.indexedProperties.add( innerKey ) ; }
			}

			if ( ! hasProperty || badProperty ) {
				log.error( "Bad index, no valid property. Config properties: %N" , Object.keys( index.properties || {} ).concat( Object.keys( index.links || {} ) ) ) ;
				continue ;
			}

			if ( index.unique ) { this.uniques.push( Object.keys( index.properties ) ) ; }

			// Create the index name using by fingerprinting
			index.name = hash.fingerprint( {
				properties: index.properties ,
				unique: index.unique ,
				partial: index.partial ,
				collation: index.collation ,
				driver: index.driver
			} ) ;

			//log.hdebug( "Indexing properties: %n" , index.properties ) ;
			index.propertyString = this._propertyString( index.properties ) ;

			if ( index.isDefaultSortCollation && ! this.defaultSortCollations[ index.propertyString ] ) {
				this.defaultSortCollations[ index.propertyString ] = index.collation ;
			}

			this.indexes[ index.name ] = index ;
		}
	}


	// Properties check (default tags, auto-sanitizer, etc)
	this.checkSchemaProperties( this.documentSchema.properties ) ;

	// Init the driver
	this.initDriver() ;
	this.initAttachmentDriver() ;
	this.initFakeDataGenerator() ;

	// Add the ID data, should come after .initDriver()
	if ( ! this.documentSchema.properties[ this.driver.idKey ] ) {
		this.documentSchema.properties[ this.driver.idKey ] = {
			type: 'objectId' ,
			sanitize: 'toObjectId' ,
			optional: true ,
			system: true ,
			rootsDbInternal: true ,
			tags: [ 'id' ]
		} ;
	}

	this.indexedProperties.add( this.driver.idKey ) ;
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
		opaque: true ,
		sanitize: [ 'toBackLink' ] ,
		default: []
		//of: { type: 'link' , sanitize: [ 'toLink' ] } ,
	} ,
	attachment: {
		opaque: true
	} ,
	attachmentSet: {
		opaque: true ,
		default: {}		// force a default value, it should not be optional
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
		// Part of the index fingerprint
		properties: { type: 'strictObject' , default: {} } ,
		unique: { type: 'boolean' , default: false } ,
		partial: { type: 'boolean' , default: false } ,
		collation: { type: 'strictObject' , optional: true } ,
		driver: { type: 'strictObject' , optional: true } ,

		// Not part of the fingerprint
		links: { type: 'strictObject' , optional: true } ,
		isDefaultSortCollation: { type: 'boolean' , default: false }
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
		freezable: {
			type: 'boolean' ,
			default: false
		} ,
		lockable: {
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



const COLLATOR = new Intl.Collator( 'en' , { numeric: true } ) ;

// Internal, create a property string of an object, key are ordered, so it can be matched with another
Collection.prototype._propertyString = function( object ) {
	return Object.keys( object ).sort( COLLATOR.compare ).join( ',' ) ;
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

	// Check some schema correctness
	if ( element.type === 'link' ) {
		if ( element.anyCollection ) {
			if ( element.allowedCollections ) {
				if ( ! Array.isArray( element.allowedCollections ) || ! element.allowedCollections.every( e => typeof e === 'string' ) ) {
					throw new Error( "Bad schema, if present, 'allowedCollections' should be an array of string" ) ;
				}
			}
		}
	}

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



Collection.prototype.validate = function( ... args ) { return doormen( this.documentSchema , ... args ) ; } ;
Collection.prototype.fakeAndValidate = function( ... args ) { return doormen( { fake: true } , this.documentSchema , ... args ) ; } ;
Collection.prototype.validatePatch = function( ... args ) { return doormen.patch( this.documentSchema , ... args ) ; } ;

Collection.prototype.validateAndUpdatePatch = function( rawDocument , patch ) {
	var validatePatch = {} ;
	doormen( { patch: validatePatch } , this.documentSchema , rawDocument ) ;
	doormen.mergePatch( patch , validatePatch ) ;
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

	var driverName = this.config.driver ,
		module_ = rootsDb.driver[ driverName ] ;

	if ( ! module_ ) {
		throw new Error( '[roots-db] Driver not found: ' + driverName ) ;
	}

	if ( typeof module_ === 'string' ) {
		// The driver is not loaded yet (because of lazy loading)
		try {
			module_ = require( module_ ) ;
		}
		catch ( error ) {
			//log.fatal( "Error: %E" , error ) ; process.exit() ;
			throw new Error( '[roots-db] Cannot load driver: ' + driverName ) ;
		}

		if ( typeof module_ !== 'function' ) {
			throw new Error( '[roots-db] Bad driver (not a function): ' + driverName ) ;
		}

		rootsDb.driver[ driverName ] = module_ ;
	}

	this.driver = new module_( this ) ;

	this.immutablePrototypes = new Set( [ ... IMMUTABLE_PROTOTYPES , ... this.driver.immutablePrototypes ] ) ;
} ;



Collection.prototype.initAttachmentDriver = function() {
	// no attachment or already connected? nothing to do!
	if ( ! this.attachmentConfig || this.attachmentDriver ) { return ; }
	//console.log( "attachment driver:" , this.attachmentConfig.driver ) ;

	var driverName = this.attachmentConfig.driver ,
		module_ = rootsDb.attachmentDriver[ driverName ] ;

	if ( ! module_ ) {
		throw new Error( '[roots-db] Attachment driver not found: ' + driverName ) ;
	}

	if ( typeof module_ === 'string' ) {
		// The driver is not loaded yet (because of lazy loading)
		try {
			module_ = require( module_ ) ;
		}
		catch ( error ) {
			throw new Error( '[roots-db] Cannot load attachment driver: ' + driverName ) ;
		}

		if ( typeof module_ !== 'function' ) {
			throw new Error( '[roots-db] Bad attachment driver (not a function): ' + driverName ) ;
		}

		rootsDb.attachmentDriver[ driverName ] = module_ ;
	}

	this.attachmentDriver = new module_( this ) ;
} ;



Collection.prototype.initFakeDataGenerator = function() {
	// already connected? nothing to do!
	if ( this.fakeDataGenerator || ! this.fakeDataGeneratorConfig?.type ) { return ; }

	var generatorType = this.fakeDataGeneratorConfig.type ,
		module_ = rootsDb.fakeDataGenerator[ generatorType ] ;

	if ( ! module_ ) {
		throw new Error( '[roots-db] Fake data generator not found: ' + generatorType ) ;
	}

	if ( typeof module_ === 'string' ) {
		// The driver is not loaded yet (because of lazy loading)
		try {
			module_ = require( module_ ) ;
		}
		catch ( error ) {
			//log.fatal( "Error: %E" , error ) ; process.exit() ;
			throw new Error( '[roots-db] Cannot load fake data generator: ' + generatorType ) ;
		}

		if ( typeof module_ !== 'function' ) {
			throw new Error( '[roots-db] Bad fake data generator (not a function): ' + generatorType ) ;
		}

		rootsDb.fakeDataGenerator[ generatorType ] = module_ ;
	}

	this.fakeDataGenerator = new module_( this ) ;

	doormen.schemaWalker(
		{
			schema: this.documentSchema ,
			schemaPath: [] ,
			options: { extraProperties: true }
		} ,
		ctx => {
			if ( ! ctx.schema.fake ) { return ; }
			ctx.schema.fakeFn = this.fakeDataGenerator.generate ;
		}
	) ;
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
Collection.prototype.buildIndexes = async function() {
	var obsoleteIndexes = [] , missingIndexes = [] ;

	// Firstly, get indexes

	var upstreamIndexes = await this.driver.getIndexes() ;


	// Secondly, drop obsolete indexes & prepare missing indexes

	for ( let key of Object.keys( upstreamIndexes ) ) {
		if ( ! this.indexes[ key ] ) { obsoleteIndexes.push( key ) ; }
	}

	for ( let key of Object.keys( this.indexes ) ) {
		if ( ! upstreamIndexes[ key ] ) { missingIndexes.push( key ) ; }
	}

	if ( obsoleteIndexes.length ) {
		for ( let indexName of obsoleteIndexes ) {
			log.info( "Removing an obsolete index on collection '%s' named '%s' on properties: %N" , this.name , indexName , Object.keys( upstreamIndexes[ indexName ].properties || {} ) ) ;
			await this.driver.dropIndex( indexName ) ;
		}
	}


	// Finally, create missing indexes

	if ( missingIndexes.length ) {
		for ( let indexName of missingIndexes ) {
			log.info( "Adding a new index on collection '%s' named '%s' on properties: %N" , this.name , indexName , Object.keys( this.indexes[ indexName ].properties ) ) ;
			await this.driver.buildIndex( this.indexes[ indexName ] ) ;
		}
	}
} ;



// Modify options in-place to add correct collation if there is a default one
Collection.prototype._checkSort = function( options ) {
	if ( ! options.sort || options.collation ) { return ; }

	var propertyString = this._propertyString( options.sort ) ,
		collation = this.defaultSortCollations[ propertyString ] ;

	if ( collation ) { options.collation = collation ; }
} ;



/* Document-oriented method */



Collection.prototype.createDocument = function( rawDocument , options ) {
	return ( new this.Document( this , rawDocument , options ) ).proxy ;
} ;



Collection.prototype.createFakeDocument = function() {
	if ( ! this.fakeDataGenerator ) {
		throw new Error( "[roots-db] no fake data generator defined on collection '" + this.name + "'" ) ;
	}

	return ( new this.Document( this , {} , { fake: true } ) ).proxy ;
} ;



Collection.prototype.createBatch = function( rawBatch , options ) {
	return new this.Batch( this , rawBatch , options ) ;
} ;



Collection.prototype.createFakeBatch = function( count ) {
	if ( ! this.fakeDataGenerator ) {
		throw new Error( "[roots-db] no fake data generator defined on collection '" + this.name + "'" ) ;
	}

	var rawBatch = [] ;
	while ( count -- ) { rawBatch.push( {} ) ; }

	return new this.Batch( this , rawBatch , { fake: true } ) ;
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
		//log.debug( "Collection.get(): Cache hit (raw)!" ) ;
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
			//log.debug( 'Collection.multiGet(): Complete cache hit!' ) ;
			return this.postBatchRetrieve( [] , cachedBatch , options.cache , options ) ;
		}

		if ( notFoundArray.length < ids.length ) {
			//log.debug( 'Collection.multiGet(): Partial cache hit!' ) ;
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



// Get a set of document by fingerprint
// Promise ready
Collection.prototype.collect = function( fingerprint , options = {} ) {
	if ( ! fingerprint || typeof fingerprint !== 'object' ) { throw new Error( "[roots-db] fingerprint should be an object" ) ; }

	// Create fingerprint if needed
	//if ( ! ( fingerprint instanceof rootsDb.Fingerprint ) ) { fingerprint = this.createFingerprint( fingerprint ) ; }

	// Use default collation when possible
	//if ( this.collation && options.collation === undefined ) { options.collation = this.collation ; }

	this._checkSort( options ) ;

	// /!\ Should be able to check the cache for a fingerprint too /!\
	return this.driver.collect( fingerprint , options ).then( rawBatch =>
		this.postBatchRetrieve( rawBatch , null , null , options )
	) ;
} ;



// Get a set of documents by query
// Promise ready
Collection.prototype.find = function( queryObject , options = {} ) {
	if ( ! queryObject || typeof queryObject !== 'object' ) { throw new Error( "[roots-db] queryObject should be an object" ) ; }

	// Use default collation when possible
	//if ( this.collation && options.collation === undefined ) { options.collation = this.collation ; }

	this._checkSort( options ) ;

	// /!\ Should be able to perform a find in the cache /!\
	return this.driver.find( queryObject , options ).then( rawBatch =>
		this.postBatchRetrieve( rawBatch , null , null , options )
	) ;
} ;



// Get a set of document's ID by query
// options.partial: return an array of objects having a _id property (a partial document)
// Promise ready
Collection.prototype.findIdList = function( queryObject , options = {} ) {
	if ( ! queryObject || typeof queryObject !== 'object' ) { throw new Error( "[roots-db] queryObject should be an object" ) ; }

	// Use default collation when possible
	//if ( this.collation && options.collation === undefined ) { options.collation = this.collation ; }

	// Not much sense here, but well...
	this._checkSort( options ) ;

	// /!\ Should be able to perform a find in the cache ??? /!\
	return this.driver.findIdList( queryObject , options ) ;
} ;



// Generator version of find()
// Promise ready
Collection.prototype.findGenerator = async function * ( queryObject , options = {} ) {
	if ( ! queryObject || typeof queryObject !== 'object' ) { throw new Error( "[roots-db] queryObject should be an object" ) ; }

	// Use default collation when possible
	//if ( this.collation && options.collation === undefined ) { options.collation = this.collation ; }

	this._checkSort( options ) ;

	// /!\ Should be able to perform a find in the cache /!\
	for await ( let rawDocument of await this.driver.findGenerator( queryObject , options ) ) {
		yield await this.postDocumentRetrieve( rawDocument , false , options ) ;
	}
} ;



/*
	Generator version of find(), but instead of returning each documents, return mini-batches.
	Useful for streaming large dataset to the client, since populate is done on each partial batch.
	On the contrary, using .find() consumes a lot of memory for large dataset, using .findGenerator() is slow when
	populate is involved.

	Options:
		batchSize: max size of the partial batches (default: 256)
*/
// Promise ready
Collection.prototype.findPartialBatchGenerator = async function * ( queryObject , options = {} ) {
	if ( ! queryObject || typeof queryObject !== 'object' ) { throw new Error( "[roots-db] queryObject should be an object" ) ; }

	// Use default collation when possible
	//if ( this.collation && options.collation === undefined ) { options.collation = this.collation ; }

	this._checkSort( options ) ;

	// /!\ Should be able to perform a find in the cache /!\

	var batchSize = options.batchSize || 256 ,
		rawBatch = [] ;

	for await ( let rawDocument of await this.driver.findGenerator( queryObject , options ) ) {
		rawBatch.push( rawDocument ) ;

		if ( rawBatch.length >= batchSize ) {
			yield await this.postBatchRetrieve( rawBatch , null , null , options ) ;
			rawBatch = [] ;
		}
	}

	if ( rawBatch.length ) {
		yield await this.postBatchRetrieve( rawBatch , null , null , options ) ;
	}
} ;



// Instead of returning a set of documents like .find(), it returns the number of matching documents by query
// Promise ready
Collection.prototype.countFound = function( queryObject = {} ) {
	return this.driver.countFound( queryObject ) ;
} ;



// Clear everything in a collection: all documents and their attachments
// BTW: use with care!
Collection.prototype.clear = function() {
	if ( this.attachmentDriver ) {
		return Promise.all( [
			this.driver.clear() ,
			this.attachmentDriver.clear()
		] ) ;
	}

	return this.driver.clear() ;
} ;



// Get a set of document
Collection.prototype.lockingFind = async function( queryObject , options , actionFn = null ) {
	if ( ! this.lockable ) { throw new Error( "[roots-db] Cannot lock document on this collection" ) ; }

	// Managing function's arguments
	if ( ! queryObject || typeof queryObject !== 'object' ) { throw new Error( "[roots-db] queryObject should be an object" ) ; }
	if ( arguments.length === 2 && typeof options === 'function' ) { actionFn = options ; options = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }

	var batch , otherBatch ,
		lockTimeout = options.lockTimeout || this.lockTimeout ;

	// /!\ Should be able to perform a find in the cache??? /!\

	if ( actionFn ) {
		return this.driver.lockingFind( queryObject , lockTimeout , options.other , async ( lockId , rawBatch , otherRawBatch ) => {
			batch = await this.postBatchRetrieve( rawBatch , null , null , options , lockId ) ;
			if ( otherRawBatch ) { otherBatch = await this.postBatchRetrieve( otherRawBatch , null , null , options ) ; }
			return actionFn( lockId , batch , otherBatch ) ;
		} ) ;
	}

	var { lockId , rawBatch , otherRawBatch } = await this.driver.lockingFind( queryObject , lockTimeout , options.other ) ;

	batch = await this.postBatchRetrieve( rawBatch , null , null , options , lockId ) ;
	if ( otherRawBatch ) { otherBatch = await this.postBatchRetrieve( otherRawBatch , null , null , options ) ; }

	return { lockId , batch , otherBatch } ;
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



Collection.prototype.postBatchRetrieve = async function( rawBatch , cachedBatch , fromCache , options , lockId = null ) {
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
			enumerateMasking: options.enumerateMasking ,
			lockId
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

