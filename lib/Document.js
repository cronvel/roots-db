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



const rootsDb = require( './rootsDb.js' ) ;
const Population = require( './Population.js' ) ;

const Promise = require( 'seventh' ) ;
const DeepProxy = require( 'nested-proxies' ) ;
const ErrorStatus = require( 'error-status' ) ;
const doormen = require( 'doormen' ) ;
const tree = require( 'tree-kit' ) ;

const fs = require( './promise-fs.js' ) ;
const deltree = Promise.promisify( require( 'fs-kit' ).deltree ) ;

const log = require( 'logfella' ).global.use( 'roots-db' ) ;



const METHODS = new Set() ;
const PROXY_OPTIONS = { pathArray: true , extra: {} } ;
const PROXY_OPTIONS_$ = Object.assign( {} , PROXY_OPTIONS , { extra: { $: true } } ) ;



const PROXY_HANDLER = {
	get: function( target , property , receiver , path ) {
		var proto , populatedDocProx ,
			rootTarget = this.root.target ,
			trueTarget = path.length <= 1 ? target.raw : target ;

		//console.log( this.extra.$ ? '$' : '.' , path ) ;
		if ( ! this.extra.$ && path.length === 1 ) {
			// Data-only access (for data using a reserved name, like .save, etc)
			if ( property === '$' ) {
				if ( ! rootTarget.$proxy ) {
					rootTarget.$proxy = new DeepProxy( rootTarget , PROXY_HANDLER , PROXY_OPTIONS_$ ) ;
				}

				return rootTarget.$proxy ;
			}

			// Wrapper access
			if ( property === '_' ) { return target ; }

			// This is a document method
			if ( rootTarget.methods.has( property ) ) {
				if ( target[ property ] === Document.prototype[ property ] ) {
					// Time to bind the function
					target[ property ] = target[ property ].bind( target ) ;
				}

				return target[ property ] ;
			}
		}

		if ( Object.prototype[ property ] ) {	// There are only functions, AFAICT
			return trueTarget[ property ].bind( trueTarget ) ;
		}

		if ( trueTarget[ property ] ) {
			if ( typeof trueTarget[ property ] === 'object' ) {
				// Populate management
				if ( ! this.extra.$ && ( populatedDocProx = rootTarget.populatedDocumentProxies.get( trueTarget[ property ] ) ) ) {
					return populatedDocProx ;
				}

				proto = Object.getPrototypeOf( trueTarget[ property ] ) ;

				if ( rootTarget.collection.immutables.has( proto ) ) {
					return trueTarget[ property ] ;
				}

				return this.nested( property , trueTarget[ property ] ) ;
			}
		}

		return trueTarget[ property ] ;
	} ,

	set: function( target , property , passedValue , receiver , path ) {
		var value ,
			rootTarget = this.root.target ,
			trueTarget = path.length <= 1 ? target.raw : target ;

		if ( passedValue && typeof passedValue === 'object' && ( passedValue._ instanceof Document ) ) {
			// Populate management
			value = { _id: passedValue._id } ;
			rootTarget.populatedDocumentProxies.set( value , passedValue._.proxy ) ;
		}
		else {
			value = passedValue ;
		}

		if ( trueTarget[ property ] === value ) { return true ; }

		trueTarget[ property ] = value ;
		rootTarget.setLocalPatch( path , value ) ;

		return true ;
	} ,

	//apply
	//construct
	getPrototypeOf: ( target , path ) => path.length > 0 ?
		Reflect.getPrototypeOf( target ) : Reflect.getPrototypeOf( target.raw ) ,
	isExtensible: ( target , path ) => path.length > 0 ?
		Reflect.isExtensible( target ) : Reflect.isExtensible( target.raw ) ,
	ownKeys: ( target , path ) => path.length > 0 ?
		Reflect.ownKeys( target ) : Reflect.ownKeys( target.raw ) ,
	preventExtensions: ( target , path ) => path.length > 0 ?
		Reflect.preventExtensions( target ) : Reflect.preventExtensions( target.raw ) ,
	setPrototypeOf: ( target , proto , path ) => path.length > 0 ?
		Reflect.setPrototypeOf( target , proto ) : Reflect.setPrototypeOf( target.raw , proto ) ,

	defineProperty: ( target , property , descriptor , path ) => path.length > 1 ?
		Reflect.defineProperty( target , property , descriptor ) : Reflect.defineProperty( target.raw , property , descriptor ) ,
	deleteProperty: ( target , property , path ) => path.length > 1 ?
		Reflect.deleteProperty( target , property ) : Reflect.deleteProperty( target.raw , property ) ,
	getOwnPropertyDescriptor: ( target , property , path ) => path.length > 1 ?
		Reflect.getOwnPropertyDescriptor( target , property ) : Reflect.getOwnPropertyDescriptor( target.raw , property ) ,
	has: ( target , property , path ) => path.length > 1 ?
		Reflect.has( target , property ) : Reflect.has( target.raw , property )
} ;



function Document( collection , rawDocument = {} , options = {} ) {
	// Already wrapped?
	if ( rawDocument._ instanceof Document ) { return rawDocument._ ; }

	if ( ! options.noHook && ! options.fromUpstream ) {
		collection.hooks.beforeCreateDocument.forEach( hook => hook( rawDocument ) ) ;
	}

	// Then validate the document
	if ( ! ( options.skipValidation !== undefined ? options.skipValidation : collection.skipValidation ) ) {
		try {
			collection.validate( rawDocument ) ;
		}
		catch ( error ) {
			error.validatorMessage = error.message ;
			error.message = '[roots-db] validator error: ' + error.message ;
			throw error ;
		}
	}

	this.collection = collection ;
	this.raw = rawDocument ;
	this.localPatch = null ;

	this.populatedDocumentProxies = new WeakMap() ;

	// /!\ Turn that to null?
	this.populated = {} ;
	this.populating = {} ;

	// Meta is a sub-object, because it could be shared when a doc is sundenly linked to a cached one
	this.meta = {
		id: collection.driver.checkId( rawDocument , true ) ,
		upstreamExists: false ,
		loaded: false ,
		saved: false ,
		deleted: false ,
		lockId: null
	} ;

	if ( options.fromUpstream ) {
		this.meta.loaded = true ;
		this.meta.upstreamExists = true ;
	}

	this.proxy = new DeepProxy( this , PROXY_HANDLER , PROXY_OPTIONS ) ;
	this.$proxy = null ;	// Lazily created, this is the proxy for .$

	this.methods = METHODS ;

	// This is to provide some unambiguous way to access the non-proxy document whatever we got
	this._ = this ;

	if ( ! options.noHook && ! options.fromUpstream ) {
		collection.hooks.afterCreateDocument.forEach( hook => hook( this.proxy ) ) ;
	}
}

module.exports = Document ;



Document.prototype.addProxyMethodNames = function( ... names ) {
	if ( this.methods === METHODS ) {
		this.methods = new Set( METHODS ) ;
	}

	names.forEach( name => {
		this.methods.add( name ) ;
		this[ name ] = this[ name ].bind( this ) ;
	} ) ;
} ;



METHODS.add( 'getId' ) ;
Document.prototype.getId = function() { return this.meta.id ; } ;

METHODS.add( 'getKey' ) ;
Document.prototype.getKey = function() { return '' + this.meta.id ; } ;



// Check if the document validate: throw an error if it doesn't
METHODS.add( 'validate' ) ;
Document.prototype.validate = function() { this.collection.validate( this.raw ) ; } ;



METHODS.add( 'save' ) ;
Document.prototype.save = async function( options = {} ) {
	if ( this.meta.deleted ) { throw new Error( 'Current document is deleted' ) ; }

	// Validation, if relevant
	if (
		! ( options.skipValidation !== undefined ? options.skipValidation : this.collection.skipValidation ) &&
		( ! this.collection.patchDrivenValidation || this.localPatch )
	) {
		try {
			log.debug( "Validate on save()" ) ;
			this.validate() ;
		}
		catch ( error ) {
			error.validatorMessage = error.message ;
			error.message = '[roots-db] validator error: ' + error.message ;
			throw error ;
		}
	}

	// Clear attachments first
	if ( options.clearAttachments && this.collection.attachmentUrl ) {
		await this.clearAttachments() ;
		log.debug( "Attachment: clearAttachments() done" ) ;
	}

	// Save attachments first
	if ( options.attachmentStreams && this.collection.attachmentUrl ) {
		await this.saveAttachmentStreams( options.attachmentStreams ) ;
		log.debug( "Attachment: checkpoint F" ) ;
	}

	// Now the three types of save...

	if ( this.meta.upstreamExists ) {
		// Full save (update)
		return this.collection.driver.update( this.meta.id , this.raw ).then( () => {
			this.meta.saved = true ;
			this.localPatch = null ;
		} ) ;
	}

	if ( options.overwrite ) {
		// overwrite wanted
		return this.collection.driver.overwrite( this.raw ).then( () => {
			this.meta.saved = true ;
			this.meta.upstreamExists = true ;
			this.localPatch = null ;
		} ) ;
	}

	// create (insert) needed
	return this.collection.driver.create( this.raw ).then( () => {
		this.meta.saved = true ;
		this.meta.upstreamExists = true ;
		this.localPatch = null ;
	} ) ;
} ;



METHODS.add( 'delete' ) ;
Document.prototype.delete = async function( options = {} ) {
	if ( this.meta.deleted ) { throw new Error( 'Current document is already deleted' ) ; }

	// Clear attachments first, then call delete() again...
	if ( ! options.dontClearAttachments && this.collection.attachmentUrl ) {
		await this.clearAttachments() ;
		log.debug( "Attachment: clearAttachments()" ) ;
	}

	return this.collection.driver.delete( this.meta.id ).then( () => {
		this.meta.deleted = true ;
		this.meta.upstreamExists = false ;
	} ) ;
} ;



// Lock the document for this application
METHODS.add( 'lock' ) ;
Document.prototype.lock = function() {
	if ( ! this.collection.canLock ) { throw new Error( 'Document of this collection cannot be locked' ) ; }
	if ( ! this.meta.upstreamExists ) { throw new Error( 'Cannot lock a document that does not exist upstream yet' ) ; }
	if ( this.meta.deleted ) { throw new Error( 'Current document is deleted' ) ; }

	// Don't do that: because of concurrency issues
	//if ( this.meta.lockId !== null ) { return Promise.resolve( null ) ; }

	return this.collection.driver.lock( this.meta.id , this.collection.lockTimeout )
		.then( lockId => {
			if ( lockId !== null ) {
				this.meta.lockId = lockId ;
			}
			return lockId ;
		} ) ;
} ;



// Lock the document for this application
METHODS.add( 'unlock' ) ;
Document.prototype.unlock = function() {
	if ( ! this.collection.canLock ) { throw new Error( 'Document of this collection cannot be unlocked' ) ; }
	if ( ! this.meta.upstreamExists ) { throw new Error( 'Cannot unlock a document that does not exist upstream yet' ) ; }
	if ( this.meta.deleted ) { throw new Error( 'Current document is deleted' ) ; }
	if ( this.meta.lockId === null ) { return Promise.resolve( false ) ; }

	return this.collection.driver.unlock( this.meta.id , this.meta.lockId ) ;
} ;



// Internal method
Document.prototype.setLocalPatch = function( pathArray , value ) {
	if ( ! this.localPatch ) { this.localPatch = {} ; }
	this.localPatch[ pathArray.join( '.' ) ] = value ;
} ;



METHODS.add( 'patch' ) ;
Document.prototype.patch = function( patch ) {
	// First, stage all the changes
	this.localPatch = tree.extend( null , this.localPatch , patch ) ;

	// Then apply the patch to the current local rawDocument
	tree.extend( { unflat: true , immutables: this.collection.immutables } , this.raw , patch ) ;
} ;



// Only relevant when directly accessing .raw without proxying to it
METHODS.add( 'stage' ) ;
Document.prototype.stage = function( paths ) {
	if ( typeof paths === 'string' ) { paths = [ paths ] ; }
	else if ( ! Array.isArray( paths ) ) { throw new TypeError( "[roots-db] stage(): argument #0 should be an a string or an array of string" ) ; }

	if ( ! paths.length ) { return ; }
	if ( ! this.localPatch ) { this.localPatch = {} ; }

	paths.forEach( path => this.localPatch[ path ] = tree.path.get( this.raw , path ) ) ;
} ;



// Commit the whole localPatch/staged stuff to the upstream
METHODS.add( 'commit' ) ;
Document.prototype.commit = async function( options = {} ) {
	if ( this.meta.deleted ) { throw new Error( 'Current document is deleted' ) ; }

	// Perform a regular save or throw an error?
	if ( ! this.meta.upstreamExists ) {
		this.localPatch = null ;
		return this.save() ;
	}

	// Validation of STAGED data
	if (
		! ( options.skipValidation !== undefined ? options.skipValidation : this.collection.skipValidation ) &&
		( ! this.collection.patchDrivenValidation || this.localPatch )
	) {
		try {
			log.debug( "Attachment: Validate on commit()" ) ;
			this.collection.validateAndUpdatePatch( this.raw , this.localPatch ) ;
		}
		catch ( error ) {
			error.validatorMessage = error.message ;
			error.message = '[roots-db] validator error: ' + error.message ;
			throw error ;
		}

		// Do not validate again, in case of recursive call
		options.skipValidation = true ;
	}

	// Save attachments first, then call commit() again...
	if ( options.attachmentStreams ) {
		await this.saveAttachmentStreams( options.attachmentStreams ) ;
		log.debug( "Attachment: checkpoint F2" ) ;
	}

	// No patch, nothing to do
	if ( ! this.localPatch ) { return ; }

	// Simple patch
	return this.collection.driver.patch( this.meta.id , this.localPatch )
		.then( () => {
			this.meta.saved = true ;
			this.localPatch = null ;
		} ) ;
} ;



/* Links */



METHODS.add( 'getLinkDetails' ) ;
Document.prototype.getLinkDetails = function( path , acceptAttachment ) {
	var metaData , schema ;

	try {
		schema = doormen.path( this.collection.documentSchema , path ) ;
	}
	catch ( error ) {
		return null ;
	}

	switch ( schema.type ) {
		case 'link' :
			if ( typeof schema.collection !== 'string' ) { return null ; }

			metaData = tree.path.get( this.raw , path ) || null ;

			return {
				type: schema.type ,
				schema: schema ,
				//hostCollection: this.collection.name ,
				hostPath: path ,
				//hostId: this.meta.id

				foreignCollection: schema.collection ,
				foreignId: metaData && metaData._id
			} ;

		case 'multiLink' :
			if ( typeof schema.collection !== 'string' ) { return null ; }

			metaData = tree.path.get( this.raw , path ) ;
			if ( ! Array.isArray( metaData ) ) { metaData = [] ; }

			return {
				type: schema.type ,
				schema: schema ,
				//hostCollection: this.collection.name ,
				hostPath: path ,
				//hostId: this.meta.id

				foreignCollection: schema.collection ,
				foreignIds: metaData.map( e => e._id )
			} ;

		case 'backLink' :
			if ( typeof schema.collection !== 'string' ) { return null ; }

			return {
				type: schema.type ,
				schema: schema ,
				//hostCollection: this.collection.name ,
				hostPath: path ,
				//hostId: this.meta.id

				foreignCollection: schema.collection ,
				foreignPath: schema.path
			} ;

		case 'attachment' :
			if ( ! acceptAttachment ) { return null ; }

			return this.getAttachmentDetails( path , schema ) ;

		default :
			return null ;
	}
} ;



/*
	Options:
		* multi `boolean`
			* true: abort with an error if the link would return a single document,
			* false: abort if the link would return a batch
*/
METHODS.add( 'getLink' ) ;
Document.prototype.getLink = async function( path , options = {} , acceptAttachment ) {
	var schema , foreignCollection , foreignSchema , target , targets , targetIds , fingerprint , query , documentProxy , batch , dbValue ;

	try {
		schema = doormen.path( this.collection.documentSchema , path ) ;
	}
	catch ( error ) {
		throw ErrorStatus.badRequest( { message: "Unexistant property/link '" + path + "'." } ) ;
	}

	switch ( schema.type ) {
		case 'link' :
			if ( typeof schema.collection !== 'string' ) {
				throw ErrorStatus.internalError( { message: "Link without collection in the schema, at path '" + path + "'." } ) ;
			}

			foreignCollection = this.collection.world.collections[ schema.collection ] ;

			if ( ! foreignCollection ) {
				throw ErrorStatus.badRequest( { message: "Link to an unexistant collection '" + schema.collection + "' at path '" + path + "'." } ) ;
			}

			if ( options.multi === true ) {
				throw ErrorStatus.badRequest( { message: "Expecting a multi-link at a single-link path, for collection '" + schema.collection + "' at path '" + path + "'." } ) ;
			}

			target = tree.path.get( this.raw , path ) ;

			if ( ! target || typeof target !== 'object' || ! target._id ) {
				throw ErrorStatus.notFound( { message: "Link not found." } ) ;
			}

			documentProxy = await foreignCollection.get( target._id , options ) ;

			// Populate it now!
			if ( ! options.raw ) { this.populatedDocumentProxies.set( target , documentProxy ) ; }

			return documentProxy ;

		case 'multiLink' :
			if ( typeof schema.collection !== 'string' ) {
				throw ErrorStatus.internalError( { message: "Multi-link without collection in the schema, at path '" + path + "'." } ) ;
			}

			foreignCollection = this.collection.world.collections[ schema.collection ] ;

			if ( ! foreignCollection ) {
				throw ErrorStatus.badRequest( { message: "Multi-link to an unexistant collection '" + schema.collection + "' at path '" + path + "'." } ) ;
			}

			if ( options.multi === false ) {
				throw ErrorStatus.badRequest( { message: "Expecting a link at a multi-link path, for collection '" + schema.collection + "' at path '" + path + "'." } ) ;
			}

			targets = tree.path.get( this.raw , path ) ;

			if ( ! Array.isArray( targets ) ) {
				// Unlike links that can eventually be null, a multiLink MUST be an array
				targets = [] ;
				tree.path.set( this.raw , path , targets ) ;
				targetIds = [] ;
				// Let .multiGet() handle the empty batch
			}
			else {
				targetIds = targets.map( e => e._id ) ;
			}

			batch = await foreignCollection.multiGet( targetIds , options ) ;

			// Populate it now!
			if ( ! options.raw ) {
				// Rewrite the multi-link with existing elements
				targets = batch.map( docProx => {
					dbValue = { _id: docProx.getId() } ;
					this.populatedDocumentProxies.set( dbValue , docProx ) ;
					return dbValue ;
				} ) ;

				tree.path.set( this.raw , path , targets ) ;
			}

			return batch ;

		case 'backLink' :
			if ( typeof schema.collection !== 'string' ) {
				throw ErrorStatus.internalError( { message: "Back-link without collection in the schema, at path '" + path + "'." } ) ;
			}

			foreignCollection = this.collection.world.collections[ schema.collection ] ;

			if ( ! foreignCollection ) {
				throw ErrorStatus.badRequest( { message: "Back-link to an unexistant collection '" + schema.collection + "' at path '" + path + "'." } ) ;
			}

			if ( options.multi === false ) {
				throw ErrorStatus.badRequest( { message: "Expecting a link at a back-link path, for collection '" + schema.collection + "' at path '" + path + "'." } ) ;
			}

			try {
				foreignSchema = doormen.path( foreignCollection.documentSchema , schema.path ) ;
			}
			catch ( error ) {
				throw ErrorStatus.badRequest( { message: "Back-link: targeted property '" + schema.path + "' of the foreign collection '" + schema.collection + "' is unexistant." } ) ;
			}

			if ( foreignSchema.type === 'link' ) {
				fingerprint = {} ;
				fingerprint[ schema.path + '._id' ] = this.meta.id ;

				batch = await foreignCollection.collect( fingerprint , options ) ;

				// Populate it now!
				if ( ! options.raw ) {
					dbValue = tree.path.get( this.raw , path ) ;

					if ( ! dbValue || typeof dbValue !== 'object' ) {
						dbValue = {} ;
						tree.path.set( this.raw , path , dbValue ) ;
					}

					this.populatedDocumentProxies.set( dbValue , batch ) ;
				}

				return batch ;
			}

			if ( foreignSchema.type === 'multiLink' ) {
				query = {} ;
				query[ schema.path + '._id' ] = { $in: [ this.meta.id ] } ;

				batch = await foreignCollection.find( query , options ) ;

				// Populate it now!
				if ( ! options.raw ) {
					dbValue = tree.path.get( this.raw , path ) ;

					if ( ! dbValue || typeof dbValue !== 'object' ) {
						dbValue = {} ;
						tree.path.set( this.raw , path , dbValue ) ;
					}

					this.populatedDocumentProxies.set( dbValue , batch ) ;
				}

				return batch ;
			}

			throw ErrorStatus.badRequest( { message: "Back-link: targeted property '" + schema.path + "' of the foreign collection '" + schema.collection + "' is neither a link or a multiLink." } ) ;

		case 'attachment' :
			if ( ! acceptAttachment ) {
				throw ErrorStatus.badRequest( { message: "Property is not a link '" + path + "'." } ) ;
			}

			return this.getAttachment( path , schema ) ;

		default :
			throw ErrorStatus.badRequest( { message: "Property is not a link '" + path + "'." } ) ;
	}
} ;



/*
	Set a link, and mark it as staged.
*/
METHODS.add( 'setLink' ) ;
Document.prototype.setLink = function( path , value ) {
	var schema , id , document , dbValue , dbValues ;

	try {
		schema = doormen.path( this.collection.documentSchema , path ) ;
	}
	catch ( error ) {
		throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
	}

	switch ( schema.type ) {
		case 'link' :
			if ( typeof schema.collection !== 'string' ) {
				throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
			}

			if ( ! value || ! value._ || ! ( value._ instanceof Document ) ) {
				throw ErrorStatus.badRequest( { message: "Provided value is not a Document." } ) ;
			}

			document = value._ ;

			if ( document.collection.name !== schema.collection ) {
				throw ErrorStatus.badRequest( {
					message: "Provided document is not part of collection '" + schema.collection + "' but '" + document.collection.name + "'."
				} ) ;
			}

			id = document.getId() ;
			dbValue = { _id: id } ;

			// Populate it now!
			this.populatedDocumentProxies.set( dbValue , document.proxy ) ;

			tree.path.set( this.raw , path , dbValue ) ;

			// Stage the change
			if ( ! this.localPatch ) { this.localPatch = {} ; }
			this.localPatch[ path ] = dbValue ;

			return ;

		case 'multiLink' :
			if ( typeof schema.collection !== 'string' ) {
				throw ErrorStatus.badRequest( { message: "Unexistant multi-link '" + path + "'." } ) ;
			}

			if ( ! value ) { value = [] ; }

			if ( value instanceof rootsDb.Batch ) {
				if ( value.collection.name !== schema.collection ) {
					throw ErrorStatus.badRequest( { message: "Provided batch is not part of collection '" + schema.collection + "' but '" + value.collection.name + "'." } ) ;
				}

				dbValues = value.map( docProx => {
					dbValue = { _id: docProx.getId() } ;

					// Populate it now!
					this.populatedDocumentProxies.set( dbValue , docProx ) ;

					return dbValue ;
				} ) ;
			}
			else if ( Array.isArray( value ) ) {
				dbValues = value.map( oneValue => {
					if ( ! oneValue || ! oneValue._ || ! ( oneValue._ instanceof Document ) ) {
						throw ErrorStatus.badRequest( { message: "Non-document provided in the multiLink array." } ) ;
					}

					document = oneValue._ ;

					if ( document.collection.name !== schema.collection ) {
						throw ErrorStatus.badRequest( {
							message: "In multiLink array, one document is not part of collection '" + schema.collection + "' but '" + document.collection.name + "'."
						} ) ;
					}

					dbValue = { _id: document.getId() } ;

					// Populate it now!
					this.populatedDocumentProxies.set( dbValue , document.proxy ) ;

					return dbValue ;
				} ) ;
			}
			else {
				throw ErrorStatus.badRequest( { message: "Bad multiLink argument." } ) ;
			}

			tree.path.set( this.raw , path , dbValues ) ;

			// Stage the change
			if ( ! this.localPatch ) { this.localPatch = {} ; }
			this.localPatch[ path ] = dbValues ;

			return  ;

		case 'backLink' :
			throw new Error( 'Not coded ATM' ) ;

		default :
			throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
	}
} ;



// Add one link to a multi-link
METHODS.add( 'addLink' ) ;
Document.prototype.addLink = function( path , value ) {
	var schema , dbValues , dbValue , document ;

	try {
		schema = doormen.path( this.collection.documentSchema , path ) ;
	}
	catch ( error ) {
		throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
	}

	switch ( schema.type ) {
		case 'multiLink' :
			if ( typeof schema.collection !== 'string' ) {
				throw ErrorStatus.badRequest( { message: "Unexistant multi-link '" + path + "'." } ) ;
			}

			if ( ! value || ! value._ || ! ( value._ instanceof Document ) ) {
				throw ErrorStatus.badRequest( { message: "Provided value is not a Document." } ) ;
			}

			document = value._ ;

			if ( document.collection.name !== schema.collection ) {
				throw ErrorStatus.badRequest( {
					message: "Provided document is not part of collection '" + schema.collection + "' but '" + document.collection.name + "'."
				} ) ;
			}

			dbValues = tree.path.get( this.raw , path ) ;
			dbValue = { _id: document.getId() } ;

			// Populate it now!
			this.populatedDocumentProxies.set( dbValue , document.proxy ) ;

			dbValues.push( dbValue ) ;

			// Useless: tree.path.get does not clone
			//tree.path.set( this.raw , path , ids ) ;

			// Stage the change
			if ( ! this.localPatch ) { this.localPatch = {} ; }
			// Not sure if mongoDB support adding a new array element by referencing the next array index, so we stage the whole array
			this.localPatch[ path ] = dbValues ;

			return ;

		case 'backLink' :
			throw new Error( 'Not coded ATM' ) ;

		default :
			throw ErrorStatus.badRequest( { message: "Unsupported link type '" + schema.type + "' for method .addLink() at path '" + path + "'." } ) ;
	}
} ;



// Remove one link from a multi-link
METHODS.add( 'removeLink' ) ;
Document.prototype.removeLink = function( path , value ) {
	var schema , dbValue , key ;

	try {
		schema = doormen.path( this.collection.documentSchema , path ) ;
	}
	catch ( error ) {
		throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
	}

	switch ( schema.type ) {
		case 'link' :
			if ( typeof schema.collection !== 'string' ) {
				throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
			}

			tree.path.set( this.raw , path , null ) ;

			// Stage the change
			if ( ! this.localPatch ) { this.localPatch = {} ; }
			this.localPatch[ path ] = null ;

			return ;

		case 'multiLink' :
			if ( typeof schema.collection !== 'string' ) {
				throw ErrorStatus.badRequest( { message: "Unexistant multi-link '" + path + "'." } ) ;
			}

			if ( value === undefined ) {
				dbValue = [] ;
			}
			else {
				if ( ! value || ! value._ || ! ( value._ instanceof Document ) ) {
					throw ErrorStatus.badRequest( { message: "Provided value is not a Document." } ) ;
				}

				let document = value._ ;

				if ( document.collection.name !== schema.collection ) {
					throw ErrorStatus.badRequest( {
						message: "Provided document is not part of collection '" + schema.collection + "' but '" + document.collection.name + "'."
					} ) ;
				}

				key = document.getKey() ;
				dbValue = tree.path.get( this.raw , path ).filter( e => '' + e._id !== key ) ;
			}

			tree.path.set( this.raw , path , dbValue ) ;

			// Stage the change
			if ( ! this.localPatch ) { this.localPatch = {} ; }
			// Not sure if mongoDB support adding a new array element by referencing the next array index, so we stage the whole array
			this.localPatch[ path ] = dbValue ;

			return ;

		case 'backLink' :
			throw new Error( 'Not coded ATM' ) ;

		default :
			throw ErrorStatus.badRequest( { message: "Unsupported link type '" + schema.type + "' for .removeLink() at path '" + path + "'." } ) ;
	}
} ;



/* Attachments */



METHODS.add( 'getAttachmentDetails' ) ;
Document.prototype.getAttachmentDetails = function( path , schemaFromGetLinkDetails ) {
	var metaData , schema ;

	if ( schemaFromGetLinkDetails ) {
		schema = schemaFromGetLinkDetails ;
	}
	else {
		try {
			schema = doormen.path( this.collection.documentSchema , path ) ;
		}
		catch ( error ) {
			return null ;
		}

		if ( schema.type !== 'attachment' ) { return null ; }
	}

	// For instance, it's the same than .getLink() for attachment...
	metaData = tree.path.get( this.raw , path ) ;

	if ( ! metaData ) { return { type: schema.type , attachment: null } ; }
	//return ErrorStatus.notFound( { message: "Link not found." } ) ;

	return {
		type: schema.type ,
		schema: schema ,
		//hostCollection: this.collection.name ,
		hostPath: path ,
		//hostId: this.meta.id
		attachment: this.restoreAttachment( metaData )
	} ;
} ;



METHODS.add( 'getAttachment' ) ;
Document.prototype.getAttachment = function( path , schemaFromGetLink ) {
	var schema , metaData ;

	if ( schemaFromGetLink ) {
		schema = schemaFromGetLink ;
	}
	else {
		try {
			schema = doormen.path( this.collection.documentSchema , path ) ;
		}
		catch ( error ) {
			throw ErrorStatus.badRequest( { message: "Unexistant attachment '" + path + "'." } ) ;
		}

		if ( schema.type !== 'attachment' ) {
			throw ErrorStatus.badRequest( { message: "Property is not an attachment '" + path + "'." } ) ;
		}
	}

	metaData = tree.path.get( this.raw , path ) ;

	if ( ! metaData ) {
		throw ErrorStatus.notFound( { message: "Attachment not found '" + path + "'." } ) ;
	}

	return this.restoreAttachment( metaData ) ;
} ;



METHODS.add( 'setAttachment' ) ;
Document.prototype.setAttachment = async function( path , attachment ) {
	var schema , details , exported ;

	if ( attachment === null ) {
		log.debug( "Attachment: checkpoint DDD .setLink() used to delete attachment" ) ;
		return this.removeAttachment( path ) ;
	}

	if ( ! ( attachment instanceof rootsDb.Attachment ) ) {
		throw new Error( '[roots-db] This link needs an Attachment instance' ) ;
	}

	try {
		schema = doormen.path( this.collection.documentSchema , path ) ;
	}
	catch ( error ) {
		throw ErrorStatus.badRequest( { message: "Unexistant attachment '" + path + "'." } ) ;
	}

	if ( schema.type !== 'attachment' ) {
		throw ErrorStatus.badRequest( { message: "Property is not an attachment '" + path + "'." } ) ;
	}

	log.debug( "Attachment debug: checkpoint D %I %I" , path , attachment ) ;

	details = tree.path.get( this.raw , path , null ) ;

	if ( details ) {
		log.debug( "Attachment: checkpoint DD .setLink(): there is already an existing attachment that should be deleted" ) ;
		await this.removeAttachment( path ) ;
	}

	exported = attachment.export() ;
	tree.path.set( this.raw , path , exported ) ;

	// Stage the change
	if ( ! this.localPatch ) { this.localPatch = {} ; }
	this.localPatch[ path ] = exported ;
} ;



METHODS.add( 'removeAttachment' ) ;
Document.prototype.removeAttachment = async function( path ) {
	var schema , details ;

	try {
		schema = doormen.path( this.collection.documentSchema , path ) ;
	}
	catch ( error ) {
		throw ErrorStatus.badRequest( { message: "Unexistant attachment '" + path + "'." } ) ;
	}

	if ( schema.type !== 'attachment' ) {
		throw ErrorStatus.badRequest( { message: "Property is not an attachment '" + path + "'." } ) ;
	}

	details = tree.path.get( this.raw , path , null ) ;

	if ( ! details ) { return ; }

	await fs.unlinkAsync( this.collection.attachmentUrl + this.meta.id + '/' + details.id )
		// Tmp
		.catch( error => {
			log.error( '[roots-db] .removeAttachment(): %E' , error ) ;
		} ) ;

	log.debug( "Attachment: checkpoint DDD .removeAttachment()" ) ;
	tree.path.set( this.raw , path , null ) ;

	// Stage the change
	if ( ! this.localPatch ) { this.localPatch = {} ; }
	this.localPatch[ path ] = null ;
} ;



METHODS.add( 'createAttachment' ) ;
Document.prototype.createAttachment = function( metaData , incoming ) {
	metaData.collectionName = this.collection.name ;
	metaData.documentId = '' + this.meta.id ;
	metaData.id = '' + this.collection.createId() ;
	metaData.baseUrl = this.collection.attachmentUrl ;

	if ( ! metaData.filename ) { metaData.filename = '' + this.meta.id ; }
	if ( ! metaData.contentType ) { metaData.contentType = 'application/octet-stream' ; }

	return new rootsDb.Attachment( metaData , incoming ) ;
} ;



// Restore an Attachment instance from the DB
// Internal
Document.prototype.restoreAttachment = function( metaData ) {
	metaData.collectionName = this.collection.name ;
	metaData.documentId = '' + this.meta.id ;
	metaData.baseUrl = this.collection.attachmentUrl ;

	return new rootsDb.Attachment( metaData ) ;
} ;



// Clear all attachments files for this document
// Internal
Document.prototype.clearAttachments = function() {
	if ( ! this.collection.attachmentUrl || ! this.meta.id ) { return Promise.resolved ; }
	return deltree( this.collection.attachmentUrl + this.meta.id ) ;
} ;



/*
	Internal.
	It saves multiple streams of attachment at once, called by .save() and .patch()
	with the 'attachmentStreams' option set to an AttachmentStreams instance.
	Streams are saved one at a time: it is designed to works with HTTP Multipart.
*/
Document.prototype.saveAttachmentStreams = async function( attachmentStreams ) {
	var index = -1 ;

	const conditionFn = () => {
		index ++ ;

		if ( index < attachmentStreams.list.length ) {
			return true ;
		}
		else if ( attachmentStreams.ended ) {
			return false ;
		}

		return new Promise( ( resolve , reject ) => {
			attachmentStreams.once( 'attachment' , () => {
				log.debug( "Attachment: documentSaveAttachmentStreams() 'attachment' event" ) ;
				resolve( true ) ;
			} ) ;

			attachmentStreams.once( 'end' , () => {
				log.debug( "Attachment: documentSaveAttachmentStreams() 'end' event" ) ;
				resolve( false ) ;
			} ) ;
		} ) ;
	} ;


	const doFn = async () => {
		log.debug( ".saveAttachmentStreams() path: %s" , attachmentStreams.list[ index ].documentPath ) ;

		var details = this.getAttachmentDetails( attachmentStreams.list[ index ].documentPath ) ;

		if ( ! details ) {
			// unexistant link, drop it now
			log.debug( "documentSaveAttachmentStreams: unexistant link" ) ;
			attachmentStreams.list[ index ].stream.resume() ;
			throw new Error( "documentSaveAttachmentStreams: unexistant link" ) ;
		}

		var attachment = this.createAttachment( attachmentStreams.list[ index ].metaData , attachmentStreams.list[ index ].stream ) ;

		log.debug( "Attachment: checkpoint B" ) ;

		try {
			await this.setAttachment( attachmentStreams.list[ index ].documentPath , attachment ) ;
		}
		catch ( error ) {
			// setAttachment failed, so drop it now
			log.error( "documentSaveAttachmentStreams: %E" , error ) ;
			attachmentStreams.list[ index ].stream.resume() ;
			throw error ;
		}

		log.debug( "Attachment: checkpoint C" ) ;

		return attachment.save() ;
	} ;


	while ( await conditionFn() ) {
		await doFn() ;
	}

	log.debug( ".saveAttachmentStreams() done" ) ;
} ;



// External and internal call
Document.prototype.populate = function( paths , options = {} , population = null ) {
	if ( ! population ) {
		population = new Population( this.collection.world , options ) ;
	}

	this.preparePopulate( paths , population , options ) ;
	//console.log( population.populate.targets ) ;
	//console.log( population.populate.refs ) ;
	return this.collection.world.populate( population , options ) ;
} ;



// Can be called by the deep-population algorithm too
Document.prototype.preparePopulate = function( paths , population , options ) {
	var details , populated , populating , documentProxy , documentProxies , dbValue ;

	if ( ! Array.isArray( paths ) ) { paths = [ paths ] ; }
	population.prepare() ;

	population.cache.addProxy( this.collection.name , this.proxy , options.noReference ) ;

	paths.forEach( path => {
		populated = this.populated[ path ] ;
		populating = this.populating[ path ] ;

		// This path was already populated, and no deepPopulation can occur
		if ( populating || ( populated && ! options.deepPopulate ) ) { return ; }

		// Mark the document as currently populating that path NOW!
		// So we will not try to populate recursively the same document for the same path
		this.populating[ path ] = true ;

		details = this.getLinkDetails( path ) ;

		// If there is no such link, skip it now! Not sure if it should raise an error or not...
		if ( ! details ) { return ; }

		switch ( details.type ) {
			case 'link' :
				if ( populated ) {
					if ( options.deepPopulate[ details.foreignCollection ] ) {
						// Get the already populated document and deep-populate it
						dbValue = tree.path.get( this.raw , path ) ;
						documentProxy = this.populatedDocumentProxies.get( dbValue ) ;
						documentProxy._.preparePopulate( options.deepPopulate[ details.foreignCollection ] , population , options ) ;
					}
					break ;
				}

				if ( details.foreignId ) {
					this.preparePopulateOneRef( details.hostPath , details.foreignId , details , population , options ) ;
				}
				break ;

			case 'multiLink' :
				if ( populated ) {
					if ( options.deepPopulate[ details.foreignCollection ] ) {
						// Get the already populated documents and deep-populate them
						dbValue = tree.path.get( this.raw , path ) ;
						documentProxies = dbValue.map( oneDbValue => this.populatedDocumentProxies.get( oneDbValue ) ) ;
						documentProxies.forEach( documentProxy_ => {
							documentProxy_._.preparePopulate( options.deepPopulate[ details.foreignCollection ] , population , options ) ;
						} ) ;
					}
					break ;
				}

				details.foreignIds.forEach( ( foreignId , index ) => {
					this.preparePopulateOneRef( details.hostPath + '[' + index + ']' , foreignId , details , population , options ) ;
				} ) ;
				break ;

			case 'backLink' :
				if ( populated ) {
					if ( options.deepPopulate[ details.foreignCollection ] ) {
						// Get the already populated documents and deep-populate them
						dbValue = tree.path.get( this.raw , path ) ;
						documentProxies = this.populatedDocumentProxies.get( dbValue ) ;
						documentProxies.forEach( documentProxy_ => {
							documentProxy_._.preparePopulate( options.deepPopulate[ details.foreignCollection ] , population , options ) ;
						} ) ;
					}
					break ;
				}

				// Check that the hostPath is already an object
				dbValue = tree.path.get( this.raw , details.hostPath ) ;

				if ( ! dbValue || typeof dbValue !== 'object' ) {
					dbValue = {} ;
					tree.path.set( this.raw , details.hostPath , dbValue ) ;
				}

				this.preparePopulateOneBackRef( details.hostPath , details , population , options ) ;
				break ;
		}
	} ) ;
} ;



// Prepare populate for one item using one foreign ID
Document.prototype.preparePopulateOneRef = function( hostPath , foreignId , details , population , options ) {
	var target , documentProxy ;

	// Try to get it out of the cache
	if ( ( documentProxy = population.cache.getProxy( details.foreignCollection , foreignId , options.noReference ) ) ) {
		log.debug( "Document#preparePopulateOneRef(): Cache hit for collection '%s' , id: %s" , details.foreignCollection , '' + foreignId ) ;

		// Populate it now!
		target = tree.path.get( this.raw , hostPath ) ;
		this.populatedDocumentProxies.set( target , documentProxy ) ;

		this.populated[ hostPath ] = true ;
		this.populating[ hostPath ] = false ;

		if ( options.deepPopulate && options.deepPopulate[ details.foreignCollection ] ) {
			documentProxy._.preparePopulate( options.deepPopulate[ details.foreignCollection ] , population , options ) ;
		}

		return ;
	}

	log.debug( "Document#preparePopulateOneRef(): Cache miss for collection '%s' , id: %s " , details.foreignCollection , '' + foreignId ) ;

	population.populate.targets.push( {
		hostDocument: this.proxy ,
		hostPath: hostPath ,
		foreignCollection: details.foreignCollection ,
		foreignId: foreignId
	} ) ;

	if ( ! population.populate.refs[ details.foreignCollection ] ) {
		population.populate.refs[ details.foreignCollection ] = new Set() ;
	}

	population.populate.refs[ details.foreignCollection ].add( '' + foreignId ) ;
} ;



// Prepare populate for multiples items using one foreign query
Document.prototype.preparePopulateOneBackRef = function( hostPath , details , population , options ) {
	//var documentProxy ;

	/*
		The cache is useless here...
		Even if it would cache-hit, we cannot be sure if we have all the result set anyway.

		/!\ Except for unique fingerprint, someday when MemoryModel will support search /!\
	*/

	population.populate.complexTargets.push( {
		hostDocument: this.proxy ,
		hostPath: hostPath ,
		foreignCollection: details.foreignCollection ,
		foreignPath: details.foreignPath ,
		foreignValue: this.meta.id
	} ) ;

	if ( ! population.populate.complexRefs[ details.foreignCollection ] ) {
		population.populate.complexRefs[ details.foreignCollection ] = {} ;
		population.populate.complexRefs[ details.foreignCollection ][ details.foreignPath ] = {} ;
	}
	else if ( ! population.populate.complexRefs[ details.foreignCollection ][ details.foreignPath ] ) {
		population.populate.complexRefs[ details.foreignCollection ][ details.foreignPath ] = {} ;
	}

	// This looks redundant, but it ensures uniqness of IDs:
	// MongoDB IDs are indeed objects, so a Set() would happily add different objects equal to the same key.
	population.populate.complexRefs[ details.foreignCollection ][ details.foreignPath ][ '' + this.meta.id ] = this.meta.id ;

	//log.fatal( "%I" , this.meta.id ) ;
} ;

