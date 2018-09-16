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

const Promise = require( 'seventh' ) ;
const DeepProxy = require( 'nested-proxies' ) ;
const ErrorStatus = require( 'error-status' ) ;
const doormen = require( 'doormen' ) ;
const tree = require( 'tree-kit' ) ;

const fs = require( 'fs' ) ;
const deltree = Promise.promisify( require( 'fs-kit' ).deltree ) ;

const log = require( 'logfella' ).global.use( 'roots-db' ) ;



const METHODS = new Set() ;
const PROXY_OPTIONS = { pathArray: true } ;



const PROXY_HANDLER = {
	get: function( target , property , receiver , path ) {
		var proto ;
		
		console.log( "path" , path ) ;
		if ( path.length > 1 ) {
			// We are already in an embedded object
			if ( target[ property ] && typeof target[ property ] === 'object' ) {
				proto = Object.getPrototypeOf( target[ property ] ) ;
				console.log( "Proto:" , proto ) ;
				
				if ( this.root.target.collection.immutables.has( proto ) ) {
					console.log( ">>> Immutable" ) ;
					return target[ property ] ;
				}
				else {
					return this.nested( property ) ;
				}
			}
			
			return target[ property ] ;
		}

		// Data-only access (for data using a reserved name, like .save, etc)
		if ( property === '$' ) { return this.nested( '$' , target.raw ) ; }
		
		// Wrapper access
		if ( property === '_' ) { return target ; }
		
		// This is a document method
		if ( METHODS.has( property ) ) {
			if ( target[ property ] === Document.prototype[ property ] ) {
				// Time to bind the function
				console.log( ">>>> Bind!" , property ) ;
				target[ property ] = target[ property ].bind( target ) ;
			}
			
			return target[ property ] ;
		}
		
		//if ( typeof Object.prototype[ property ] === 'function' ) {
		if ( Object.prototype[ property ] ) {	// There are only function, AFAICT
			console.log( ">>>> runtime Bind!" , property ) ;
			return target.raw[ property ].bind( target.raw ) ;
		}

		if ( target.raw[ property ] && typeof target.raw[ property ] === 'object' ) {
			proto = Object.getPrototypeOf( target.raw[ property ] ) ;
			console.log( "Proto:" , proto ) ;
			
			if ( target.collection.immutables.has( proto ) ) {
				console.log( ">>> Immutable" ) ;
				return target.raw[ property ] ;
			}
			else {
				return this.nested( property , target.raw[ property ] ) ;
			}
		}

		return target.raw[ property ] ;
	} ,
	set: function( target , property , value , receiver , path ) {
		if ( path.length > 1 ) {
			if ( target[ property ] === value ) { return true ; }
			target[ property ] = value ;
			if ( path[ 0 ] === '$' ) { path.slice( 1 ) ; }
			this.root.target.setLocalPatch( path , value ) ;
		}
		else {
			if ( target.raw[ property ] === value ) { return true ; }
			target.raw[ property ] = value ;
			if ( path[ 0 ] === '$' ) { path.slice( 1 ) ; }
			this.root.target.setLocalPatch( path , value ) ;
		}
		
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
		Reflect.has( target , property ) : Reflect.has( target.raw , property ) ,
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
	// this.staged = {} ;	// obsolete?
	
	// /!\ Turn that to null?
	this.populated = {} ;
	this.populating = {} ;

	// Meta is a sub-object, because it could be shared when a doc is sundenly linked to a cached one
	this.meta = {
		id: collection.driver.checkId( rawDocument , true ) ,
		upstreamExists: false ,
		loaded: false ,
		saved: false ,
		deleted: false
	} ;

	if ( options.fromUpstream ) {
		this.meta.loaded = true ;
		this.meta.upstreamExists = true ;
	}
	
	this.proxy = new DeepProxy( this , PROXY_HANDLER , PROXY_OPTIONS ) ;
	
	// This is to provide some unambiguous way to access the non-proxy document whatever we got
	this._ = this ;
	
	if ( ! options.noHook && ! options.fromUpstream ) {
		collection.hooks.afterCreateDocument.forEach( hook => hook( this.proxy ) ) ;
	}
}

module.exports = Document ;



METHODS.add( 'getId' ) ;
Document.prototype.getId = function() { return this.meta.id ; } ;



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
			return ;
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
		console.log( "\n\n???????????  FULL SAVE\n\n" ) ;
		return this.collection.driver.update( this.meta.id , this.raw ).then( () => {
			this.meta.saved = true ;
			this.localPatch = null ;
			//this.staged = {} ;
		} ) ;
	}
	
	if ( options.overwrite ) {
		// overwrite wanted
		console.log( "\n\n???????????  OVERWRITE SAVE\n\n" ) ;
		return this.collection.driver.overwrite( this.raw ).then( () => {
			this.meta.saved = true ;
			this.meta.upstreamExists = true ;
			this.localPatch = null ;
			//this.staged = {} ;
		} ) ;
	}

	// create (insert) needed
	console.log( "\n\n???????????  CREATE SAVE\n\n" ) ;
	return this.collection.driver.create( this.raw ).then( () => {
		this.meta.saved = true ;
		this.meta.upstreamExists = true ;
		this.localPatch = null ;
		//this.staged = {} ;
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
	else if ( ! this.meta.upstreamExists ) { throw new Error( 'Cannot lock a document that does not exist upstream yet' ) ; }
	else if ( this.meta.deleted ) { throw new Error( 'Current document is deleted' ) ; }

	return this.collection.driver.lockById( this.meta.id , this.collection.lockTimeout ) ;
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



// On relevant when directly accessing .raw without proxying to it
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
	var empty = true ;

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
			//this.collection.validate( this.raw ) ;
			//this.collection.validatePatch( this.staged ) ;
			this.collection.validateAndUpdatePatch( this.raw , this.staged ) ;
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
		await this.saveAttachmentStreams( options.attachmentStreams , { stage: true } ) ;
		log.debug( "Attachment: checkpoint F2" ) ;
	}

	// No patch, nothing to do
	if ( ! this.localPatch ) { return ; }
	
	// Simple patch
	return this.collection.driver.patch( this.meta.id , this.localPatch )
		.then( () => {
			this.meta.saved = true ;
			this.localPatch = null ;
		} )
} ;



/* Links */



METHODS.add( 'getLinkDetails' ) ;
Document.prototype.getLinkDetails = function( path ) {
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

			metaData = tree.path.get( this.raw , path ) ;

			return {
				type: schema.type ,
				schema: schema ,
				//hostCollection: this.collection.name ,
				hostPath: path ,
				//hostId: this.meta.id

				foreignCollection: schema.collection ,
				foreignId: metaData
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
				foreignIds: metaData
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

		default :
			return null ;
	}
} ;



// Missing: 'attachment' type
/*
	Options:
		* multi `boolean`
			* true: abort with an error if the link would return a single document,
			* false: abort if the link would return a batch
*/
METHODS.add( 'getLink' ) ;
Document.prototype.getLink = function( path , options = {} ) {
	var schema , foreignCollection , foreignSchema , targetId , targetIds , metaData , fingerprint , query ;

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

			targetId = tree.path.get( this.raw , path ) ;

			if ( ! targetId ) {
				throw ErrorStatus.notFound( { message: "Link not found." } ) ;
			}

			return foreignCollection.get( targetId , options ) ;

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

			targetIds = tree.path.get( this.raw , path ) ;

			if ( ! Array.isArray( targetIds ) ) {
				// Unlike links that can eventually be null, a multiLink MUST be an array
				targetIds = [] ;
				tree.path.set( this.raw , path , targetIds ) ;
				// Let .multiGet() handle the empty batch
			}

			return foreignCollection.multiGet( targetIds , options ) ;

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
				fingerprint[ schema.path ] = this.meta.id ;
				return foreignCollection.collect( fingerprint , options ) ;
			}
			
			if ( foreignSchema.type === 'multiLink' ) {
				query = {} ;
				query[ schema.path ] = { $in: [ this.meta.id ] } ;
				return foreignCollection.find( query , options ) ;
			}
			
			throw ErrorStatus.badRequest( { message: "Back-link: targeted property '" + schema.path + "' of the foreign collection '" + schema.collection + "' is neither a link or a multiLink." } ) ;

		case 'attachment' :
			if ( options.noAttachment || options.multi === true ) {
				callback( ErrorStatus.notFound( { message: "Unexpected attachment link." } ) ) ;
				return ;
			}

			metaData = tree.path.get( this.raw , path ) ;

			if ( ! metaData ) {
				callback( ErrorStatus.notFound( { message: "Attachment link not found." } ) ) ;
				return ;
			}

			callback( undefined , this.restoreAttachment( metaData ) ) ;
			return ;

		default :
			throw ErrorStatus.badRequest( { message: "Property is not a link/attachment '" + path + "'." } ) ;
	}

} ;



// Missing: 'attachment' type
/*
	Set a link, and mark it as staged.
*/
METHODS.add( 'setLink' ) ;
Document.prototype.setLink = function( path , value ) {
	var schema , document , details , id , ids , exported ;

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
			
			let document = value._ ;
			
			if ( document.collection.name !== schema.collection ) {
				throw ErrorStatus.badRequest( {
					message: "Provided document is not part of collection '" + schema.collection + "' but '" + document.collection.name + "'."
				} ) ;
			}
			
			id = document.getId() ;
			tree.path.set( this.raw , path , id ) ;

			// Stage the change
			if ( ! this.localPatch ) { this.localPatch = {} ; }
			this.localPatch[ path ] = id ;

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
				
				ids = value.map( document => document.getId() ) ;
			}
			else if ( Array.isArray( value ) ) {
				ids = value.map( documentOrRaw => {
					if ( ! documentOrRaw || typeof documentOrRaw !== 'object' ) {
						throw ErrorStatus.badRequest( { message: "Non-document provided in the multiLink array." } ) ;
					}
					
					if ( ( documentOrRaw instanceof Document ) || ( documentOrRaw._ instanceof Document ) ) {
						if ( documentOrRaw._.collection.name !== schema.collection ) {
							throw ErrorStatus.badRequest( {
								message: "Some provided document in the multiLink array are not part of collection '" + schema.collection + "'."
							} ) ;
						}
						
						return documentOrRaw.getId() ;
					}
					
					var id = documentOrRaw[ this.collection.driver.idKey ] ;
					
					if ( id === undefined || id === null ) {
						throw ErrorStatus.badRequest( { message: "Some provided raw document in the multiLink array have no ID." } ) ;
					}
					
					return id ;
				} ) ;
			}
			else {
				throw ErrorStatus.badRequest( { message: "Bad multiLink argument." } ) ;
			}

			tree.path.set( this.raw , path , ids ) ;

			// Stage the change
			if ( ! this.localPatch ) { this.localPatch = {} ; }
			this.localPatch[ path ] = ids ;

			return  ;

		case 'backLink' :
			throw new Error( 'Not coded ATM' ) ;

		case 'attachment' :
			log.debug( "Attachment debug: checkpoint D %I %I" , path , value ) ;

			details = tree.path.get( this.raw , path , null ) ;

			if ( details ) {
				log.debug( "Attachment: checkpoint DD .setLink(): there is already an existing attachment that should be deleted" ) ;

				// /!\ This function is synchronous ATM!!! /!\
				// This is the only use-case featuring asyncness, keep the whole function sync and ignore errors???

				fs.unlink( this.collection.attachmentUrl + this.meta.id + '/' + details.id , ( error ) => {
					if ( error ) { log.error( '[roots-db] .setLink()/attachment/unlink: %E' , error ) ; }
				} ) ;
			}

			if ( value === null ) {
				log.debug( "Attachment: checkpoint DDD .setLink() used to delete attachment" ) ;
				tree.path.set( this.raw , path , null ) ;

				// Stage the change
				this.staged[ path ] = null ;
				this.hasLocalChanges = true ;
			}
			else if ( ! ( value instanceof rootsDb.Attachment ) ) {
				throw new Error( '[roots-db] This link needs an Attachment instance' ) ;
			}
			else {
				exported = value.export() ;
				tree.path.set( this.raw , path , exported ) ;

				// Stage the change
				this.staged[ path ] = exported ;
				this.hasLocalChanges = true ;
			}

			return ;

		default :
			throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
	}
} ;



// Add one link to a multi-link
METHODS.add( 'addLink' ) ;
Document.prototype.addLink = function( path , value ) {
	var schema , ids ;

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
			
			let document = value._ ;
			
			if ( document.collection.name !== schema.collection ) {
				throw ErrorStatus.badRequest( {
					message: "Provided document is not part of collection '" + schema.collection + "' but '" + document.collection.name + "'."
				} ) ;
			}
			
			ids = tree.path.get( this.raw , path ) ;
			ids.push( document.getId() ) ;
			
			// Useless: tree.path.get does not clone
			//tree.path.set( this.raw , path , ids ) ;

			return ;

		case 'backLink' :
			throw new Error( 'Not coded ATM' ) ;

		default :
			throw ErrorStatus.badRequest( { message: "Unsupported link type '" + schema.type + "' at path '" + path + "'." } ) ;
	}

} ;



// Remove one link from a multi-link
METHODS.add( 'removeLink' ) ;
Document.prototype.removeLink = function( path , value ) {
	var schema , ids ;

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
			
			let document = value._ ;
			
			if ( document.collection.name !== schema.collection ) {
				throw ErrorStatus.badRequest( {
					message: "Provided document is not part of collection '" + schema.collection + "' but '" + document.collection.name + "'."
				} ) ;
			}
			
			ids = tree.path.get( this.raw , path ).filter( id => id.toString() !== document.getId().toString() ) ;
			tree.path.set( this.raw , path , ids ) ;

			return ;

		case 'backLink' :
			throw new Error( 'Not coded ATM' ) ;

		default :
			throw ErrorStatus.badRequest( { message: "Unsupported link type '" + schema.type + "' at path '" + path + "'." } ) ;
	}

} ;



/* Attachments */



Document.prototype.clearAttachments = function() {
	if ( ! this.collection.attachmentUrl || ! this.meta.id ) { return Promise.resolved ; }
	return deltree( this.collection.attachmentUrl + this.meta.id ) ;
} ;



return ;



// First pass
DocumentWrapper.prototype.populate = function( paths , options = {} ) {
	this.preparePopulate( paths , options ) ;
	return this.collection.world.populate( options ) ;
} ;



// First pass in progress
DocumentWrapper.prototype.preparePopulate = function preparePopulate( paths , options ) {
	var i , iMax , j , jMax , details , populated , populating , documents ;

	if ( ! options.populateData ) {
		options.populateData = {
			targets: [] , refs: {} , complexTargets: [] , complexRefs: {}
		} ;
	}

	if ( ! Array.isArray( paths ) ) { paths = [ paths ] ; }

	if ( ! options.cache ) {
		// The cache creation is forced here!
		options.cache = this.collection.world.createMemoryModel( { lazy: true } ) ;
		options.cache.add( this.collection.name , this.raw , options.noReference ) ;
	}

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

		// /!\ For instance, it is not possible to populate back-links /!\
		//if ( details.type === 'attachment' || ! details.foreignId ) { return ; }

		switch ( details.type ) {
			case 'link' :
				if ( populated ) {
					if ( options.deepPopulate[ details.foreignCollection ] ) {
						tree.path.get( this.raw , path ).$.preparePopulate( options.deepPopulate[ details.foreignCollection ] , options ) ;
					}
					break ;
				}

				if ( details.foreignId ) {
					this.preparePopulateOneRef( 'set' , details.hostPath , details.foreignId , details , options ) ;
				}
				break ;

			case 'multiLink' :
				if ( populated ) {
					if ( options.deepPopulate[ details.foreignCollection ] ) {
						documents = tree.path.get( this.raw , path ) ;
						for ( j = 0 , jMax = documents.length ; j < jMax ; j ++ ) {
							documents[ j ].$.preparePopulate( options.deepPopulate[ details.foreignCollection ] , options ) ;
						}
					}
					break ;
				}

				for ( j = 0 , jMax = details.foreignIds.length ; j < jMax ; j ++ ) {
					this.preparePopulateOneRef( 'set' , details.hostPath + '[' + j + ']' , details.foreignIds[ j ] , details , options ) ;
				}
				break ;

			case 'backLink' :
				if ( populated ) {
					if ( options.deepPopulate[ details.foreignCollection ] ) {
						documents = tree.path.get( this.raw , path ) ;
						for ( j = 0 , jMax = documents.length ; j < jMax ; j ++ ) {
							documents[ j ].$.preparePopulate( options.deepPopulate[ details.foreignCollection ] , options ) ;
						}
					}
					break ;
				}

				// Force the hostPath to be an empty array now!
				tree.path.set( this.raw , details.hostPath , [] ) ;
				this.preparePopulateOneBackRef( 'set' , details.hostPath , details , options ) ;
				break ;
		}
	} ) ;
} ;



// Prepare populate for one item using one foreign ID
DocumentWrapper.prototype.preparePopulateOneRef = function preparePopulateOneRef( operation , hostPath , foreignId , details , options ) {
	var document ;

	log.debug( "DocumentWrapper#preparePopulateOneRef(): Checking the cache for collection '%s' , id.toString(): %s " , details.foreignCollection , foreignId.toString() ) ;
	// Try to get it out of the cache
	if ( ( document = options.cache.get( details.foreignCollection , foreignId , options.noReference ) ) ) {
		log.debug( 'Populate: cache hit for one link!' ) ;
		tree.path[ operation ]( this.raw , hostPath , document ) ;
		this.populated[ hostPath ] = true ;
		this.populating[ hostPath ] = false ;

		if ( options.deepPopulate && options.deepPopulate[ details.foreignCollection ] ) {
			document.$.preparePopulate( options.deepPopulate[ details.foreignCollection ] , options ) ;
		}

		return ;
	}

	options.populateData.targets.push( {
		operation: operation ,
		hostDocument: this.raw ,
		hostPath: hostPath ,
		foreignCollection: details.foreignCollection ,
		foreignId: foreignId
	} ) ;

	if ( ! options.populateData.refs[ details.foreignCollection ] ) {
		options.populateData.refs[ details.foreignCollection ] = new Set() ;
	}

	options.populateData.refs[ details.foreignCollection ].add( foreignId.toString() ) ;
} ;



// Prepare populate for multiples items using one foreign query
DocumentWrapper.prototype.preparePopulateOneBackRef = function preparePopulateOneBackRef( operation , hostPath , details , options ) {
	//var document ;

	/*
		The cache is useless here...
		Even if it would cache-hit, we cannot be sure if we have all the result set anyway.

		/!\ Except for unique fingerprint, someday when MemoryModel will support search /!\
	*/

	options.populateData.complexTargets.push( {
		operation: operation ,
		hostDocument: this.raw ,
		hostPath: hostPath ,
		foreignCollection: details.foreignCollection ,
		foreignPath: details.foreignPath ,
		foreignValue: this.meta.id
	} ) ;

	if ( ! options.populateData.complexRefs[ details.foreignCollection ] ) {
		options.populateData.complexRefs[ details.foreignCollection ] = {} ;
		options.populateData.complexRefs[ details.foreignCollection ][ details.foreignPath ] = {} ;
	}
	else if ( ! options.populateData.complexRefs[ details.foreignCollection ][ details.foreignPath ] ) {
		options.populateData.complexRefs[ details.foreignCollection ][ details.foreignPath ] = {} ;
	}

	// This looks redundant, but it ensures uniqness of IDs
	options.populateData.complexRefs[ details.foreignCollection ][ details.foreignPath ][ this.meta.id.toString() ] = this.meta.id ;
} ;






/* Attachments */



DocumentWrapper.prototype.createAttachment = function createAttachment( metaData , incoming ) {
	metaData.collectionName = this.collection.name ;
	metaData.documentId = this.meta.id.toString() ;
	metaData.id = this.collection.createId().toString() ;
	metaData.baseUrl = this.collection.attachmentUrl ;

	if ( ! metaData.filename ) { metaData.filename = this.meta.id.toString() ; }
	if ( ! metaData.contentType ) { metaData.contentType = 'application/octet-stream' ; }

	return rootsDb.Attachment.create( metaData , incoming ) ;
} ;



// Restore an Attachment from the DB
DocumentWrapper.prototype.restoreAttachment = function restoreAttachment( metaData ) {
	metaData.collectionName = this.collection.name ;
	metaData.documentId = this.meta.id.toString() ;
	metaData.baseUrl = this.collection.attachmentUrl ;

	return rootsDb.Attachment.create( metaData ) ;
} ;



DocumentWrapper.prototype.saveAttachmentStreams = function documentSaveAttachmentStreams( attachmentStreams , options , callback ) {
	// Function arguments management
	if ( ! options || typeof options !== 'object' ) {
		if ( typeof options === 'function' ) { callback = options ; }
		options = {} ;
	}

	if ( typeof callback !== 'function' ) { callback = noop ; }

	this._saveAttachmentStreams( attachmentStreams , options ).callback( callback ) ;
} ;



// Async function, should be merged with the previous once rootsDB API is full-Promise
DocumentWrapper.prototype._saveAttachmentStreams = async function( attachmentStreams , options ) {
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


	const doFn = () => {

		var linkDetails , attachment ;

		log.debug( "Attachment: checkpoint A" ) ;

		linkDetails = this.getLinkDetails( attachmentStreams.list[ index ].documentPath ) ;

		if ( ! linkDetails ) {
			// unexistant link, drop it now
			log.debug( "documentSaveAttachmentStreams: unexistant link" ) ;
			attachmentStreams.list[ index ].stream.resume() ;
			throw new Error( "documentSaveAttachmentStreams: unexistant link" ) ;
		}

		attachment = linkDetails.attachment ;

		log.debug( "Attachment: checkpoint A2 existing attachment? %I" , attachment ) ;

		// Check if there is already an existing attachment
		if ( ! attachment || ! ( attachment instanceof rootsDb.Attachment ) ) {
			// There is no attachment yet, create a new one
			attachment = this.createAttachment(
				attachmentStreams.list[ index ].metaData ,
				attachmentStreams.list[ index ].stream
			) ;
			log.debug( "Attachment: checkpoint A3 creating a new attachment: %I" , attachment ) ;
		}
		else {
			// There is already an attachment, update it!
			attachment.update(
				attachmentStreams.list[ index ].metaData ,
				attachmentStreams.list[ index ].stream
			) ;
			log.debug( "Attachment: checkpoint A3 updating the attachment: %I" , attachment ) ;
		}

		log.debug( "Attachment: checkpoint B" ) ;

		try {
			this.setLink( attachmentStreams.list[ index ].documentPath , attachment ) ;

			// Stage the link details, if wanted...
			if ( options.stage ) { this.stage( attachmentStreams.list[ index ].documentPath ) ; }
		}
		catch ( error ) {
			// setLink failed, so drop it now
			log.error( "documentSaveAttachmentStreams: %E" , error ) ;
			attachmentStreams.list[ index ].stream.resume() ;
			throw error ;
		}

		log.debug( "Attachment: checkpoint C" ) ;

		//attachmentStreams.list[ index ].attachment = attachment ;
		return Promise.promisify( attachment.save , attachment )() ;
	} ;


	while ( await conditionFn() ) {
		await doFn() ;
	}
} ;


