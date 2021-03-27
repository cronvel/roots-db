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
const Population = require( './Population.js' ) ;

const Promise = require( 'seventh' ) ;
const DeepProxy = require( 'nested-proxies' ) ;
const ErrorStatus = require( 'error-status' ) ;
const doormen = require( 'doormen' ) ;
const tree = require( 'tree-kit' ) ;
const dotPath = tree.dotPath ;

const fs = require( 'fs' ) ;
const deltree = require( 'fs-kit' ).deltree ;

const log = require( 'logfella' ).global.use( 'roots-db' ) ;



const METHODS = new Set() ;
const DEEP_METHODS = new Map() ;

const PROXY_OPTIONS = { pathArray: true , data: {} } ;
const PROXY_OPTIONS_$ = Object.assign( {} , PROXY_OPTIONS , { data: { $: true } } ) ;



const PROXY_HANDLER = {
	get: function( target , property , receiver , path ) {
		var proto , populatedDocProx , deepMethod ,
			subSchema = this.rData ,
			rootTarget = this.root.target ,
			trueTarget = path.length <= 1 ? target.raw : target ;

		// Attachment should be treated differently
		//if ( subSchema.type === 'attachment' ) { console.error( "Attachment detected" ) ; }
		
		//console.log( this.data.$ ? '$' : '.' , path ) ;
		if ( ! this.data.$ && path.length === 1 ) {
			// Data-only access (for data using a reserved name, like .save, etc)
			if ( property === '$' ) {
				if ( ! rootTarget.$proxy ) {
					rootTarget.$proxy = new DeepProxy( rootTarget , PROXY_HANDLER , PROXY_OPTIONS_$ , rootTarget.collection.documentSchema ) ;
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

		// This is a document deep-method
		if ( rootTarget.deepMethods && ( deepMethod = rootTarget.deepMethods.get( property ) ) ) {
			return ( ... args ) => deepMethod( rootTarget , path.slice( 0 , -1 ) , trueTarget , ... args ) ;
		}

		if ( Object.prototype[ property ] ) {	// There are only functions, AFAICT
			return trueTarget[ property ].bind( trueTarget ) ;
		}

		if ( trueTarget[ property ] ) {
			if ( typeof trueTarget[ property ] === 'object' ) {
				// Populate management
				if ( ! this.data.$ && ( populatedDocProx = rootTarget.populatedDocumentProxies.get( trueTarget[ property ] ) ) ) {
					return populatedDocProx ;
				}

				//proto = Object.getPrototypeOf( trueTarget[ property ] ) ;
				//if ( rootTarget.collection.immutables.has( proto ) ) {
				if ( ( trueTarget[ property ]._ instanceof Document ) || rootTarget.collection.immutables.has( Object.getPrototypeOf( trueTarget[ property ] ) ) ) {
					return trueTarget[ property ] ;
				}

				//console.error( ">>> Schema: ", subSchema ) ;
				subSchema = doormen.directSubSchema( subSchema , property ) ;
				//console.error( "  >>> Sub-Schema: ", subSchema ) ;
				return this.nested( property , subSchema , trueTarget[ property ] ) ;
			}
		}

		return trueTarget[ property ] ;
	} ,

	set: function( target , property , passedValue , receiver , path ) {
		var value ,
			rootTarget = this.root.target ,
			trueTarget = path.length <= 1 ? target.raw : target ;

		//if ( passedValue && typeof passedValue === 'object' && ( passedValue._ instanceof Document ) ) {
		if ( passedValue && typeof passedValue === 'object' ) {
			// Populate management, it search for Document instances, that are in fact link, and replace them by the correct dbValue,
			// while setting populated document proxies at the same time.
			value = rootTarget.proxySetRecursiveLinkCheck( passedValue ) ;
		}
		else {
			value = passedValue ;
		}

		// If the new value is the same as the existing one: do nothing.
		// Also if both values 'are equal' in the 'doormen sens': do nothing. It's *VERY* important for versioning, it avoids creating unnecessary versions.
		if (
			trueTarget[ property ] === value ||
			( typeof trueTarget[ property ] === 'object' && typeof value === 'object' && doormen.isEqual( trueTarget[ property ] , value ) )
		) {
			return true ;
		}

		if ( rootTarget.versioning && rootTarget.meta.upstreamExists && ! rootTarget.rawOrigin ) {
			//rootTarget.rawOrigin = tree.clone( rootTarget.raw ) ;
			rootTarget.rawOrigin = tree.extend( { deep: true , immutables: rootTarget.collection.immutables } , {} , rootTarget.raw ) ;
		}

		trueTarget[ property ] = value ;
		rootTarget.addLocalChange( path ) ;

		return true ;
	} ,

	deleteProperty: function( target , property , path ) {
		var rootTarget = this.root.target ,
			trueTarget = path.length <= 1 ? target.raw : target ;

		// First check if there is really something to do
		if ( trueTarget[ property ] === undefined ) { return true ; }

		if ( rootTarget.versioning && rootTarget.meta.upstreamExists && ! rootTarget.rawOrigin ) {
			//rootTarget.rawOrigin = tree.clone( rootTarget.raw ) ;
			rootTarget.rawOrigin = tree.extend( { deep: true , immutables: rootTarget.collection.immutables } , {} , rootTarget.raw ) ;
		}

		rootTarget.addLocalChange( path ) ;
		return Reflect.deleteProperty( trueTarget , property ) ;
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

	this.versioning = !! collection.versioning && collection.world.versionCollection ;

	if ( ! options.fromUpstream && this.versioning ) {
		// Add versioning data, but it will be ultimately modified at save-time, still it provides userland with meaningful values
		rawDocument._version = 1 ;
		rawDocument._lastModified = new Date() ;
	}

	// Then validate the document
	if ( ! options.skipValidation && collection.validateOnCreate ) {
		try {
			rawDocument = collection.validate( rawDocument ) ;
		}
		catch ( error ) {
			error.validatorMessage = error.message ;
			error.message = '[roots-db] validator error: ' + error.message ;
			throw error ;
		}
	}

	this.collection = collection ;
	this.raw = rawDocument ;
	this.localChanges = null ;

	// Store original upstream version to avoid retrieving it on each change.
	// The original is cloned only at the right time
	this.rawOrigin = null ;

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
		lockId: null ,
		syncTime: null	// Last time the data was in sync with the upstream
	} ;

	if ( options.fromUpstream ) {
		this.meta.loaded = true ;
		this.meta.upstreamExists = true ;
		this.meta.syncTime = Date.now() ;
	}

	this.proxy = new DeepProxy( this , PROXY_HANDLER , PROXY_OPTIONS , this.collection.documentSchema ) ;
	this.$proxy = null ;	// Lazily created, this is the proxy for .$

	this.tagMask = null ;
	this.populateTagMask = null ;
	if ( options.tagMask ) { this.setTagMask( options.tagMask ) ; }
	if ( options.populateTagMask ) { this.setPopulateTagMask( options.populateTagMask ) ; }

	this.methods = METHODS ;
	this.deepMethods = DEEP_METHODS ;

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



Document.prototype.addDeepProxyMethod = function( name , method ) {
	if ( this.deepMethods === DEEP_METHODS ) {
		this.deepMethods = new Map( DEEP_METHODS ) ;
	}

	this.deepMethods.add( name , method ) ;
} ;



// Populate management when directly setting through the proxy.
// It should check that no Document are passed (i.e. link), and if it is, it transforms that to a link and add it to populated document proxies
// Assume the argument is already an object.
Document.prototype.proxySetRecursiveLinkCheck = function( passedValue ) {
	var value , key , index , innerValue ;

	//log.hdebug( "proxySetRecursiveLinkCheck" ) ;

	if ( passedValue._ instanceof Document ) {
		//log.hdebug( "    document detected" ) ;
		value = { _id: passedValue._id } ;
		this.populatedDocumentProxies.set( value , passedValue._.proxy ) ;
		return value ;
	}

	if ( this.collection.immutables.has( Object.getPrototypeOf( passedValue ) ) ) {
		//log.hdebug( "    immutable detected" ) ;
		return passedValue ;
	}

	if ( Array.isArray( passedValue ) ) {
		//log.hdebug( "    array detected %Y" , passedValue ) ;
		for ( index = 0 ; index < passedValue.length ; index ++ ) {
			//log.hdebug( "    %s: %Y" , index , passedValue[ index ] ) ;
			if ( passedValue[ index ] && typeof passedValue[ index ] === 'object' ) {
				innerValue = this.proxySetRecursiveLinkCheck( passedValue[ index ] ) ;
				if ( innerValue !== passedValue[ index ] ) {
					if ( ! value ) { value = [ ... passedValue ] ; }
					value[ index ] = innerValue ;
				}
			}
		}

		return value || passedValue ;
	}

	//log.hdebug( "    object detected" ) ;
	for ( key in passedValue ) {
		if ( passedValue[ key ] && typeof passedValue[ key ] === 'object' ) {
			innerValue = this.proxySetRecursiveLinkCheck( passedValue[ key ] ) ;
			if ( innerValue !== passedValue[ key ] ) {
				if ( ! value ) { value = Object.assign( passedValue ) ; }
				value[ key ] = innerValue ;
			}
		}
	}

	return value || passedValue ;
} ;



METHODS.add( 'getId' ) ;
Document.prototype.getId = function() { return this.meta.id ; } ;

METHODS.add( 'getKey' ) ;
Document.prototype.getKey = function() { return '' + this.meta.id ; } ;

METHODS.add( 'hasLocalChanges' ) ;
Document.prototype.hasLocalChanges = function() { return !! this.localChanges ; } ;

// Check if the document validate: throw an error if it doesn't
METHODS.add( 'validate' ) ;
Document.prototype.validate = function() { this.collection.validate( this.raw ) ; } ;



// Return the proxy of a clone
METHODS.add( 'clone' ) ;
Document.prototype.clone = function() {
	var rawClone = rootsDb.misc.clone( this.raw ) ;
	delete rawClone._id ;
	delete rawClone._version ;
	delete rawClone._lastModified ;

	return ( new Document( this.collection , rawClone ) ).proxy ;
} ;



METHODS.add( 'setTagMask' ) ;
Document.prototype.setTagMask = function( tagMask ) {
	if ( tagMask instanceof Set ) {
		this.tagMask = tagMask ;
	}
	else if ( Array.isArray( tagMask ) ) {
		this.tagMask = new Set( tagMask ) ;
	}
	else if ( typeof tagMask === 'string' ) {
		this.tagMask = new Set() ;
		this.tagMask.add( tagMask ) ;
	}
	else if ( tagMask === null ) {
		this.tagMask = null ;
	}
} ;



METHODS.add( 'setPopulateTagMask' ) ;
Document.prototype.setPopulateTagMask = function( populateTagMask ) {
	if ( populateTagMask instanceof Set ) {
		this.populateTagMask = populateTagMask ;
	}
	else if ( Array.isArray( populateTagMask ) ) {
		this.populateTagMask = new Set( populateTagMask ) ;
	}
	else if ( typeof populateTagMask === 'string' ) {
		this.populateTagMask = new Set() ;
		this.populateTagMask.add( populateTagMask ) ;
	}
	else if ( populateTagMask === null ) {
		this.populateTagMask = null ;
	}
} ;



METHODS.add( 'reload' ) ;
Document.prototype.reload = async function( options = {} ) {
	if ( ! this.meta.upstreamExists || this.meta.deleted ) {
		throw new Error( ".reload() upstream does not exist" ) ;
	}

	this.raw = await this.collection.driver.get( this.meta.id ) ;

	if ( ! this.raw ) {
		throw ErrorStatus.notFound( { message: 'Upstream document gone (not found)' } ) ;
	}

	this.meta.upstreamExists = true ;
	this.meta.syncTime = Date.now() ;
	this.meta.deleted = false ;
	this.localChanges = null ;
	this.rawOrigin = null ;
} ;



// Like .reload() but only if .syncTime is too old
METHODS.add( 'refresh' ) ;
Document.prototype.refresh = async function( options = {} ) {
	if ( ! this.meta.syncTime ) {
		throw new Error( ".refresh() upstream have never been in sync" ) ;
	}

	if ( Date.now() - this.meta.syncTime >= this.collection.refreshTimeout ) {
		await this.reload() ;
	}
} ;



METHODS.add( 'save' ) ;
Document.prototype.save = async function( options = {} ) {
	var rawUpstream , done = false ;

	if ( this.meta.deleted ) { throw new Error( 'Current document is deleted' ) ; }

	// Validation, if relevant
	if ( options.validate || ( ! options.skipValidation && this.collection.validateOnSave && this.localChanges ) ) {
		try {
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
	}

	// Save attachments first
	if ( options.attachmentStreams && this.collection.attachmentUrl ) {
		await this.saveAttachmentStreams( options.attachmentStreams ) ;
	}

	// Now the three types of save...

	if ( this.meta.upstreamExists ) {
		// Full save (update)
		if ( this.versioning && this.rawOrigin ) {
			// So we need to backup the existing document

			// Ensure everything is ok
			this.rawOrigin._version = this.rawOrigin._version || 1 ;
			this.rawOrigin._lastModified = this.rawOrigin._lastModified || new Date() ;

			// Create a link to the current document
			this.rawOrigin._activeVersion = { _id: this.meta.id , _collection: this.collection.name } ;

			// Remove _id, it should have its own
			delete this.rawOrigin._id ;

			while ( ! done ) {
				try {
					await this.collection.world.versionCollection.driver.create( this.rawOrigin ) ;
					done = true ;
				}
				catch ( error ) {
					this.forwardError( error , this.collection.world.versionCollection , 'versionDuplicateKey' ) ;
					// This is most probably a race conditions, retry with version ++
					this.rawOrigin._version ++ ;
				}
			}

			this.raw._version = this.rawOrigin._version + 1 ;
			this.raw._lastModified = new Date() ;
			this.rawOrigin = null ;
		}

		try {
			await this.collection.driver.update( this.meta.id , this.raw ) ;
		}
		catch ( error ) {
			this.forwardError( error ) ;
		}

		this.meta.saved = true ;
		this.meta.syncTime = Date.now() ;
		this.localChanges = null ;
	}
	else if ( options.overwrite ) {
		// create (insert/overwrite) wanted
		if ( this.versioning ) {
			// So we need to save the existing document
			rawUpstream = await this.collection.driver.get( this.meta.id ) ;

			if ( rawUpstream ) {
				// Create a link to the current document
				rawUpstream._activeVersion = { _id: this.meta.id , _collection: this.collection.name } ;

				// Remove _id, it should have its own
				delete rawUpstream._id ;

				while ( ! done ) {
					try {
						await this.collection.world.versionCollection.driver.create( rawUpstream ) ;
						done = true ;
					}
					catch ( error ) {
						this.forwardError( error , this.collection.world.versionCollection , 'versionDuplicateKey' ) ;
						// This is most probably a race conditions, retry with version ++
						rawUpstream._version ++ ;
					}
				}

				this.raw._version = rawUpstream._version + 1 ;
			}
			else {
				this.raw._version = 1 ;
			}

			this.raw._lastModified = new Date() ;
			this.rawOrigin = null ;
		}

		try {
			await this.collection.driver.overwrite( this.raw ) ;
		}
		catch ( error ) {
			this.forwardError( error ) ;
		}

		this.meta.saved = true ;
		this.meta.syncTime = Date.now() ;
		this.meta.upstreamExists = true ;
		this.localChanges = null ;
	}
	else {
		// create (insert) needed
		if ( this.versioning ) {
			this.raw._version = 1 ;
			this.raw._lastModified = new Date() ;
		}

		try {
			await this.collection.driver.create( this.raw ) ;
		}
		catch ( error ) {
			this.forwardError( error ) ;
		}

		this.meta.saved = true ;
		this.meta.syncTime = Date.now() ;
		this.meta.upstreamExists = true ;
		this.localChanges = null ;
	}
} ;



METHODS.add( 'delete' ) ;
Document.prototype.delete = async function( options = {} ) {
	if ( this.meta.deleted ) { throw new Error( 'Current document is already deleted' ) ; }

	// Clear attachments first, then call delete() again...
	if ( ! options.dontClearAttachments && this.collection.attachmentUrl ) {
		await this.clearAttachments() ;
	}

	if ( this.meta.upstreamExists && this.versioning ) {
		// So we need to backup the existing document
		if ( ! this.rawOrigin ) {
			// Here we don't need a perfect clone, since we will destroy it anyway, go for the fastest option
			this.rawOrigin = Object.assign( {} , this.raw ) ;
		}

		// Ensure everything is ok
		this.rawOrigin._version = this.rawOrigin._version || 1 ;
		this.rawOrigin._lastModified = this.rawOrigin._lastModified || new Date() ;

		// /!\ Create a dead-link to the current document?
		// This could be useful to actually restore the deleted document
		this.rawOrigin._activeVersion = { _id: this.meta.id , _collection: this.collection.name } ;

		// Remove _id, it should have its own
		delete this.rawOrigin._id ;

		try {
			await this.collection.world.versionCollection.driver.create( this.rawOrigin ) ;
		}
		catch ( error ) {
			this.forwardError( error , this.collection.world.versionCollection ) ;
		}

		this.rawOrigin = null ;
	}

	try {
		await this.collection.driver.delete( this.meta.id ) ;
	}
	catch ( error ) {
		this.forwardError( error ) ;
	}

	this.meta.deleted = true ;
	this.meta.syncTime = Date.now() ;
	this.meta.upstreamExists = false ;
} ;



METHODS.add( 'patch' ) ;
Document.prototype.patch = function( patch , options = {} ) {
	try {
		if ( options.validate || this.collection.validateOnPatch ) {
			if ( options.allowedTags ) {
				patch = doormen.patch( { allowedTags: options.allowedTags } , this.collection.documentSchema , patch ) ;
			}
			else {
				//this.collection.validatePatch( patch ) ;
				patch = doormen.patch( this.collection.documentSchema , patch ) ;
			}
		}
	}
	catch ( error ) {
		error.validatorMessage = error.message ;
		error.message = '[roots-db] validator error: ' + error.message ;
		throw error ;
	}

	// New: now we use doormen's .applyPatch() instead of tree.extend(), since patch now have commands
	//tree.extend( { unflat: true , immutables: this.collection.immutables } , this.proxy.$ , patch ) ;
	doormen.applyPatch( this.proxy.$ , patch ) ;
} ;



// Only relevant when directly accessing .raw without proxying to it
METHODS.add( 'stage' ) ;
Document.prototype.stage = function( paths ) {
	if ( typeof paths === 'string' ) { this.addLocalChange( paths.split( '.' ) ) ; return ; }
	if ( ! Array.isArray( paths ) ) { throw new TypeError( "[roots-db] stage(): argument #0 should be an a string or an array of string" ) ; }
	paths.forEach( path => this.addLocalChange( path.split( '.' ) ) ) ;
} ;



// Commit the whole localChanges/staged stuff to the upstream
METHODS.add( 'commit' ) ;
Document.prototype.commit = async function( options = {} ) {
	var dbPatch , done = false ;

	if ( this.meta.deleted ) { throw new Error( 'Current document is deleted' ) ; }

	// Perform a regular save or throw an error?
	if ( ! this.meta.upstreamExists ) {
		this.localChanges = null ;
		return this.save() ;
	}

	// Validation of STAGED data
	if ( options.validate || ( ! options.skipValidation && this.collection.validateOnCommit && this.localChanges ) ) {
		try {
			// /!\ WARNING: dbPatch.set is incomplete since it does not contains unset
			//log.warning( "/!\\ WARNING: dbPatch.set is incomplete since it does not contain .unset" ) ;
			dbPatch = this.buildDbPatch() ;
			this.collection.validateAndUpdatePatch( this.raw , dbPatch.set ) ;

			// /!\ validateAndUpdatePatch() is not well optimized, it would be better if only constraints were enforced (doormen.checkConstraints())
		}
		catch ( error ) {
			error.validatorMessage = error.message ;
			error.message = '[roots-db] validator error: ' + error.message ;
			throw error ;
		}
	}

	// Save attachments first, then call commit() again...
	if ( options.attachmentStreams ) {
		await this.saveAttachmentStreams( options.attachmentStreams ) ;

		// The dbPatch have changed, rebuild it even if it exists...
		dbPatch = this.buildDbPatch() ;
	}

	// If it was not computed, compute it now!
	if ( dbPatch === undefined ) { dbPatch = this.buildDbPatch() ; }

	// No dbPatch, nothing to do
	if ( ! dbPatch ) { return ; }

	if ( this.versioning && this.rawOrigin ) {
		// So we need to backup the existing document

		// Ensure everything is ok
		this.rawOrigin._version = this.rawOrigin._version || 1 ;
		this.rawOrigin._lastModified = this.rawOrigin._lastModified || new Date() ;

		// Create a link to the current document
		this.rawOrigin._activeVersion = { _id: this.meta.id , _collection: this.collection.name } ;

		// Remove _id, it should have its own
		delete this.rawOrigin._id ;

		while ( ! done ) {
			try {
				await this.collection.world.versionCollection.driver.create( this.rawOrigin ) ;
				done = true ;
			}
			catch ( error ) {
				this.forwardError( error , this.collection.world.versionCollection , 'versionDuplicateKey' ) ;
				// This is most probably a race conditions, retry with version ++
				this.rawOrigin._version ++ ;
			}
		}

		// Don't forget to add _version and _lastModified to the patch
		if ( ! dbPatch.set ) { dbPatch.set = {} ; }
		dbPatch.set._version = this.raw._version = this.rawOrigin._version + 1 ;
		dbPatch.set._lastModified = this.raw._lastModified = new Date() ;
		this.rawOrigin = null ;
	}

	// Simple patch
	try {
		await this.collection.driver.patch( this.meta.id , dbPatch ) ;
	}
	catch ( error ) {
		this.forwardError( error ) ;
	}

	this.meta.saved = true ;
	this.localChanges = null ;

	// This is not a SYNC, only part of the doc was sent
	//this.meta.syncTime = Date.now() ;
} ;



// Internal method, the path MUST BE an Array
Document.prototype.addLocalChange = function( pathArray ) {
	if ( ! pathArray.length ) { return ; }

	var pathPart , pointer , i ,
		iLeaf = pathArray.length - 1 ;

	if ( ! this.localChanges ) { this.localChanges = {} ; }
	pointer = this.localChanges ;

	// Branch navigation (excluding the leaf)
	for ( i = 0 ; i < iLeaf ; i ++ ) {
		pathPart = pathArray[ i ] ;

		// There is already a branch, follow it
		if ( pathPart in pointer ) {
			pointer = pointer[ pathPart ] ;

			// If pointer is falsy, it's past an already existing leaf, so do nothing...
			if ( ! pointer ) { return ; }
		}
		else {
			pointer = pointer[ pathPart ] = {} ;
		}
	}

	pathPart = pathArray[ iLeaf ] ;
	pointer[ pathPart ] = null ;
} ;



Document.prototype.buildDbPatch = function() {
	if ( ! this.localChanges ) { return null ; }
	var dbPatch = {} ;
	recursiveBuildDbPatch( dbPatch , [] , this.localChanges , this.raw ) ;
	return dbPatch ;
} ;



function recursiveBuildDbPatch( dbPatch , path , changeNode , dataNode ) {
	var key ;

	if ( ! changeNode ) {
		if ( dataNode === undefined ) {
			if ( ! dbPatch.unset ) { dbPatch.unset = {} ; }
			dbPatch.unset[ path.join( '.' ) ] = null ;
		}
		else {
			if ( ! dbPatch.set ) { dbPatch.set = {} ; }
			dbPatch.set[ path.join( '.' ) ] = dataNode ;
		}

		return ;
	}

	for ( key in changeNode ) {
		path.push( key ) ;
		recursiveBuildDbPatch(
			dbPatch ,
			path ,
			changeNode[ key ] ,
			dataNode && typeof dataNode === 'object' ? dataNode[ key ] : undefined
		) ;
		path.pop() ;
	}
}



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



/* Links */



METHODS.add( 'getLinkDetails' ) ;
Document.prototype.getLinkDetails = function( path , acceptAttachment ) {
	var metadata , schema , foreignCollection , foreignSchema ;

	try {
		schema = doormen.path( this.collection.documentSchema , path ) ;
	}
	catch ( error ) {
		return null ;
	}

	switch ( schema.type ) {
		case 'link' :
			if ( ( ! schema.collection || typeof schema.collection !== 'string' ) && ! schema.anyCollection ) { return null ; }

			metadata = dotPath.get( this.raw , path ) || null ;

			return {
				type: schema.type ,
				schema: schema ,
				//hostCollection: this.collection.name ,
				hostPath: path ,
				//hostId: this.meta.id

				anyCollection: !! schema.anyCollection ,
				foreignCollection: schema.anyCollection ? ( metadata && metadata._collection ) : schema.collection ,
				foreignId: metadata && metadata._id
			} ;

		case 'multiLink' :
			if ( ! schema.collection || typeof schema.collection !== 'string' ) { return null ; }

			metadata = dotPath.get( this.raw , path ) ;
			if ( ! Array.isArray( metadata ) ) { metadata = [] ; }

			return {
				type: schema.type ,
				schema: schema ,
				//hostCollection: this.collection.name ,
				hostPath: path ,
				//hostId: this.meta.id

				foreignCollection: schema.collection ,
				foreignIds: metadata.map( e => e._id )
			} ;

		case 'backLink' :
			if ( ! schema.collection || typeof schema.collection !== 'string' ) { return null ; }

			foreignCollection = this.collection.world.collections[ schema.collection ] ;
			if ( ! foreignCollection ) { return null ; }

			try {
				foreignSchema = doormen.path( foreignCollection.documentSchema , schema.path ) ;
			}
			catch ( error ) {
				return null ;
			}

			return {
				type: schema.type ,
				schema: schema ,
				//hostCollection: this.collection.name ,
				hostPath: path ,
				//hostId: this.meta.id

				foreignAnyCollection: !! ( foreignSchema.type === 'link' && foreignSchema.anyCollection ) ,
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
	var schema ;

	try {
		schema = doormen.path( this.collection.documentSchema , path ) ;
	}
	catch ( error ) {
		throw ErrorStatus.badRequest( { message: "Unexistant property/link '" + path + "'." } ) ;
	}

	switch ( schema.type ) {
		case 'link' :
			if ( options.multi === true ) {
				throw ErrorStatus.badRequest( { message: "Expecting a multi-link at a single-link path, for collection '" + this.collection.name + "' at path '" + path + "'." } ) ;
			}

			return this.getSingleLink( schema , path , options ) ;

		case 'multiLink' :
			if ( options.multi === false ) {
				throw ErrorStatus.badRequest( { message: "Expecting a link at a multi-link path, for collection '" + this.collection.name + "' at path '" + path + "'." } ) ;
			}

			return this.getMultiLink( schema , path , options ) ;

		case 'backLink' :
			if ( options.multi === false ) {
				throw ErrorStatus.badRequest( { message: "Expecting a link at a back-link path, for collection '" + this.collection.name + "' at path '" + path + "'." } ) ;
			}

			return this.getBackLink( schema , path , options ) ;

		case 'attachment' :
			if ( ! acceptAttachment ) {
				throw ErrorStatus.badRequest( { message: "Property is not a link '" + path + "'." } ) ;
			}

			return this.getAttachment( path , schema ) ;

		default :
			throw ErrorStatus.badRequest( { message: "Property is not a link '" + path + "'." } ) ;
	}
} ;



// Internal
Document.prototype.getSingleLink = async function( schema , path , options ) {
	var foreignCollectionName , foreignCollection , target , documentProxy ;

	target = dotPath.get( this.raw , path ) ;

	if ( ! target || typeof target !== 'object' || ! target._id ) {
		throw ErrorStatus.notFound( { message: "Link not found." } ) ;
	}

	if ( schema.anyCollection ) {
		if ( ! target._collection ) {
			throw ErrorStatus.notFound( { message: "Link on 'anyCollection' without a _collection." } ) ;
		}

		foreignCollectionName = target._collection ;
	}
	else {
		if ( typeof schema.collection !== 'string' ) {
			throw ErrorStatus.internalError( { message: "Link without collection in the schema, for collection '" + this.collection.name + "' at path '" + path + "'." } ) ;
		}

		foreignCollectionName = schema.collection ;
	}

	foreignCollection = this.collection.world.collections[ foreignCollectionName ] ;

	if ( ! foreignCollection ) {
		throw ErrorStatus.badRequest( { message: "Link to an unexistant collection '" + foreignCollectionName + "' at path '" + path + "'." } ) ;
	}

	try {
		documentProxy = await foreignCollection.get( target._id , options ) ;
	}
	catch ( error ) {
		if ( error.type === 'notFound' && ! this.collection.isVersion ) {
			log.warning( "Dead link detected on document %s of collection '%s', path: %s -- fixing it now" , this.getKey() , this.collection.name , path ) ;
			dotPath.set( this.proxy , path , null ) ;
			this.save() ;
		}

		throw error ;
	}

	// Populate it now!
	if ( ! options.raw ) { this.populatedDocumentProxies.set( target , documentProxy ) ; }

	return documentProxy ;
} ;



// Internal
Document.prototype.getMultiLink = async function( schema , path , options ) {
	var foreignCollection , targets , targetIds , batch , dbValue , originalTargetCount ;

	if ( typeof schema.collection !== 'string' ) {
		throw ErrorStatus.internalError( { message: "Multi-link without collection in the schema, for collection '" + this.collection.name + "' at path '" + path + "'." } ) ;
	}

	// No anyCollection support planned for multiLink ATM (too much quirks)
	//if ( schema.anyCollection ) { throw ErrorStatus.badRequest( { message: "Multi-link does not support 'anyCollection'." } ) ; }

	foreignCollection = this.collection.world.collections[ schema.collection ] ;

	if ( ! foreignCollection ) {
		throw ErrorStatus.badRequest( { message: "Multi-link to an unexistant collection '" + schema.collection + "', for collection '" + this.collection.name + "' at path '" + path + "'." } ) ;
	}

	targets = dotPath.get( this.raw , path ) ;
	originalTargetCount = targets.length ;

	if ( ! Array.isArray( targets ) ) {
		// Unlike links that can eventually be null, a multiLink MUST be an array
		targets = [] ;
		dotPath.set( this.raw , path , targets ) ;
		targetIds = [] ;
		// Let .multiGet() handle the empty batch
	}
	else {
		targetIds = targets.map( e => e._id ) ;
	}

	batch = await foreignCollection.multiGet( targetIds , options ) ;

	// Populate it now!
	if ( ! options.raw ) {
		targets = batch.map( docProx => {
			dbValue = { _id: docProx.getId() } ;
			this.populatedDocumentProxies.set( dbValue , docProx ) ;
			return dbValue ;
		} ) ;

		if ( ! this.collection.isVersion ) {
			// Rewrite the multi-link with existing elements
			dotPath.set( this.raw , path , targets ) ;

			if ( originalTargetCount !== targets.length ) {
				log.warning( "Dead link inside multi-link detected on document %s of collection '%s', path: %s -- fixing it now" , this.getKey() , this.collection.name , path ) ;
				this.save() ;
			}
		}
	}

	return batch ;
} ;



// Internal
Document.prototype.getBackLink = async function( schema , path , options ) {
	var foreignCollection , foreignSchema , fingerprint , query , batch , dbValue ;

	if ( typeof schema.collection !== 'string' ) {
		throw ErrorStatus.internalError( { message: "Back-link without collection in the schema, for collection '" + this.collection.name + "' at path '" + path + "'." } ) ;
	}

	foreignCollection = this.collection.world.collections[ schema.collection ] ;

	if ( ! foreignCollection ) {
		throw ErrorStatus.badRequest( { message: "Back-link to an unexistant collection '" + schema.collection + "', for collection '" + this.collection.name + "' at path '" + path + "'." } ) ;
	}

	try {
		foreignSchema = doormen.path( foreignCollection.documentSchema , schema.path ) ;
	}
	catch ( error ) {
		throw ErrorStatus.badRequest( { message: "Back-link: targeted property '" + schema.path + "' of the foreign collection '" + schema.collection + "' is unexistant, for collection '" + this.collection.name + "' at path '" + path + "'." } ) ;
	}


	if ( foreignSchema.type === 'link' ) {
		fingerprint = {} ;
		fingerprint[ schema.path + '._id' ] = this.meta.id ;

		if ( foreignSchema.anyCollection ) {
			fingerprint[ schema.path + '._collection' ] = this.collection.name ;
		}

		batch = await foreignCollection.collect( fingerprint , options ) ;

		// Populate it now!
		if ( ! options.raw ) {
			dbValue = dotPath.get( this.raw , path ) ;

			if ( ! dbValue || typeof dbValue !== 'object' ) {
				dbValue = {} ;
				dotPath.set( this.raw , path , dbValue ) ;
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
			dbValue = dotPath.get( this.raw , path ) ;

			if ( ! dbValue || typeof dbValue !== 'object' ) {
				dbValue = {} ;
				dotPath.set( this.raw , path , dbValue ) ;
			}

			this.populatedDocumentProxies.set( dbValue , batch ) ;
		}

		return batch ;
	}

	throw ErrorStatus.badRequest( { message: "Back-link: targeted property '" + schema.path + "' of the foreign collection '" + schema.collection + "' is neither a link nor a multiLink, for collection '" + this.collection.name + "' at path '" + path + "'." } ) ;
} ;



/*
	Set a link, and mark it as staged.
*/
METHODS.add( 'setLink' ) ;
Document.prototype.setLink = function( path , value ) {
	var schema ;

	try {
		schema = doormen.path( this.collection.documentSchema , path ) ;
	}
	catch ( error ) {
		throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
	}

	switch ( schema.type ) {
		case 'link' :
			return this.setSingleLink( schema , path , value ) ;
		case 'multiLink' :
			return this.setMultiLink( schema , path , value ) ;
		case 'backLink' :
			// Maybe backLink must be read-only anyway
			throw new Error( 'Not coded ATM' ) ;
		case 'attachment' :
			throw new Error( 'Attachment not supported by .setLink()' ) ;
		default :
			throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
	}
} ;



// Internal
Document.prototype.setSingleLink = function( schema , path , value ) {
	var id , document , dbValue ;

	if ( ! value || ! value._ || ! ( value._ instanceof Document ) ) {
		throw ErrorStatus.badRequest( { message: "Provided value is not a Document." } ) ;
	}

	document = value._ ;
	id = document.getId() ;

	if ( schema.anyCollection ) {
		dbValue = { _id: id , _collection: document.collection.name } ;
	}
	else {
		if ( typeof schema.collection !== 'string' ) {
			throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
		}

		if ( document.collection.name !== schema.collection ) {
			throw ErrorStatus.badRequest( {
				message: "Provided document is not part of collection '" + schema.collection + "' but '" + document.collection.name + "'."
			} ) ;
		}

		dbValue = { _id: id } ;
	}

	// Populate it now!
	this.populatedDocumentProxies.set( dbValue , document.proxy ) ;

	dotPath.set( this.raw , path , dbValue ) ;

	// Stage the change
	this.stage( path ) ;
} ;



// Internal
Document.prototype.setMultiLink = function( schema , path , values ) {
	var document , dbValues , keys ;

	if ( typeof schema.collection !== 'string' ) {
		throw ErrorStatus.badRequest( { message: "Unexistant multi-link '" + path + "'." } ) ;
	}

	if ( ! values ) { values = [] ; }

	keys = new Set() ;

	if ( values instanceof rootsDb.Batch ) {
		if ( values.collection.name !== schema.collection ) {
			throw ErrorStatus.badRequest( { message: "Provided batch is not part of collection '" + schema.collection + "' but '" + values.collection.name + "'." } ) ;
		}

		// First, enforce uniqness
		values = values.filter( docProx => {
			var key = docProx.getKey() ;
			if ( keys.has( key ) ) { return false ; }
			keys.add( key ) ;
			return true ;
		} ) ;

		// Then map to dbValues
		dbValues = values.map( docProx => {
			var dbValue = { _id: docProx.getId() } ;
			this.populatedDocumentProxies.set( dbValue , docProx ) ;	// Populate it now!
			return dbValue ;
		} ) ;
	}
	else if ( Array.isArray( values ) ) {
		// First, enforce uniqness
		values = values.filter( oneValue => {
			if ( ! oneValue || ! oneValue._ || ! ( oneValue._ instanceof Document ) ) {
				throw ErrorStatus.badRequest( { message: "Non-document provided in the multiLink array." } ) ;
			}

			var key = oneValue._.getKey() ;
			if ( keys.has( key ) ) { return false ; }
			keys.add( key ) ;
			return true ;
		} ) ;

		// Then map to dbValues
		dbValues = values.map( oneValue => {
			document = oneValue._ ;

			if ( document.collection.name !== schema.collection ) {
				throw ErrorStatus.badRequest( {
					message: "In multiLink array, one document is not part of collection '" + schema.collection + "' but '" + document.collection.name + "'."
				} ) ;
			}

			var dbValue = { _id: document.getId() } ;
			this.populatedDocumentProxies.set( dbValue , document.proxy ) ;	// Populate it now!
			return dbValue ;
		} ) ;
	}
	else {
		throw ErrorStatus.badRequest( { message: "Bad multiLink argument." } ) ;
	}

	dotPath.set( this.raw , path , dbValues ) ;

	// Stage the change
	this.stage( path ) ;
} ;



// Add one link to a multi-link
METHODS.add( 'addLink' ) ;
Document.prototype.addLink = function( path , value ) {
	var schema ;

	try {
		schema = doormen.path( this.collection.documentSchema , path ) ;
	}
	catch ( error ) {
		throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
	}

	switch ( schema.type ) {
		case 'link' :
			return this.addSingleLink( schema , path , value ) ;
		case 'multiLink' :
			return this.addMultiLink( schema , path , value ) ;
		case 'backLink' :
			// Maybe backLink must be read-only anyway
			throw new Error( 'Not coded ATM' ) ;
		default :
			throw ErrorStatus.badRequest( { message: "Unsupported link type '" + schema.type + "' for method .addLink() at path '" + path + "'." } ) ;
	}
} ;



// Internal
Document.prototype.addSingleLink = function( schema , path , value ) {
	// It performs a setSingleLink() if there is no link yet
	if ( dotPath.get( this.raw , path ) ) {
		throw ErrorStatus.badRequest( { message: "addLink() can't set pre-existing (single) link '" + path + "'." } ) ;
	}

	return this.setSingleLink( schema , path , value ) ;
} ;



// Internal
Document.prototype.addMultiLink = function( schema , path , value ) {
	var dbValues , dbValue , document , key ;

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

	key = document.getKey() ;
	dbValues = dotPath.get( this.raw , path ) ;

	// First, enforce uniqness
	if ( dbValues.some( existingDbValue => key === '' + existingDbValue._id ) ) {
		// Nothing to do, the document is already linked
		return ;
	}

	dbValue = { _id: document.getId() } ;

	// Populate it now!
	this.populatedDocumentProxies.set( dbValue , document.proxy ) ;

	dbValues.push( dbValue ) ;

	// Useless: dotPath.get does not clone
	//dotPath.set( this.raw , path , ids ) ;

	// Stage the change
	// Not sure if mongoDB supports adding a new array element by referencing the next array index, so we stage the whole array
	this.stage( path ) ;
} ;



// Remove one link from a multi-link
METHODS.add( 'removeLink' ) ;
Document.prototype.removeLink = function( path , value ) {
	var schema ;
	try {
		schema = doormen.path( this.collection.documentSchema , path ) ;
	}
	catch ( error ) {
		throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
	}

	switch ( schema.type ) {
		case 'link' :
			return this.removeSingleLink( schema , path , value ) ;
		case 'multiLink' :
			return this.removeMultiLink( schema , path , value ) ;
		case 'backLink' :
			// Maybe backLink must be read-only anyway
			throw new Error( 'Not coded ATM' ) ;
		default :
			throw ErrorStatus.badRequest( { message: "Unsupported link type '" + schema.type + "' for .removeLink() at path '" + path + "'." } ) ;
	}
} ;



// Internal
Document.prototype.removeSingleLink = function( schema , path ) {
	if ( typeof schema.collection !== 'string' ) {
		throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
	}

	dotPath.set( this.raw , path , null ) ;

	// Stage the change
	this.stage( path ) ;
} ;



// Internal
Document.prototype.removeMultiLink = function( schema , path , value ) {
	var dbValue , key ;

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
		dbValue = dotPath.get( this.raw , path ).filter( e => '' + e._id !== key ) ;
	}

	dotPath.set( this.raw , path , dbValue ) ;

	// Stage the change
	// Not sure if mongoDB supports complex removal an array element by referencing, so we stage the whole array
	this.stage( path ) ;
} ;



/* Attachments */



METHODS.add( 'getAttachmentDetails' ) ;
Document.prototype.getAttachmentDetails = function( path , schemaFromGetLinkDetails ) {
	var metadata , schema ;

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
	metadata = dotPath.get( this.raw , path ) ;

	if ( ! metadata ) { return { type: schema.type , attachment: null } ; }

	return {
		type: schema.type ,
		schema: schema ,
		//hostCollection: this.collection.name ,
		hostPath: path ,
		//hostId: this.meta.id
		attachment: this.restoreAttachment( metadata )
	} ;
} ;



METHODS.add( 'getAttachment' ) ;
Document.prototype.getAttachment = function( path , schemaFromGetLink ) {
	var schema , metadata ;

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

	metadata = dotPath.get( this.raw , path ) ;

	if ( ! metadata ) {
		throw ErrorStatus.notFound( { message: "Attachment not found '" + path + "'." } ) ;
	}

	return this.restoreAttachment( metadata ) ;
} ;



METHODS.add( 'setAttachment' ) ;
Document.prototype.setAttachment = async function( path , attachment ) {
	var schema , details , exported ;

	if ( attachment === null ) {
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

	details = dotPath.get( this.raw , path ) ;

	if ( details ) {
		await this.removeAttachment( path ) ;
	}

	exported = attachment.export() ;
	dotPath.set( this.raw , path , exported ) ;

	// Stage the change
	this.stage( path ) ;
} ;



METHODS.add( 'removeAttachment' ) ;
Document.prototype.removeAttachment = async function( path ) {
	var schema , metadata , attachment ;

	try {
		schema = doormen.path( this.collection.documentSchema , path ) ;
	}
	catch ( error ) {
		throw ErrorStatus.badRequest( { message: "Unexistant attachment '" + path + "'." } ) ;
	}

	if ( schema.type !== 'attachment' ) {
		throw ErrorStatus.badRequest( { message: "Property is not an attachment '" + path + "'." } ) ;
	}

	metadata = dotPath.get( this.raw , path ) ;

	if ( ! metadata ) { return ; }

	attachment = this.restoreAttachment( metadata ) ;

	try {
		await attachment.delete() ;
	}
	catch ( error ) {
		log.error( '[roots-db] .removeAttachment(): %E' , error ) ;
	}

	dotPath.set( this.raw , path , null ) ;

	// Stage the change
	this.stage( path ) ;
} ;



// Create a new attachment
METHODS.add( 'createAttachment' ) ;
Document.prototype.createAttachment = function( params , incoming ) {
	return new rootsDb.Attachment(
		{
			collectionName: this.collection.name ,
			documentId: '' + this.meta.id ,
			id: '' + this.collection.createId() ,
			driver: this.collection.attachmentDriver ,
			filename: params.filename || '' + this.meta.id ,
			contentType: params.contentType || 'application/octet-stream' ,
			fileSize: params.fileSize || null ,
			hash: params.hash || null ,
			hashType: this.collection.attachmentHashType ,	// Force hashType to collection's param
			metadata: params.metadata || null
		} ,
		incoming
	) ;
} ;



// Restore an Attachment instance from the DB
// Internal
Document.prototype.restoreAttachment = function( params ) {
	return new rootsDb.Attachment( {
		collectionName: this.collection.name ,
		documentId: '' + this.meta.id ,
		id: params.id ,
		driver: this.collection.attachmentDriver ,
		filename: params.filename || '' + this.meta.id ,
		contentType: params.contentType || 'application/octet-stream' ,
		fileSize: params.fileSize || null ,
		hash: params.hash || null ,
		hashType: params.hashType || null ,		// DO NOT FORCE hashType, it's coming from the DB
		metadata: params.metadata || null
	} ) ;
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
				resolve( true ) ;
			} ) ;

			attachmentStreams.once( 'end' , () => {
				resolve( false ) ;
			} ) ;
		} ) ;
	} ;


	const doFn = async () => {
		var details = this.getAttachmentDetails( attachmentStreams.list[ index ].documentPath ) ;

		if ( ! details ) {
			// unexistant link, drop it now
			attachmentStreams.list[ index ].stream.resume() ;
			throw new Error( "documentSaveAttachmentStreams: unexistant link" ) ;
		}

		var attachment = this.createAttachment( attachmentStreams.list[ index ].metadata , attachmentStreams.list[ index ].stream ) ;

		try {
			await this.setAttachment( attachmentStreams.list[ index ].documentPath , attachment ) ;
		}
		catch ( error ) {
			// setAttachment failed, so drop it now
			attachmentStreams.list[ index ].stream.resume() ;
			throw error ;
		}

		return attachment.save() ;
	} ;


	while ( await conditionFn() ) {
		await doFn() ;
	}
} ;



// External and internal call
METHODS.add( 'populate' ) ;
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
						dbValue = dotPath.get( this.raw , path ) ;
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
						dbValue = dotPath.get( this.raw , path ) ;
						documentProxies = dbValue.map( oneDbValue => this.populatedDocumentProxies.get( oneDbValue ) ) ;
						documentProxies.forEach( documentProxy_ => {
							documentProxy_._.preparePopulate( options.deepPopulate[ details.foreignCollection ] , population , options ) ;
						} ) ;
					}
					break ;
				}

				details.foreignIds.forEach( ( foreignId , index ) => {
					this.preparePopulateOneRef( details.hostPath + '.' + index , foreignId , details , population , options ) ;
				} ) ;
				break ;

			case 'backLink' :
				if ( populated ) {
					if ( options.deepPopulate[ details.foreignCollection ] ) {
						// Get the already populated documents and deep-populate them
						dbValue = dotPath.get( this.raw , path ) ;
						documentProxies = this.populatedDocumentProxies.get( dbValue ) ;
						documentProxies.forEach( documentProxy_ => {
							documentProxy_._.preparePopulate( options.deepPopulate[ details.foreignCollection ] , population , options ) ;
						} ) ;
					}
					break ;
				}

				// Check that the hostPath is already an object
				dbValue = dotPath.get( this.raw , details.hostPath ) ;

				if ( ! dbValue || typeof dbValue !== 'object' ) {
					dbValue = {} ;
					dotPath.set( this.raw , details.hostPath , dbValue ) ;
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

		// Update populateTagMask if needed
		if ( options.populateTagMask ) {
			documentProxy.setPopulateTagMask( options.populateTagMask ) ;
		}

		// Populate it now!
		target = dotPath.get( this.raw , hostPath ) ;
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
		foreignAnyCollection: details.foreignAnyCollection ,
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



const IS_EQUAL_UNORDERED = { unordered: true } ;
const VERSION_INDEX_PROP = [ '_version' , '_activeVersion._id' , '_activeVersion._collection' ] ;

// Usually called inside a promise.catch(), it adds some info to the error and re-throw
Document.prototype.forwardError = function( error , collection = this.collection , excludeCode = null ) {
	error.message += " on collection '" + collection.name + "'" ;
	error.collection = collection.name ;

	if ( error.code === 'duplicateKey' && error.indexName ) {
		error.message += ' on index: ' + error.indexName ;

		// Add index's properties informations to the error
		if ( collection.indexes[ error.indexName ] ) {
			error.indexProperties = Object.keys( collection.indexes[ error.indexName ].properties ) ;

			if ( doormen.isEqual( error.indexProperties , VERSION_INDEX_PROP , IS_EQUAL_UNORDERED ) ) {
				// Re-qualify the error
				error.code = 'versionDuplicateKey' ;
			}

			error.message += ' constructed with: ' + error.indexProperties.join( ',' ) ;
		}

		//log.error( "Duplicate key error: %E\nInspection: %Y" , error , error ) ;
	}
	//else if ( error.code === 'keyTooLargeToIndex' ) {}

	if ( error.code === excludeCode ) { return ; }

	throw error ;
} ;



DEEP_METHODS.set( '__enumerate__' , ( rootTarget , path , trueTarget , documentDepth ) => {
	var tagMask = documentDepth && documentDepth > 1 ? rootTarget.populateTagMask || rootTarget.tagMask : rootTarget.tagMask ;

	if ( ! tagMask ) {
		return Reflect.ownKeys( trueTarget ) ;
	}

	var schema = doormen.path( rootTarget.collection.documentSchema , path , true ) ;	// use the noSubmasking option here

	if ( schema === null ) {
		// We are in a sub-schema of a noSubmasking parent, so we ignore tag-masking
		return Reflect.ownKeys( trueTarget ) ;
	}

	var tagMasked = doormen.tagMask( schema , trueTarget , tagMask , 1 ) ;
	//log.error( "path: %J schema: %Y\n tags: %Y , tagMasked: %Y" , path , schema , tagMask , tagMasked ) ;

	if ( tagMasked ) { return Object.keys( tagMasked ) ; }

	return [] ;
} ) ;

