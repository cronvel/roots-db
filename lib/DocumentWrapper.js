/*
	The Cedric's Swiss Knife (CSK) - CSK RootsDB

	Copyright (c) 2015 - 2016 Cédric Ronvel 
	
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

var tree = require( 'tree-kit' ) ;
var async = require( 'async-kit' ) ;
var doormen = require( 'doormen' ) ;
var fs = require( 'fs' ) ;
var fsKit = require( 'fs-kit' ) ;
var ErrorStatus = require( 'error-status' ) ;
var log = require( 'logfella' ).global.use( 'roots-db' ) ;

function noop() {}



function DocumentWrapper() { throw new Error( "Use DocumentWrapper.create() instead" ) ; }
module.exports = DocumentWrapper ;



/*
	DocumentWrapper.create( collection , rawDocument , options )
	ALL ARGUMENTS are MANDATORY!
*/
DocumentWrapper.create = function documentCreate( collection , rawDocument , options )
{
	// Already wrapped?
	if ( rawDocument.$ instanceof DocumentWrapper ) { return rawDocument.$ ; }
	
	var wrapper = Object.create( DocumentWrapper.prototype ) ;
	wrapper.create( collection , rawDocument , options ) ;
	return wrapper ;
} ;



DocumentWrapper.prototype.create = function documentCreate( collection , rawDocument , options )
{
	// First check ID (any $id should be removed before validation)
	var id = collection.driver.checkId( rawDocument , true ) ;
	
	// Then validate the document
	if ( ! ( options.skipValidation !== undefined ? options.skipValidation : collection.skipValidation ) )
	{
		try {
			collection.validate( rawDocument ) ;
		}
		catch ( error ) {
			error.validatorMessage = error.message ;
			error.message = '[roots-db] validator error: ' + error.message ;
			throw error ;
		}
	}
	
	Object.defineProperties( this , {
		document: { value: rawDocument , enumerable: true , writable: true } ,
		id: { value: id , enumerable: true } ,
		//world: { value: collection.world } ,
		collection: { value: collection } ,
		//meta: { value: {} , enumerable: true } ,
		//suspected: { value: false , writable: true , enumerable: true } ,
		populated: { value: {} , enumerable: true } ,
		localPatch: { value: false , enumerable: true , writable: true } ,
		staged: { value: {} , enumerable: true , writable: true } ,
		loaded: { value: options.fromUpstream ? true : false , writable: true , enumerable: true } ,
		saved: { value: false , writable: true , enumerable: true } ,
		deleted: { value: false , writable: true , enumerable: true } ,
		upstreamExists: { value: options.fromUpstream ? true : false , writable: true , enumerable: true }
	} ) ;
	
	if ( options.fromUpstream )
	{
		this.loaded = true ;
		this.upstreamExists = true ;
	}
	
	Object.defineProperty( rawDocument , '$' , { value: this } ) ;
} ;



// callback( error )
DocumentWrapper.prototype.save = function documentSave( options , callback )
{
	var self = this ;
	
	if ( ! options || typeof options !== 'object' )
	{
		if ( typeof options === 'function' ) { callback = options ; }
		options = {} ;
	}
	
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	//if ( this.suspected ) { throw new Error( '[roots-db] cannot save a suspected document - it is on the TODO LIST already' ) ; }
	if ( this.deleted ) { throw new Error( 'Current document is deleted' ) ; }
	
	// Validation
	if (
		! ( options.skipValidation !== undefined ? options.skipValidation : this.collection.skipValidation ) &&
		( ! this.collection.patchDrivenValidation || this.localPatch )
	)
	{
		try {
			log.debug( "Validate on save()" ) ;
			this.collection.validate( this.document ) ;
		}
		catch ( error ) {
			error.validatorMessage = error.message ;
			error.message = '[roots-db] validator error: ' + error.message ;
			callback( error ) ;
			return ;
		}
		
		// Do not validate again, in case of recursive call
		options.skipValidation = true ;
	}
	
	// Clear attachments first, then call save() again...
	if ( options.clearAttachments && this.collection.attachmentUrl )
	{
		this.clearAttachments( function( error ) {
			log.debug( "Attachment debug: clearAttachments()" ) ;
			if ( error ) { callback( error ) ; return ; }
			delete options.clearAttachments ;
			self.save.call( self , options , callback ) ;
		} ) ;
		
		return ;
	}
	
	// Save attachments first, then call save() again...
	if ( options.attachmentStreams && this.collection.attachmentUrl )
	{
		this.saveAttachmentStreams( options.attachmentStreams , function( error ) {
			log.debug( "Attachment debug: checkpoint F" ) ;
			if ( error ) { callback( error ) ; return ; }
			delete options.attachmentStreams ;
			self.save.call( self , options , callback ) ;
		} ) ;
		
		return ;
	}
	
	if ( this.upstreamExists )
	{
		// Full save (update)
		this.collection.driver.update( this.id , this.document , function( error ) {
			
			if ( error ) { callback( error ) ; return ; }
			
			self.saved = true ;
			self.localPatch = false ;
			callback() ;
		} ) ;
	}
	else if ( options.overwrite )
	{
		// overwrite wanted
		this.collection.driver.overwrite( this.document , function( error )
		{
			if ( error ) { callback( error ) ; return ; }
			
			self.saved = true ;
			self.upstreamExists = true ;
			self.localPatch = false ;
			callback() ;
		} ) ;
	}
	else
	{
		// create (insert) needed
		this.collection.driver.createDocument( this.document , function( error )
		{
			if ( error ) { callback( error ) ; return ; }
			
			self.saved = true ;
			self.upstreamExists = true ;
			self.localPatch = false ;
			callback() ;
		} ) ;
	}
} ;



DocumentWrapper.prototype.patch = function documentPatch( patch )
{
	// First, stage all the changes
	tree.extend( null , this.staged , patch ) ;
	
	// Then apply the patch to the current local rawDocument
	tree.extend( { unflat: true , deepFilter: this.collection.driver.objectFilter } , this.document , patch ) ;
	
	// Mark it as patched
	this.localPatch = true ;
} ;



DocumentWrapper.prototype.stage = function documentStage( paths )
{
	var i , length ;
	
	if ( typeof paths === 'string' ) { paths = [ paths ] ; }
	else if ( ! Array.isArray( paths ) ) { throw new TypeError( "[roots-db] stage(): argument #0 should be an a string or an array of string" ) ; }
	
	length = paths.length ;
	
	for ( i = 0 ; i < length ; i ++ )
	{
		this.staged[ paths[ i ] ] = tree.path.get( this.document , paths[ i ] ) ;
	}
	
	// Mark it as patched
	this.localPatch = true ;
} ;



// Commit all staged stuff to the upstream
DocumentWrapper.prototype.commit = function documentCommit( options , callback )
{
	var self = this ;
	
	if ( ! options || typeof options !== 'object' )
	{
		if ( typeof options === 'function' ) { callback = options ; }
		options = {} ;
	}
	
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	//if ( this.suspected ) { throw new Error( '[roots-db] cannot save a suspected document - it is on the TODO LIST already' ) ; }
	if ( this.deleted ) { throw new Error( 'Current document is deleted' ) ; }
	
	// Validation
	if (
		! ( options.skipValidation !== undefined ? options.skipValidation : this.collection.skipValidation ) &&
		( ! this.collection.patchDrivenValidation || this.localPatch )
	)
	{
		try {
			log.debug( "Attachment debug: Validate on commit()" ) ;
			this.collection.validate( this.document ) ;
		}
		catch ( error ) {
			error.validatorMessage = error.message ;
			error.message = '[roots-db] validator error: ' + error.message ;
			callback( error ) ;
			return ;
		}
		
		// Do not validate again, in case of recursive call
		options.skipValidation = true ;
	}
	
	// Save attachments first, then call commit() again...
	if ( options.attachmentStreams )
	{
		this.saveAttachmentStreams( options.attachmentStreams , { stage: true } , function( error ) {
			log.debug( "Attachment debug: checkpoint F2" ) ;
			if ( error ) { callback( error ) ; return ; }
			delete options.attachmentStreams ;
			self.commit.call( self , options , callback ) ;
		} ) ;
		
		return ;
	}
	
	// Perform a regular save or throw an error?
	if ( ! this.upstreamExists )
	{
		this.staged = {} ;
		return this.save( callback ) ;
	}
	
	// Simple patch
	this.collection.driver.patch( this.id , this.staged , function( error ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		self.staged = {} ;
		self.saved = true ;
		self.localPatch = false ;
		callback() ;
	} ) ;
} ;



DocumentWrapper.prototype.delete = function documentDelete( options , callback )
{
	var self = this ;
	
	if ( ! options || typeof options !== 'object' )
	{
		if ( typeof options === 'function' ) { callback = options ; }
		options = {} ;
	}
	
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	//if ( this.suspected ) { throw new Error( '[roots-db] cannot delete a suspected document - it is on the TODO LIST already' ) ; }
	if ( this.deleted ) { throw new Error( 'Current document is already deleted' ) ; }
	
	// Clear attachments first, then call delete() again...
	if ( ! options.dontClearAttachments && this.collection.attachmentUrl )
	{
		this.clearAttachments( function( error ) {
			log.debug( "Attachment debug: clearAttachments()" ) ;
			if ( error ) { callback( error ) ; return ; }
			options.dontClearAttachments = true ;
			self.delete.call( self , options , callback ) ;
		} ) ;
		
		return ;
	}
	
	this.collection.driver.delete( this.id , function( error )
	{
		if ( error ) { callback( error ) ; return ; }
		
		self.deleted = true ;
		self.upstreamExists = false ;
		
		callback() ;
	} ) ;
} ;



// Lock the document for this application
DocumentWrapper.prototype.lock = function documentLock( callback )
{
	var self = this ;
	
	if ( ! this.collection.canLock ) { throw new Error( 'Document of this collection cannot be locked' ) ; }
	else if ( ! this.upstreamExists ) { throw new Error( 'Cannot lock a document that does not exist upstream yet' ) ; }
	else if ( this.deleted ) { throw new Error( 'Current document is deleted' ) ; }
	
	// /!\ Use of _id is DEPRECATED /!\
	this.collection.driver.lock( { _id: this.id } , this.collection.lockTimeout , function( error , locked , lockId )
	{
		if ( error ) { callback( error ) ; return ; }
		callback( undefined , !! locked , lockId ) ;
	} ) ;
} ;



DocumentWrapper.prototype.populate = function populate( paths , options , callback )
{
	if ( ! options || typeof options !== 'object' )
	{
		if ( typeof options === 'function' ) { callback = options ; }
		options = {} ;
	}
	
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	this.preparePopulate( paths , options ) ;
	
	this.collection.world.populate( options , callback ) ;
} ;



DocumentWrapper.prototype.preparePopulate = function preparePopulate( paths , options )
{
	var i , iMax , j , jMax , details ;
	
	if ( ! options.populateData ) { options.populateData = { targets: [] , refs: {} , complexTargets: [] , complexRefs: {} } ; }
	
	if ( ! Array.isArray( paths ) ) { paths = [ paths ] ; }
	
	if ( ! options.cache )
	{
		// The cache creation is forced here!
		options.cache = this.collection.world.createMemoryModel( { lazy: true } ) ;
		options.cache.add( this.collection.name , this.document , options.noReference ) ;
	}
	
	for ( i = 0 , iMax = paths.length ; i < iMax ; i ++ )
	{
		// This path was already populated
		if ( this.populated[ paths[ i ] ] ) { continue ; }
		
		details = this.getLinkDetails( paths[ i ] ) ;
		
		// If there is no such link, skip it now! Not sure if it should raise an error or not...
		if ( ! details ) { continue ; }
		
		// /!\ For instance, it is not possible to populate back-links /!\
		//if ( details.type === 'attachment' || ! details.foreignId ) { continue ; }
		
		switch ( details.type )
		{
			case 'link' :
				if ( details.foreignId )
				{
					this.preparePopulateOneRef( 'set' , details.hostPath , details.foreignId , details , options ) ;
				}
				break ;
			
			case 'multiLink' :
				for ( j = 0 , jMax = details.foreignIds.length ; j < jMax ; j ++ )
				{
					this.preparePopulateOneRef( 'set' , details.hostPath + '[' + j + ']' , details.foreignIds[ j ] , details , options ) ;
				}
				break ;
			
			case 'backLink' :
				// Force the hostPath to be an empty array now!
				tree.path.set( this.document , details.hostPath , [] ) ;
				this.preparePopulateOneBackRef( 'set' , details.hostPath , details , options ) ;
				break ;
		}
	}
} ;



// Prepare populate for one item using one foreign ID
DocumentWrapper.prototype.preparePopulateOneRef = function preparePopulateOneRef( operation , hostPath , foreignId , details , options )
{
	var document ;
	
	// Try to get it out of the cache
	if ( ( document = options.cache.get( this.collection.name , foreignId , options.noReference ) ) )
	{
		log.debug( 'Populate: cache hit for one link!' ) ;
		tree.path[ operation ]( this.document , hostPath , document ) ;
		this.populated[ hostPath ] = true ;
		
		if ( options.deepPopulate && options.deepPopulate[ details.foreignCollection ] )
		{
			document.$.preparePopulate( options.deepPopulate[ details.foreignCollection ] , options ) ;
		}
		
		return ;
	}
	
	options.populateData.targets.push( {
		operation: operation ,
		hostDocument: this.document ,
		hostPath: hostPath ,
		foreignCollection: details.foreignCollection ,
		foreignId: foreignId
	} ) ;
	
	if ( ! options.populateData.refs[ details.foreignCollection ] )
	{
		options.populateData.refs[ details.foreignCollection ] = new Set() ;
	}
	
	options.populateData.refs[ details.foreignCollection ].add( foreignId.toString() ) ;
} ;



// Prepare populate for multiples items using one foreign query
DocumentWrapper.prototype.preparePopulateOneBackRef = function preparePopulateOneBackRef( operation , hostPath , details , options )
{
	//var document ;
	
	/*
		The cache is useless here...
		Even if it would cache-hit, we cannot be sure if we have all the result set anyway.
		
		/!\ Except for unique fingerprint, someday when MemoryModel will support search /!\
	*/
	
	options.populateData.complexTargets.push( {
		operation: operation ,
		hostDocument: this.document ,
		hostPath: hostPath ,
		foreignCollection: details.foreignCollection ,
		foreignPath: details.foreignPath ,
		foreignValue: this.id
	} ) ;
	
	if ( ! options.populateData.complexRefs[ details.foreignCollection ] )
	{
		options.populateData.complexRefs[ details.foreignCollection ] = {} ;
		options.populateData.complexRefs[ details.foreignCollection ][ details.foreignPath ] = {} ;
	}
	else if ( ! options.populateData.complexRefs[ details.foreignCollection ][ details.foreignPath ] )
	{
		options.populateData.complexRefs[ details.foreignCollection ][ details.foreignPath ] = {} ;
	}
	
	// This looks redundant, but it ensures uniqness of IDs
	options.populateData.complexRefs[ details.foreignCollection ][ details.foreignPath ][ this.id.toString() ] = this.id ;
} ;



DocumentWrapper.prototype.getLinkDetails = function getLinkDetails( path )
{
	var metaData ,
		schema = doormen.path( this.collection.documentSchema , path ) ;
	
	if ( ! schema ) { return null ; }
	
	switch ( schema.type )
	{
		case 'link' :
			if ( typeof schema.collection !== 'string' ) { return null ; }
			
			return {
				type: schema.type ,
				schema: schema ,
				//hostCollection: this.collection.name ,
				hostPath: path ,
				//hostId: this.id
				
				foreignCollection: schema.collection ,
				foreignId: tree.path.get( this.document , path )
			} ;
		
		case 'multiLink' :
			if ( typeof schema.collection !== 'string' ) { return null ; }
			
			return {
				type: schema.type ,
				schema: schema ,
				//hostCollection: this.collection.name ,
				hostPath: path ,
				//hostId: this.id
				
				foreignCollection: schema.collection ,
				foreignIds: tree.path.get( this.document , path )
			} ;
		
		case 'backLink' :
			if ( typeof schema.collection !== 'string' ) { return null ; }
			
			return {
				type: schema.type ,
				schema: schema ,
				//hostCollection: this.collection.name ,
				hostPath: path ,
				//hostId: this.id
				
				foreignCollection: schema.collection ,
				foreignPath: schema.path ,
			} ;
			
		case 'attachment' :
			// For instance, it's the same than .getLink() for attachment...
			metaData = tree.path.get( this.document , path ) ;
			
			if ( ! metaData ) { return { type: schema.type , attachment: null } ; }
			//return ErrorStatus.notFound( { message: "Link not found." } ) ;
			
			return {
				type: schema.type ,
				schema: schema ,
				//hostCollection: this.collection.name ,
				hostPath: path ,
				//hostId: this.id
				attachment: this.restoreAttachment( metaData )
			} ;
		
		default :
			return null ;
	}
} ;



DocumentWrapper.prototype.getLink = function getLink( path , options , callback )
{
	var schema , targetId , targetIds , metaData , fingerprint ;
	
	if ( ! options || typeof options !== 'object' )
	{
		if ( typeof options === 'function' ) { callback = options ; }
		options = {} ;
	}
	
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	schema = doormen.path( this.collection.documentSchema , path ) ;
	
	if ( ! schema )
	{
		callback( ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ) ;
		return ;
	}
	
	
	switch ( schema.type )
	{
		case 'link' :
			if ( typeof schema.collection !== 'string' )
			{
				callback( ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ) ;
				return ;
			}
			
			targetId = tree.path.get( this.document , path ) ;
			
			if ( ! targetId )
			{
				callback( ErrorStatus.notFound( { message: "Link not found." } ) ) ;
				return ;
			}
			
			this.collection.world.collections[ schema.collection ].get( targetId , options , callback ) ;
			return ;
		
		case 'multiLink' :
			if ( typeof schema.collection !== 'string' )
			{
				callback( ErrorStatus.badRequest( { message: "Unexistant multi-link '" + path + "'." } ) ) ;
				return ;
			}
			
			targetIds = tree.path.get( this.document , path ) ;
			
			if ( ! Array.isArray( targetIds ) )
			{
				callback( ErrorStatus.notFound( { message: "Multi-link not found." } ) ) ;
				return ;
			}
			
			this.collection.world.collections[ schema.collection ].multiGet( targetIds , options , callback ) ;
			return ;
		
		case 'backLink' :
			if ( typeof schema.collection !== 'string' )
			{
				callback( ErrorStatus.badRequest( { message: "Unexistant back-link '" + path + "'." } ) ) ;
				return ;
			}
			
			fingerprint = {} ;
			fingerprint[ schema.path ] = this.id ;
			
			this.collection.world.collections[ schema.collection ].collect( fingerprint , options , callback ) ;
			return ;
		
		case 'attachment' :
			if ( options.noAttachment )
			{ 
				callback( ErrorStatus.notFound( { message: "Attachment link not found." } ) ) ;
				return ;
			}
			
			metaData = tree.path.get( this.document , path ) ;
			
			if ( ! metaData )
			{
				callback( ErrorStatus.notFound( { message: "Attachment link not found." } ) ) ;
				return ;
			}
			
			callback( undefined , this.restoreAttachment( metaData ) ) ;
			return ;
		
		default :
			callback( ErrorStatus.badRequest( { message: "Unexistant attachment link '" + path + "'." } ) ) ;
			return ;
	}
	
} ;



DocumentWrapper.prototype.setLink = function setLink( path , value )
{
	var schema , details , ids ;
	
	schema = doormen.path( this.collection.documentSchema , path ) ;
	
	if ( ! schema )
	{
		throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
	}
	
	
	switch ( schema.type )
	{
		case 'link' :
			if ( typeof schema.collection !== 'string' )
			{
				throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
			}
			
			if ( ! value || ! value.$ || ! ( value.$ instanceof DocumentWrapper ) || value.$.collection.name !== schema.collection )
			{
				throw ErrorStatus.badRequest( {
					message: "Provided document is not part of collection '" + schema.collection + "' but '" + value.$.collection.name + "'."
				} ) ;
			}
			
			tree.path.set( this.document , path , value.$.id ) ;
			return ;
		
		case 'multiLink' :
			if ( typeof schema.collection !== 'string' )
			{
				throw ErrorStatus.badRequest( { message: "Unexistant multi-link '" + path + "'." } ) ;
			}
			
			if ( ! value ) { value = [] ; }
			
			if ( value.$ && ( value.$ instanceof rootsDb.BatchWrapper ) && value.$.collection.name === schema.collection )
			{
				ids = value.map( rootsDb.misc.mapIds ) ;
				tree.path.set( this.document , path , ids ) ;
				return ;
			}
			else if ( Array.isArray( value ) )
			{
				try {
					ids = value.map( rootsDb.misc.mapIdsAndCheckCollection.bind( undefined , schema.collection ) ) ;
				}
				catch ( error ) {
					console.error( error ) ;
					throw ErrorStatus.badRequest( {
						message: "Some provided document in the batch are not part of collection '" + schema.collection + "'."
					} ) ;
				}
				
				tree.path.set( this.document , path , ids ) ;
				return ;
			}
			
			throw ErrorStatus.badRequest( {
				message: "Provided batch is not part of collection '" + schema.collection + "' but '" + ( value.$ && value.$.collection.name ) + "'."
			} ) ;
		
		case 'backLink' :
			throw new Error( 'Not coded ATM' ) ;
			
		case 'attachment' :
			log.debug( "Attachment debug: checkpoint D %I" , arguments ) ;
			
			if ( value === null )
			{
				log.debug( "Attachment debug: checkpoint DD .setLink() used to delete attachment" ) ;
				details = tree.path.get( this.document , path , null ) ;
				tree.path.set( this.document , path , null ) ;
				
				// /!\ This function is synchronous ATM!!! /!\
				// This is the only use-case featuring asyncness, keep the whole function sync and ignore errors???
				
				fs.unlink( this.collection.attachmentUrl + this.id + '/' + details.id , function( error ) {
					if ( error ) { log.error( '[roots-db] .setLink()/attachment/unlink: %E' , error ) ; }
				} ) ;
				
				return ;
			}
			else if ( ! ( value instanceof rootsDb.Attachment ) )
			{
				throw new Error( '[roots-db] This link needs an Attachment instance' ) ;
			}
			
			tree.path.set( this.document , path , value.export() ) ;
			return ;
		
		default :
			throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
	}
	
} ;



// Add one link to a multi-link
DocumentWrapper.prototype.addLink = function addLink( path , value )
{
	var schema , details , ids ;
	
	schema = doormen.path( this.collection.documentSchema , path ) ;
	
	if ( ! schema )
	{
		throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
	}
	
	
	switch ( schema.type )
	{
		case 'multiLink' :
			if ( typeof schema.collection !== 'string' )
			{
				throw ErrorStatus.badRequest( { message: "Unexistant multi-link '" + path + "'." } ) ;
			}
			
			if ( ! value || ! value.$ || ! ( value.$ instanceof DocumentWrapper ) || value.$.collection.name !== schema.collection )
			{
				throw ErrorStatus.badRequest( {
					message: "Provided document is not part of collection '" + schema.collection + "' but '" + value.$.collection.name + "'."
				} ) ;
			}
			
			ids = tree.path.get( this.document , path ) ;
			ids.push( value.$.id ) ;
			tree.path.set( this.document , path , ids ) ;
			
			return ;
		
		case 'backLink' :
			throw new Error( 'Not coded ATM' ) ;
			
		default :
			throw ErrorStatus.badRequest( { message: "Unsupported link type '" + schema.type + "' at path '" + path + "'." } ) ;
	}
	
} ;



// Remove one link from a multi-link
DocumentWrapper.prototype.unlink = function unlink( path , value )
{
	var schema , details , ids , index ;
	
	schema = doormen.path( this.collection.documentSchema , path ) ;
	
	if ( ! schema )
	{
		throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
	}
	
	
	switch ( schema.type )
	{
		case 'multiLink' :
			if ( typeof schema.collection !== 'string' )
			{
				throw ErrorStatus.badRequest( { message: "Unexistant multi-link '" + path + "'." } ) ;
			}
			
			if ( ! value || ! value.$ || ! ( value.$ instanceof DocumentWrapper ) || value.$.collection.name !== schema.collection )
			{
				throw ErrorStatus.badRequest( {
					message: "Provided document is not part of collection '" + schema.collection + "' but '" + value.$.collection.name + "'."
				} ) ;
			}
			
			//console.error( "tree.path.get():" , tree.path.get( this.document , path ) ) ;
			ids = tree.path.get( this.document , path ).filter( rootsDb.misc.filterOutId.bind( undefined , value.$.id ) ) ;
			tree.path.set( this.document , path , ids ) ;
			
			return ;
		
		case 'backLink' :
			throw new Error( 'Not coded ATM' ) ;
			
		default :
			throw ErrorStatus.badRequest( { message: "Unsupported link type '" + schema.type + "' at path '" + path + "'." } ) ;
	}
	
} ;





			/* Attachments */



DocumentWrapper.prototype.createAttachment = function createAttachment( metaData , incoming )
{
	metaData.collectionName = this.collection.name ;
	metaData.documentId = this.id.toString() ;
	metaData.id = this.collection.createId().toString() ;
	metaData.baseUrl = this.collection.attachmentUrl ;
	
	if ( ! metaData.filename ) { metaData.filename = this.id.toString() ; }
	if ( ! metaData.contentType ) { metaData.contentType = 'application/octet-stream' ; }
	
	return rootsDb.Attachment.create( metaData , incoming ) ;
} ;



// Restore an Attachment from the DB
DocumentWrapper.prototype.restoreAttachment = function restoreAttachment( metaData )
{
	metaData.collectionName = this.collection.name ;
	metaData.documentId = this.id.toString() ;
	metaData.baseUrl = this.collection.attachmentUrl ;
	
	return rootsDb.Attachment.create( metaData ) ;
} ;



DocumentWrapper.prototype.clearAttachments = function documentClearAttachments( callback )
{
	if ( ! this.collection.attachmentUrl || ! this.id ) { callback() ; }	// Error or not?
	
	fsKit.deltree( this.collection.attachmentUrl + this.id , callback ) ;
} ;



DocumentWrapper.prototype.saveAttachmentStreams = function documentSaveAttachmentStreams( attachmentStreams , options , callback )
{
	var self = this , index = -1 , attachment ;
	
	// Function arguments management
	if ( ! options || typeof options !== 'object' )
	{
		if ( typeof options === 'function' ) { callback = options ; }
		options = {} ;
	}
	
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	
	
	async.while( function( error , results , whileCallback ) {
		
		var calledBack ;
		
		index ++ ;
		
		if ( error )
		{
			log.error( ".saveAttachmentStreams() - While error: %E" , error ) ;
			whileCallback( false ) ;
			return ;
		}
		
		if ( index < attachmentStreams.list.length )
		{
			whileCallback( true ) ;
			return ;
		}
		else if ( attachmentStreams.ended )
		{
			whileCallback( false ) ;
			return ;
		}
		
		calledBack = false ;
		
		attachmentStreams.once( 'attachment' , function() {
			log.debug( "Attachment debug: documentSaveAttachmentStreams() 'attachment' event" ) ;
			if ( ! calledBack ) { whileCallback( true ) ; calledBack = true ; }
		} ) ;
		
		attachmentStreams.once( 'end' , function() {
			log.debug( "Attachment debug: documentSaveAttachmentStreams() 'end' event" ) ;
			if ( ! calledBack ) { whileCallback( false ) ; calledBack = true ; }
		} ) ;
	} )
	.do( [
		function( doCallback ) {
			log.debug( "Attachment debug: checkpoint A" ) ;
			
			// Check if there is already an existing attachment
			attachment = self.getLinkDetails( attachmentStreams.list[ index ].documentPath ).attachment ;
			log.debug( "Attachment debug: checkpoint A2 existing attachment? %I" , attachment ) ;
			
			if ( ! attachment || ! ( attachment instanceof rootsDb.Attachment ) )
			{
				// There is no attachment yet, create a new one
				attachment = self.createAttachment(
					attachmentStreams.list[ index ].metaData ,
					attachmentStreams.list[ index ].stream
				) ;
				log.debug( "Attachment debug: checkpoint A3 creating a new attachment: %I" , attachment ) ;
			}
			else
			{
				// There is already an attachment, update it!
				attachment.update(
					attachmentStreams.list[ index ].metaData ,
					attachmentStreams.list[ index ].stream
				) ;
				log.debug( "Attachment debug: checkpoint A3 updating the attachment: %I" , attachment ) ;
			}
			
			log.debug( "Attachment debug: checkpoint B" ) ;
			
			try {
				self.setLink( attachmentStreams.list[ index ].documentPath , attachment ) ;
				
				// Stage the link details, if wanted...
				if ( options.stage ) { self.stage( attachmentStreams.list[ index ].documentPath ) ; }
			}
			catch ( error ) {
				// setLink failed, so drop it now
				log.error( "documentSaveAttachmentStreams: %E" , error ) ;
				attachmentStreams.list[ index ].stream.resume() ;
				doCallback( error ) ;
				return ;
			}
			log.debug( "Attachment debug: checkpoint C" ) ;
			
			//attachmentStreams.list[ index ].attachment = attachment ;
			attachment.save( doCallback ) ;
		}
	] )
	.exec( function( error ) {
		log.debug( "Attachment debug: checkpoint E -- End of processing" ) ;
		if ( error ) { callback( error ) ; return ; }
		callback() ;
	} ) ;
} ;


