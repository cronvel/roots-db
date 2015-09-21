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

var tree = require( 'tree-kit' ) ;
var async = require( 'async-kit' ) ;
var doormen = require( 'doormen' ) ;
var fsKit = require( 'fs-kit' ) ;
var ErrorStatus = require( 'error-status' ) ;



/*
	DocumentWrapper( collection , rawDocument , options )
	
	ALL ARGUMENTS are MANDATORY!
	Internal usage only.
*/
function DocumentWrapper( collection , rawDocument , options )
{
	// Already wrapped?
	if ( rawDocument.$ instanceof DocumentWrapper ) { return rawDocument.$ ; }
	
	// First check ID (any $id should be removed before validation)
	var id = collection.driver.checkId( rawDocument , true ) ;
	
	// Then validate the document
	if ( ! options.skipValidation )
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
	
	var wrapper = Object.create( DocumentWrapper.prototype , {
		document: { value: rawDocument , enumerable: true , writable: true } ,
		id: { value: id , enumerable: true } ,
		//world: { value: collection.world } ,
		collection: { value: collection } ,
		//meta: { value: {} , enumerable: true } ,
		//suspected: { value: false , writable: true , enumerable: true } ,
		staged: { value: {} , enumerable: true , writable: true } ,
		loaded: { value: options.fromUpstream ? true : false , writable: true , enumerable: true } ,
		saved: { value: false , writable: true , enumerable: true } ,
		deleted: { value: false , writable: true , enumerable: true } ,
		upstreamExists: { value: options.fromUpstream ? true : false , writable: true , enumerable: true }
	} ) ;
	
	
	if ( options.fromUpstream )
	{
		wrapper.loaded = true ;
		wrapper.upstreamExists = true ;
	}
	
	Object.defineProperty( rawDocument , '$' , { value: wrapper } ) ;
	
	return wrapper ;
}



module.exports = DocumentWrapper ;



function noop() {}



// callback( error )
DocumentWrapper.prototype.save = function documentSave( options , callback )
{
	var self = this , args = arguments ;
	
	//console.error( "\n\n>>>>>>>>>>>>>>>>>>>>>>>> 0: documentSave()\n\n" ) ;
	
	if ( ! options || typeof options !== 'object' )
	{
		if ( typeof options === 'function' ) { callback = options ; }
		options = {} ;
	}
	
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	//if ( this.suspected ) { throw new Error( '[roots-db] cannot save a suspected document - it is on the TODO LIST already' ) ; }
	if ( this.deleted ) { throw new Error( 'Current document is deleted' ) ; }
	
	// Clear attachments first, then call save() again...
	if ( options.clearAttachments && this.collection.attachmentUrl )
	{
		this.clearAttachments( function( error ) {
			console.error( "\n\n>>>>>>>>>>>>>>>>>>>>>>>> clearAttachments()\n\n" ) ;
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
			console.error( "\n\n>>>>>>>>>>>>>>>>>>>>>>>> F\n\n" ) ;
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
			callback() ;
		} ) ;
	}
	else
	{
		// create (insert) needed
		this.collection.driver.create( this.document , function( error )
		{
			if ( error ) { callback( error ) ; return ; }
			
			self.saved = true ;
			self.upstreamExists = true ;
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
	
	//console.log( this.staged ) ;
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
	
	// Save attachments first, then call commit() again...
	if ( options.attachmentStreams )
	{
		this.saveAttachmentStreams( options.attachmentStreams , { stage: true } , function( error ) {
			console.error( "\n\n>>>>>>>>>>>>>>>>>>>>>>>> F\n\n" ) ;
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
	
	this.collection.driver.delete( this.id , function( error )
	{
		if ( error ) { callback( error ) ; return ; }
		
		self.deleted = true ;
		self.upstreamExists = false ;
		
		callback() ;
	} ) ;
} ;



DocumentWrapper.prototype.clearAttachments = function documentClearAttachments( callback )
{
	if ( ! this.collection.attachmentUrl ) { callback() ; }	// Error or not?
	
	fsKit.deltree( this.collection.attachmentUrl , callback ) ;
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
			console.error( "While error:" , error ) ;
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
			console.error( "\n\n>>>>>>>>>>>>>>>>>>>>>>>> documentSaveAttachmentStreams() 'attachment' event\n\n" ) ;
			if ( ! calledBack ) { whileCallback( true ) ; calledBack = true ; }
		} ) ;
		
		attachmentStreams.once( 'end' , function() {
			console.error( "\n\n>>>>>>>>>>>>>>>>>>>>>>>> documentSaveAttachmentStreams() 'end' event\n\n" ) ;
			if ( ! calledBack ) { whileCallback( false ) ; calledBack = true ; }
		} ) ;
	} )
	.do( [
		function( doCallback ) {
			console.error( "\n\n>>>>>>>>>>>>>>>>>>>>>>>> A\n\n" ) ;
			
			// Check if there is already an existing attachment
			attachment = self.getLinkDetails( attachmentStreams.list[ index ].documentPath ) ;
			console.error( "\n\n>>>>>>>>>>>>>>>>>>>>>>>> A2 existing attachment?" , attachment , "\n\n" ) ;
			
			if ( ! attachment || ! ( attachment instanceof rootsDb.Attachment ) )
			{
				// There is no attachment yet, create a new one
				attachment = self.createAttachment(
					attachmentStreams.list[ index ].metaData ,
					attachmentStreams.list[ index ].stream
				) ;
				console.error( "\n\n>>>>>>>>>>>>>>>>>>>>>>>> A3 creating a new attachment:" , attachment , "\n\n" ) ;
			}
			else
			{
				// There is already an attachment, update it!
				attachment.update(
					attachmentStreams.list[ index ].metaData ,
					attachmentStreams.list[ index ].stream
				) ;
				console.error( "\n\n>>>>>>>>>>>>>>>>>>>>>>>> A3 updating the attachment:" , attachment , "\n\n" ) ;
			}
			
			console.error( "\n\n>>>>>>>>>>>>>>>>>>>>>>>> B\n\n" ) ;
			
			try {
				self.setLink( attachmentStreams.list[ index ].documentPath , attachment ) ;
				
				// Stage the link details, if wanted...
				if ( options.stage ) { self.stage( attachmentStreams.list[ index ].documentPath ) ; }
			}
			catch ( error ) {
				// setLink failed, so drop it now
				console.error( "documentSaveAttachmentStreams: " , error ) ;
				attachmentStreams.list[ index ].stream.resume() ;
				doCallback( error ) ;
				return ;
			}
			console.error( "\n\n>>>>>>>>>>>>>>>>>>>>>>>> C\n\n" ) ;
			
			//attachmentStreams.list[ index ].attachment = attachment ;
			attachment.save( doCallback ) ;
		}
	] )
	.exec( function( error ) {
		console.error( "\n\n>>>>>>>>>>>>>>>>>>>>>>>> E: the End\n\n" ) ;
		if ( error ) { callback( error ) ; return ; }
		callback() ;
	} ) ;
} ;



DocumentWrapper.prototype.getLinkDetails = function getLinkDetails( path )
{
	var linkData , metaData ,
		schema = doormen.path( this.collection.documentSchema , path ) ;
	
	if ( ! schema ) { return null ; }
	
	switch ( schema.type )
	{
		case 'link' :
			if ( typeof schema.collection !== 'string' ) { return null ; }
			
			return {
				id: tree.path.get( this.document , path ) ,
				collection: schema.collection
			} ;
		
		case 'attachment' :
			// For instance, it's the same than .getLink() for attachment...
			metaData = tree.path.get( this.document , path ) ;
			
			if ( ! metaData ) { return null ; }
			//return ErrorStatus.notFound( { message: "Link not found." } ) ;
			
			return this.restoreAttachment( metaData ) ;
		
		default :
			return null ;
	}
} ;



DocumentWrapper.prototype.getLink = function getLink( path , options , callback )
{
	var schema , targetId , metaData ;
	
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
		
		case 'attachment' :
			metaData = tree.path.get( this.document , path ) ;
			
			if ( ! metaData )
			{
				callback( ErrorStatus.notFound( { message: "Link not found." } ) ) ;
				return ;
			}
			
			callback( undefined , this.restoreAttachment( metaData ) ) ;
			return ;
		
		default :
			callback( ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ) ;
			return ;
	}
	
} ;



DocumentWrapper.prototype.setLink = function setLink( path , value )
{
	var schema ;
	
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
		
		case 'attachment' :
			console.error( "\n\n>>>>>>>>>>>>>>>>>>>>>>>> D" , arguments , "\n\n" ) ;
			
			if ( ! ( value instanceof rootsDb.Attachment ) )
			{
				throw new Error( '[roots-db] This link needs an Attachment instance' ) ;
			}
			
			tree.path.set( this.document , path , value.export() ) ;
			return ;
		
		default :
			throw ErrorStatus.badRequest( { message: "Unexistant link '" + path + "'." } ) ;
	}
	
} ;



DocumentWrapper.prototype.createAttachment = function createAttachment( metaData , incoming )
{
	metaData.collectionName = this.collection.name ;
	metaData.documentId = this.id.toString() ;
	metaData.id = this.collection.createId().toString() ;
	metaData.baseUrl = this.collection.attachmentUrl ;
	
	if ( ! metaData.filename ) { metaData.filename = this.id.toString() ; }
	if ( ! metaData.contentType ) { metaData.contentType = 'application/octet-stream' ; }
	
	return rootsDb.Attachment( metaData , incoming ) ;
} ;



// Restore an Attachment from the DB
DocumentWrapper.prototype.restoreAttachment = function restoreAttachment( metaData )
{
	metaData.collectionName = this.collection.name ;
	metaData.documentId = this.id.toString() ;
	metaData.baseUrl = this.collection.attachmentUrl ;
	
	return rootsDb.Attachment( metaData ) ;
} ;




