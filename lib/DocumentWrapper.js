/*
	The Cedric's Swiss Knife (CSK) - CSK Object-Document Mapping

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
		staged: { value: {} , enumerable: true , writable: true } ,
		id: { value: id , enumerable: true } ,
		//world: { value: collection.world } ,
		collection: { value: collection } ,
		meta: { value: {} , enumerable: true } ,
		//suspected: { value: false , writable: true , enumerable: true } ,
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
	var self = this ;
	
	if ( ! options || typeof options !== 'object' )
	{
		if ( typeof options === 'function' ) { callback = options ; }
		options = {} ;
	}
	
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	//if ( this.suspected ) { throw new Error( '[roots-db] cannot save a suspected document - it is on the TODO LIST already' ) ; }
	if ( this.deleted ) { throw new Error( 'Current document is deleted' ) ; }
	
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









			/* Old 'Suspect' document... */


// Should probably have its instance type of its own
function oldDocumentWrapper( collection , rawDocument , options )
{
	// [...]
	
	var wrapper , deepInherit ;
	
	// Suspect is set when the object is in a state where it may exist upstream but should be loaded first
	if ( options.suspected )
	{
		wrapper.suspected = true ;
		wrapper.upstream = deepInherit( null , collection.suspectedBase ) ;
		wrapper.$ = deepInherit( null , wrapper.upstream ) ;
		wrapper.id = null ;
		wrapper.fingerprint = null ;
		wrapper.witness = null ;
		
		Object.defineProperty( wrapper.$ , '' , { value: wrapper } ) ;	// link to the parent
		
		if ( options.fingerprint && typeof options.fingerprint === 'object' )
		{
			var fingerprint ;
			
			// Check if we have a unique fingerprint
			if ( options.fingerprint instanceof rootsDb.FingerprintWrapper ) { fingerprint = options.fingerprint ; }
			else { fingerprint = this.createFingerprint( options.fingerprint ) ; }
			
			if ( fingerprint.unique )
			{
				tree.extend( { own: true } , wrapper.upstream , fingerprint.partialDocument ) ;
				//console.log( '<<<<<<<<<< wrapper.upstream:' , wrapper.upstream ) ;
				wrapper.fingerprint = fingerprint ;
			}
		}
		
		if ( options.witness && typeof options.witness === 'object' && Object.keys( options.witness ).length )
		{
			wrapper.witness = options.witness ;
		}
		
		if ( options.id )
		{
			if ( typeof options.id !== 'string' && ! options.id.toString ) { throw new Error( '[roots-db] provided id cannot be converted to a string' ) ; }
			Object.defineProperty( wrapper , 'id' , { value: collection.driver.createId( wrapper.$ , options.id ) , enumerable: true } ) ;
		}
		
		if ( ! wrapper.id && ! wrapper.fingerprint && ! wrapper.witness )
		{
			throw new Error( '[roots-db] cannot instanciate a suspect without id, fingerprint or witness' ) ;
		}
		
		return wrapper ;
	}
}



// Reveal a suspected Document
DocumentWrapper.prototype.reveal = function documentReveal( options , callback )
{
	if ( ! options || typeof options !== 'object' )
	{
		if ( typeof options === 'function' ) { callback = options ; }
		options = {} ;
	}
	
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	if ( ! this.suspected || ( options.idOnly && this.id ) ) { callback( undefined , this ) ; return ; }
	
	if ( this.id ) { this.revealById( options , callback ) ; return ; }
	
	if ( this.fingerprint ) { this.revealByFingerprint( options , callback ) ; return ; }
	
	if ( this.witness ) { this.revealByWitness( options , callback ) ; return ; }
} ;



DocumentWrapper.prototype.revealById = function documentRevealById( options , callback )
{
	var idString , deepInherit = this.collection.deepInherit , self = this ;
	
	if ( typeof this.id === 'string' ) { idString = this.id ; }
	else if ( this.id.toString ) { idString = this.id.toString() ; }
	else { throw new Error( '[roots-db] provided id cannot be converted to a string' ) ; }
	
	this.collection.driver.get( this.id , function( error , rawDocument ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		if ( ! rawDocument )
		{
			self.suspected = false ;
			self.deleted = true ;
			self.upstreamExists = false ;
			callback( ErrorStatus.notFound( { message: 'Document not found' } ) ) ;
			return ;
		}
		
		self.suspected = false ;
		self.loaded = true ;
		self.upstreamExists = true ;
		
		self.upstream = deepInherit( rawDocument , self.collection.documentBase ) ;
		deepInherit( self.$ , self.upstream ) ;
		delete self.$[ self.collection.driver.idKey ] ;
		
		callback( undefined , self ) ;
	} ) ;
} ;



DocumentWrapper.prototype.revealByFingerprint = function documentRevealByFingerprint( options , callback )
{
	var deepInherit = this.collection.deepInherit , self = this ;
	
	if ( ! ( this.fingerprint instanceof rootsDb.FingerprintWrapper ) ) { throw new Error( '[roots-db] no fingerprint for this suspect' ) ; }
	
	this.collection.driver.getUnique( this.fingerprint.$ , function( error , rawDocument ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		if ( ! rawDocument )
		{
			self.suspected = false ;
			self.deleted = true ;
			self.upstreamExists = false ;
			callback( ErrorStatus.notFound( { message: 'Document not found' } ) ) ;
			return ;
		}
		
		self.suspected = false ;
		self.loaded = true ;
		self.upstreamExists = true ;
		
		self.upstream = deepInherit( rawDocument , self.collection.documentBase ) ;
		deepInherit( self.$ , self.upstream ) ;
		delete self.$[ self.collection.driver.idKey ] ;
		
		callback( undefined , self ) ;
	} ) ;
} ;



DocumentWrapper.prototype.revealByWitness = function documentRevealByWitness( options , callback )
{
	var self = this ;
	
	if ( ! this.witness || typeof this.witness !== 'object' ) { throw new Error( '[roots-db] no witness for this suspect' ) ; }
	
	switch ( this.witness.type )
	{
		case 'link' :
			if ( this.witness.document.suspected )
			{
				// Do not transmit options.idOnly
				this.witness.document.reveal( {} , function( error ) {
					if ( error ) { callback( error ) ; return ; }
					self.revealByWitness( options , callback ) ;
				} ) ;
				
				return ;
			}
			
			if ( ! this.witness.document.$[ this.witness.property ] )
			{
				this.deleted = true ;
				callback( ErrorStatus.notFound( { message: 'Document not found' } ) ) ;
				return ;
			}
			
			this.id = this.witness.document.$[ this.witness.property ] ;
			
			if ( options.idOnly ) { callback( undefined , this ) ; return ; }
			
			this.revealById( options , callback ) ;
			
			break ;
		
		// those type cannot exist for a document:
		//case 'backlink' :
		default :
			throw new Error( '[roots-db] Cannot reveal batch with this type of witness: ' + this.witness.type ) ;
	}
} ;



// Useful?
// Return a one-value state
DocumentWrapper.prototype.state = function documentState()
{
	if ( this.deleted ) { return 'deleted' ; }
	
	if ( this.suspected )
	{
		if ( this.upstreamExists ) { return 'existing-suspect' ; }
		return 'suspected' ;
	}
	
	if ( this.upstreamExists )
	{
		if ( ( this.saved || this.loaded ) && Object.keys( this.$ ).length === 0 ) { return 'synced' ; }
		return 'exists' ;
	}
	
	return 'app-side' ;
} ;





