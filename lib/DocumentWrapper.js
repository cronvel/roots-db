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



DocumentWrapper.prototype.getLink = function getLink( property , options , callback )
{
	var targetCollection ;
	
	if ( ! options || typeof options !== 'object' )
	{
		if ( typeof options === 'function' ) { callback = options ; }
		options = {} ;
	}
	
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	targetCollection = this.collection.links[ property ] ;
	
	if ( ! targetCollection ) { throw new Error( "[roots-db] Unexistant link '" + property + "'." ) ; }
	
	return this.collection.world.collections[ targetCollection ].get( this.document[ property ] , options , callback ) ;
} ;



DocumentWrapper.prototype.setLink = function setLink( property , document )
{
	var targetCollection ;
	
	targetCollection = this.collection.links[ property ] ;
	
	if ( ! targetCollection ) { throw new Error( "[roots-db] Unexistant link '" + property + "'." ) ; }
	
	if ( ! document.$ || document.$.collection.name !== targetCollection )
	{
		throw new Error( "[roots-db] Provided document is not part of collection '" +
			targetCollection + "' but '" + document.$.collection.name + "'." ) ;
	}
	
	this.document[ property ] = document.$.id ;
} ;



