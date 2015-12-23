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

//var tree = require( 'tree-kit' ) ;
var ErrorStatus = require( 'error-status' ) ;

function noop() {}



/*
	FEATURE TODO:
		* set common properties with batch.$
*/



function BatchWrapper() { throw new Error( "Use BatchWrapper.create() instead" ) ; }
module.exports = BatchWrapper ;



/*
	BatchWrapper.create( collection , rawBatch , options )
	ALL ARGUMENTS are MANDATORY!
*/
BatchWrapper.create = function batchCreate( collection , rawBatch , options )
{
	// Already wrapped?
	if ( rawBatch.$ instanceof BatchWrapper ) { return rawBatch.$ ; }
	
	var wrapper = Object.create( BatchWrapper.prototype ) ;
	wrapper.create( collection , rawBatch , options ) ;
	return wrapper ;
} ;



BatchWrapper.prototype.create = function batchCreate( collection , rawBatch , options )
{
	var i , length ;
	
	// Already wrapped?
	
	// This can be costly on large batch
	length = rawBatch.length ;
	for ( i = 0 ; i < length ; i ++ )
	{
		collection.DocumentWrapper.create( collection , rawBatch[ i ] , options ) ;
	}
	
	// Validation?
	
	Object.defineProperties( this , {
		batch: { value: rawBatch } ,
		world: { value: collection.world } ,
		collection: { value: collection } ,
		//suspected: { writable: true, value: false } ,
		
		// Useful here?
		loaded: { value: options.fromUpstream ? true : false , writable: true , enumerable: true } ,
		saved: { value: false , writable: true , enumerable: true } ,
		deleted: { value: false , writable: true , enumerable: true } ,
		upstreamExists: { value: options.fromUpstream ? true : false , writable: true , enumerable: true }
	} ) ;
	
	// Useful?
	if ( options.fromUpstream )
	{
		this.loaded = true ;
		this.upstreamExists = true ;
	}
	
	Object.defineProperty( rawBatch , '$' , { value: this } ) ;
} ;



BatchWrapper.prototype.index = function index()
{
	var i ,
		iMax = this.batch.length ,
		batchIndex = {} ;
	
	iMax = this.batch.length ;
	
	for ( i = 0 ; i < iMax ; i ++ )
	{
		batchIndex[ this.batch[ i ].$.id.toString() ] = this.batch[ i ] ;
	}
	
	return batchIndex ;
} ;



BatchWrapper.rawBatchIndex = function rawBatchIndex( collection , rawBatch )
{
	var i ,
		idKey = collection.driver.idKey ,
		iMax = rawBatch.length ,
		batchIndex = {} ;
	
	iMax = rawBatch.length ;
	
	for ( i = 0 ; i < iMax ; i ++ )
	{
		batchIndex[ rawBatch[ i ][ idKey ].toString() ] = rawBatch[ i ] ;
	}
	
	return batchIndex ;
} ;



