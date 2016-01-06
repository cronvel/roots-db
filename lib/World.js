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

var async = require( 'async-kit' ) ;
var tree = require( 'tree-kit' ) ;
var log = require( 'logfella' ).global.use( 'roots-db' ) ;

function noop() {}



function World() { throw new Error( "Use World.create() instead" ) ; }
module.exports = World ;



World.create = function worldCreate()
{
	var world = Object.create( World.prototype ) ;
	world.create() ;
	return world ;
} ;



World.prototype.create = function worldCreate()
{
	Object.defineProperties( this , {
		collections: { value: {} , enumerable: true }
	} ) ;
} ;



World.prototype.createCollection = function worldCreateCollection( name , schema )
{
	return ( this.collections[ name ] = rootsDb.Collection.create( this , name , schema ) ) ;
} ;



World.prototype.createMemoryModel = function worldCreateMemoryModel( options )
{
	return rootsDb.MemoryModel.create( this , options ) ;
} ;



World.prototype.populate = function populate( options , callback )
{
	var self = this ;
	
	// Nothing to do...
	if ( ! options.populateData || ! options.populateData.targets.length )
	{
		callback() ;
		return ;
	}
	
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	
	// First, collect objects
	async.map( options.populateData.refs , function( ids , collectionName , mapCallback ) {
		
		var collection = self.collections[ collectionName ] ;
		
		if ( ids instanceof Set ) { ids = Array.from( ids ) ; }
		
		log.debug( "World populate: multiGet on %s: %I" , collectionName , ids ) ;
		
		collection.driver.multiGet( ids , function( error , rawBatch ) {
			
			if ( error ) { mapCallback( error , {} ) ; return ; }
			
			mapCallback( undefined , rootsDb.BatchWrapper.rawBatchIndex( collection , rawBatch ) ) ;
		} ) ;
	} )
	.exec( function( error , batchs ) {
		
		var i , details , collection , document ,
			iMax = options.populateData.targets.length ;
		
		if ( error ) { callback( error ) ; return ; }
		
		for ( i = 0 ; i < iMax ; i ++ )
		{
			details = options.populateData.targets[ i ] ;
			document = batchs[ details.foreignCollection ][ details.foreignId ] ;
			
			if ( options.memory )
			{
				collection = self.collections[ details.foreignCollection ] ;
				collection.DocumentWrapper.create( collection , document , { fromUpstream: true , skipValidation: true } ) ;
				options.memory.add( details.foreignCollection , document , options.noReference ) ;
			}
			// Should we call the DocumentWrapper here?
			//else {}
			
			tree.path.set( details.hostDocument , details.hostPath , document ) ;
		}
		
		callback() ;
	} ) ;
} ;




