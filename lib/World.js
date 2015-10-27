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

var async = require( 'async-kit' ) ;
var tree = require( 'tree-kit' ) ;
var logger = require( 'logger-kit' ).global.use( 'roots-db' ) ;





function World()
{
	var world = Object.create( World.prototype , {
		collections: { value: {} , enumerable: true }
	} ) ;
	
	return world ;
}

World.prototype.constructor = World ;
module.exports = World ;



function noop() {}



World.prototype.createCollection = function worldCreateCollection( name , schema )
{
	return ( this.collections[ name ] = rootsDb.Collection( this , name , schema ) ) ;
} ;



World.prototype.createMemoryModel = function worldCreateMemoryModel()
{
	return rootsDb.MemoryModel( this ) ;
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
		
		logger.debug( "World populate: multiGet on %s: %I" , collectionName , ids ) ;
		
		collection.driver.multiGet( ids , function( error , rawBatch ) {
			
			if ( error ) { mapCallback( error , {} ) ; return ; }
			
			mapCallback( undefined , rootsDb.BatchWrapper.rawBatchIndex( collection , rawBatch ) ) ;
		} ) ;
	} )
	.exec( function( error , batchs ) {
		
		var i , details ,
			iMax = options.populateData.targets.length ;
		
		if ( error ) { callback( error ) ; return ; }
		
		for ( i = 0 ; i < iMax ; i ++ )
		{
			details = options.populateData.targets[ i ] ;
			
			// Should we call the DocumentWrapper here?
			tree.path.set( details.object , details.path , batchs[ details.collection ][ details.id ] ) ;
		}
		
		callback() ;
	} ) ;
} ;




