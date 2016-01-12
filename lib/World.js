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
	
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	if ( ! options.populateData || ( ! options.populateData.targets.length && ! options.populateData.complexTargets.length ) )
	{
		// Fast exit
		callback() ;
		return ;
	}
	
	options.populatingData = options.populateData ;
	delete options.populateData ;
	
	async.series( [
		this.simplePopulate.bind( this , options ) ,
		this.complexPopulate.bind( this , options )
	] )
	.exec( function() {
		if ( ! options.populateData || ( ! options.populateData.targets.length && ! options.populateData.complexTargets.length ) )
		{
			// Fast exit
			callback() ;
			return ;
		}
		
		// Recursivity...
		log.debug( 'Deep populate recursivity, populateData: %I' , options.populateData ) ;
		self.populate( options , callback ) ;
	} ) ;
} ;



World.prototype.simplePopulate = function simplePopulate( options , callback )
{
	var self = this ;
	
	// Nothing to do...
	if ( ! options.populatingData.targets.length ) { callback() ; return ; }
	
	
	// First, collect objects
	async.map( options.populatingData.refs , function( ids , collectionName , mapCallback ) {
		
		var collection = self.collections[ collectionName ] ;
		
		if ( ids instanceof Set ) { ids = Array.from( ids ) ; }
		
		log.debug( "World populate: multiGet on %s: %I" , collectionName , ids ) ;
		
		collection.driver.multiGet( ids , function( error , rawBatch ) {
			
			if ( error ) { mapCallback( error , {} ) ; return ; }
			
			mapCallback( undefined , rootsDb.BatchWrapper.raw.index( rawBatch ) ) ;
		} ) ;
	} )
	.exec( function( error , batchs ) {
		
		var i , iMax , details , collection , document ;
		
		if ( error ) { callback( error ) ; return ; }
		
		for ( i = 0 , iMax = options.populatingData.targets.length ; i < iMax ; i ++ )
		{
			details = options.populatingData.targets[ i ] ;
			document = batchs[ details.foreignCollection ][ details.foreignId ] ;
			
			// Mark the document as populated for this path NOW!
			if ( details.hostDocument.$ ) { details.hostDocument.$.populated[ details.hostPath ] = true ; }
			
			if ( options.memory || options.deepPopulate )
			{
				// if options.memory, then options.memory === options.cache
				document = options.cache.add( details.foreignCollection , document , options.noReference ) ;
				
				collection = self.collections[ details.foreignCollection ] ;
				collection.DocumentWrapper.create( collection , document , { fromUpstream: true , skipValidation: true } ) ;
				
				if ( options.deepPopulate && options.deepPopulate[ details.foreignCollection ] )
				{
					document.$.preparePopulate( options.deepPopulate[ details.foreignCollection ] , options ) ;
				}
			}
			// Should we call the DocumentWrapper here?
			//else {}
			
			tree.path[ details.operation ]( details.hostDocument , details.hostPath , document ) ;
		}
		
		callback() ;
	} ) ;
} ;



World.prototype.complexPopulate = function complexPopulate( options , callback )
{
	var self = this ;
	
	// Nothing to do...
	if ( ! options.populatingData.complexTargets.length ) { callback() ; return ; }
	
	//console.error( "\ncomplexPopulate complex refs:" , options.populatingData.complexRefs ) ;
	//console.error( "\ncomplexPopulate complex targets:" , options.populatingData.complexTargets ) ;
	
	// First, collect objects
	async.map( options.populatingData.complexRefs , function( pathObjectValues , collectionName , outerMapCallback ) {
		
		var collection = self.collections[ collectionName ] ;
		
		async.map( pathObjectValues , function( objectValues , path , innerMapCallback ) {
			
			var key , values = [] , query = {} ;
			
			for ( key in objectValues ) { values.push( objectValues[ key ] ) ; }
			
			query[ path ] = { $in: values } ;
			//console.error( "\ncollectionName:" , collectionName , "\nquery:" , query ) ;
			
			log.debug( "World complex populate: find on %s %s: %I" , collectionName , path , values ) ;
			
			collection.driver.find( query , function( error , rawBatch ) {
				
				//console.error( "\ncomplexPopulate inner map callback:" , arguments ) ;
				if ( error ) { innerMapCallback( error , {} ) ; return ; }
				
				innerMapCallback( undefined , rootsDb.BatchWrapper.raw.indexPathOfId( rawBatch , path ) ) ;
			} ) ;
		} )
		.exec( outerMapCallback ) ;
	} )
	.exec( function( error , batchStructure ) {
		
		var i , iMax , j , jMax , details , collection , batch ;
		
		//console.error( "\ncomplexPopulate final callback batchStructure:" , batchStructure ) ;
		//console.error( "\ncomplexPopulate final callback batchStructure:" , batchStructure.users.job ) ;
		if ( error ) { callback( error ) ; return ; }
		
		for ( i = 0 , iMax = options.populatingData.complexTargets.length ; i < iMax ; i ++ )
		{
			details = options.populatingData.complexTargets[ i ] ;
			batch = batchStructure[ details.foreignCollection ][ details.foreignPath ][ details.foreignValue ] ;
			
			// Mark the document as populated for this path
			if ( details.hostDocument.$ ) { details.hostDocument.$.populated[ details.hostPath ] = true ; }
			
			if ( options.memory || options.deepPopulate )
			{
				collection = self.collections[ details.foreignCollection ] ;
				
				for ( j = 0 , jMax = batch.length ; j < jMax ; j ++ )
				{
					// if options.memory, then options.memory === options.cache
					batch[ j ] = options.cache.add( details.foreignCollection , batch[ j ] , options.noReference ) ;
					
					collection.DocumentWrapper.create( collection , batch[ j ] , { fromUpstream: true , skipValidation: true } ) ;
					
					if ( options.deepPopulate && options.deepPopulate[ details.foreignCollection ] )
					{
						batch[ j ].$.preparePopulate( options.deepPopulate[ details.foreignCollection ] , options ) ;
					}
				}
			}
			// Should we call the DocumentWrapper here?
			//else {}
			
			tree.path[ details.operation ]( details.hostDocument , details.hostPath , batch ) ;
		}
		
		callback() ;
	} ) ;
} ;



