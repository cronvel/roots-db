/*
	Roots DB
	
	Copyright (c) 2014 - 2016 Cédric Ronvel
	
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



// Load modules
var rootsDb = require( './rootsDb.js' ) ;

var Promise = require( 'seventh' ) ;

var tree = require( 'tree-kit' ) ;
var log = require( 'logfella' ).global.use( 'roots-db' ) ;
var doormen = require( 'doormen' ) ;

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
	var collection ;
	
	// Validate the schema now
	doormen( rootsDb.Collection.schema , schema ) ;
	
	this.collections[ name ] = ( schema.Collection || rootsDb.Collection ).create( this , name , schema , true ) ;
	
	return this.collections[ name ] ;
} ;



World.prototype.createMemoryModel = function worldCreateMemoryModel( options )
{
	return rootsDb.MemoryModel.create( this , options ) ;
} ;



World.prototype.populate = function populate( options , callback )
{
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	if ( ! options.populateData || ( ! options.populateData.targets.length && ! options.populateData.complexTargets.length ) )
	{
		// Fast exit
		callback() ;
		return ;
	}
	
	options.populatingData = options.populateData ;
	delete options.populateData ;
	
	if ( ! options.populateDepth ) { options.populateDepth = 0 ; options.populateDbQueries = 0 ; }
	options.populateDepth ++ ;
	
	log.debug( 'Entering World#populate() with a depth of %i' , options.populateDepth ) ;
	
	this.simplePopulate( options )
	.then( () => this.complexPopulate( options ) )
	.then( () => {
		if ( ! options.populateData || ( ! options.populateData.targets.length && ! options.populateData.complexTargets.length ) )
		{
			// Fast exit
			return ;
		}
		
		// Recursivity...
		log.debug( 'Deep populate recursivity, populateData: %I' , options.populateData ) ;
		return Promise.promisify( this.populate , this )( options ) ;
	} )
	.callback( callback ) ;
} ;



// Return a promise
World.prototype.simplePopulate = function simplePopulate( options )
{
	// Nothing to do...
	if ( ! options.populatingData.targets.length ) { return Promise.resolve() ; }
	
	return Promise.mapObject( options.populatingData.refs , ( ids , collectionName ) => {
		
		var collection = this.collections[ collectionName ] ;
		
		if ( ids instanceof Set ) { ids = Array.from( ids ) ; }
		
		log.debug( "World populate: multiGet on collection '%s': %I" , collectionName , ids ) ;
		
		options.populateDbQueries ++ ;
		
		return collection.driver.multiGet( ids ).then(
			rawBatch => rootsDb.BatchWrapper.raw.index( rawBatch ) ) ;
	
	} ).then( batchs => {
		
		var i , iMax , details , collection , document ;
		
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
				
				if ( ! document.$ )
				{
					collection = this.collections[ details.foreignCollection ] ;
					collection.DocumentWrapper.create( collection , document , { fromUpstream: true , skipValidation: true } ) ;
				}
				
				if ( options.deepPopulate && options.deepPopulate[ details.foreignCollection ] )
				{
					document.$.preparePopulate( options.deepPopulate[ details.foreignCollection ] , options ) ;
				}
			}
			// Should we call the DocumentWrapper here?
			//else {}
			
			tree.path[ details.operation ]( details.hostDocument , details.hostPath , document ) ;
			
			// Mark the document as no more populating for this path
			if ( details.hostDocument.$ ) { details.hostDocument.$.populating[ details.hostPath ] = false ; }
		}
	} ) ;
} ;



// Return a promise
World.prototype.complexPopulate = function complexPopulate( options , callback )
{
	// Nothing to do...
	if ( ! options.populatingData.complexTargets.length ) { return Promise.resolve() ; }
	
	//console.error( "\ncomplexPopulate complex refs:" , options.populatingData.complexRefs ) ;
	//console.error( "\ncomplexPopulate complex targets:" , options.populatingData.complexTargets ) ;
	
	// First, collect objects
	return Promise.mapObject( options.populatingData.complexRefs , ( pathObjectValues , collectionName ) => {
	
		var collection = this.collections[ collectionName ] ;
		
		return Promise.mapObject( pathObjectValues , ( objectValues , path ) => {
			
			var key , values = [] , query = {} ;
			
			for ( key in objectValues ) { values.push( objectValues[ key ] ) ; }
			
			query[ path ] = { $in: values } ;
			//console.error( "\ncollectionName:" , collectionName , "\nquery:" , query ) ;
			
			log.debug( "World complex populate: find on collection '%s' with path '%s' having values in %I" , collectionName , path , values ) ;
			
			options.populateDbQueries ++ ;
			
			return collection.driver.find( query ).then(
				rawBatch => rootsDb.BatchWrapper.raw.indexPathOfId( rawBatch , path ) ) ;
		} ) ;
	
	} ).then( batchStructure => {
		
		var i , iMax , j , jMax , details , collection , batch ;
		
		//console.error( "\ncomplexPopulate final callback batchStructure:" , batchStructure ) ;
		//console.error( "\ncomplexPopulate final callback batchStructure:" , batchStructure.users.job ) ;
		
		for ( i = 0 , iMax = options.populatingData.complexTargets.length ; i < iMax ; i ++ )
		{
			details = options.populatingData.complexTargets[ i ] ;
			batch = batchStructure[ details.foreignCollection ][ details.foreignPath ][ details.foreignValue ] ;
			
			// Mark the document as populated for this path
			if ( details.hostDocument.$ ) { details.hostDocument.$.populated[ details.hostPath ] = true ; }
			
			if ( options.memory || options.deepPopulate )
			{
				collection = this.collections[ details.foreignCollection ] ;
				
				for ( j = 0 , jMax = batch.length ; j < jMax ; j ++ )
				{
					// if options.memory, then options.memory === options.cache
					batch[ j ] = options.cache.add( details.foreignCollection , batch[ j ] , options.noReference ) ;
					
					if ( ! batch[ j ].$ )
					{
						collection.DocumentWrapper.create( collection , batch[ j ] , { fromUpstream: true , skipValidation: true } ) ;
					}
					
					if ( options.deepPopulate && options.deepPopulate[ details.foreignCollection ] )
					{
						log.debug( 'World#complexPopulate() preparePopulate for an element of the batch, paths: %I , options: %I' , options.deepPopulate[ details.foreignCollection ] , options ) ;
						batch[ j ].$.preparePopulate( options.deepPopulate[ details.foreignCollection ] , options ) ;
					}
				}
			}
			// Should we call the DocumentWrapper here?
			//else {}
			
			tree.path[ details.operation ]( details.hostDocument , details.hostPath , batch ) ;
			
			// Mark the document as no more populating for this path
			if ( details.hostDocument.$ ) { details.hostDocument.$.populating[ details.hostPath ] = false ; }
		}
	} ) ;
} ;


