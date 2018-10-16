/*
	Roots DB

	Copyright (c) 2014 - 2018 Cédric Ronvel

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



function World() {
	this.collections = {} ;
}

module.exports = World ;



// Backward compat
World.create = ( ... args ) => new World( ... args ) ;



World.prototype.createCollection = function( name , schema ) {
	this.collections[ name ] = new ( schema.Collection || rootsDb.Collection )( this , name , schema ) ;
	return this.collections[ name ] ;
} ;



World.prototype.createMemoryModel = function( options ) {
	return new rootsDb.MemoryModel( this , options ) ;
} ;



World.prototype.populate = async function( population , options ) {
	if ( ! population.populate || ( ! population.populate.targets.length && ! population.populate.complexTargets.length ) ) {
		// Fast exit
		return ;
	}

	population.populating = population.populate ;
	population.populate = null ;
	population.depth ++ ;

	log.debug( 'Entering World#populate() with a depth of %i' , population.depth ) ;

	await this.simplePopulate( population , options ) ;
	//await this.complexPopulate( population , options ) ;
	
	if ( ! population.populate || ( ! population.populate.targets.length && ! population.populate.complexTargets.length ) ) {
		// Fast exit
		return ;
	}
	
	// Recursivity...
	log.debug( 'Deep populate recursivity, populateData: %I' , population.populate ) ;
	return this.populate( population , options ) ;
} ;



// Return a promise
World.prototype.simplePopulate = async function( population , options ) {
	// Nothing to do...
	if ( ! population.populating.targets.length ) { return ; }

	var batchs = await Promise.mapObject( population.populating.refs , ( ids , collectionName ) => {
		var collection = this.collections[ collectionName ] ;
		if ( ids instanceof Set ) { ids = Array.from( ids ) ; }
		log.debug( "World populate: multiGet on collection '%s' (query #%i):\n%I" , collectionName , population.dbQueries , ids ) ;
		population.dbQueries ++ ;
		return collection.driver.multiGet( ids ).then( rawBatch => rootsDb.Batch.raw.index( rawBatch ) ) ;
	} ) ;
	

	population.populating.targets.forEach( target => {
		console.log( "target:" , target.hostDocument ) ;
		var collection , documentProxy , targetObject ,
			rawDocument = batchs[ target.foreignCollection ][ target.foreignId ] ;

		// Mark the document as populated for this path NOW!
		target.hostDocument._.populated[ target.hostPath ] = true ;

		collection = this.collections[ target.foreignCollection ] ;
		rawDocument = population.cache.addRaw( target.foreignCollection , rawDocument , options.noReference ) ;
		documentProxy = population.cache.getProxyFromRaw( target.foreignCollection , rawDocument ) ;
		
		if ( options.deepPopulate && options.deepPopulate[ target.foreignCollection ] ) {
			documentProxy._.preparePopulate( options.deepPopulate[ target.foreignCollection ] , options , population ) ;
		}

		targetObject = tree.path.get( target.hostDocument._.raw , target.hostPath ) ;
		target.hostDocument._.populatedDocumentProxies.set( targetObject , documentProxy ) ;
		
		// Mark the Document as no more populating for this path
		target.hostDocument._.populating[ target.hostPath ] = false ;
	} ) ;
} ;



// Return a promise
World.prototype.complexPopulate = function( population , options ) {
	// Nothing to do...
	if ( ! population.populating.complexTargets.length ) { return Promise.resolve() ; }

	//console.error( "\ncomplexPopulate complex refs:" , population.populating.complexRefs ) ;
	//console.error( "\ncomplexPopulate complex targets:" , population.populating.complexTargets ) ;

	// First, collect objects
	return Promise.mapObject( population.populating.complexRefs , ( pathObjectValues , collectionName ) => {

		var collection = this.collections[ collectionName ] ;

		return Promise.mapObject( pathObjectValues , ( objectValues , path ) => {

			var key , values = [] , query = {} ;

			for ( key in objectValues ) { values.push( objectValues[ key ] ) ; }

			query[ path ] = { $in: values } ;
			//console.error( "\ncollectionName:" , collectionName , "\nquery:" , query ) ;

			log.debug( "World complex populate: find on collection '%s' with path '%s' having values in %I" , collectionName , path , values ) ;

			population.dbQueries ++ ;

			return collection.driver.find( query ).then(
				rawBatch => rootsDb.Batch.raw.indexPathOfId( rawBatch , path ) ) ;
		} ) ;

	} ).then( batchStructure => {

		var i , iMax , j , jMax , details , collection , batch ;

		//console.error( "\ncomplexPopulate final callback batchStructure:" , batchStructure ) ;
		//console.error( "\ncomplexPopulate final callback batchStructure:" , batchStructure.users.job ) ;

		for ( i = 0 , iMax = population.populating.complexTargets.length ; i < iMax ; i ++ ) {
			details = population.populating.complexTargets[ i ] ;
			batch = batchStructure[ details.foreignCollection ][ details.foreignPath ][ details.foreignValue ] ;

			// Mark the rawDocument as populated for this path
			if ( details.hostDocument.$ ) { details.hostDocument.$.populated[ details.hostPath ] = true ; }

			if ( options.memory || options.deepPopulate ) {
				collection = this.collections[ details.foreignCollection ] ;

				for ( j = 0 , jMax = batch.length ; j < jMax ; j ++ ) {
					// if options.memory, then options.memory === options.cache
					batch[ j ] = options.cache.add( details.foreignCollection , batch[ j ] , options.noReference ) ;

					if ( ! batch[ j ].$ ) {
						 batch[ j ] = new collection.Document( collection , batch[ j ] , { fromUpstream: true , skipValidation: true } ) ;
					}

					if ( options.deepPopulate && options.deepPopulate[ details.foreignCollection ] ) {
						log.debug( 'World#complexPopulate() preparePopulate for an element of the batch, paths: %I , options: %I' , options.deepPopulate[ details.foreignCollection ] , options ) ;
						batch[ j ].$.preparePopulate( options.deepPopulate[ details.foreignCollection ] , options ) ;
					}
				}
			}
			// Should we call the Document here?
			//else {}

			tree.path[ details.operation ]( details.hostDocument , details.hostPath , batch ) ;

			// Mark the rawDocument as no more populating for this path
			if ( details.hostDocument.$ ) { details.hostDocument.$.populating[ details.hostPath ] = false ; }
		}
	} ) ;
} ;


