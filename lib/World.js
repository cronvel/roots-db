/*
	Roots DB

	Copyright (c) 2014 - 2021 Cédric Ronvel

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



const rootsDb = require( './rootsDb.js' ) ;

const fs = require( 'fs' ) ;
const path = require( 'path' ) ;

const Promise = require( 'seventh' ) ;

const tree = require( 'tree-kit' ) ;
const log = require( 'logfella' ).global.use( 'roots-db' ) ;
const doormen = require( 'doormen' ) ;

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



World.prototype.createAndInitCollection = async function( name , schema ) {
	var collection = this.createCollection( name , schema ) ;
	await collection.init() ;
	return collection ;
} ;



World.prototype.createVersionCollection = function( name , schema ) {
	if ( this.versionCollection ) {
		throw new Error( 'Version collection already created' ) ;
	}

	this.versionCollection = this.collections[ name ] = new rootsDb.VersionCollection( this , name , schema ) ;
	return this.versionCollection ;
} ;



World.prototype.createAndInitVersionCollection = async function( name , schema ) {
	var collection = this.createVersionCollection( name , schema ) ;
	await collection.init() ;
	return collection ;
} ;



World.prototype.createMemoryModel = function( options ) {
	return new rootsDb.MemoryModel( this , options ) ;
} ;



World.prototype.populate = async function( population , options ) {
	while (
		population.depth < population.depthLimit
		&& population.populate
		&& ( population.populate.targets.length || population.populate.complexTargets.length )
	) {
		population.populating = population.populate ;
		population.populate = null ;
		population.depth ++ ;

		log.debug( 'Iteration #%i of World#populate()' , population.depth ) ;

		if ( population.populating.targets.length ) {
			await this.simplePopulate( population , options ) ;
		}

		if ( population.populating.complexTargets.length ) {
			await this.complexPopulate( population , options ) ;
		}
	}

	// If any, save all documents having dead links now, at the end of the populate process
	if ( population.documentsHavingDeadLinks.size ) {
		for ( let document of population.documentsHavingDeadLinks ) {
			log.warning( "About to save the documents %s of collection '%s' that have had dead-links previously fixed" , document.getKey() , document._.collection.name ) ;
			try {
				await document.save( { validate: true } ) ;
			}
			catch ( error ) {
				log.error( "World#populate() dead-links: save document failed, error: %E" , error ) ;
			}
		}
	}
} ;



// Return a promise
World.prototype.simplePopulate = async function( population , options ) {

	var batchs = await Promise.mapObject( population.populating.refs , ( ids , collectionName ) => {
		var collection = this.collections[ collectionName ] ;
		if ( ids instanceof Set ) { ids = Array.from( ids ) ; }
		log.debug( "World populate: multiGet on collection '%s' (query #%i):\n%I" , collectionName , population.dbQueries , ids ) ;
		population.dbQueries ++ ;
		return collection.driver.multiGet( ids ).then( rawBatch => rootsDb.Batch.raw.index( rawBatch ) ) ;
	} ) ;


	population.populating.targets.forEach( target => {
		//console.log( "target:" , target.hostDocument ) ;
		var documentProxy , targetObject ,
			collection = this.collections[ target.foreignCollection ] ,
			rawDocument = batchs[ target.foreignCollection ][ target.foreignId ] ;

		// Mark the document as populated for this path NOW!
		target.hostDocument._.populated[ target.hostPath ] = true ;

		if ( rawDocument ) {
			rawDocument = population.cache.addRaw( target.foreignCollection , rawDocument , options.noReference ) ;
			documentProxy = population.cache.getProxyFromRaw( target.foreignCollection , rawDocument ) ;

			if ( options.populateTagMask ) {
				documentProxy.setPopulateTagMask( options.populateTagMask ) ;
			}

			if ( options.deepPopulate && options.deepPopulate[ target.foreignCollection ] ) {
				documentProxy._.preparePopulate( options.deepPopulate[ target.foreignCollection ] , population , options ) ;
			}

			targetObject = tree.dotPath.get( target.hostDocument._.raw , target.hostPath ) ;
			target.hostDocument._.populatedDocumentProxies.set( targetObject , documentProxy ) ;
		}
		else {
			// dead/broken link
			log.warning( "Dead link detected on document %s of collection '%s', path: %s" , target.hostDocument.getKey() , target.hostDocument._.collection.name , target.hostPath ) ;
			//tree.dotPath.set( target.hostDocument._.raw , target.hostPath , null ) ;
			tree.dotPath.set( target.hostDocument , target.hostPath , null ) ;
			population.documentsHavingDeadLinks.add( target.hostDocument ) ;
		}

		// Mark the Document as no more populating for this path
		target.hostDocument._.populating[ target.hostPath ] = false ;
	} ) ;
} ;



// Return a promise
World.prototype.complexPopulate = async function( population , options ) {
	//console.error( "\ncomplexPopulate complex refs:" , population.populating.complexRefs ) ;
	//console.error( "\ncomplexPopulate complex targets:" , population.populating.complexTargets ) ;

	// First, collect objects
	var batchStructure = await Promise.mapObject( population.populating.complexRefs , ( pathToValueObject , collectionName ) => {
		var collection = this.collections[ collectionName ] ;

		// Nested loop
		//return Promise.mapObject( pathToValueObject , ( hashToValueObject , path ) => {
		return Promise.mapObject( pathToValueObject , async ( hashToValueObject , path ) => {
			//log.fatal( "hashToValueObject: %I" , hashToValueObject ) ;
			var values = Object.values( hashToValueObject ) , query = {} ;
			query[ path + '._id' ] = { $in: values } ;
			log.debug( "World complex populate: find on collection '%s' with path '%s' having values in %I" , collectionName , path , values ) ;
			//log.debug( "Query: %I" , query ) ;
			population.dbQueries ++ ;
			return collection.driver.find( query ).then( rawBatch => rootsDb.Batch.raw.indexPathOfId( rawBatch , path , '_id' ) ) ;
		} ) ;
	} ) ;

	//console.error( "\ncomplexPopulate final batchStructure:" , batchStructure ) ;
	//console.error( "\ncomplexPopulate final batchStructure.users.job:" , batchStructure.users.job ) ;

	population.populating.complexTargets.forEach( target => {
		//console.log( "target:" , target.hostDocument ) ;
		var targetObject ,
			collection = this.collections[ target.foreignCollection ] ,
			rawBatch = batchStructure[ target.foreignCollection ][ target.foreignPath ][ '' + target.foreignValue ] ;

		if ( ! rawBatch ) {
			log.debug( "World complex populate: nothing found for %s.%s.%s" , target.foreignCollection , target.foreignPath , target.foreignValue ) ;
			return ;
		}

		// Mark the rawDocument as populated for this path
		target.hostDocument._.populated[ target.hostPath ] = true ;

		if ( target.foreignAnyCollection ) {
			// We have to filter batch on collection
			rawBatch = rawBatch.filter( rawDocument =>
				tree.dotPath.get( rawDocument , target.foreignPath + '._collection' ) === target.hostDocument._.collection.name
			) ;
		}

		rawBatch = rawBatch.map( ( rawDocument , index ) => {
			// Manage the cache and population of each object, return a new array of documentProxy
			rawDocument = population.cache.addRaw( target.foreignCollection , rawDocument , options.noReference ) ;
			var documentProxy = population.cache.getProxyFromRaw( target.foreignCollection , rawDocument ) ;

			if ( options.populateTagMask ) {
				documentProxy.setPopulateTagMask( options.populateTagMask ) ;
			}

			if ( options.deepPopulate && options.deepPopulate[ target.foreignCollection ] ) {
				log.debug( 'World#complexPopulate() preparePopulate for an element of the batch, paths: %I , options: %I' , options.deepPopulate[ target.foreignCollection ] , options ) ;
				documentProxy._.preparePopulate( options.deepPopulate[ target.foreignCollection ] , population , options ) ;
			}

			return documentProxy ;
		} ) ;

		// Convert it to a true Batch array-like object
		var batch = new collection.Batch( collection , rawBatch , { fromUpstream: true , skipValidation: true } ) ;

		// Document#preparePopulate() ensure that the target object exists
		targetObject = tree.dotPath.get( target.hostDocument._.raw , target.hostPath ) ;
		target.hostDocument._.populatedDocumentProxies.set( targetObject , batch ) ;

		// Mark the Document as no more populating for this path
		target.hostDocument._.populating[ target.hostPath ] = false ;
	} ) ;
} ;



// Import data, in-memory process, not appropriate for big DB migration
World.prototype.import = async function( mappingFile , options = {} ) {
	var concurrency = options.concurrency || 50 ,
		mapping = null ;
	
	if ( mappingFile && typeof mappingFile === 'object' ) {
		mapping = mappingFile ;
		mappingFile = null ;
	}
	else {
		let mappingExt = path.extname( mappingFile ).slice( 1 ) ;

		log.hdebug( "Mapping file '%s'" , mappingFile ) ;

		if ( mappingExt === 'json' ) {
			mapping = require( mappingFile ) ;
		}

		if ( ! mapping || typeof mapping !== 'object' ) {
			throw new Error( "Can't load mapping file: " + mappingFile ) ;
		}
	}


	var baseDir = path.dirname( mappingFile ) ,
		perCollectionRawBatch = {} ;


	// First step, collect raw batches

	for ( let sourceParams of mapping.sources ) {
		if ( ! sourceParams || typeof sourceParams !== 'object' ) {
			log.error( "Source is not an object: %I" , sourceParams ) ;
			continue ;
		}

		if ( ! sourceParams.collection ) {
			log.error( "Source without collection: %I" , sourceParams ) ;
		}
		else if ( ! this.collections[ sourceParams.collection ] ) {
			log.error( "Unknown source's collection: %s" , sourceParams.collection ) ;
		}

		if ( ! sourceParams.type ) {
			if ( sourceParams.file ) {
				sourceParams.type = path.extname( sourceParams.file ).slice( 1 ) ;
			}
			else {
				log.error( "Source without type: %I" , sourceParams ) ;
				continue ;
			}
		}

		if ( ! rootsDb.hasImporter( sourceParams.type ) ) {
			log.error( "No importer found for file extension '%s'" , sourceParams.type ) ;
			continue ;
		}

		if ( sourceParams.file ) {
			if ( ! path.isAbsolute( sourceParams.file ) ) {
				sourceParams.file = path.join( baseDir , sourceParams.file ) ;
			}

			log.hdebug( "Source file '%s'" , sourceParams.file ) ;
		}
		
		let collection = this.collections[ sourceParams.collection ] ;

		if ( ! perCollectionRawBatch[ sourceParams.collection ] ) { perCollectionRawBatch[ sourceParams.collection ] = [] ; }
		let rawBatch = perCollectionRawBatch[ sourceParams.collection ] ;

		let Importer = require( rootsDb.importer[ sourceParams.type ] ) ;
		let type = Importer.type || sourceParams.type ;
		
		let importerParams = Object.assign( {
				baseDir ,
				format: mapping.format?.[ type ] || collectionMapping.format?.[ type ]
			} ,
			sourceParams
		) ;

		let importer = new Importer( importerParams ) ;
		
		importer.on( 'rawDocument' , rawDocument => {
			rawBatch.push( rawDocument ) ;
			log.hdebug( "Received rawDocument: %I" , rawDocument ) ;
		} ) ;

		await importer.import() ;
	}


	// Second step, restore links

	// (code)


	// Third step, save to DB

	for ( let collectionName in perCollectionRawBatch ) {
		let rawBatch = perCollectionRawBatch[ collectionName ] ;
		let collection = this.collections[ collectionName ] ;

		await Promise.concurrent( concurrency , rawBatch , rawDocument => {
			let document = collection.createDocument( rawDocument ) ;
			return document.save() ;
		} ) ;
	}
} ;

