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

//const fs = require( 'fs' ) ;
const path = require( 'path' ) ;

const Promise = require( 'seventh' ) ;

const tree = require( 'tree-kit' ) ;
const dotPath = tree.dotPath ;
const hash = require( 'hash-kit' ) ;
const doormen = require( 'doormen' ) ;

const log = require( 'logfella' ).global.use( 'roots-db' ) ;

//function noop() {}



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
			//collection = this.collections[ target.foreignCollection ] ,
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
		//return Promise.mapObject( pathToValueObject , ( hashToValueObject , path_ ) => {
		return Promise.mapObject( pathToValueObject , async ( hashToValueObject , path_ ) => {
			//log.fatal( "hashToValueObject: %I" , hashToValueObject ) ;
			var values = Object.values( hashToValueObject ) , query = {} ;
			query[ path_ + '._id' ] = { $in: values } ;
			log.debug( "World complex populate: find on collection '%s' with path '%s' having values in %I" , collectionName , path_ , values ) ;
			//log.debug( "Query: %I" , query ) ;
			population.dbQueries ++ ;
			return collection.driver.find( query ).then( rawBatch => rootsDb.Batch.raw.indexPathOfId( rawBatch , path_ , '_id' ) ) ;
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
World.prototype.import = async function( mappingFile , options = {} , stats = {} ) {
	var concurrency = options.concurrency || 50 ,
		mapping = null ,
		baseDir = null ,
		perCollectionRawBatch = {} ,
		perCollectionBatch = {} ,
		collectionForeignIdIndexes = {} ,
		restoredLinkBatch = [] ,
		duplicateKeyRetries = options.duplicateKeyRetries && options.onDuplicateKey ? options.duplicateKeyRetries : 0 ,
		importId = hash.randomBase36String( 24 ) ;


	stats.step = 0 ;
	stats.stepStr = '' ;
	stats.documents = 0 ;
	stats.savedDocuments = 0 ;
	stats.embeddedDocuments = 0 ;
	stats.links = 0 ;
	stats.linkedDocuments = 0 ;
	stats.savedLinkedDocuments = 0 ;

	// Timers:
	stats.start = Date.now() ;
	stats.duration = null ;
	stats.importToMemoryStart = null ;
	stats.importToMemoryDuration = null ;
	stats.saveToDbStart = null ;
	stats.saveToDbDuration = null ;
	stats.restoreLinksStart = null ;
	stats.restoreLinksDuration = null ;
	stats.saveRestoredLinksToDbStart = null ;
	stats.saveRestoredLinksToDbDuration = null ;

	// Errors:
	stats.duplicatedIds = 0 ;
	stats.orphanEmbeddedDocuments = 0 ;
	stats.orphanLinks = 0 ;


	if ( mappingFile && typeof mappingFile === 'object' ) {
		mapping = mappingFile ;
		mappingFile = null ;
		baseDir = options.baseDir || process.cwd() ;
	}
	else {
		let mappingExt = path.extname( mappingFile ).slice( 1 ) ;

		//log.hdebug( "Mapping file '%s'" , mappingFile ) ;

		if ( mappingExt === 'json' ) {
			mapping = require( mappingFile ) ;
		}

		if ( ! mapping || typeof mapping !== 'object' ) {
			throw new Error( "Can't load mapping file: " + mappingFile ) ;
		}

		baseDir = path.dirname( mappingFile ) ;
	}

	// Add Doormen's sanitizers to available converters
	mapping.converters = Object.assign( {} , doormen.sanitizers , mapping.converters ) ;

	// Sort the sources, so embedded data are populated later, after the host is registered
	mapping.sources.sort( ( a , b ) => ( a.embedded ? 1 : 0 ) - ( b.embedded ? 1 : 0 ) ) ;


	// First step, collect raw batches
	stats.step = 1 ;
	stats.stepStr = '1/4 Import documents to memory' ;
	stats.importToMemoryStart = Date.now() ;

	for ( let sourceParams of mapping.sources ) {
		if ( ! sourceParams || typeof sourceParams !== 'object' ) {
			log.error( "Source is not an object: %I" , sourceParams ) ;
			continue ;
		}

		if ( ! sourceParams.collection ) {
			log.error( "Source without collection: %I" , sourceParams ) ;
			continue ;
		}
		else if ( ! this.collections[ sourceParams.collection ] ) {
			log.error( "Unknown source's collection: %s" , sourceParams.collection ) ;
			continue ;
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

		// Convert all valueMapping to array
		if ( sourceParams.valueMapping ) {
			for ( let property in sourceParams.valueMapping ) {
				if ( ! Array.isArray( sourceParams.valueMapping[ property ] ) ) {
					sourceParams.valueMapping[ property ] = [ sourceParams.valueMapping[ property ] ] ;
				}
			}
		}

		if ( ! perCollectionRawBatch[ sourceParams.collection ] ) { perCollectionRawBatch[ sourceParams.collection ] = [] ; }
		let rawBatch = perCollectionRawBatch[ sourceParams.collection ] ;

		let Importer = require( rootsDb.importer[ sourceParams.type ] ) ;
		let type = Importer.type || sourceParams.type ;

		let importerParams = Object.assign(
			{
				baseDir ,
				format: mapping.format?.[ type ] || sourceParams.format?.[ type ]
			} ,
			sourceParams
		) ;

		let importer = new Importer( importerParams ) ;

		importer.on( 'rawDocument' , rawDocument => {
			let embedded = sourceParams.embedded ;

			if ( embedded && embedded.hostProperty && embedded.embeddedIdProperty ) {
				let id = dotPath.get( rawDocument , embedded.embeddedIdProperty ) ;
				let index = collectionForeignIdIndexes[ sourceParams.collection ] ;

				if ( id !== undefined && id !== null && index && index.has( id ) ) {
					let hostDocument = index.get( id ) ;
					let mappedRawDocument = this.mapImportedRawDocument( rawDocument , sourceParams , mapping.converters , importId ) ;
					dotPath.append( hostDocument , embedded.hostProperty , mappedRawDocument ) ;
					stats.embeddedDocuments ++ ;
				}
				else {
					stats.orphanEmbeddedDocuments ++ ;
				}
			}
			else {
				let mappedRawDocument = this.mapImportedRawDocument( rawDocument , sourceParams , mapping.converters , importId ) ;

				let id = mappedRawDocument._import._foreignId ;

				if ( id !== undefined && id !== null ) {
					let index = collectionForeignIdIndexes[ sourceParams.collection ] ;
					if ( ! index ) { index = collectionForeignIdIndexes[ sourceParams.collection ] = new Map() ; }

					if ( index.has( id ) ) { stats.duplicatedIds ++ ; }
					else { index.set( id , mappedRawDocument ) ; }
				}

				//log.hdebug( "Received rawDocuments: %I -> %I" , rawDocument , mappedRawDocument ) ;
				rawBatch.push( mappedRawDocument ) ;
				stats.documents ++ ;
			}
		} ) ;

		await importer.import() ;
	}

	stats.importToMemoryDuration = Date.now() - stats.importToMemoryStart ;


	// Second step, save to DB
	stats.step = 2 ;
	stats.stepStr = '2/4 Save documents to DB' ;
	stats.saveToDbStart = Date.now() ;

	for ( let collectionName in perCollectionRawBatch ) {
		let rawBatch = perCollectionRawBatch[ collectionName ] ;
		if ( ! rawBatch.length ) { continue ; }

		// We will store the batch of actual RootsDB documents
		if ( ! perCollectionBatch[ collectionName ] ) { perCollectionBatch[ collectionName ] = [] ; }
		let batch = perCollectionBatch[ collectionName ] ;

		let collection = this.collections[ collectionName ] ;

		if ( options.clearCollections ) {
			await collection.clear() ;
		}

		await Promise.concurrent( concurrency , rawBatch , async ( rawDocument ) => {
			if ( options.initDocument ) { options.initDocument( rawDocument , collectionName ) ; }
			
			let document ,
				saved = false ,
				retryCount = 0 ;

			try {
				document = collection.createDocument( rawDocument ) ;
			}
			catch ( error ) {
				log.error( "Can't create document: \n%I\n\nError: %E" , rawDocument , error ) ;
				throw error ;
			}

			while ( ! saved ) {
				try {
					await document.save() ;
					saved = true ;
				}
				catch ( error ) {
					if ( error.code !== 'duplicateKey' || retryCount ++ >= duplicateKeyRetries ) {
						log.error( "Can't insert document: \n%I\n\nError: %E" , rawDocument , error ) ;
						throw error ;
					}

					if ( ! options.onDuplicateKey( collection , document , error ) ) {
						log.error( "Can't insert document ('onDuplicateKey' hook failed): \n%I\n\nError: %E" , rawDocument , error ) ;
						throw error ;
					}
				}
			}

			// Store the RootsDB documents now
			batch.push( document ) ;
			stats.savedDocuments ++ ;

			// In the index, replace the rawDocument by the RootsDB document, it will be used by the restoring links step
			let id = rawDocument._import._foreignId ;

			if ( id !== undefined && id !== null ) {
				let index = collectionForeignIdIndexes[ collectionName ] ;
				index.set( id , document ) ;
			}
		} ) ;
	}

	stats.saveToDbDuration = Date.now() - stats.saveToDbStart ;


	// Third step, restore links, if any...
	stats.step = 3 ;
	stats.stepStr = '3/4 Restore links' ;
	stats.restoreLinksStart = Date.now() ;

	if ( mapping.links && typeof mapping.links === 'object' ) {
		for ( let collectionName in mapping.links ) {
			let batch = perCollectionBatch[ collectionName ] ;
			if ( ! batch.length ) { continue ; }

			for ( let document of batch ) {
				let changed = false ;

				for ( let linkProperty in mapping.links[ collectionName ] ) {
					let linkParams = mapping.links[ collectionName ][ linkProperty ] ;
					let index = collectionForeignIdIndexes[ linkParams.collection ] ;
					//log.hdebug( "index size: %i , keys: %I" , index.size , [ ... index.keys() ] ) ;

					switch ( linkParams.idType ) {
						// Only "foreignId" mode is supported ATM
						case 'foreignId' :
						default : {
							let id = dotPath.get( document , linkParams.idProperty ) ;
							//log.hdebug( "Link id: %I, index has: %I, document: %I" , id , index.has( id ) , document ) ;

							if ( id !== undefined && id !== null && index && index.has( id ) ) {
								let linkedDocument = index.get( id ) ;
								document.setLink( linkProperty , linkedDocument ) ;
								changed = true ;
								stats.links ++ ;
							}
							else {
								stats.orphanLinks ++ ;
							}

							linkParams.idProperty ;
							break ;
						}
					}
				}

				if ( changed ) {
					restoredLinkBatch.push( document ) ;
					stats.linkedDocuments ++ ;
				}
			}
		}
	}

	stats.restoreLinksDuration = Date.now() - stats.restoreLinksStart ;


	// Fourth step, save to DB restored links, if any...
	stats.step = 4 ;
	stats.stepStr = '4/4 Save restored links to DB' ;
	stats.saveRestoredLinksToDbStart = Date.now() ;

	if ( restoredLinkBatch.length ) {
		await Promise.concurrent( concurrency , restoredLinkBatch , async ( document ) => {
			try {
				await document.save() ;
				stats.savedLinkedDocuments ++ ;
			}
			catch ( error ) {
				log.error( "Can't save document with restored link: \n%I\n\nError: %E" , document , error ) ;
				throw error ;
			}
		} ) ;
	}

	stats.saveRestoredLinksToDbDuration = Date.now() - stats.saveRestoredLinksToDbStart ;

	// Total duration
	stats.duration = Date.now() - stats.start ;

	log.hdebug( "Import stats: %Y" , stats ) ;
} ;



World.prototype.mapImportedRawDocument = function( rawDocument , params , converters , importId ) {
	var mappedRawDocument = {
		_import: {}
	} ;

	if ( params.propertyMapping ) {
		for ( let toProperty in params.propertyMapping ) {
			let fromProperty = params.propertyMapping[ toProperty ] ;
			let value = dotPath.get( rawDocument , fromProperty ) ;
			let valueConverters = params.valueMapping?.[ toProperty ] ;

			if ( valueConverters ) {
				for ( let valueConverter of valueConverters ) {
					if ( ! converters.simple[ valueConverter ] ) {
						throw new Error( "Converter '" + valueConverter + "' not found" ) ;
					}

					try {
						value = converters.simple[ valueConverter ]( value ) ;
					}
					catch ( error ) {
						log.error( "Converting property '%s' to '%s' with converter '%s' failed.\nInput value: %Y" , fromProperty , toProperty , valueConverter , value ) ;
						throw error ;
					}
				}
			}

			if ( value !== undefined ) {
				dotPath.set( mappedRawDocument , toProperty , value ) ;
			}
		}
	}

	if ( params.compoundMapping ) {
		for ( let toProperty in params.compoundMapping ) {
			let compoundConverter = params.compoundMapping[ toProperty ] ;

			if ( ! converters.compound[ compoundConverter ] ) {
				throw new Error( "Converter '" + compoundConverter + "' not found" ) ;
			}

			let value = converters.compound[ compoundConverter ]( rawDocument ) ;
			let valueConverters = params.valueMapping?.[ toProperty ] ;

			if ( valueConverters ) {
				for ( let valueConverter of valueConverters ) {
					if ( ! converters.simple[ valueConverter ] ) {
						throw new Error( "Converter '" + valueConverter + "' not found" ) ;
					}

					value = converters.simple[ valueConverter ]( value ) ;
				}
			}

			if ( value !== undefined ) {
				dotPath.set( mappedRawDocument , toProperty , value ) ;
			}
		}
	}

	// Force an _importId
	mappedRawDocument._import._importId = importId ;

	return mappedRawDocument ;
} ;

