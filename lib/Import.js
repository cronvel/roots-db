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
const wildDotPath = tree.wildDotPath ;
const hash = require( 'hash-kit' ) ;
const doormen = require( 'doormen' ) ;

const log = require( 'logfella' ).global.use( 'roots-db' ) ;



function Import( world , mappingFile , options = {} , stats = {} ) {
	this.world = world ;
	this.mappingFile = mappingFile ;
	this.baseDir = null ;

	this.mapping = null ;

	this.concurrency = options.concurrency || 50 ;
	this.onDuplicateKey = options.onDuplicateKey ;
	this.duplicateKeyRetries = options.duplicateKeyRetries && this.onDuplicateKey ? options.duplicateKeyRetries : 0 ;
	this.clearCollections = options.clearCollections ;
	this.initDocument = options.initDocument ;

	this.perCollectionRawBatch = {} ;
	this.perCollectionBatch = {} ;
	this.collectionForeignIdIndexes = {} ;
	this.restoredLinkBatch = [] ;
	this.importId = hash.randomBase36String( 24 ) ;


	if ( this.mappingFile && typeof this.mappingFile === 'object' ) {
		this.mapping = this.mappingFile ;
		this.mappingFile = null ;
		this.baseDir = options.baseDir || process.cwd() ;
	}
	else {
		let mappingExt = path.extname( this.mappingFile ).slice( 1 ) ;

		//log.hdebug( "Mapping file '%s'" , this.mappingFile ) ;

		if ( mappingExt === 'json' ) {
			this.mapping = require( this.mappingFile ) ;
		}

		if ( ! this.mapping || typeof this.mapping !== 'object' ) {
			throw new Error( "Can't load mapping file: " + this.mappingFile ) ;
		}

		this.baseDir = path.dirname( this.mappingFile ) ;
	}

	// If the mapping file includes "duplicateKeyRetries", use it...
	if ( this.mapping.duplicateKeyRetries ) { this.duplicateKeyRetries = this.mapping.duplicateKeyRetries ; }

	// Add Doormen's sanitizers to available converters
	this.mapping.converters = Object.assign( {} , doormen.sanitizers , this.mapping.converters ) ;

	// Sort the sources, so embedded data are populated later, after the host is registered
	this.mapping.sources.sort( ( a , b ) => ( a.embedded ? 1 : 0 ) - ( b.embedded ? 1 : 0 ) ) ;


	this.stats = stats ;
	Import.createImportStats( this.stats ) ;
}

module.exports = Import ;



// Import data, in-memory process, not appropriate for big DB migration.
// If memory limit is hit, run node with the option: --max-old-space-size=8192 (or whatever size you want)
Import.prototype.import = async function() {
	this.stats.startTime = Date.now() ;
	( { heapUsed: this.stats.startingHeapMemory , external: this.stats.startingExternalMemory } = process.memoryUsage() ) ;

	await this.retrieveDocuments() ;
	await this.importToMemory() ;
	await this.saveDocuments() ;
	this.restoreLinks() ;
	await this.saveLinkingDocuments() ;

	// Total duration
	this.stats.duration = Date.now() - this.stats.startTime ;
	//log.hdebug( "Import stats: %[10l50000]Y" , this.stats ) ;
} ;



Import.prototype.retrieveDocuments = async function() {
	// Second step, save to DB
	this.stats.step = 1 ;
	this.stats.stepStr = '1/5 Retrieve pre-existing documents from DB' ;
	this.stats.retrieveFromDbStartTime = Date.now() ;

	if ( this.mapping.retrieve ) {
		for ( let sourceParams of this.mapping.retrieve ) {
			if ( ! sourceParams || typeof sourceParams !== 'object' ) {
				log.error( "Retrieve-source is not an object: %I" , sourceParams ) ;
				continue ;
			}

			let collectionName = sourceParams.collection ;

			if ( ! collectionName ) {
				log.error( "Retrieve-source without collection: %I" , sourceParams ) ;
				continue ;
			}
			else if ( ! this.world.collections[ collectionName ] ) {
				log.error( "Unknown retrieve-source's collection: %s" , collectionName ) ;
				continue ;
			}


			// Collection stats
			if ( ! this.stats.perCollections[ collectionName ] ) {
				this.stats.perCollections[ collectionName ] = Import.createImportSubStats() ;
			}

			let collection = this.world.collections[ collectionName ] ;
			let collectionStats = this.stats.perCollections[ collectionName ] ;

			let fingerprint = sourceParams.fingerprint && typeof sourceParams.fingerprint === 'object' ? sourceParams.fingerprint : {} ;
			let batch = await collection.collect( fingerprint ) ;

			for ( let document of batch ) {
				let id = dotPath.get( document , sourceParams.idKey || '_id' ) ;

				if ( id !== undefined && id !== null && id !== '' ) {
					if ( id && typeof id === 'object' ) { id = '' + id ; }

					let index = this.collectionForeignIdIndexes[ collectionName ] ;
					if ( ! index ) { index = this.collectionForeignIdIndexes[ collectionName ] = new Map() ; }

					if ( index.has( id ) ) {
						this.stats.duplicatedIds ++ ;
						collectionStats.duplicatedIds ++ ;
					}
					else {
						index.set( id , document ) ;
						this.stats.retrievedDocuments ++ ;
						collectionStats.retrievedDocuments ++ ;
					}
				}
			}
		}
	}

	log.hdebug( "Retrieved: %i documents" , this.stats.retrievedDocuments ) ;
	this.stats.retrieveFromDbDuration = Date.now() - this.stats.retrieveFromDbStartTime ;
	( { heapUsed: this.stats.retrieveFromDbHeapMemory , external: this.stats.retrieveFromDbExternalMemory } = process.memoryUsage() ) ;
} ;



Import.prototype.importToMemory = async function() {
	// First step, collect raw batches
	this.stats.step = 2 ;
	this.stats.stepStr = '2/5 Import documents to memory' ;
	this.stats.importToMemoryStartTime = Date.now() ;

	for ( let sourceParams of this.mapping.sources ) {
		if ( ! sourceParams || typeof sourceParams !== 'object' ) {
			log.error( "Source is not an object: %I" , sourceParams ) ;
			continue ;
		}

		if ( ! sourceParams.collection ) {
			log.error( "Source without collection: %I" , sourceParams ) ;
			continue ;
		}
		else if ( ! this.world.collections[ sourceParams.collection ] ) {
			log.error( "Unknown source's collection: %s" , sourceParams.collection ) ;
			continue ;
		}


		// Collection stats
		if ( ! this.stats.perCollections[ sourceParams.collection ] ) {
			this.stats.perCollections[ sourceParams.collection ] = Import.createImportSubStats() ;
		}

		let collectionStats = this.stats.perCollections[ sourceParams.collection ] ;


		sourceParams.fileId = sourceParams.file ;

		if ( ! sourceParams.type ) {
			if ( sourceParams.file ) {
				sourceParams.type = path.extname( sourceParams.file ).slice( 1 ) ;
			}
			else {
				log.error( "Source without type: %I" , sourceParams ) ;
				continue ;
			}
		}


		// Source file stats
		let sourceFileStats = null ;

		if ( sourceParams.fileId ) {
			if ( ! this.stats.perSourceFiles[ sourceParams.fileId ] ) {
				this.stats.perSourceFiles[ sourceParams.fileId ] = Import.createImportSubStats() ;
			}

			sourceFileStats = this.stats.perSourceFiles[ sourceParams.fileId ] ;
		}


		if ( ! rootsDb.hasImporter( sourceParams.type ) ) {
			log.error( "No importer found for file extension '%s'" , sourceParams.type ) ;
			continue ;
		}

		if ( sourceParams.file ) {
			if ( ! path.isAbsolute( sourceParams.file ) ) {
				sourceParams.file = path.join( this.baseDir , sourceParams.file ) ;
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

		if ( ! this.perCollectionRawBatch[ sourceParams.collection ] ) { this.perCollectionRawBatch[ sourceParams.collection ] = [] ; }
		let rawBatch = this.perCollectionRawBatch[ sourceParams.collection ] ;

		let Importer = require( rootsDb.importer[ sourceParams.type ] ) ;
		let type = Importer.type || sourceParams.type ;

		let importerParams = Object.assign(
			{
				baseDir: this.baseDir ,
				// sourceParams can override the format, if a specific file has different options...
				format: this.mapping.format?.[ type ]
			} ,
			sourceParams
		) ;

		let importer = new Importer( importerParams ) ;

		importer.on( 'rawDocument' , rawDocument => {
			let embedded = sourceParams.embedded?.hostProperty && sourceParams.embedded?.embeddedIdProperty ? sourceParams.embedded : null ;

			// First, apply the pre-filter
			if ( sourceParams.preFilter && this.mapping.filters?.[ sourceParams.preFilter ] ) {
				if ( this.mapping.filters[ sourceParams.preFilter ]( rawDocument ) ) {
					if ( embedded ) {
						this.stats.filteredInEmbeddedDocuments ++ ;
						collectionStats.filteredInEmbeddedDocuments ++ ;
						if ( sourceFileStats ) { sourceFileStats.filteredInEmbeddedDocuments ++ ; }
					}
					else {
						this.stats.filteredInDocuments ++ ;
						collectionStats.filteredInDocuments ++ ;
						if ( sourceFileStats ) { sourceFileStats.filteredInDocuments ++ ; }
					}
				}
				else {
					if ( embedded ) {
						this.stats.filteredOutEmbeddedDocuments ++ ;
						collectionStats.filteredOutEmbeddedDocuments ++ ;
						if ( sourceFileStats ) { sourceFileStats.filteredOutEmbeddedDocuments ++ ; }
					}
					else {
						this.stats.filteredOutDocuments ++ ;
						collectionStats.filteredOutDocuments ++ ;
						if ( sourceFileStats ) { sourceFileStats.filteredOutDocuments ++ ; }
					}

					return ;
				}
			}

			if ( embedded ) {
				let id = dotPath.get( rawDocument , embedded.embeddedIdProperty ) ;
				let index = this.collectionForeignIdIndexes[ sourceParams.collection ] ;

				if ( id !== undefined && id !== null && id !== '' && index && index.has( id ) ) {
					let hostDocument = index.get( id ) ;
					let mappedRawDocument = this.mapImportedRawDocument( rawDocument , sourceParams ) ;

					// Apply the post-filters
					if ( sourceParams.postFilter && this.mapping.filters?.[ sourceParams.postFilter ] ) {
						if ( this.mapping.filters[ sourceParams.postFilter ]( mappedRawDocument ) ) {
							this.stats.filteredInEmbeddedDocuments ++ ;
							collectionStats.filteredInEmbeddedDocuments ++ ;
							if ( sourceFileStats ) { sourceFileStats.filteredInEmbeddedDocuments ++ ; }
						}
						else {
							this.stats.filteredOutEmbeddedDocuments ++ ;
							collectionStats.filteredOutEmbeddedDocuments ++ ;
							if ( sourceFileStats ) { sourceFileStats.filteredOutEmbeddedDocuments ++ ; }
							return ;
						}
					}

					if ( Array.isArray( sourceParams.embeddedDedupProperties ) ) {
						let existingList = dotPath.get( hostDocument , embedded.hostProperty ) ;
						if ( existingList ) {
							let isDup = existingList.some( existing => {
								for ( let property of sourceParams.embeddedDedupProperties ) {
									if ( ! doormen.isEqual( existing[ property ] , mappedRawDocument[ property ] ) ) { return false ; }
								}

								return true ;
							} ) ;

							if ( isDup ) {
								this.stats.dedupedEmbeddedDocuments ++ ;
								collectionStats.dedupedEmbeddedDocuments ++ ;
								if ( sourceFileStats ) { sourceFileStats.dedupedEmbeddedDocuments ++ ; }
								return ;
							}
						}
					}

					dotPath.append( hostDocument , embedded.hostProperty , mappedRawDocument ) ;
					this.stats.embeddedDocuments ++ ;
					collectionStats.embeddedDocuments ++ ;
					if ( sourceFileStats ) { sourceFileStats.embeddedDocuments ++ ; }
				}
				else {
					this.stats.orphanEmbeddedDocuments ++ ;
					collectionStats.orphanEmbeddedDocuments ++ ;
					if ( sourceFileStats ) { sourceFileStats.orphanEmbeddedDocuments ++ ; }
				}
			}
			else {
				let mappedRawDocument = this.mapImportedRawDocument( rawDocument , sourceParams ) ;

				// Apply the post-filters
				if ( sourceParams.postFilter && this.mapping.filters?.[ sourceParams.postFilter ] ) {
					if ( this.mapping.filters[ sourceParams.postFilter ]( mappedRawDocument ) ) {
						this.stats.filteredInDocuments ++ ;
						collectionStats.filteredInDocuments ++ ;
						if ( sourceFileStats ) { sourceFileStats.filteredInDocuments ++ ; }
					}
					else {
						this.stats.filteredOutDocuments ++ ;
						collectionStats.filteredOutDocuments ++ ;
						if ( sourceFileStats ) { sourceFileStats.filteredOutDocuments ++ ; }
						return ;
					}
				}

				let id = mappedRawDocument._import._foreignId ;

				if ( id !== undefined && id !== null && id !== '' ) {
					let index = this.collectionForeignIdIndexes[ sourceParams.collection ] ;
					if ( ! index ) { index = this.collectionForeignIdIndexes[ sourceParams.collection ] = new Map() ; }

					if ( index.has( id ) ) {
						this.stats.duplicatedIds ++ ;
						collectionStats.duplicatedIds ++ ;
						if ( sourceFileStats ) { sourceFileStats.duplicatedIds ++ ; }
					}
					else {
						index.set( id , mappedRawDocument ) ;
					}
				}

				//log.hdebug( "Received rawDocuments: %I -> %I" , rawDocument , mappedRawDocument ) ;
				rawBatch.push( mappedRawDocument ) ;
				this.stats.documents ++ ;
				collectionStats.documents ++ ;
				if ( sourceFileStats ) { sourceFileStats.documents ++ ; }
			}
		} ) ;

		await importer.import() ;
	}

	this.stats.importToMemoryDuration = Date.now() - this.stats.importToMemoryStartTime ;
	( { heapUsed: this.stats.importToMemoryHeapMemory , external: this.stats.importToMemoryExternalMemory } = process.memoryUsage() ) ;
} ;



Import.prototype.saveDocuments = async function() {
	// Second step, save to DB
	this.stats.step = 3 ;
	this.stats.stepStr = '3/5 Save documents to DB' ;
	this.stats.saveToDbStartTime = Date.now() ;

	for ( let collectionName in this.perCollectionRawBatch ) {
		let rawBatch = this.perCollectionRawBatch[ collectionName ] ;
		if ( ! rawBatch.length ) { continue ; }

		// We will store the batch of actual RootsDB documents
		if ( ! this.perCollectionBatch[ collectionName ] ) { this.perCollectionBatch[ collectionName ] = [] ; }
		let batch = this.perCollectionBatch[ collectionName ] ;

		let collection = this.world.collections[ collectionName ] ;
		let collectionStats = this.stats.perCollections[ collectionName ] ;

		if ( this.clearCollections ) {
			await collection.clear() ;
		}

		await Promise.concurrent( this.concurrency , rawBatch , async ( rawDocument ) => {
			if ( this.initDocument ) { this.initDocument( rawDocument , collectionName ) ; }

			let document ,
				saved = false ,
				retryCount = 0 ;

			try {
				document = collection.createDocument( rawDocument ) ;
			}
			catch ( error ) {
				log.error( "Can't create document of collection '%s': \n%[8l10000]I\n\nError: %E" , collectionName , rawDocument , error ) ;
				throw error ;
			}

			let sourceFileStats = rawDocument._import._fileSource ? this.stats.perSourceFiles[ rawDocument._import._fileSource ] : null ;

			while ( ! saved ) {
				try {
					await document.save() ;
					saved = true ;
				}
				catch ( error ) {
					if ( error.code !== 'duplicateKey' || retryCount ++ >= this.duplicateKeyRetries ) {
						log.error( "Can't insert document: \n%I\n\nError: %E" , rawDocument , error ) ;
						throw error ;
					}

					if ( ! this.onDuplicateKey || ! this.onDuplicateKey( collection , document , error ) ) {
						if ( this.mapping.deduplicators?.[ collectionName ] && this.mapping.deduplicators?.[ collectionName ]( document , error.indexProperties ) ) {
							this.stats.dedupedDocuments ++ ;
							collectionStats.dedupedDocuments ++ ;
							if ( sourceFileStats ) { sourceFileStats.dedupedDocuments ++ ; }
						}
						else {
							log.error( "Can't insert document, can't dedup duplicateKey: \n%I\n\nError: %E" , rawDocument , error ) ;
							throw error ;
						}
					}
				}
			}

			// Store the RootsDB documents now
			batch.push( document ) ;
			this.stats.savedDocuments ++ ;
			collectionStats.savedDocuments ++ ;
			if ( sourceFileStats ) { sourceFileStats.savedDocuments ++ ; }

			// In the index, replace the rawDocument by the RootsDB document, it will be used by the restoring links step
			let id = rawDocument._import._foreignId ;

			if ( id !== undefined && id !== null && id !== '' ) {
				let index = this.collectionForeignIdIndexes[ collectionName ] ;
				index.set( id , document ) ;
			}
		} ) ;
	}

	this.stats.saveToDbDuration = Date.now() - this.stats.saveToDbStartTime ;
	( { heapUsed: this.stats.saveToDbHeapMemory , external: this.stats.saveToDbExternalMemory } = process.memoryUsage() ) ;

	// Attempt to free some memory, raw documents will not be used anymore
	this.perCollectionRawBatch = null ;
} ;



Import.prototype.restoreLinks = function() {
	// Third step, restore links, if any...
	this.stats.step = 4 ;
	this.stats.stepStr = '4/5 Restore links' ;
	this.stats.restoreLinksStartTime = Date.now() ;

	if ( this.mapping.links && typeof this.mapping.links === 'object' ) {
		for ( let collectionName in this.mapping.links ) {
			let batch = this.perCollectionBatch[ collectionName ] ;
			if ( ! batch.length ) { continue ; }

			let collectionStats = this.stats.perCollections[ collectionName ] ;

			for ( let document of batch ) {
				let sourceFileStats = document._import._fileSource ? this.stats.perSourceFiles[ document._import._fileSource ] : null ;

				let linkingData = {
					changed: false ,
					document ,
					collectionStats ,
					sourceFileStats
				} ;

				for ( let linkParams of this.mapping.links[ collectionName ] ) {
					let subDocuments =
						linkParams.embedded ? wildDotPath.getPathValueMap( document , linkParams.embedded ) :
						{ "": document } ;

					linkingData.idType = linkParams.idType ;

					for ( let subPath in subDocuments ) {
						let subDocument = linkingData.subDocument = subDocuments[ subPath ] ;
						linkingData.linkProperty = subPath ? subPath + '.' + linkParams.property : linkParams.property ;

						let toCollection = linkingData.toCollection =
							linkParams.collectionProperty ? dotPath.get( subDocument , linkParams.collectionProperty ) :
							linkParams.collection ;

						linkingData.index = this.collectionForeignIdIndexes[ toCollection ] ;

						if ( ! collectionStats.perLinkedCollections[ toCollection ] ) {
							collectionStats.perLinkedCollections[ toCollection ] = {
								links: 0 ,
								orphanLinks: 0
							} ;
						}

						linkingData.linkedCollectionStats = collectionStats.perLinkedCollections[ toCollection ] ;

						//log.hdebug( "index size: %i , keys: %I" , index.size , [ ... index.keys() ] ) ;

						if ( linkParams.idProperty ) {
							linkingData.idProperty = linkParams.idProperty ;
							this.restoreOneSingleLink( linkingData ) ;
						}
						else if ( linkParams.idListProperty ) {
							linkingData.idListProperty = linkParams.idListProperty ;
							this.restoreOneMultiLink( linkingData ) ;
						}
					}
				}

				if ( linkingData.changed ) {
					this.restoredLinkBatch.push( document ) ;
					this.stats.linkingDocuments ++ ;
					collectionStats.linkingDocuments ++ ;
					if ( sourceFileStats ) { sourceFileStats.linkingDocuments ++ ; }
				}
			}
		}
	}

	this.stats.restoreLinksDuration = Date.now() - this.stats.restoreLinksStartTime ;
	( { heapUsed: this.stats.restoreLinksHeapMemory , external: this.stats.restoreLinksExternalMemory } = process.memoryUsage() ) ;

	// Attempt to free some memory? This should not do much, but unused documents have a chance to be GC'ed...
	this.perCollectionBatch = null ;
	this.collectionForeignIdIndexes = null ;
} ;



Import.prototype.saveLinkingDocuments = async function() {
	// Fourth step, save to DB restored links, if any...
	this.stats.step = 5 ;
	this.stats.stepStr = '5/5 Save restored links to DB' ;
	this.stats.saveRestoredLinksToDbStartTime = Date.now() ;

	if ( this.restoredLinkBatch.length ) {
		await Promise.concurrent( this.concurrency , this.restoredLinkBatch , async ( document ) => {
			let collectionStats = this.stats.perCollections[ document._.collection.name ] ;
			let sourceFileStats = document._import._fileSource ? this.stats.perSourceFiles[ document._import._fileSource ] : null ;

			try {
				await document.save() ;
				this.stats.savedLinkingDocuments ++ ;
				collectionStats.savedLinkingDocuments ++ ;
				if ( sourceFileStats ) { sourceFileStats.savedLinkingDocuments ++ ; }
			}
			catch ( error ) {
				log.error( "Can't save document with restored link: \n%I\n\nError: %E" , document , error ) ;
				throw error ;
			}
		} ) ;
	}

	this.stats.saveRestoredLinksToDbDuration = Date.now() - this.stats.saveRestoredLinksToDbStartTime ;
	( { heapUsed: this.stats.saveRestoredLinksToDbHeapMemory , external: this.stats.saveRestoredLinksToDbExternalMemory } = process.memoryUsage() ) ;


	return this.stats ;
} ;



Import.prototype.mapImportedRawDocument = function( rawDocument , params ) {
	var mappedRawDocument = {
		_import: {}
	} ;

	if ( params.staticMapping ) {
		for ( let toProperty in params.staticMapping ) {
			let value = params.staticMapping[ toProperty ] ;

			// Is it useful to map static values? Maybe for values that are not compatible with KFG?
			let valueConverters = params.valueMapping?.[ toProperty ] ;
			if ( valueConverters ) { value = this.convertValue( value , valueConverters , toProperty ) ; }

			if ( value !== undefined ) {
				dotPath.set( mappedRawDocument , toProperty , value ) ;
			}
		}
	}

	if ( params.propertyMapping ) {
		for ( let toProperty in params.propertyMapping ) {
			let fromProperty = params.propertyMapping[ toProperty ] ;
			let value = dotPath.get( rawDocument , fromProperty ) ;

			let valueConverters = params.valueMapping?.[ toProperty ] ;
			if ( valueConverters ) { value = this.convertValue( value , valueConverters , toProperty ) ; }

			if ( value !== undefined ) {
				dotPath.set( mappedRawDocument , toProperty , value ) ;
			}
		}
	}

	if ( params.compoundMapping ) {
		for ( let toProperty in params.compoundMapping ) {
			let compoundConverter = params.compoundMapping[ toProperty ] ;

			if ( ! this.mapping.converters.compound[ compoundConverter ] ) {
				throw new Error( "Converter '" + compoundConverter + "' not found" ) ;
			}

			let value = this.mapping.converters.compound[ compoundConverter ]( rawDocument ) ;

			let valueConverters = params.valueMapping?.[ toProperty ] ;
			if ( valueConverters ) { value = this.convertValue( value , valueConverters , toProperty ) ; }

			if ( value !== undefined ) {
				dotPath.set( mappedRawDocument , toProperty , value ) ;
			}
		}
	}

	// Force an _importId
	mappedRawDocument._import._importId = this.importId ;
	if ( params.fileId ) { mappedRawDocument._import._fileSource = params.fileId ; }

	return mappedRawDocument ;
} ;



Import.prototype.convertValue = function( value , valueConverters , toProperty ) {
	for ( let valueConverter of valueConverters ) {
		if ( ! this.mapping.converters.simple[ valueConverter ] ) {
			throw new Error( "Converter '" + valueConverter + "' not found" ) ;
		}

		try {
			value = this.mapping.converters.simple[ valueConverter ]( value ) ;
		}
		catch ( error ) {
			log.error( "Converting to property '%s' with converter '%s' failed.\nInput value: %Y" , toProperty , valueConverter , value ) ;
			throw error ;
		}
	}

	return value ;
} ;



Import.prototype.restoreOneSingleLink = function( linkingData ) {
	let {
		document , subDocument , index ,
		idType , idProperty , linkProperty ,
		collectionStats , linkedCollectionStats , sourceFileStats
	} = linkingData ;

	switch ( idType ) {
		// Only "foreignId" mode is supported ATM
		case 'foreignId' :
		default : {
			let id = dotPath.get( subDocument , idProperty ) ;
			//log.hdebug( "Link id: %I, index has: %I, document: %I" , id , index.has( id ) , document ) ;

			if ( id !== undefined && id !== null && id !== '' ) {
				if ( index && index.has( id ) ) {
					let linkedDocument = index.get( id ) ;
					document.setLink( linkProperty , linkedDocument ) ;
					linkingData.changed = true ;
					this.stats.links ++ ;
					collectionStats.links ++ ;
					linkedCollectionStats.links ++ ;
					if ( sourceFileStats ) { sourceFileStats.links ++ ; }
				}
				else {
					this.stats.orphanLinks ++ ;
					collectionStats.orphanLinks ++ ;
					linkedCollectionStats.orphanLinks ++ ;
					if ( sourceFileStats ) { sourceFileStats.orphanLinks ++ ; }
				}
			}

			break ;
		}
	}
} ;



Import.prototype.restoreOneMultiLink = function( linkingData ) {
	let {
		document , subDocument , index ,
		idType , idListProperty , linkProperty ,
		collectionStats , linkedCollectionStats , sourceFileStats
	} = linkingData ;

	switch ( idType ) {
		// Only "foreignId" mode is supported ATM
		case 'foreignId' :
		default : {
			let ids = dotPath.get( subDocument , idListProperty ) ;
			//log.hdebug( "Link id: %I, index has: %I, document: %I" , id , index.has( id ) , document ) ;

			if ( ! Array.isArray( ids ) || ! ids.length ) { return ; }

			let linkedDocuments = [] ;

			for ( let id of ids ) {
				if ( id !== undefined && id !== null && id !== '' ) {
					if ( index && index.has( id ) ) {
						linkedDocuments.push( index.get( id ) ) ;
						linkingData.changed = true ;
						this.stats.links ++ ;
						collectionStats.links ++ ;
						linkedCollectionStats.links ++ ;
						if ( sourceFileStats ) { sourceFileStats.links ++ ; }
					}
					else {
						this.stats.orphanLinks ++ ;
						collectionStats.orphanLinks ++ ;
						linkedCollectionStats.orphanLinks ++ ;
						if ( sourceFileStats ) { sourceFileStats.orphanLinks ++ ; }
					}
				}
			}

			document.setLink( linkProperty , linkedDocuments ) ;

			break ;
		}
	}
} ;



Import.createImportStats = function( stats = {} ) {
	stats.step = 0 ;
	stats.stepStr = '' ;

	stats.retrievedDocuments = 0 ;
	stats.documents = 0 ;
	stats.savedDocuments = 0 ;
	stats.embeddedDocuments = 0 ;
	stats.links = 0 ;
	stats.linkingDocuments = 0 ;
	stats.savedLinkingDocuments = 0 ;
	stats.filteredInDocuments = 0 ;
	stats.filteredOutDocuments = 0 ;
	stats.filteredInEmbeddedDocuments = 0 ;
	stats.filteredOutEmbeddedDocuments = 0 ;
	stats.dedupedDocuments = 0 ;
	stats.dedupedEmbeddedDocuments = 0 ;

	// Timers:
	stats.startTime = null ;
	stats.duration = null ;
	stats.retrieveFromDbStartTime = null ;
	stats.retrieveFromDbDuration = null ;
	stats.importToMemoryStartTime = null ;
	stats.importToMemoryDuration = null ;
	stats.saveToDbStartTime = null ;
	stats.saveToDbDuration = null ;
	stats.restoreLinksStartTime = null ;
	stats.restoreLinksDuration = null ;
	stats.saveRestoredLinksToDbStartTime = null ;
	stats.saveRestoredLinksToDbDuration = null ;

	// Memory usage
	stats.startingHeapMemory = null ;
	stats.startingExternalMemory = null ;
	stats.retrieveFromDbHeapMemory = null ;
	stats.retrieveFromDbExternalMemory = null ;
	stats.importToMemoryHeapMemory = null ;
	stats.importToMemoryExternalMemory = null ;
	stats.saveToDbHeapMemory = null ;
	stats.saveToDbExternalMemory = null ;
	stats.restoreLinksHeapMemory = null ;
	stats.restoreLinksExternalMemory = null ;
	stats.saveRestoredLinksToDbHeapMemory = null ;
	stats.saveRestoredLinksToDbExternalMemory = null ;

	// Errors:
	stats.duplicatedIds = 0 ;
	stats.orphanEmbeddedDocuments = 0 ;
	stats.orphanLinks = 0 ;

	stats.perCollections = {} ;
	stats.perSourceFiles = {} ;

	return stats ;
} ;



Import.createImportSubStats = function() {
	return {
		retrievedDocuments: 0 ,
		documents: 0 ,
		savedDocuments: 0 ,
		embeddedDocuments: 0 ,
		links: 0 ,
		linkingDocuments: 0 ,
		savedLinkingDocuments: 0 ,
		filteredInDocuments: 0 ,
		filteredOutDocuments: 0 ,
		filteredInEmbeddedDocuments: 0 ,
		filteredOutEmbeddedDocuments: 0 ,
		dedupedDocuments: 0 ,
		dedupedEmbeddedDocuments: 0 ,

		duplicatedIds: 0 ,
		orphanEmbeddedDocuments: 0 ,
		orphanLinks: 0 ,

		perLinkedCollections: {}
	} ;
} ;

