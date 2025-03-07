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

/* global describe, it, before, after, beforeEach, expect, teaTime */

"use strict" ;



const rootsDb = require( '..' ) ;
const util = require( 'util' ) ;
const mongodb = require( 'mongodb' ) ;
const fs = require( 'fs' ) ;

const path = require( 'path' ) ;
const crypto = require( 'crypto' ) ;
const hash = require( 'hash-kit' ) ;
const string = require( 'string-kit' ) ;
const tree = require( 'tree-kit' ) ;
const streamKit = require( 'stream-kit' ) ;

const Promise = require( 'seventh' ) ;

const ErrorStatus = require( 'error-status' ) ;
ErrorStatus.alwaysCapture = true ;

const doormen = require( 'doormen' ) ;

const logfella = require( 'logfella' ) ;

if ( global.teaTime ) {
	logfella.global.setGlobalConfig( { minLevel: teaTime.cliManager.parsedArgs.log } ) ;
}

const log = logfella.global.use( 'unit-test' ) ;



// Test options
//testOption( 'driver' , 'attachment-driver' ) ;
testOption( 'attachment-driver' , 'importer' , 'fake-data-generator' ) ;

var ATTACHMENT_MODE , BASE_ATTACHMENT_URL , IMPORTER , FAKE_DATA_GENERATOR ;
const FAKE_DATA_GENERATOR_LOCALE = 'fr' ;
const ATTACHMENT_PUBLIC_BASE_URL = 'http://example.cdn.net/example' ;

switch ( getTestOption( 'attachment-driver' ) ) {
	case 's3' :
		ATTACHMENT_MODE = 's3' ;
		BASE_ATTACHMENT_URL = require( './s3-config.local.json' ).attachmentUrl ;
		break ;
	case 'file' :
	default :
		ATTACHMENT_MODE = 'file' ;
		BASE_ATTACHMENT_URL = 'file://' + __dirname + '/tmp' ;
		break ;
}

switch ( getTestOption( 'importer' ) ) {
	case 'csv' :
		IMPORTER = 'csv' ;
		break ;
	default :
		break ;
}

switch ( getTestOption( 'fake-data-generator' ) ) {
	case 'faker' :
		FAKE_DATA_GENERATOR = 'faker' ;
		break ;
	default :
		break ;
}

// Init extensions
rootsDb.initExtensions() ;

// Create the world...
const world = new rootsDb.World() ;

// Collections...
var versions , counters , users , jobs , schools , towns , products , stores , lockables , freezables , immutableProperties , nestedLinks , anyCollectionLinks , images , versionedItems , extendables ;

const versionsDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/versions' ,
	attachmentUrl: BASE_ATTACHMENT_URL + '/versions'
} ;

const countersDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/counters' ,
	attachmentUrl: BASE_ATTACHMENT_URL + '/counters'
} ;

const usersDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/users' ,
	attachmentUrl: BASE_ATTACHMENT_URL + '/users' ,
	attachmentPublicBaseUrl: ATTACHMENT_PUBLIC_BASE_URL ,
	fakeDataGenerator: {
		type: FAKE_DATA_GENERATOR ,
		locale: FAKE_DATA_GENERATOR_LOCALE
	} ,
	properties: {
		firstName: {
			type: 'string' ,
			maxLength: 30 ,
			fake: 'person.firstName' ,
			default: 'Joe'
		} ,
		lastName: {
			type: 'string' ,
			maxLength: 30 ,
			fake: 'person.lastName' ,
			default: 'Doe'
		} ,
		godfather: {
			type: 'link' ,
			optional: true ,
			collection: 'users'
		} ,
		connection: {
			type: 'strictObject' ,
			optional: true ,
			of: { type: 'link' , collection: 'users' }
		} ,
		job: {
			type: 'link' ,
			optional: true ,
			collection: 'jobs'
		} ,
		memberSid: {
			optional: true ,
			type: 'string' ,
			maxLength: 30 ,
			tags: [ 'id' ]
		} ,
		avatar: {
			type: 'attachment' ,
			optional: true
		} ,
		publicKey: {
			type: 'attachment' ,
			optional: true
		} ,
		file: {
			type: 'attachment' ,
			optional: true
		}
	} ,
	indexes: [
		{ links: { "job": 1 } } ,
		{ links: { "job": 1 } , properties: { memberSid: 1 } , unique: true }
	] ,
	hooks: {
		afterCreateDocument: document => {
			document.memberSid = '' + document.firstName + ' ' + document.lastName ;
		}
	} ,
	refreshTimeout: 50
} ;

const expectedDefaultUser = { firstName: 'Joe' , lastName: 'Doe' , memberSid: 'Joe Doe' } ;

const jobsDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/jobs' ,
	fakeDataGenerator: {
		type: FAKE_DATA_GENERATOR ,
		locale: FAKE_DATA_GENERATOR_LOCALE
	} ,
	properties: {
		title: {
			type: 'string' ,
			maxLength: 50 ,
			fake: 'enum' ,
			fakeParams: [ 'frontend-dev' , 'backend-dev' , 'sysadmin' ] ,
			default: 'unemployed'
		} ,
		salary: {
			type: 'integer' ,
			sanitize: 'toInteger' ,
			default: 0
		} ,
		users: { type: 'backLink' , collection: 'users' , path: 'job' } ,
		schools: { type: 'backLink' , collection: 'schools' , path: 'jobs' }
	}
} ;

const schoolsDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/schools' ,
	properties: {
		title: {
			type: 'string' ,
			maxLength: 50
		} ,
		jobs: {
			type: 'multiLink' ,
			collection: 'jobs'
		}
	} ,
	indexes: [
		{
			properties: { title: 1 } ,
			unique: true ,
			// used with the collation unit test
			collation: {
				locale: 'en' ,
				caseLevel: true ,
				numericOrdering: true
			} ,
			isDefaultSortCollation: true
		}
	]
} ;

const townsDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/towns' ,
	properties: {
		name: { type: 'string' , tags: [ 'id' ] } ,
		meta: {
			type: 'strictObject' ,
			default: {} ,
			tags: [ 'meta' ] ,
			//noSubmasking: true ,
			extraProperties: true ,
			properties: {
				rank: {
					tags: [ 'rank' ] ,
					optional: true ,
					sanitize: 'toInteger'
				}
			}
		}
	} ,
	indexes: [
		{ properties: { name: 1 , "meta.country": 1 } , unique: true }
	]
} ;

const productsDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/products' ,
	properties: {
		name: { type: 'string' , tags: [ 'id' ] } ,
		price: { type: 'number' }
	}
} ;

const storesDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/stores' ,
	properties: {
		name: { type: 'string' , tags: [ 'id' ] } ,
		products: {
			type: 'array' ,
			default: [] ,
			of: {
				type: 'strictObject' ,
				properties: {
					product: {
						type: 'link' ,
						collection: 'products'
					} ,
					quantity: {
						type: 'integer' ,
						sanitize: 'toInteger'
					}
				}
			}
		} ,
		// Does not make much sense, but we need to test wild populate on multiLink too
		productBatches: {
			type: 'array' ,
			default: [] ,
			of: {
				type: 'strictObject' ,
				properties: {
					batch: {
						type: 'multiLink' ,
						collection: 'products'
					} ,
					quantity: {
						type: 'integer'
					}
				}
			}
		}
	}
} ;

const lockablesDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/lockables' ,
	lockable: true ,
	lockTimeout: 40 ,
	properties: {
		data: { type: 'string' }
	} ,
	indexes: []
} ;

const freezablesDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/freezables' ,
	freezable: true ,
	properties: {
		name: { type: 'string' } ,
		data: { type: 'strictObject' }
	} ,
	indexes: []
} ;

const immutablePropertiesDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/immutableProperties' ,
	properties: {
		name: { type: 'string' } ,
		immutableData: {
			type: 'string' ,
			immutable: true
		}
	} ,
	indexes: []
} ;

const nestedLinksDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/nestedLinks' ,
	properties: {
		name: { type: 'string' } ,
		nested: {
			type: 'strictObject' ,
			default: {} ,
			properties: {
				link: {
					type: 'link' ,
					optional: true ,
					collection: 'nestedLinks'
				} ,
				multiLink: {
					type: 'multiLink' ,
					collection: 'nestedLinks'
				} ,
				backLinkOfLink: {
					type: 'backLink' ,
					collection: 'nestedLinks' ,
					path: 'nested.link'
				} ,
				backLinkOfMultiLink: {
					type: 'backLink' ,
					collection: 'nestedLinks' ,
					path: 'nested.multiLink'
				}
			}
		}
	} ,
	indexes: []
} ;

const anyCollectionLinksDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/anyCollectionLinks' ,
	properties: {
		name: { type: 'string' } ,
		link: {
			type: 'link' ,
			optional: true ,
			anyCollection: true
		} ,
		backLink: {
			type: 'backLink' ,
			collection: 'anyCollectionLinks' ,
			path: 'link'
		}
	} ,
	indexes: []
} ;

const imagesDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/images' ,
	attachmentUrl: BASE_ATTACHMENT_URL + '/images' ,
	attachmentPublicBaseUrl: ATTACHMENT_PUBLIC_BASE_URL ,
	properties: {
		name: { type: 'string' } ,
		fileSet: { type: 'attachmentSet' }
	} ,
	indexes: []
} ;

const versionedItemsDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/versionedItems' ,
	versioning: true ,
	properties: {
		name: { type: 'string' } ,
		p1: { type: 'string' , optional: true } ,
		p2: { type: 'string' , optional: true } ,
		p3: { type: 'strictObject' , optional: true } ,
		p4: { type: 'date' , optional: true } ,
		versions: {
			type: 'backLink' ,
			collection: 'versions' ,
			path: '_activeVersion'
		}
	} ,
	indexes: []
} ;

// Only used for assertion tests:
const USERS_ATTACHMENT_DIR = ( BASE_ATTACHMENT_URL + '/users/' ).slice( 7 ) ;		// .slice(7) to remove the file:// part
const USERS_ATTACHMENT_URL = BASE_ATTACHMENT_URL + '/users' ;
const IMAGES_ATTACHMENT_DIR = ( BASE_ATTACHMENT_URL + '/images/' ).slice( 7 ) ;



function Extended( collection , rawDoc , options ) {
	rootsDb.Document.call( this , collection , rawDoc , options ) ;
	this.addProxyMethodNames( 'getNormalized' ) ;
}

Extended.prototype = Object.create( rootsDb.Document.prototype ) ;
Extended.prototype.constructor = Extended ;

Extended.prototype.getNormalized = function() {
	return this.proxy.data.toLowerCase() ;
} ;



// Batch *MUST* be extended using the 'extends' keyword
class ExtendedBatch extends rootsDb.Batch {
	constructor( collection , rawDoc , options ) {
		super( collection , rawDoc , options ) ;
	}

	foo() {
		var str = '' ;
		this.forEach( item => str += item.data ) ;
		return str ;
	}
}



const extendablesDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/extendables' ,
	Document: Extended ,
	Batch: ExtendedBatch ,
	properties: {
		data: { type: 'string' }
	} ,
	meta: {} ,
	indexes: []
} ;



/* Utils */



// drop DB collection: drop all collections
// This is surprisingly slow even for empty collection, so we can't use that all over the place like it should...
function dropDBCollections() {
	//console.log( "dropDBCollections" ) ;
	return Promise.all( [
		dropCollection( versions ) ,
		dropCollection( counters ) ,
		dropCollection( users ) ,
		dropCollection( jobs ) ,
		dropCollection( schools ) ,
		dropCollection( towns ) ,
		dropCollection( products ) ,
		dropCollection( stores ) ,
		dropCollection( lockables ) ,
		dropCollection( freezables ) ,
		dropCollection( immutableProperties ) ,
		dropCollection( nestedLinks ) ,
		dropCollection( anyCollectionLinks ) ,
		dropCollection( images ) ,
		dropCollection( versionedItems ) ,
		dropCollection( extendables )
	] ) ;
}



// clear DB: remove every item, so we can safely test
function clearDB() {
	return Promise.all( [
		versions.clear() ,
		counters.clear() ,
		users.clear() ,
		jobs.clear() ,
		schools.clear() ,
		towns.clear() ,
		products.clear() ,
		stores.clear() ,
		lockables.clear() ,
		freezables.clear() ,
		immutableProperties.clear() ,
		nestedLinks.clear() ,
		anyCollectionLinks.clear() ,
		images.clear() ,
		versionedItems.clear() ,
		extendables.clear()
	] ) ;
}



// clear DB indexes: remove all indexes
function clearDBIndexes() {
	return Promise.all( [
		clearCollectionIndexes( versions ) ,
		clearCollectionIndexes( counters ) ,
		clearCollectionIndexes( users ) ,
		clearCollectionIndexes( jobs ) ,
		clearCollectionIndexes( schools ) ,
		clearCollectionIndexes( towns ) ,
		clearCollectionIndexes( products ) ,
		clearCollectionIndexes( stores ) ,
		clearCollectionIndexes( lockables ) ,
		clearCollectionIndexes( freezables ) ,
		clearCollectionIndexes( immutableProperties ) ,
		clearCollectionIndexes( nestedLinks ) ,
		clearCollectionIndexes( anyCollectionLinks ) ,
		clearCollectionIndexes( images ) ,
		clearCollectionIndexes( versionedItems ) ,
		clearCollectionIndexes( extendables )
	] ).then( () => { log.verbose( "All indexes cleared" ) ; } ) ;
}



function dropCollection( collection ) {
	return collection.driver.rawInit()
		.then( () => collection.driver.raw.drop() )
		.catch( error => {
			if ( error.code === 26 ) { return ; }	// NS not found, nothing to drop!
			throw error ;
		} ) ;
}



function clearCollectionIndexes( collection ) {
	return collection.driver.rawInit()
		.then( () => collection.driver.raw.dropIndexes() )
		.catch( error => {
			if ( error.code === 26 ) { return ; }	// NS not found, nothing to clear!
			throw error ;
		} ) ;
}

// Sort function creator for ascending sorting.
// Also works with string, but it's not a natural sort,
// only by char-code, so mixed case will not be sorted alphabetically.
function ascendingSortFn( key ) {
	return ( a , b ) => {
		let valueA = a[ key ] ;
		let valueB = b[ key ] ;
		if ( valueA && valueA === 'object' ) { valueA = '' + valueA ; }
		if ( valueB && valueB === 'object' ) { valueB = '' + valueB ; }
		return valueA > valueB ? 1 : valueA < valueB ? - 1 : 0
	} ;
}



/* Tests */



// Force creating the collection
before( async () => {
	versions = await world.createAndInitVersionsCollection( 'versions' , versionsDescriptor ) ;
	expect( versions ).to.be.a( rootsDb.VersionsCollection ) ;

	counters = await world.createAndInitCountersCollection( 'counters' , countersDescriptor ) ;
	expect( counters ).to.be.a( rootsDb.CountersCollection ) ;

	users = await world.createAndInitCollection( 'users' , usersDescriptor ) ;
	expect( users ).to.be.a( rootsDb.Collection ) ;

	jobs = await world.createAndInitCollection( 'jobs' , jobsDescriptor ) ;
	expect( jobs ).to.be.a( rootsDb.Collection ) ;

	schools = await world.createAndInitCollection( 'schools' , schoolsDescriptor ) ;
	expect( schools ).to.be.a( rootsDb.Collection ) ;

	towns = await world.createAndInitCollection( 'towns' , townsDescriptor ) ;
	expect( towns ).to.be.a( rootsDb.Collection ) ;

	products = await world.createAndInitCollection( 'products' , productsDescriptor ) ;
	expect( products ).to.be.a( rootsDb.Collection ) ;

	stores = await world.createAndInitCollection( 'stores' , storesDescriptor ) ;
	expect( stores ).to.be.a( rootsDb.Collection ) ;

	lockables = await world.createAndInitCollection( 'lockables' , lockablesDescriptor ) ;
	expect( lockables ).to.be.a( rootsDb.Collection ) ;

	freezables = await world.createAndInitCollection( 'freezables' , freezablesDescriptor ) ;
	expect( freezables ).to.be.a( rootsDb.Collection ) ;

	immutableProperties = await world.createAndInitCollection( 'immutableProperties' , immutablePropertiesDescriptor ) ;
	expect( immutableProperties ).to.be.a( rootsDb.Collection ) ;

	nestedLinks = await world.createAndInitCollection( 'nestedLinks' , nestedLinksDescriptor ) ;
	expect( nestedLinks ).to.be.a( rootsDb.Collection ) ;

	anyCollectionLinks = await world.createAndInitCollection( 'anyCollectionLinks' , anyCollectionLinksDescriptor ) ;
	expect( anyCollectionLinks ).to.be.a( rootsDb.Collection ) ;

	images = await world.createAndInitCollection( 'images' , imagesDescriptor ) ;
	expect( images ).to.be.a( rootsDb.Collection ) ;

	versionedItems = await world.createAndInitCollection( 'versionedItems' , versionedItemsDescriptor ) ;
	expect( versionedItems ).to.be.a( rootsDb.Collection ) ;

	extendables = await world.createAndInitCollection( 'extendables' , extendablesDescriptor ) ;
	expect( extendables ).to.be.a( rootsDb.Collection ) ;
} ) ;



describe( "Collection" , () => {
	it( "Some collection tests" ) ;
} ) ;



describe( "ID" , () => {

	it( "should create ID (like Mongo ID)" , () => {
		expect( users.createId().toString() ).to.match( /^[0-9a-f]{24}$/ ) ;
		expect( users.createId().toString() ).to.match( /^[0-9a-f]{24}$/ ) ;
		expect( users.createId().toString() ).to.match( /^[0-9a-f]{24}$/ ) ;
		expect( users.createId().toString() ).to.match( /^[0-9a-f]{24}$/ ) ;
		expect( users.createId().toString() ).to.match( /^[0-9a-f]{24}$/ ) ;
	} ) ;

	it( "$id in document" ) ;
	it( "$id in fingerprint" ) ;
	it( "$id in criteria (queryObject)" ) ;
} ) ;



describe( "Document creation" , () => {

	it( "document's schema test" , () => {
		const fakeFn = users.documentSchema.properties.firstName.fakeFn ;
		expect( users.documentSchema ).to.equal(
			{
				url: "mongodb://localhost:27017/rootsDb/users" ,
				attachmentUrl: USERS_ATTACHMENT_URL ,
				attachmentPublicBaseUrl: ATTACHMENT_PUBLIC_BASE_URL ,
				fakeDataGenerator: {
					type: FAKE_DATA_GENERATOR ,
					locale: FAKE_DATA_GENERATOR_LOCALE
				} ,
				properties: {
					firstName: {
						type: "string" , maxLength: 30 , default: "Joe" , tags: [ "content" ] , fake: "person.firstName" , fakeFn , inputHint: "text"
					} ,
					lastName: {
						type: "string" , maxLength: 30 , default: "Doe" , tags: [ "content" ] , fake: "person.lastName" , fakeFn , inputHint: "text"
					} ,
					godfather: {
						type: "link" , optional: true , collection: "users" , tags: [ "content" ] , sanitize: [ "toLink" ] , opaque: true , inputHint: "embedded"
					} ,
					connection: {
						type: "strictObject" ,
						optional: true ,
						of: {
							type: "link" , collection: "users" , sanitize: [ "toLink" ] , opaque: true , tags: [ "content" ] , inputHint: "embedded"
						} ,
						tags: [ "content" ] ,
						inputHint: "embedded"
					} ,
					job: {
						type: "link" , optional: true , collection: "jobs" , tags: [ "content" ] , sanitize: [ "toLink" ] , opaque: true , inputHint: "embedded"
					} ,
					memberSid: {
						optional: true , type: "string" , maxLength: 30 , tags: [ "id" ] , inputHint: "text"
					} ,
					avatar: {
						type: "attachment" , optional: true , tags: [ "content" ] , opaque: true , inputHint: "file"
					} ,
					publicKey: {
						type: "attachment" , optional: true , tags: [ "content" ] , opaque: true , inputHint: "file"
					} ,
					file: {
						type: "attachment" , optional: true , tags: [ "content" ] , opaque: true , inputHint: "file"
					} ,
					_id: {
						type: "objectId" , sanitize: "toObjectId" , optional: true , system: true , rootsDbInternal: true , tags: [ "id" ]
					} ,
					_import: {
						extraProperties: true ,
						inputHint: "embedded" ,
						optional: true ,
						properties: {
							_foreignId: {
								inputHint: "text" ,
								optional: true ,
								system: true ,
								tags: [ "system" ] ,
								type: "string"
							} ,
							_importId: {
								inputHint: "text" ,
								system: true ,
								tags: [ "system" ] ,
								type: "string"
							}
						} ,
						system: true ,
						tags: [ "system" ] ,
						type: "object"
					}
				} ,
				indexes: [
					{
						name: 'dtIpeAj6wiUxW7T-CjLiq2OEB_Y' , properties: { "job._id": 1 } , links: { job: 1 } , unique: false , partial: false , isDefaultSortCollation: false , propertyString: "job._id"
					} ,
					{
						name: 'uvmuRnXOw6DtAwUy1GyTH9ctXRI' , properties: { "job._id": 1 , memberSid: 1 } , links: { job: 1 } , unique: true , partial: false , isDefaultSortCollation: false , propertyString: "job._id,memberSid"
					}
				] ,
				hooks: users.documentSchema.hooks ,
				versioning: false ,
				lockable: false ,
				lockTimeout: 1000 ,
				freezable: false ,
				refreshTimeout: 50 ,
				Batch: users.documentSchema.Batch ,
				Collection: users.documentSchema.Collection ,
				Document: users.documentSchema.Document
			}
		) ;

		expect( schools.documentSchema ).to.equal(
			{
				url: "mongodb://localhost:27017/rootsDb/schools" ,
				properties: {
					_id: {
						type: "objectId" , sanitize: "toObjectId" , optional: true , system: true , rootsDbInternal: true , tags: [ "id" ]
					} ,
					_import: {
						extraProperties: true ,
						inputHint: "embedded" ,
						optional: true ,
						properties: {
							_foreignId: {
								inputHint: "text" ,
								optional: true ,
								system: true ,
								tags: [ "system" ] ,
								type: "string"
							} ,
							_importId: {
								inputHint: "text" ,
								system: true ,
								tags: [ "system" ] ,
								type: "string"
							}
						} ,
						system: true ,
						tags: [ "system" ] ,
						type: "object"
					} ,
					title: {
						type: 'string' ,
						maxLength: 50 ,
						inputHint: "text" ,
						tags: [ "content" ]
					} ,
					jobs: {
						type: 'multiLink' ,
						collection: 'jobs' ,
						constraints: [ {
							convert: "toString" , enforce: "unique" , noEmpty: true , path: "_id" , resolve: true
						} ] ,
						default: [] ,
						inputHint: "embedded" ,
						of: {
							type: "link" , inputHint: "embedded" , opaque: true , sanitize: [ "toLink" ] , tags: [ "content" ]
						} ,
						opaque: true ,
						sanitize: [ "toMultiLink" ] ,
						tags: [ "content" ]
					}
				} ,
				indexes: [
					{
						name: "_pY6Lhgiky-udo38l_7umMnJMx8" ,
						properties: { title: 1 } ,
						propertyString: "title" ,
						unique: true ,
						partial: false ,
						collation: {
							locale: 'en' ,
							caseLevel: true ,
							numericOrdering: true
						} ,
						isDefaultSortCollation: true
					}
				] ,
				hooks: schools.documentSchema.hooks ,
				versioning: false ,
				lockable: false ,
				lockTimeout: 1000 ,
				freezable: false ,
				refreshTimeout: 1000 ,
				Batch: schools.documentSchema.Batch ,
				Collection: schools.documentSchema.Collection ,
				Document: schools.documentSchema.Document
			}
		) ;

		expect( versions.documentSchema ).to.equal(
			{
				url: "mongodb://localhost:27017/rootsDb/versions" ,
				attachmentUrl: BASE_ATTACHMENT_URL + '/versions' ,
				extraProperties: true ,
				properties: {
					_activeVersion: {
						type: "link" , anyCollection: true , inputHint: "embedded" , opaque: true , sanitize: [ "toLink" ] , system: true , rootsDbInternal: true , tags: [ "systemContent" ]
					} ,
					_id: {
						type: "objectId" , sanitize: "toObjectId" , optional: true , system: true , rootsDbInternal: true , tags: [ "id" ]
					} ,
					_import: {
						extraProperties: true ,
						inputHint: "embedded" ,
						optional: true ,
						properties: {
							_foreignId: {
								inputHint: "text" ,
								optional: true ,
								system: true ,
								tags: [ "system" ] ,
								type: "string"
							} ,
							_importId: {
								inputHint: "text" ,
								system: true ,
								tags: [ "system" ] ,
								type: "string"
							}
						} ,
						system: true ,
						tags: [ "system" ] ,
						type: "object"
					} ,
					_lastModified: {
						defaultFn: "now" , inputHint: "date" , sanitize: [ "toDate" ] , system: true , rootsDbInternal: true , tags: [ "systemContent" ] , type: "date"
					} ,
					_version: {
						default: 1 , inputHint: "text" , sanitize: [ "toInteger" ] , system: true , rootsDbInternal: true , tags: [ "systemContent" ] , type: "integer"
					}
				} ,
				indexes: [
					{
						name: "zy0mKRcFu6DN8Qc4AukylJk4JTo" ,
						properties: {
							"_activeVersion._collection": 1 ,
							"_activeVersion._id": 1
						} ,
						propertyString: "_activeVersion._collection,_activeVersion._id" ,
						links: { _activeVersion: 1 } ,
						unique: false ,
						partial: false ,
						isDefaultSortCollation: false
					} ,
					{
						name: "2XJyW613Fh2K4PAFqnu8T1CaHdg" ,
						properties: {
							"_activeVersion._collection": 1 ,
							"_activeVersion._id": 1 ,
							_version: 1
						} ,
						propertyString: "_activeVersion._collection,_activeVersion._id,_version" ,
						links: { _activeVersion: 1 } ,
						unique: true ,
						partial: false ,
						isDefaultSortCollation: false
					}
				] ,
				hooks: versions.documentSchema.hooks ,
				versioning: false ,
				lockable: false ,
				lockTimeout: 1000 ,
				freezable: false ,
				refreshTimeout: 1000 ,
				Batch: versions.documentSchema.Batch ,
				Collection: versions.documentSchema.Collection ,
				Document: versions.documentSchema.Document
			}
		) ;
	} ) ;

	it( "should create a document with default values" , () => {
		var user = users.createDocument() ;

		expect( user ).to.be.an( Object ) ;
		expect( user.$ ).to.be.an( Object ) ;
		expect( user._ ).to.be.a( rootsDb.Document ) ;
		expect( user._id ).to.be.an( mongodb.ObjectId ) ;
		expect( user.getId() ).to.be.an( mongodb.ObjectId ) ;
		expect( user._id ).to.be( user.getId() ) ;

		expect( user ).to.partially.equal( expectedDefaultUser ) ;
		expect( user.$ ).to.partially.equal( expectedDefaultUser ) ;


		var town = towns.createDocument( { name: 'Paris' } ) ;

		expect( town.meta ).to.be.an( Object ) ;
		expect( town.meta._ ).to.be.undefined() ;
	} ) ;

	it( "should create a document with valid data" , () => {
		var user = users.createDocument( {
			firstName: 'Bobby' ,
			lastName: 'Fischer'
		} ) ;

		expect( user ).to.be.an( Object ) ;
		expect( user.$ ).to.be.an( Object ) ;
		expect( user._ ).to.be.a( rootsDb.Document ) ;
		expect( user._id ).to.be.an( mongodb.ObjectId ) ;
		expect( user.getId() ).to.be.an( mongodb.ObjectId ) ;
		expect( user._id ).to.be( user.getId() ) ;

		expect( user ).to.equal( {
			_id: user._id ,
			firstName: 'Bobby' ,
			lastName: 'Fischer' ,
			memberSid: 'Bobby Fischer'
		} ) ;
	} ) ;

	it( "should create a document and enumerate it with tag-masking with the special __enumerate__ function" , () => {
		var user , town ;

		user = users.createDocument( {
			firstName: 'Bobby' ,
			lastName: 'Fischer'
		} ) ;
		expect( user.__enumerate__() ).to.only.contain( '_id' , '_collection' , 'firstName' , 'lastName' , 'memberSid' ) ;

		user.setTagMask( [ 'id' ] ) ;
		expect( user.__enumerate__() ).to.only.contain( '_id' , '_collection' , 'memberSid' ) ;

		// Directly on creation
		user = users.createDocument( {
			firstName: 'Bobby' ,
			lastName: 'Fischer'
		} , {
			tagMask: [ 'id' ]
		} ) ;
		expect( user.__enumerate__() ).to.only.contain( '_id' , '_collection' , 'memberSid' ) ;

		town = towns.createDocument( {
			name: 'Paris' ,
			meta: {
				rank: 1 ,
				population: '2200K' ,
				country: 'France'
			}
		} ) ;
		expect( town.__enumerate__() ).to.only.contain( '_id' , '_collection' , 'name' , 'meta' ) ;
		expect( town.meta.__enumerate__() ).to.only.contain( 'rank' , 'population' , 'country' ) ;

		town.setTagMask( [ 'meta' , 'rank' ] ) ;
		expect( town.__enumerate__() ).to.only.contain( '_collection' , 'meta' ) ;
		expect( town.meta.__enumerate__() ).to.only.contain( 'rank' , 'population' , 'country' ) ;

		town.setTagMask( [ 'meta' ] ) ;
		expect( town.__enumerate__() ).to.only.contain( '_collection' , 'meta' ) ;
		expect( town.meta.__enumerate__() ).to.only.contain( 'population' , 'country' ) ;
	} ) ;

	it( "should create a document and modify it" , () => {
		var user = users.createDocument( {
			firstName: 'Bobby' ,
			lastName: 'Fischer'
		} ) ;

		var id = user.getId() ;

		expect( user ).to.equal( {
			_id: id ,
			firstName: 'Bobby' ,
			lastName: 'Fischer' ,
			memberSid: 'Bobby Fischer'
		} ) ;

		user.firstName = 'Robert' ;

		expect( user ).to.equal( {
			_id: id ,
			firstName: 'Robert' ,
			lastName: 'Fischer' ,
			memberSid: 'Bobby Fischer'
		} ) ;
	} ) ;

	it( "should create a document with embedded data and modify it" , () => {
		var town = towns.createDocument( {
			name: 'Paris' ,
			meta: {
				population: '2200K' ,
				country: 'France'
			}
		} ) ;

		var id = town.getId() ;

		expect( town.$ ).to.equal( { _id: id , name: 'Paris' , meta: { population: '2200K' , country: 'France' } } ) ;
		expect( town.$.meta.population ).to.be( '2200K' ) ;

		expect( town.meta.population ).to.be( '2200K' ) ;
		expect( town.meta ).to.equal( { population: '2200K' , country: 'France' } ) ;
		expect( town ).to.equal( { _id: id , name: 'Paris' , meta: { population: '2200K' , country: 'France' } } ) ;

		town.meta.population = '2500K' ;
		expect( town.meta.population ).to.be( '2500K' ) ;
		expect( town.meta ).to.equal( { population: '2500K' , country: 'France' } ) ;
		expect( town ).to.equal( { _id: id , name: 'Paris' , meta: { population: '2500K' , country: 'France' } } ) ;
	} ) ;

	it( "should throw when trying to create a document that does not validate the schema" , () => {
		var user ;

		expect( () => {
			user = users.createDocument( {
				firstName: true ,
				lastName: 3
			} ) ;
		} ).to.throw.a( doormen.ValidatorError ) ;

		expect( () => {
			user = users.createDocument( {
				firstName: 'Bobby' ,
				lastName: 'Fischer' ,
				extra: 'property'
			} ) ;
		} ).to.throw.a( doormen.ValidatorError ) ;
	} ) ;

	it( "should create a document with embedded data and use method of objects through the proxy" , () => {
		var town = towns.createDocument( {
			name: 'Paris' ,
			meta: {
				population: '2200K' ,
				country: 'France' ,
				someArray: [ 'one' , 'two' ]
			}
		} ) ;

		var id = town.getId() ;

		expect( town ).to.equal( { _id: id , name: 'Paris' , meta: { population: '2200K' , country: 'France' , someArray: [ 'one' , 'two' ] } } ) ;
		expect( town.hasOwnProperty ).to.be.a( 'function' ) ;
		expect( town.hasOwnProperty( 'name' ) ).to.be.true() ;
		expect( town.hasOwnProperty( 'names' ) ).to.be.false() ;

		expect( town.meta.hasOwnProperty ).to.be.a( 'function' ) ;
		expect( town.meta.hasOwnProperty( 'country' ) ).to.be.true() ;
		expect( town.meta.hasOwnProperty( 'countries' ) ).to.be.false() ;

		expect( town.meta.someArray.slice ).to.be.a( 'function' ) ;
		expect( town.meta.someArray.slice() ).not.to.be( town.meta.someArray ) ;
		expect( town.meta.someArray.slice() ).to.equal( [ 'one' , 'two' ] ) ;

		town.meta.someArray.push( 'three' ) ;
		expect( town.meta.someArray ).to.equal( [ 'one' , 'two' , 'three' ] ) ;
	} ) ;
} ) ;



describe( "Clone documents" , () => {

	it( "should clone a document" , () => {
		var user = users.createDocument( {
			firstName: 'Bobby' ,
			lastName: 'Fischer'
		} ) ;

		expect( user.getId() ).to.be.an( mongodb.ObjectId ) ;
		expect( user._id ).to.be( user.getId() ) ;

		expect( user ).to.equal( {
			_id: user._id ,
			firstName: 'Bobby' ,
			lastName: 'Fischer' ,
			memberSid: 'Bobby Fischer'
		} ) ;

		var clone = user.clone() ;

		expect( clone.getId() ).to.be.an( mongodb.ObjectId ) ;
		expect( clone._id ).to.be( clone.getId() ) ;

		// The ID should be different
		expect( '' + clone._id ).not.to.be( '' + user._id ) ;

		expect( clone ).to.equal( {
			_id: clone._id ,
			firstName: 'Bobby' ,
			lastName: 'Fischer' ,
			memberSid: 'Bobby Fischer'
		} ) ;

		clone.firstName = 'Bob' ;

		expect( clone ).to.equal( {
			_id: clone._id ,
			firstName: 'Bob' ,
			lastName: 'Fischer' ,
			memberSid: 'Bobby Fischer'
		} ) ;

		expect( user ).to.equal( {
			_id: user._id ,
			firstName: 'Bobby' ,
			lastName: 'Fischer' ,
			memberSid: 'Bobby Fischer'
		} ) ;
	} ) ;
} ) ;



describe( "Get documents" , () => {

	beforeEach( clearDB ) ;

	it( "should get an existing document" , async () => {
		var user = users.createDocument( {
			firstName: 'John' ,
			lastName: 'McGregor'
		} ) ;

		var id = user.getId() ;

		await user.save() ;
		var dbUser = await users.get( id ) ;

		expect( dbUser ).to.be.an( Object ) ;
		expect( dbUser._ ).to.be.a( rootsDb.Document ) ;
		expect( dbUser._id ).to.be.an( mongodb.ObjectId ) ;
		expect( dbUser._id ).to.equal( id ) ;
		expect( dbUser ).to.equal( {
			_id: dbUser._id , firstName: 'John' , lastName: 'McGregor' , memberSid: 'John McGregor'
		} ) ;
	} ) ;

	it( "when trying to get an unexistant document, an ErrorStatus (type: notFound) should be issued" , async () => {
		// Unexistant ID
		var id = new mongodb.ObjectId() ;

		await expect( () => users.get( id ) ).to.reject.with.an( ErrorStatus , { type: 'notFound' } ) ;
		await expect( () => users.get( id , { raw: true } ) ).to.reject.with.an( ErrorStatus , { type: 'notFound' } ) ;
	} ) ;

	it( "should get an existing document in raw mode" , async () => {
		var user = users.createDocument( {
			firstName: 'John' ,
			lastName: 'McGregor'
		} ) ;

		var id = user.getId() ;

		await user.save() ;

		var rawDbUser = await users.get( id , { raw: true } ) ;

		expect( rawDbUser._ ).not.to.be.a( rootsDb.Document ) ;
		expect( rawDbUser._id ).to.be.an( mongodb.ObjectId ) ;
		expect( rawDbUser._id ).to.equal( id ) ;
		expect( rawDbUser ).to.equal( {
			_id: rawDbUser._id , firstName: 'John' , lastName: 'McGregor' , memberSid: 'John McGregor'
		} ) ;
	} ) ;
} ) ;



describe( "Save documents" , () => {

	beforeEach( clearDB ) ;

	it( "should save (create) correctly" , async () => {
		var user = users.createDocument( {
			firstName: 'Jack'
		} ) ;

		var id = user.getId() ;

		await user.save() ;
		await expect( users.get( id ) ).to.eventually.equal( {
			_id: id , firstName: 'Jack' , lastName: 'Doe' , memberSid: 'Jack Doe'
		} ) ;

		expect( user ).to.equal( {
			_id: id , firstName: 'Jack' , lastName: 'Doe' , memberSid: 'Jack Doe'
		} ) ;
	} ) ;

	it( "should save (create) correctly and then modify and save again (update the whole document)" , async () => {
		var user = users.createDocument( {
			firstName: 'Jack'
		} ) ;

		var id = user.getId() ;

		await user.save() ;
		await expect( users.get( id ) ).to.eventually.equal( {
			_id: id , firstName: 'Jack' , lastName: 'Doe' , memberSid: 'Jack Doe'
		} ) ;

		expect( user ).to.equal( {
			_id: id , firstName: 'Jack' , lastName: 'Doe' , memberSid: 'Jack Doe'
		} ) ;

		user.firstName = 'Jim' ;

		expect( user ).to.equal( {
			_id: id , firstName: 'Jim' , lastName: 'Doe' , memberSid: 'Jack Doe'
		} ) ;

		await user.save() ;
		await expect( users.get( id ) ).to.eventually.equal( {
			_id: id , firstName: 'Jim' , lastName: 'Doe' , memberSid: 'Jack Doe'
		} ) ;
	} ) ;

	it( "should save a full document so parallel save *DO* overwrite each others" , async () => {
		var user = users.createDocument( {
			firstName: 'Johnny B.' ,
			lastName: 'Starks'
		} ) ;

		var id = user.getId() ;

		await user.save() ;
		var dbUser = await users.get( id ) ;

		expect( dbUser._id ).to.equal( id ) ;
		expect( dbUser ).to.equal( {
			_id: id , firstName: 'Johnny B.' , lastName: 'Starks' , memberSid: 'Johnny B. Starks'
		} ) ;

		user.lastName = 'Smith' ;
		dbUser.firstName = 'Joey' ;

		await Promise.all( [
			user.save() ,
			dbUser.save()
		] ) ;

		await expect( users.get( id ) ).to.eventually.equal( {
			_id: id , firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny B. Starks'
		} ) ;
	} ) ;

	it( "should save and retrieve embedded data" , async () => {
		var town = towns.createDocument( {
			name: 'Paris' ,
			meta: {
				population: '2200K' ,
				country: 'France'
			}
		} ) ;

		var id = town.getId() ;

		await town.save() ;
		await expect( towns.get( id ) ).to.eventually.equal( { _id: id , name: 'Paris' , meta: { population: '2200K' , country: 'France' } } ) ;
	} ) ;

} ) ;



describe( "Delete documents" , () => {

	beforeEach( clearDB ) ;

	it( "should delete a document" , async () => {
		var user = users.createDocument( {
			firstName: 'John' ,
			lastName: 'McGregor'
		} ) ;
		var id = user.getId() ;
		await user.save() ;
		await expect( users.get( id ) ).to.eventually.equal( {
			_id: id , firstName: 'John' , lastName: 'McGregor' , memberSid: "John McGregor"
		} ) ;

		await user.delete() ;
		await expect( () => users.get( id ) ).to.reject.with.an( ErrorStatus , { type: 'notFound' } ) ;
	} ) ;
} ) ;



describe( "Refresh documents" , () => {

	beforeEach( clearDB ) ;

	it( "should refresh a document" , async () => {
		var user = users.createDocument( {
			firstName: 'John' ,
			lastName: 'McGregor'
		} ) ;
		var id = user.getId() ;
		await user.save() ;

		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.equal( {
			_id: id , firstName: 'John' , lastName: 'McGregor' , memberSid: "John McGregor"
		} ) ;
		dbUser.firstName = 'Duncan' ;
		await dbUser.save() ;

		await user.refresh() ;
		expect( user ).to.equal( {
			_id: id , firstName: 'John' , lastName: 'McGregor' , memberSid: "John McGregor"
		} ) ;

		await Promise.resolveTimeout( 60 ) ;
		await user.refresh() ;
		expect( user ).to.equal( {
			_id: id , firstName: 'Duncan' , lastName: 'McGregor' , memberSid: "John McGregor"
		} ) ;

		dbUser.firstName = 'Robert' ;
		await dbUser.save() ;

		await user.refresh() ;
		expect( user ).to.equal( {
			_id: id , firstName: 'Duncan' , lastName: 'McGregor' , memberSid: "John McGregor"
		} ) ;

		await Promise.resolveTimeout( 60 ) ;
		await user.refresh() ;
		expect( user ).to.equal( {
			_id: id , firstName: 'Robert' , lastName: 'McGregor' , memberSid: "John McGregor"
		} ) ;

	} ) ;
} ) ;



describe( "Patch, auto-staging, manual staging and commit documents" , () => {

	beforeEach( clearDB ) ;

	it( "auto-staging setter and the .commit() method" , async () => {
		var user = users.createDocument( {
			firstName: 'Johnny' ,
			lastName: 'Starks'
		} ) ;

		var id = user.getId() ;

		await user.save() ;
		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.equal( {
			_id: user._id , firstName: 'Johnny' , lastName: 'Starks' , memberSid: 'Johnny Starks'
		} ) ;

		dbUser.firstName = 'Joey' ;
		expect( dbUser ).to.equal( {
			_id: id , firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny Starks'
		} ) ;
		expect( dbUser._.buildDbPatch().set ).to.equal( { firstName: 'Joey' } ) ;

		await dbUser.commit() ;
		await expect( users.get( id ) ).to.eventually.equal( {
			_id: id , firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny Starks'
		} ) ;

		dbUser.firstName = 'Jack' ;
		dbUser.lastName = 'Smith' ;
		expect( dbUser ).to.equal( {
			_id: id , firstName: 'Jack' , lastName: 'Smith' , memberSid: 'Johnny Starks'
		} ) ;
		expect( dbUser._.buildDbPatch().set ).to.equal( { firstName: 'Jack' , lastName: 'Smith' } ) ;

		await dbUser.commit() ;
		await expect( users.get( id ) ).to.eventually.equal( {
			_id: id , firstName: 'Jack' , lastName: 'Smith' , memberSid: 'Johnny Starks'
		} ) ;
	} ) ;

	it( "manual staging and the .commit() method" , async () => {
		var user = users.createDocument( {
			firstName: 'Johnny' ,
			lastName: 'Starks'
		} ) ;

		var id = user.getId() ;

		await user.save() ;
		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.equal( {
			_id: user._id , firstName: 'Johnny' , lastName: 'Starks' , memberSid: 'Johnny Starks'
		} ) ;

		dbUser._.raw.firstName = 'Joey' ;
		expect( dbUser ).to.equal( {
			_id: id , firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny Starks'
		} ) ;
		expect( dbUser._.buildDbPatch() ).to.be( null ) ;

		// Nothing will be commited
		await dbUser.commit() ;
		await expect( users.get( id ) ).to.eventually.equal( {
			_id: id , firstName: 'Johnny' , lastName: 'Starks' , memberSid: 'Johnny Starks'
		} ) ;

		// Now it will be commited
		dbUser.stage( 'firstName' ) ;
		await dbUser.commit() ;
		await expect( users.get( id ) ).to.eventually.equal( {
			_id: id , firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny Starks'
		} ) ;
	} ) ;

	it( "apply a patch then commit" , async () => {
		var user = users.createDocument( {
			firstName: 'Johnny' ,
			lastName: 'Starks'
		} ) ;

		var id = user.getId() ;

		await user.save() ;
		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.equal( {
			_id: user._id , firstName: 'Johnny' , lastName: 'Starks' , memberSid: 'Johnny Starks'
		} ) ;

		dbUser.patch( { firstName: 'Joey' } ) ;
		expect( dbUser ).to.equal( {
			_id: id , firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny Starks'
		} ) ;
		expect( dbUser._.buildDbPatch().set ).to.equal( { firstName: 'Joey' } ) ;

		await dbUser.commit() ;
		await expect( users.get( id ) ).to.eventually.equal( {
			_id: id , firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny Starks'
		} ) ;

		dbUser.patch( { firstName: 'Jack' , lastName: 'Smith' } ) ;
		expect( dbUser ).to.equal( {
			_id: id , firstName: 'Jack' , lastName: 'Smith' , memberSid: 'Johnny Starks'
		} ) ;
		expect( dbUser._.buildDbPatch().set ).to.equal( { firstName: 'Jack' , lastName: 'Smith' } ) ;

		await dbUser.commit() ;
		await expect( users.get( id ) ).to.eventually.equal( {
			_id: id , firstName: 'Jack' , lastName: 'Smith' , memberSid: 'Johnny Starks'
		} ) ;
	} ) ;

	it( "apply a patch with commands then commit" , async () => {
		var school = schools.createDocument( {
			title: 'Computer Science'
		} ) ;

		var id = school.getId() ;

		var job1 = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var job1Id = job1.getId() ;

		var job2 = jobs.createDocument( {
			title: 'sysadmin' ,
			salary: 55000
		} ) ;

		var job2Id = job2.getId() ;

		var job3 = jobs.createDocument( {
			title: 'front-end developer' ,
			salary: 54000
		} ) ;

		var job3Id = job3.getId() ;

		school.setLink( 'jobs' , [ job1 , job2 ] ) ;

		await Promise.all( [ job1.save() , job2.save() , job3.save() , school.save() ] ) ;

		var dbSchool = await schools.get( id ) ;
		expect( dbSchool ).to.equal( {
			_id: id , title: 'Computer Science' , jobs: [ { _id: job1Id } , { _id: job2Id } ]
		} ) ;

		dbSchool.patch( { jobs: { $push: job3Id } } , { validate: true } ) ;
		expect( dbSchool ).to.equal( {
			_id: id , title: 'Computer Science' , jobs: [ { _id: job1Id } , { _id: job2Id } , { _id: job3Id } ]
		} ) ;

		await dbSchool.save() ;

		dbSchool = await schools.get( id ) ;
		expect( dbSchool ).to.equal( {
			_id: id , title: 'Computer Science' , jobs: [ { _id: job1Id } , { _id: job2Id } , { _id: job3Id } ]
		} ) ;
	} ) ;

	it( "staging/commit and embedded data" , async () => {
		var town = towns.createDocument( {
			name: 'Paris' ,
			meta: {
				population: '2200K' ,
				country: 'France'
			}
		} ) ;

		var id = town.getId() ;

		await town.save() ;
		var dbTown = await towns.get( id ) ;
		expect( dbTown ).to.equal( { _id: id , name: 'Paris' , meta: { population: '2200K' , country: 'France' } } ) ;

		dbTown.patch( { "meta.population": "2300K" } ) ;
		await dbTown.commit() ;
		await expect( towns.get( id ) ).to.eventually.equal( { _id: id , name: 'Paris' , meta: { population: '2300K' , country: 'France' } } ) ;

		dbTown.meta.population = "2500K" ;
		expect( dbTown._.buildDbPatch().set ).to.equal( { "meta.population": "2500K" } ) ;
		await dbTown.commit() ;
		await expect( towns.get( id ) ).to.eventually.equal( { _id: id , name: 'Paris' , meta: { population: '2500K' , country: 'France' } } ) ;
	} ) ;

	it( "parallel and non-overlapping commit should not overwrite each others" , async () => {
		var user = users.createDocument( {
			firstName: 'Johnny' ,
			lastName: 'Starks'
		} ) ;

		var id = user.getId() ;

		await user.save() ;
		var dbUser = await users.get( id ) ;

		user.patch( { lastName: 'Smith' } ) ;
		dbUser.firstName = 'Joey' ;
		expect( user ).to.equal( {
			_id: id , firstName: 'Johnny' , lastName: 'Smith' , memberSid: 'Johnny Starks'
		} ) ;
		expect( dbUser ).to.equal( {
			_id: id , firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny Starks'
		} ) ;

		await Promise.all( [
			user.commit() ,
			dbUser.commit()
		] ) ;

		await expect( users.get( id ) ).to.eventually.equal( {
			_id: id , firstName: 'Joey' , lastName: 'Smith' , memberSid: 'Johnny Starks'
		} ) ;
	} ) ;

	it( "testing internal local change with overwriting and depth mixing changes" , async () => {
		var town = towns.createDocument( {
			name: 'Paris' ,
			meta: {
				population: '2200K' ,
				country: 'France'
			}
		} ) ;

		town._.localChanges = null ;
		town._._addLocalChange( [ 'meta' , 'country' ] ) ;
		expect( town._.localChanges ).to.equal( { meta: { country: null } } ) ;
		expect( town._.buildDbPatch().set ).to.equal( { "meta.country": "France" } ) ;
		town._._addLocalChange( [ 'meta' ] ) ;
		expect( town._.localChanges ).to.equal( { meta: null } ) ;
		expect( town._.buildDbPatch().set ).to.equal( { meta: { population: "2200K" , country: "France" } } ) ;

		town._.localChanges = null ;
		town._._addLocalChange( [ 'meta' ] ) ;
		expect( town._.localChanges ).to.equal( { meta: null } ) ;
		expect( town._.buildDbPatch().set ).to.equal( { meta: { population: "2200K" , country: "France" } } ) ;
		town._._addLocalChange( [ 'meta' , 'country' ] ) ;
		expect( town._.localChanges ).to.equal( { meta: null } ) ;
		expect( town._.buildDbPatch().set ).to.equal( { meta: { population: "2200K" , country: "France" } } ) ;
		town._._addLocalChange( [ 'meta' , 'population' ] ) ;
		expect( town._.localChanges ).to.equal( { meta: null } ) ;
		expect( town._.buildDbPatch().set ).to.equal( { meta: { population: "2200K" , country: "France" } } ) ;

		town._.localChanges = null ;
		town._._addLocalChange( [ 'meta' , 'population' ] ) ;
		town._._addLocalChange( [ 'meta' , 'country' ] ) ;
		expect( town._.localChanges ).to.equal( { meta: { population: null , country: null } } ) ;
		expect( town._.buildDbPatch().set ).to.equal( { "meta.population": "2200K" , "meta.country": "France" } ) ;
		town._._addLocalChange( [ 'meta' ] ) ;
		expect( town._.localChanges ).to.equal( { meta: null } ) ;
		expect( town._.buildDbPatch().set ).to.equal( { meta: { population: "2200K" , country: "France" } } ) ;

		town._.localChanges = null ;
		town._._addLocalChange( [ 'meta' ] ) ;
		town._._addLocalChange( [ 'meta' ] ) ;
		expect( town._.localChanges ).to.equal( { meta: null } ) ;

		town._.localChanges = null ;
		town._._addLocalChange( [ 'meta' , 'population' ] ) ;
		town._._addLocalChange( [ 'meta' , 'population' ] ) ;
		expect( town._.localChanges ).to.equal( { meta: { population: null } } ) ;
	} ) ;

	it( "overwriting and depth mixing staging" , async () => {
		var town = towns.createDocument( {
			name: 'Paris' ,
			meta: {
				population: '2200K' ,
				country: 'France'
			}
		} ) ;

		var id = town.getId() ;

		await town.save() ;
		var dbTown = await towns.get( id ) ;
		expect( dbTown ).to.equal( { _id: id , name: 'Paris' , meta: { population: '2200K' , country: 'France' } } ) ;

		dbTown.meta.population = "2300K" ;
		dbTown.meta = { population: "2400K" , country: "La France" } ;
		await dbTown.commit() ;
		await expect( towns.get( id ) ).to.eventually.equal( { _id: id , name: 'Paris' , meta: { population: '2400K' , country: 'La France' } } ) ;
	} ) ;

	it( "delete and auto-staging" , async () => {
		var town = towns.createDocument( {
			name: 'Paris' ,
			meta: {
				population: '2200K' ,
				country: 'France'
			}
		} ) ;

		var id = town.getId() ;

		await town.save() ;
		var dbTown = await towns.get( id ) ;
		expect( dbTown ).to.equal( { _id: id , name: 'Paris' , meta: { population: '2200K' , country: 'France' } } ) ;

		delete dbTown.meta.population ;
		await dbTown.commit() ;
		await expect( towns.get( id ) ).to.eventually.equal( { _id: id , name: 'Paris' , meta: { country: 'France' } } ) ;
	} ) ;
} ) ;



describe( "Fingerprint" , () => {

	it( "should create a fingerprint" , () => {
		var f ;

		f = users.createFingerprint( { firstName: 'Terry' } ) ;

		expect( f ).to.be.an( rootsDb.Fingerprint ) ;
		expect( f.def ).to.equal( { firstName: 'Terry' } ) ;
		expect( f.partial ).to.equal( { firstName: 'Terry' } ) ;

		f = users.createFingerprint( { "path.to.data": "my data" } ) ;

		expect( f ).to.be.an( rootsDb.Fingerprint ) ;
		expect( f.def ).to.equal( { "path.to.data": "my data" } ) ;
		expect( f.partial ).to.equal( { path: { to: { data: "my data" } } } ) ;
	} ) ;

	it( "should create a fingerprint from a partial document" , () => {
		var f = users.createFingerprint( { path: { to: { data: "my data" } } } , true ) ;

		expect( f ).to.be.an( rootsDb.Fingerprint ) ;
		expect( f.def ).to.equal( { "path.to.data": "my data" } ) ;
		expect( f.partial ).to.equal( { path: { to: { data: "my data" } } } ) ;
	} ) ;

	it( "should detect uniqueness correctly" , () => {
		expect( users.createFingerprint( { _id: '123456789012345678901234' } ).unique ).to.be( true ) ;
		expect( users.createFingerprint( { firstName: 'Terry' } ).unique ).to.be( false ) ;
		expect( users.createFingerprint( { firstName: 'Terry' , lastName: 'Bogard' } ).unique ).to.be( false ) ;
		expect( users.createFingerprint( { _id: '123456789012345678901234' , firstName: 'Terry' , lastName: 'Bogard' } ).unique ).to.be( true ) ;
		expect( users.createFingerprint( { "job._id": '123456789012345678901234' } ).unique ).to.be( false ) ;
		expect( users.createFingerprint( { memberSid: 'terry-bogard' } ).unique ).to.be( false ) ;
		expect( users.createFingerprint( { "job._id": '123456789012345678901234' , memberSid: 'terry-bogard' } ).unique ).to.be( true ) ;
	} ) ;
} ) ;



describe( "Get documents by unique fingerprint" , () => {

	beforeEach( clearDB ) ;

	it( "should get a document by a unique fingerprint" , async () => {
		var user = users.createDocument( {
			firstName: 'Bill' ,
			lastName: "Cut'throat"
		} ) ;

		var userId = user.getId() ;
		var memberSid = user.memberSid ;

		var job = jobs.createDocument() ;
		var jobId = job.getId() ;
		user.job = jobId ;

		await user.save() ;
		await job.save() ;

		await expect( users.getUnique( { memberSid: memberSid , "job._id": jobId } ) ).to.eventually.equal( {
			_id: userId , job: { _id: jobId } , firstName: 'Bill' , lastName: "Cut'throat" , memberSid: "Bill Cut'throat"
		} ) ;
	} ) ;

	it( "when trying to get a document with a non-unique fingerprint, an ErrorStatus (type: badRequest) should be issued" , async () => {
		var user = users.createDocument( {
			firstName: 'Bill' ,
			lastName: "Tannen"
		} ) ;

		var id = user.getId() ;
		await user.save() ;

		await expect( () => users.getUnique( { firstName: 'Bill' , lastName: "Tannen" } ) ).to.reject.with.an( ErrorStatus , { type: 'badRequest' } ) ;
	} ) ;

	it( "should get a document by a unique fingerprint with deep ref (to embedded data)" , async () => {
		var localBatch = towns.createBatch( [
			{
				name: 'Paris' ,
				meta: {
					country: 'France' ,
					capital: true
				}
			} ,
			{
				name: 'Tokyo' ,
				meta: {
					country: 'Japan' ,
					capital: true
				}
			} ,
			{
				name: 'New York' ,
				meta: {
					country: 'USA' ,
					capital: false
				}
			}
		] ) ;

		expect( localBatch ).to.have.length( 3 ) ;

		await localBatch.save() ;

		var town = await towns.getUnique( { name: 'Tokyo' , "meta.country": 'Japan' } ) ;

		expect( town ).to.equal( {
			_id: town._id ,
			name: 'Tokyo' ,
			meta: {
				country: 'Japan' ,
				capital: true
			}
		} ) ;
	} ) ;
} ) ;



describe( "Batch creation" , () => {

	it( "should create an empty batch" , () => {
		var userBatch = users.createBatch() ;

		expect( Array.isArray( userBatch ) ).to.be.ok() ;
		expect( userBatch ).to.be.an( Array ) ;
		expect( userBatch ).to.be.a( rootsDb.Batch ) ;
		expect( userBatch ).to.have.length( 0 ) ;
	} ) ;

	it( "should create a batch with few default documents" , () => {
		var userBatch = users.createBatch( [ {} , {} ] ) ;

		expect( Array.isArray( userBatch ) ).to.be.ok() ;
		expect( userBatch ).to.be.an( Array ) ;
		expect( userBatch ).to.be.a( rootsDb.Batch ) ;
		expect( userBatch ).to.have.length( 2 ) ;

		expect( userBatch[ 0 ] ).to.be.an( Object ) ;
		expect( userBatch[ 0 ].$ ).to.be.an( Object ) ;
		expect( userBatch[ 0 ]._ ).to.be.a( rootsDb.Document ) ;
		expect( userBatch[ 0 ]._id ).to.be.an( mongodb.ObjectId ) ;
		expect( userBatch[ 0 ].getId() ).to.be.an( mongodb.ObjectId ) ;
		expect( userBatch[ 0 ]._id ).to.be( userBatch[ 0 ].getId() ) ;
		expect( userBatch[ 0 ] ).to.partially.equal( expectedDefaultUser ) ;
		expect( userBatch[ 0 ].$ ).to.partially.equal( expectedDefaultUser ) ;

		expect( userBatch[ 1 ] ).to.be.an( Object ) ;
		expect( userBatch[ 1 ].$ ).to.be.an( Object ) ;
		expect( userBatch[ 1 ]._ ).to.be.a( rootsDb.Document ) ;
		expect( userBatch[ 1 ]._id ).to.be.an( mongodb.ObjectId ) ;
		expect( userBatch[ 1 ].getId() ).to.be.an( mongodb.ObjectId ) ;
		expect( userBatch[ 1 ]._id ).to.be( userBatch[ 1 ].getId() ) ;
		expect( userBatch[ 1 ] ).to.partially.equal( expectedDefaultUser ) ;
		expect( userBatch[ 1 ].$ ).to.partially.equal( expectedDefaultUser ) ;
	} ) ;

	it( "should create a batch with few documents with valid data" , () => {
		var userBatch = users.createBatch( [
			{ firstName: 'Bobby' , lastName: 'Fischer' } ,
			{ firstName: 'John' , lastName: 'Smith' }
		] ) ;

		expect( Array.isArray( userBatch ) ).to.be.ok() ;
		expect( userBatch ).to.be.an( Array ) ;
		expect( userBatch ).to.be.a( rootsDb.Batch ) ;
		expect( userBatch ).to.have.length( 2 ) ;

		expect( userBatch[ 0 ] ).to.be.an( Object ) ;
		expect( userBatch[ 0 ].$ ).to.be.an( Object ) ;
		expect( userBatch[ 0 ]._ ).to.be.a( rootsDb.Document ) ;
		expect( userBatch[ 0 ]._id ).to.be.an( mongodb.ObjectId ) ;
		expect( userBatch[ 0 ].getId() ).to.be.an( mongodb.ObjectId ) ;
		expect( userBatch[ 0 ]._id ).to.be( userBatch[ 0 ].getId() ) ;
		expect( userBatch[ 0 ] ).to.equal( {
			_id: userBatch[ 0 ].getId() , firstName: 'Bobby' , lastName: 'Fischer' , memberSid: 'Bobby Fischer'
		} ) ;

		expect( userBatch[ 1 ] ).to.be.an( Object ) ;
		expect( userBatch[ 1 ].$ ).to.be.an( Object ) ;
		expect( userBatch[ 1 ]._ ).to.be.a( rootsDb.Document ) ;
		expect( userBatch[ 1 ]._id ).to.be.an( mongodb.ObjectId ) ;
		expect( userBatch[ 1 ].getId() ).to.be.an( mongodb.ObjectId ) ;
		expect( userBatch[ 1 ]._id ).to.be( userBatch[ 1 ].getId() ) ;
		expect( userBatch[ 1 ] ).to.partially.equal( {
			_id: userBatch[ 1 ].getId() , firstName: 'John' , lastName: 'Smith' , memberSid: 'John Smith'
		} ) ;
	} ) ;

	it( "batch should inherit Array methods and constructs" , () => {
		var count , seen ;

		var userBatch = users.createBatch( [
			{ firstName: 'Bobby' , lastName: 'Fischer' } ,
			{ firstName: 'John' , lastName: 'Smith' }
		] ) ;

		// .push()
		userBatch.push( { firstName: 'Kurisu' , lastName: 'Makise' } ) ;
		expect( userBatch ).to.have.length( 3 ) ;
		expect( userBatch[ 2 ] ).to.be.an( Object ) ;
		expect( userBatch[ 2 ].$ ).to.be.an( Object ) ;
		expect( userBatch[ 2 ]._ ).to.be.a( rootsDb.Document ) ;
		expect( userBatch[ 2 ]._id ).to.be.an( mongodb.ObjectId ) ;
		expect( userBatch[ 2 ].getId() ).to.be.an( mongodb.ObjectId ) ;
		expect( userBatch[ 2 ]._id ).to.be( userBatch[ 2 ].getId() ) ;
		expect( userBatch[ 2 ] ).to.partially.equal( {
			_id: userBatch[ 2 ].getId() , firstName: 'Kurisu' , lastName: 'Makise' , memberSid: 'Kurisu Makise'
		} ) ;

		// .forEach()
		count = 0 ;
		seen = [] ;
		userBatch.forEach( doc => {
			expect( doc._ ).to.be.a( rootsDb.Document ) ;
			seen.push( doc.lastName ) ;
			count ++ ;
		} ) ;
		expect( count ).to.be( 3 ) ;
		expect( seen ).to.equal( [ 'Fischer' , 'Smith' , 'Makise' ] ) ;

		// for ... of
		count = 0 ;
		seen = [] ;
		for ( let doc of userBatch ) {
			expect( doc._ ).to.be.a( rootsDb.Document ) ;
			seen.push( doc.lastName ) ;
			count ++ ;
		}
		expect( count ).to.be( 3 ) ;
		expect( seen ).to.equal( [ 'Fischer' , 'Smith' , 'Makise' ] ) ;
	} ) ;

	it( "should save a whole batch" , async () => {
		var userBatch = users.createBatch( [
			{ firstName: 'Bobby' , lastName: 'Fischer' } ,
			{ firstName: 'John' , lastName: 'Smith' }
		] ) ;

		await userBatch.save() ;

		await expect( users.get( userBatch[ 0 ].getId() ) ).to.eventually.equal(
			{
				_id: userBatch[ 0 ].getId() , firstName: 'Bobby' , lastName: 'Fischer' , memberSid: 'Bobby Fischer'
			}
		) ;

		await expect( users.get( userBatch[ 1 ].getId() ) ).to.eventually.equal(
			{
				_id: userBatch[ 1 ].getId() , firstName: 'John' , lastName: 'Smith' , memberSid: 'John Smith'
			}
		) ;
	} ) ;
} ) ;



describe( "Multi Get" , () => {

	beforeEach( clearDB ) ;

	it( "should get multiple document using an array of IDs" , async () => {
		var map , batch ;

		var marleys = users.createBatch( [
			{ firstName: 'Bob' , lastName: 'Marley' } ,
			{ firstName: 'Julian' , lastName: 'Marley' } ,
			{ firstName: 'Stephen' , lastName: 'Marley' } ,
			{ firstName: 'Ziggy' , lastName: 'Marley' } ,
			{ firstName: 'Rita' , lastName: 'Marley' }
		] ) ;

		expect( marleys ).to.have.length( 5 ) ;
		var ids = marleys.map( doc => doc.getId() ) ;

		await marleys.save() ;

		batch = await users.multiGet( ids ) ;

		expect( batch ).to.be.a( rootsDb.Batch ) ;
		expect( batch ).to.have.length( 5 ) ;

		// MongoDB may shuffle things up, so we don't use an array here
		map = {} ;

		batch.forEach( doc => {
			expect( doc._ ).to.be.a( rootsDb.Document ) ;
			expect( doc.firstName ).to.be.ok() ;
			expect( doc.lastName ).to.equal( 'Marley' ) ;
			map[ doc.firstName ] = doc ;
		} ) ;

		expect( map ).to.only.have.own.keys( 'Bob' , 'Julian' , 'Stephen' , 'Ziggy' , 'Rita' ) ;
		expect( map ).to.equal( {
			Bob: {
				_id: marleys[ 0 ].getId() , firstName: 'Bob' , lastName: 'Marley' , memberSid: 'Bob Marley'
			} ,
			Julian: {
				_id: marleys[ 1 ].getId() , firstName: 'Julian' , lastName: 'Marley' , memberSid: 'Julian Marley'
			} ,
			Stephen: {
				_id: marleys[ 2 ].getId() , firstName: 'Stephen' , lastName: 'Marley' , memberSid: 'Stephen Marley'
			} ,
			Ziggy: {
				_id: marleys[ 3 ].getId() , firstName: 'Ziggy' , lastName: 'Marley' , memberSid: 'Ziggy Marley'
			} ,
			Rita: {
				_id: marleys[ 4 ].getId() , firstName: 'Rita' , lastName: 'Marley' , memberSid: 'Rita Marley'
			}
		} ) ;


		// Same with a subset of what is in the DB
		batch = await users.multiGet( [ marleys[ 2 ].getId() , marleys[ 4 ].getId() ] ) ;

		expect( batch ).to.be.a( rootsDb.Batch ) ;
		expect( batch ).to.have.length( 2 ) ;

		// MongoDB may shuffle things up, so we don't use an array here
		map = {} ;

		batch.forEach( doc => {
			expect( doc._ ).to.be.a( rootsDb.Document ) ;
			expect( doc.firstName ).to.be.ok() ;
			expect( doc.lastName ).to.equal( 'Marley' ) ;
			map[ doc.firstName ] = doc ;
		} ) ;

		expect( map ).to.equal( {
			Stephen: {
				_id: marleys[ 2 ].getId() , firstName: 'Stephen' , lastName: 'Marley' , memberSid: 'Stephen Marley'
			} ,
			Rita: {
				_id: marleys[ 4 ].getId() , firstName: 'Rita' , lastName: 'Marley' , memberSid: 'Rita Marley'
			}
		} ) ;
	} ) ;
} ) ;



describe( "Collect by fingerprint" , () => {

	beforeEach( clearDB ) ;

	it( "should collect a batch using a non-unique fingerprint" , async () => {
		var localBatch = users.createBatch( [
			{ firstName: 'Bob' , lastName: 'Marley' } ,
			{ firstName: 'Julian' , lastName: 'Marley' } ,
			{ firstName: 'Mr' , lastName: 'X' } ,
			{ firstName: 'Stephen' , lastName: 'Marley' } ,
			{ firstName: 'Ziggy' , lastName: 'Marley' } ,
			{ firstName: 'Thomas' , lastName: 'Jefferson' } ,
			{ firstName: 'Rita' , lastName: 'Marley' }
		] ) ;

		expect( localBatch ).to.have.length( 7 ) ;

		await localBatch.save() ;

		var batch = await users.collect( { lastName: 'Marley' } ) ;

		expect( batch ).to.be.a( rootsDb.Batch ) ;
		expect( batch ).to.have.length( 5 ) ;

		// MongoDB may shuffle things up, so we don't use an array here
		var map = {} ;

		batch.forEach( doc => {
			expect( doc._ ).to.be.a( rootsDb.Document ) ;
			expect( doc.firstName ).to.be.ok() ;
			expect( doc.lastName ).to.equal( 'Marley' ) ;
			map[ doc.firstName ] = doc ;
		} ) ;

		expect( map ).to.only.have.own.keys( 'Bob' , 'Julian' , 'Stephen' , 'Ziggy' , 'Rita' ) ;
		expect( map ).to.equal( {
			Bob: {
				_id: localBatch[ 0 ].getId() , firstName: 'Bob' , lastName: 'Marley' , memberSid: 'Bob Marley'
			} ,
			Julian: {
				_id: localBatch[ 1 ].getId() , firstName: 'Julian' , lastName: 'Marley' , memberSid: 'Julian Marley'
			} ,
			Stephen: {
				_id: localBatch[ 3 ].getId() , firstName: 'Stephen' , lastName: 'Marley' , memberSid: 'Stephen Marley'
			} ,
			Ziggy: {
				_id: localBatch[ 4 ].getId() , firstName: 'Ziggy' , lastName: 'Marley' , memberSid: 'Ziggy Marley'
			} ,
			Rita: {
				_id: localBatch[ 6 ].getId() , firstName: 'Rita' , lastName: 'Marley' , memberSid: 'Rita Marley'
			}
		} ) ;
	} ) ;

	it( "should collect a batch using a fingerprint with deep ref (to embedded data)" , async () => {
		var map , batch ;

		var localBatch = towns.createBatch( [
			{
				name: 'Paris' ,
				meta: {
					country: 'France' ,
					capital: true
				}
			} ,
			{
				name: 'Tokyo' ,
				meta: {
					country: 'Japan' ,
					capital: true
				}
			} ,
			{
				name: 'New York' ,
				meta: {
					country: 'USA' ,
					capital: false
				}
			} ,
			{
				name: 'Washington' ,
				meta: {
					country: 'USA' ,
					capital: true
				}
			} ,
			{
				name: 'San Francisco' ,
				meta: {
					country: 'USA' ,
					capital: false
				}
			}
		] ) ;

		expect( localBatch ).to.have.length( 5 ) ;

		await localBatch.save() ;

		batch = await towns.collect( { "meta.country": "USA" } ) ;

		expect( batch ).to.be.a( rootsDb.Batch ) ;
		expect( batch ).to.have.length( 3 ) ;

		// MongoDB may shuffle things up, so we don't use an array here
		map = {} ;

		batch.forEach( doc => {
			expect( doc._ ).to.be.a( rootsDb.Document ) ;
			map[ doc.name ] = doc ;
		} ) ;

		expect( map ).to.equal( {
			"New York": { _id: localBatch[ 2 ].getId() , name: "New York" , meta: { country: "USA" , capital: false } } ,
			"Washington": { _id: localBatch[ 3 ].getId() , name: "Washington" , meta: { country: "USA" , capital: true } } ,
			"San Francisco": { _id: localBatch[ 4 ].getId() , name: "San Francisco" , meta: { country: "USA" , capital: false } }
		} ) ;

		batch = await towns.collect( { "meta.country": "USA" ,  "meta.capital": false } ) ;

		expect( batch ).to.have.length( 2 ) ;

		// MongoDB may shuffle things up, so we don't use an array here
		map = {} ;

		batch.forEach( doc => {
			expect( doc._ ).to.be.a( rootsDb.Document ) ;
			map[ doc.name ] = doc ;
		} ) ;

		expect( map ).to.equal( {
			"New York": { _id: localBatch[ 2 ].getId() , name: "New York" , meta: { country: "USA" , capital: false } } ,
			"San Francisco": { _id: localBatch[ 4 ].getId() , name: "San Francisco" , meta: { country: "USA" , capital: false } }
		} ) ;
	} ) ;
} ) ;



describe( "Find with a query object" , () => {

	beforeEach( clearDB ) ;

	it( "should find documents (in a batch) using a queryObject" , async () => {
		var localBatch = users.createBatch( [
			{ firstName: 'Bob' , lastName: 'Marley' } ,
			{ firstName: 'Julian' , lastName: 'Marley' } ,
			{ firstName: 'Mr' , lastName: 'X' } ,
			{ firstName: 'Stephen' , lastName: 'Marley' } ,
			{ firstName: 'Ziggy' , lastName: 'Marley' } ,
			{ firstName: 'Thomas' , lastName: 'Jefferson' } ,
			{ firstName: 'Rita' , lastName: 'Marley' }
		] ) ;

		expect( localBatch ).to.have.length( 7 ) ;

		await localBatch.save() ;

		var batch = await users.find( { firstName: { $regex: /^[thomasstepn]+$/ , $options: 'i' } } ) ;

		expect( batch ).to.be.a( rootsDb.Batch ) ;
		expect( batch ).to.have.length( 2 ) ;

		// MongoDB may shuffle things up, so we don't use an array here
		var map = {} ;

		batch.forEach( doc => {
			expect( doc._ ).to.be.a( rootsDb.Document ) ;
			map[ doc.firstName ] = doc ;
		} ) ;

		expect( map ).to.equal( {
			Stephen: {
				_id: localBatch[ 3 ].getId() , firstName: 'Stephen' , lastName: 'Marley' , memberSid: 'Stephen Marley'
			} ,
			Thomas: {
				_id: localBatch[ 5 ].getId() , firstName: 'Thomas' , lastName: 'Jefferson' , memberSid: 'Thomas Jefferson'
			}
		} ) ;
	} ) ;

	it( "should count found documents using a queryObject" , async () => {
		var localBatch = users.createBatch( [
			{ firstName: 'Bob' , lastName: 'Marley' } ,
			{ firstName: 'Julian' , lastName: 'Marley' } ,
			{ firstName: 'Mr' , lastName: 'X' } ,
			{ firstName: 'Stephen' , lastName: 'Marley' } ,
			{ firstName: 'Ziggy' , lastName: 'Marley' } ,
			{ firstName: 'Thomas' , lastName: 'Jefferson' } ,
			{ firstName: 'Rita' , lastName: 'Marley' }
		] ) ;

		expect( localBatch ).to.have.length( 7 ) ;

		await localBatch.save() ;

		await expect( users.countFound( {} ) ).to.eventually.be( 7 ) ;
		await expect( users.countFound() ).to.eventually.be( 7 ) ;
		await expect( users.countFound( { lastName: 'Marley' } ) ).to.eventually.be( 5 ) ;
		await expect( users.countFound( { firstName: { $regex: /^[thomasstepn]+$/ , $options: 'i' } } ) ).to.eventually.be( 2 ) ;
	} ) ;

	it( "skip, limit and sort" , async () => {
		var marleys = [
			users.createDocument( { firstName: 'Bob' , lastName: 'Marley' } ) ,
			users.createDocument( { firstName: 'Julian' , lastName: 'Marley' } ) ,
			users.createDocument( { firstName: 'Thomas' , lastName: 'Jefferson' } ) ,
			users.createDocument( { firstName: 'Stephen' , lastName: 'Marley' } ) ,
			users.createDocument( { firstName: 'Mr' , lastName: 'X' } ) ,
			users.createDocument( { firstName: 'Ziggy' , lastName: 'Marley' } ) ,
			users.createDocument( { firstName: 'Rita' , lastName: 'Marley' } )
		] ;

		await Promise.map( marleys , user => user.save() ) ;
		var dbBatch = await users.find( {} , { skip: 1 , limit: 2 , sort: { firstName: 1 } } ) ;

		expect( dbBatch ).to.have.length( 2 ) ;
		expect( dbBatch ).to.be.partially.like( [
			{ firstName: 'Julian' , lastName: 'Marley' } ,
			{ firstName: 'Mr' , lastName: 'X' }
		] ) ;
	} ) ;

	it( "Sort on a collection with an index, with collation" , async () => {
		var someSchools = [
			schools.createDocument( { title: 'a school' } ) ,
			schools.createDocument( { title: 'that school' } ) ,
			schools.createDocument( { title: 'That school' } ) ,
			schools.createDocument( { title: 'A school' } ) ,
			schools.createDocument( { title: 'School1' } ) ,
			schools.createDocument( { title: 'School10' } ) ,
			schools.createDocument( { title: 'School101' } ) ,
			schools.createDocument( { title: 'School2' } ) ,
			schools.createDocument( { title: 'School3' } ) ,
		] ;

		await Promise.map( someSchools , school => school.save() ) ;
		var dbBatch = await schools.find( {} , {
			collation: { locale: 'en' , caseLevel: true , numericOrdering: true } ,
			sort: { title: 1 }
		} ) ;

		//expect( dbBatch ).to.have.length( 2 ) ;
		//log.hdebug( "Batch: %Y" , [ ... dbBatch ] ) ;
		expect( dbBatch ).to.be.partially.like( [
			{ title: "a school" } ,
			{ title: "A school" } ,
			{ title: "School1" } ,
			{ title: "School2" } ,
			{ title: "School3" } ,
			{ title: "School10" } ,
			{ title: "School101" } ,
			{ title: "that school" } ,
			{ title: "That school" }
		] ) ;

		
		// Implicit collation: it should use the collation of the index having [title] as property (isDefaultSortCollation in the schema)
		dbBatch = await schools.find( {} , { sort: { title: 1 } } ) ;

		//expect( dbBatch ).to.have.length( 2 ) ;
		//log.hdebug( "Batch: %Y" , [ ... dbBatch ] ) ;
		expect( dbBatch ).to.be.partially.like( [
			{ title: "a school" } ,
			{ title: "A school" } ,
			{ title: "School1" } ,
			{ title: "School2" } ,
			{ title: "School3" } ,
			{ title: "School10" } ,
			{ title: "School101" } ,
			{ title: "that school" } ,
			{ title: "That school" }
		] ) ;
	} ) ;
} ) ;



describe( "Find (generator) documents with a query object (serialized)" , () => {

	beforeEach( clearDB ) ;

	it( "should find documents (in a batch) using a queryObject" , async () => {
		var localBatch = users.createBatch( [
			{ firstName: 'Bob' , lastName: 'Marley' } ,
			{ firstName: 'Julian' , lastName: 'Marley' } ,
			{ firstName: 'Mr' , lastName: 'X' } ,
			{ firstName: 'Stephen' , lastName: 'Marley' } ,
			{ firstName: 'Ziggy' , lastName: 'Marley' } ,
			{ firstName: 'Thomas' , lastName: 'Jefferson' } ,
			{ firstName: 'Rita' , lastName: 'Marley' }
		] ) ;

		expect( localBatch ).to.have.length( 7 ) ;

		await localBatch.save() ;

		var startDate = Date.now() ,
			docs = [] ;

		for await ( let doc of users.findGenerator( { firstName: { $regex: /^[thomasepnbo]+$/ , $options: 'i' } } ) ) {
			expect( doc._ ).to.be.a( rootsDb.Document ) ;
			docs.push( doc ) ;
			// Force a timeout
			await Promise.resolveTimeout( 50 ) ;
		}

		// Check serialization time
		expect( Date.now() - startDate ).to.be.greater.than( 150 ) ;

		expect( docs ).to.be.an( Array ) ;
		expect( docs ).to.have.length( 3 ) ;

		// Sort that first...
		docs.sort( ascendingSortFn( 'firstName' ) ) ;

		expect( docs ).to.be.partially.like( [
			{ firstName: 'Bob' , lastName: 'Marley' , memberSid: 'Bob Marley' } ,
			{ firstName: 'Stephen' , lastName: 'Marley' , memberSid: 'Stephen Marley' } ,
			{ firstName: 'Thomas' , lastName: 'Jefferson' , memberSid: 'Thomas Jefferson' }
		] ) ;
	} ) ;
} ) ;



describe( "Find IDs with a query object" , () => {

	beforeEach( clearDB ) ;

	it( "should find documents (in a batch) using a queryObject" , async () => {
		var localBatch = users.createBatch( [
			{ firstName: 'Bob' , lastName: 'Marley' } ,
			{ firstName: 'Julian' , lastName: 'Marley' } ,
			{ firstName: 'Mr' , lastName: 'X' } ,
			{ firstName: 'Stephen' , lastName: 'Marley' } ,
			{ firstName: 'Ziggy' , lastName: 'Marley' } ,
			{ firstName: 'Thomas' , lastName: 'Jefferson' } ,
			{ firstName: 'Rita' , lastName: 'Marley' }
		] ) ;

		expect( localBatch ).to.have.length( 7 ) ;

		await localBatch.save() ;

		var idList = await users.findIdList( { firstName: { $regex: /^[thomasstepn]+$/ , $options: 'i' } } ) ;

		expect( idList ).to.be.an( Array ) ;
		expect( idList ).to.have.length( 2 ) ;
		expect( idList ).to.equal.unordered( [ localBatch[ 3 ].getId() , localBatch[ 5 ].getId() ] ) ;

		
		idList = await users.findIdList( { firstName: { $regex: /^[thomasstepn]+$/ , $options: 'i' } } , { partial: true } ) ;

		expect( idList ).to.be.an( Array ) ;
		expect( idList ).to.have.length( 2 ) ;
		expect( idList ).to.equal.unordered( [ { _id: localBatch[ 3 ].getId() } , { _id: localBatch[ 5 ].getId() } ] ) ;
	} ) ;

	it( "skip, limit and sort" , async () => {
		var localBatch = users.createBatch( [
			{ firstName: 'Bob' , lastName: 'Marley' } ,
			{ firstName: 'Julian' , lastName: 'Marley' } ,
			{ firstName: 'Stephen' , lastName: 'Marley' } ,
			{ firstName: 'Ziggy' , lastName: 'Marley' } ,
			{ firstName: 'Thomas' , lastName: 'Jefferson' } ,
			{ firstName: 'Rita' , lastName: 'Marley' }
		] ) ;

		expect( localBatch ).to.have.length( 6 ) ;

		await localBatch.save() ;

		var idList = await users.findIdList( {} , { skip: 1 , limit: 2 , sort: { firstName: 1 } } ) ;

		expect( idList ).to.be.an( Array ) ;
		expect( idList ).to.have.length( 2 ) ;
		expect( idList ).to.equal.unordered( [ localBatch[ 1 ].getId() , localBatch[ 5 ].getId() ] ) ;


		idList = await users.findIdList( {} , { skip: 1 , limit: 2 , sort: { firstName: 1 } , partial: true } ) ;

		expect( idList ).to.be.an( Array ) ;
		expect( idList ).to.have.length( 2 ) ;
		expect( idList ).to.equal.unordered( [ { _id: localBatch[ 1 ].getId() } , { _id: localBatch[ 5 ].getId() } ] ) ;
	} ) ;
} ) ;



describe( "Links" , () => {

	beforeEach( clearDB ) ;

	it( "should retrieve details of an inactive link" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var userId = user.getId() ;

		expect( user.getLinkDetails( 'job' ) ).to.equal( {
			type: 'link' ,
			anyCollection: false ,
			foreignCollection: 'jobs' ,
			foreignId: null ,
			hostPath: 'job' ,
			schema: {
				collection: 'jobs' ,
				optional: true ,
				type: 'link' ,
				opaque: true ,
				tags: [ 'content' ] ,
				sanitize: [ 'toLink' ] ,
				inputHint: "embedded"
			}
		} ) ;

		// Same on saved documents...
		await user.save() ;
		var dbUser = await users.get( userId ) ;

		expect( dbUser.getLinkDetails( 'job' ) ).to.equal( {
			type: 'link' ,
			anyCollection: false ,
			foreignCollection: 'jobs' ,
			foreignId: null ,
			hostPath: 'job' ,
			schema: {
				collection: 'jobs' ,
				optional: true ,
				type: 'link' ,
				opaque: true ,
				tags: [ 'content' ] ,
				sanitize: [ 'toLink' ] ,
				inputHint: "embedded"
			}
		} ) ;
	} ) ;

	it( "should retrieve details of an active link (setLink then getLinkDetails)" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var userId = user.getId() ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var jobId = job.getId() ;

		user.setLink( 'job' , job ) ;

		expect( user.job._id ).to.equal( jobId ) ;
		expect( user.getLinkDetails( 'job' ) ).to.equal( {
			type: 'link' ,
			anyCollection: false ,
			foreignCollection: 'jobs' ,
			foreignId: jobId ,
			hostPath: 'job' ,
			schema: {
				collection: 'jobs' ,
				optional: true ,
				type: 'link' ,
				opaque: true ,
				tags: [ 'content' ] ,
				sanitize: [ 'toLink' ] ,
				inputHint: "embedded"
			}
		} ) ;

		// Same on saved documents...
		await user.save() ;
		await job.save() ;
		var dbUser = await users.get( userId ) ;
		var dbJob = await jobs.get( jobId ) ;

		expect( dbUser.job._id ).to.equal( jobId ) ;
		expect( user.getLinkDetails( 'job' ) ).to.equal( {
			type: 'link' ,
			anyCollection: false ,
			foreignCollection: 'jobs' ,
			foreignId: jobId ,
			hostPath: 'job' ,
			schema: {
				collection: 'jobs' ,
				optional: true ,
				type: 'link' ,
				opaque: true ,
				tags: [ 'content' ] ,
				sanitize: [ 'toLink' ] ,
				inputHint: "embedded"
			}
		} ) ;
	} ) ;

	it( "should retrieve an active link" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var userId = user.getId() ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var jobId = job.getId() ;

		user.setLink( 'job' , job ) ;
		expect( user ).to.equal( {
			_id: userId ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			job: job
		} ) ;

		expect( user.$ ).to.equal( {
			_id: userId ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			job: { _id: jobId }
		} ) ;

		await user.save() ;
		await job.save() ;
		var dbUser = await users.get( userId ) ;

		expect( dbUser.job._id ).to.equal( jobId ) ;
		await expect( dbUser.getLink( 'job' ) ).to.eventually.equal( {
			_id: jobId ,
			title: "developer" ,
			salary: 60000 ,
			users: [] ,
			schools: []
		} ) ;
	} ) ;

	it( "should retrieve an active deep (nested) link" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		var connectionA = users.createDocument( {
			firstName: 'John' ,
			lastName: 'Fergusson'
		} ) ;

		var connectionB = users.createDocument( {
			firstName: 'Andy' ,
			lastName: 'Fergusson'
		} ) ;

		//console.log( job ) ;
		var connectionAId = connectionA.getId() ;
		var connectionBId = connectionB.getId() ;

		// Link the documents!
		user.setLink( 'connection.A' , connectionA ) ;
		user.setLink( 'connection.B' , connectionB ) ;

		expect( user.$.connection.A ).to.equal( { _id: connectionAId } ) ;
		expect( user.$.connection.B ).to.equal( { _id: connectionBId } ) ;
		expect( user.connection.A ).to.equal( connectionA ) ;
		expect( user.connection.B ).to.equal( connectionB ) ;

		await Promise.all( [ connectionA.save() , connectionB.save() , user.save() ] ) ;

		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.equal( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			connection: {
				A: { _id: connectionAId } ,
				B: { _id: connectionBId }
			} ,
			memberSid: 'Jilbert Polson'
		} ) ;

		await expect( user.getLink( "connection.A" ) ).to.eventually.equal( {
			_id: connectionAId ,
			firstName: 'John' ,
			lastName: "Fergusson" ,
			memberSid: "John Fergusson"
		} ) ;

		await expect( user.getLink( "connection.B" ) ).to.eventually.equal( {
			_id: connectionBId ,
			firstName: 'Andy' ,
			lastName: "Fergusson" ,
			memberSid: "Andy Fergusson"
		} ) ;
	} ) ;

	it( "unexistant links, non-link properties" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		var connectionA = users.createDocument( {
			firstName: 'John' ,
			lastName: 'Fergusson'
		} ) ;

		var connectionB = users.createDocument( {
			firstName: 'Andy' ,
			lastName: 'Fergusson'
		} ) ;

		var connectionAId = connectionA.getId() ;
		var connectionBId = connectionB.getId() ;

		user.setLink( 'connection.A' , connectionA ) ;
		expect( () => user.setLink( 'unexistant' , connectionB ) ).to.throw( ErrorStatus , { type: 'badRequest' } ) ;
		expect( () => user.setLink( 'firstName' , connectionB ) ).to.throw( ErrorStatus , { type: 'badRequest' } ) ;
		expect( () => user.setLink( 'firstName.blah' , connectionB ) ).to.throw( ErrorStatus , { type: 'badRequest' } ) ;

		await Promise.all( [ connectionA.save() , connectionB.save() , user.save() ] ) ;


		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.equal( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			connection: {
				A: { _id: connectionAId }
			} ,
			memberSid: 'Jilbert Polson'
		} ) ;

		await expect( user.getLink( "connection.A" ) ).to.eventually.equal( {
			_id: connectionAId ,
			firstName: 'John' ,
			lastName: "Fergusson" ,
			memberSid: "John Fergusson"
		} ) ;

		await expect( () => user.getLink( "connection.B" ) ).to.reject.with( ErrorStatus , { type: 'notFound' } ) ;
		await expect( () => user.getLink( "unexistant" ) ).to.reject.with( ErrorStatus , { type: 'badRequest' } ) ;
		await expect( () => user.getLink( "unexistant.unexistant" ) ).to.reject.with( ErrorStatus , { type: 'badRequest' } ) ;
		await expect( () => user.getLink( "firstName" ) ).to.reject.with( ErrorStatus , { type: 'badRequest' } ) ;
		await expect( () => user.getLink( "firstName.blah" ) ).to.reject.with( ErrorStatus , { type: 'badRequest' } ) ;
	} ) ;

	it( "direct link assignment" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var userId = user.getId() ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var jobId = job.getId() ;

		// Direct assignment
		user.job = job ;

		expect( user ).to.equal( {
			_id: userId ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			job: job
		} ) ;

		expect( user.$ ).to.equal( {
			_id: userId ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			job: { _id: jobId }
		} ) ;

		// Check stringification
		expect( JSON.stringify( user ) ).to.be( '{"firstName":"Jilbert","lastName":"Polson","_id":"' + userId.toString() + '","memberSid":"Jilbert Polson","job":{"title":"developer","salary":60000,"users":[],"schools":[],"_id":"' + jobId.toString() + '"}}' ) ;
		expect( JSON.stringify( user.$ ) ).to.be( '{"firstName":"Jilbert","lastName":"Polson","_id":"' + userId.toString() + '","memberSid":"Jilbert Polson","job":{"_id":"' + jobId.toString() + '"}}' ) ;

		await job.save() ;
		await user.save() ;
		var dbUser = await users.get( userId ) ;

		await expect( users.get( userId ) ).to.eventually.equal( {
			_id: userId ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			job: { _id: jobId }
		} ) ;
	} ) ;
} ) ;



describe( "Multi-links" , () => {

	beforeEach( clearDB ) ;

	it( "should create, save, retrieve, add and remove multi-links" , async () => {
		var map , batch , dbSchool ;

		var school = schools.createDocument( {
			title: 'Computer Science'
		} ) ;

		var id = school.getId() ;

		var job1 = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var job1Id = job1.getId() ;

		var job2 = jobs.createDocument( {
			title: 'sysadmin' ,
			salary: 55000
		} ) ;

		var job2Id = job2.getId() ;

		var job3 = jobs.createDocument( {
			title: 'front-end developer' ,
			salary: 54000
		} ) ;

		var job3Id = job3.getId() ;

		// First test

		school.setLink( 'jobs' , [ job1 , job2 ] ) ;
		expect( school._.raw.jobs ).to.equal( [ { _id: job1Id } , { _id: job2Id } ] ) ;
		expect( school.$.jobs ).to.equal( [ { _id: job1Id } , { _id: job2Id } ] ) ;
		expect( school.jobs ).to.equal( [ job1 , job2 ] ) ;

		await Promise.all( [ job1.save() , job2.save() , job3.save() , school.save() ] ) ;
		await expect( schools.get( id ) ).to.eventually.equal( { _id: id , title: 'Computer Science' , jobs: [ { _id: job1Id } , { _id: job2Id } ] } ) ;

		batch = await school.getLink( "jobs" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.title ] = doc ; } ) ;

		expect( map ).to.equal( {
			developer: {
				_id: job1Id , title: 'developer' , salary: 60000 , users: [] , schools: []
			} ,
			sysadmin: {
				_id: job2Id , title: 'sysadmin' , salary: 55000 , users: [] , schools: []
			}
		} ) ;

		// Test auto-populate on .getLink()
		dbSchool = await schools.get( id ) ;

		batch = await dbSchool.getLink( "jobs" ) ;
		expect( batch ).to.be.a( rootsDb.Batch ) ;
		expect( dbSchool.$.jobs ).to.equal( [ { _id: job1Id } , { _id: job2Id } ] ) ;
		expect( dbSchool.jobs ).to.equal( [ job1 , job2 ] ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.title ] = doc ; } ) ;

		expect( map ).to.equal( {
			developer: {
				_id: job1Id , title: 'developer' , salary: 60000 , users: [] , schools: []
			} ,
			sysadmin: {
				_id: job2Id , title: 'sysadmin' , salary: 55000 , users: [] , schools: []
			}
		} ) ;

		// Second test

		school.addLink( 'jobs' , job3 ) ;
		expect( school._.raw.jobs ).to.equal( [ { _id: job1Id } , { _id: job2Id } , { _id: job3Id } ] ) ;
		expect( school.$.jobs ).to.equal( [ { _id: job1Id } , { _id: job2Id } , { _id: job3Id } ] ) ;
		expect( school.jobs ).to.equal( [ job1 , job2 , job3 ] ) ;

		await school.save() ;

		batch = await school.getLink( "jobs" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.title ] = doc ; } ) ;

		expect( map ).to.equal( {
			developer: {
				_id: job1Id , title: 'developer' , salary: 60000 , users: [] , schools: []
			} ,
			sysadmin: {
				_id: job2Id , title: 'sysadmin' , salary: 55000 , users: [] , schools: []
			} ,
			"front-end developer": {
				_id: job3Id , title: 'front-end developer' , salary: 54000 , users: [] , schools: []
			}
		} ) ;

		// Third test

		school.removeLink( 'jobs' , job2 ) ;
		expect( school.$.jobs ).to.equal( [ { _id: job1Id } , { _id: job3Id } ] ) ;
		expect( school.jobs ).to.equal( [ job1 , job3 ] ) ;

		await school.save() ;

		batch = await school.getLink( "jobs" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.title ] = doc ; } ) ;

		expect( map ).to.equal( {
			developer: {
				_id: job1Id , title: 'developer' , salary: 60000 , users: [] , schools: []
			} ,
			"front-end developer": {
				_id: job3Id , title: 'front-end developer' , salary: 54000 , users: [] , schools: []
			}
		} ) ;
	} ) ;

	it( "should create, save, retrieve, add and remove deep (nested) multi-links" , async () => {
		var map , batch , dbRootDoc ;

		var rootDoc = nestedLinks.createDocument( { name: 'root' } ) ;
		var id = rootDoc.getId() ;

		var childDoc1 = nestedLinks.createDocument( { name: 'child1' } ) ;
		var childDoc2 = nestedLinks.createDocument( { name: 'child2' } ) ;
		var childDoc3 = nestedLinks.createDocument( { name: 'child3' } ) ;

		// First test

		rootDoc.setLink( 'nested.multiLink' , [ childDoc1 , childDoc2 ] ) ;
		expect( rootDoc.$.nested.multiLink ).to.equal( [ { _id: childDoc1.getId() } , { _id: childDoc2.getId() } ] ) ;
		expect( rootDoc.nested.multiLink ).to.equal( [ childDoc1 , childDoc2 ] ) ;

		await Promise.all( [ rootDoc.save() , childDoc1.save() , childDoc2.save() , childDoc3.save() ] ) ;
		await expect( nestedLinks.get( id ) ).to.eventually.equal( {
			_id: id ,
			name: 'root' ,
			nested: {
				backLinkOfLink: [] ,
				backLinkOfMultiLink: [] ,
				multiLink: [ { _id: childDoc1.getId() } , { _id: childDoc2.getId() } ]
			}
		} ) ;

		batch = await rootDoc.getLink( "nested.multiLink" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.name ] = doc ; } ) ;

		expect( map ).to.equal( {
			child1: { _id: childDoc1.getId() , name: "child1" , nested: {} } ,
			child2: { _id: childDoc2.getId() , name: "child2" , nested: {} }
		} ) ;

		// Test auto-populate on .getLink()
		dbRootDoc = await nestedLinks.get( id ) ;

		batch = await dbRootDoc.getLink( "nested.multiLink" ) ;
		expect( dbRootDoc.$.nested.multiLink ).to.equal( [ { _id: childDoc1.getId() } , { _id: childDoc2.getId() } ] ) ;
		expect( dbRootDoc.nested.multiLink ).to.equal( [ childDoc1 , childDoc2 ] ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.name ] = doc ; } ) ;

		expect( map ).to.equal( {
			child1: { _id: childDoc1.getId() , name: "child1" , nested: {} } ,
			child2: { _id: childDoc2.getId() , name: "child2" , nested: {} }
		} ) ;

		// Second test

		rootDoc.addLink( 'nested.multiLink' , childDoc3 ) ;
		expect( rootDoc.$.nested.multiLink ).to.equal( [ { _id: childDoc1.getId() } , { _id: childDoc2.getId() } , { _id: childDoc3.getId() } ] ) ;
		expect( rootDoc.nested.multiLink ).to.equal( [ childDoc1 , childDoc2 , childDoc3 ] ) ;
		await rootDoc.save() ;

		batch = await rootDoc.getLink( "nested.multiLink" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.name ] = doc ; } ) ;

		expect( map ).to.equal( {
			child1: { _id: childDoc1.getId() , name: "child1" , nested: {} } ,
			child2: { _id: childDoc2.getId() , name: "child2" , nested: {} } ,
			child3: { _id: childDoc3.getId() , name: "child3" , nested: {} }
		} ) ;

		// Third test

		rootDoc.removeLink( 'nested.multiLink' , childDoc2 ) ;
		expect( rootDoc.$.nested.multiLink ).to.equal( [ { _id: childDoc1.getId() } , { _id: childDoc3.getId() } ] ) ;
		expect( rootDoc.nested.multiLink ).to.equal( [ childDoc1 , childDoc3 ] ) ;
		await rootDoc.save() ;

		batch = await rootDoc.getLink( "nested.multiLink" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.name ] = doc ; } ) ;

		expect( map ).to.equal( {
			child1: { _id: childDoc1.getId() , name: "child1" , nested: {} } ,
			child3: { _id: childDoc3.getId() , name: "child3" , nested: {} }
		} ) ;
	} ) ;

	it( "should enforce link uniqness" , async () => {
		var map , batch , dbSchool ;

		var school = schools.createDocument( {
			title: 'Computer Science'
		} ) ;

		var id = school.getId() ;

		var job1 = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var job1Id = job1.getId() ;

		var job2 = jobs.createDocument( {
			title: 'sysadmin' ,
			salary: 55000
		} ) ;

		var job2Id = job2.getId() ;

		var job3 = jobs.createDocument( {
			title: 'front-end developer' ,
			salary: 54000
		} ) ;

		var job3Id = job3.getId() ;

		// .setLink() and uniqness
		school.setLink( 'jobs' , [ job1 , job2 , job1 ] ) ;
		expect( school.jobs ).to.equal( [ job1 , job2 ] ) ;

		// .addLink() and uniqness
		school.addLink( 'jobs' , job2 ) ;
		expect( school.jobs ).to.equal( [ job1 , job2 ] ) ;

		// direct access do not enforce uniqness until validation
		school.jobs = [ job1 , job2 , job1 ] ;
		expect( school.jobs ).to.equal( [ job1 , job2 , job1 ] ) ;

		await Promise.all( [ job1.save() , job2.save() , job3.save() , school.save() ] ) ;

		// Duplicated links should be removed now
		await expect( schools.get( id ) ).to.eventually.equal( { _id: id , title: 'Computer Science' , jobs: [ { _id: job1Id } , { _id: job2Id } ] } ) ;

		batch = await school.getLink( "jobs" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.title ] = doc ; } ) ;

		expect( map ).to.equal( {
			developer: {
				_id: job1Id , title: 'developer' , salary: 60000 , users: [] , schools: []
			} ,
			sysadmin: {
				_id: job2Id , title: 'sysadmin' , salary: 55000 , users: [] , schools: []
			}
		} ) ;
	} ) ;
} ) ;



describe( "Back-links" , () => {

	beforeEach( clearDB ) ;

	it( "back-link of single link" , async () => {
		var map , batch ;

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		var user2 = users.createDocument( {
			firstName: 'Tony' ,
			lastName: 'P.'
		} ) ;

		var id2 = user2.getId() ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		//console.log( job ) ;
		var jobId = job.getId() ;

		// Link the documents!
		user.setLink( 'job' , job ) ;

		await Promise.all( [ user.save() , user2.save() , job.save() ] ) ;

		var dbJob = await jobs.get( jobId ) ;
		expect( dbJob ).to.equal( {
			_id: jobId , title: 'developer' , salary: 60000 , users: [] , schools: []
		} ) ;

		expect( dbJob.getLinkDetails( "users" ) ).to.equal( {
			type: 'backLink' ,
			foreignCollection: 'users' ,
			foreignAnyCollection: false ,
			hostPath: 'users' ,
			foreignPath: 'job' ,
			schema: {
				collection: 'users' ,
				//optional: true ,
				type: 'backLink' ,
				opaque: true ,
				default: [] ,
				tags: [ 'content' ] ,
				sanitize: [ 'toBackLink' ] ,
				path: 'job' ,
				inputHint: "embedded"
			}
		} ) ;

		batch = await job.getLink( "users" ) ;
		expect( batch ).to.be.like( [
			{
				_id: id ,
				firstName: 'Jilbert' ,
				lastName: 'Polson' ,
				memberSid: 'Jilbert Polson' ,
				job: { _id: jobId }
			}
		] ) ;

		expect( job ).to.be.like( {
			_id: jobId ,
			title: 'developer' ,
			salary: 60000 ,
			users: [
				{
					_id: id ,
					firstName: 'Jilbert' ,
					lastName: 'Polson' ,
					memberSid: 'Jilbert Polson' ,
					job: { _id: jobId }
				}
			] ,
			schools: []
		} ) ;

		expect( job.$ ).to.be.like( {
			_id: jobId ,
			title: 'developer' ,
			salary: 60000 ,
			users: [] ,
			schools: []
		} ) ;

		expect( job.$.users ).to.equal( [] ) ;

		user2.setLink( 'job' , job ) ;
		await user2.save() ;

		batch = await job.getLink( "users" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.firstName ] = doc ; } ) ;

		expect( map ).to.equal( {
			Jilbert: {
				_id: id , firstName: 'Jilbert' , lastName: 'Polson' , memberSid: 'Jilbert Polson' , job: { _id: jobId }
			} ,
			Tony: {
				_id: id2 , firstName: 'Tony' , lastName: 'P.' , memberSid: 'Tony P.' , job: { _id: jobId }
			}
		} ) ;

		// Sort it first
		job.users.sort( ascendingSortFn( 'firstName' ) ) ;

		expect( job ).to.be.like( {
			_id: jobId ,
			title: 'developer' ,
			salary: 60000 ,
			users: [
				{
					_id: id ,
					firstName: 'Jilbert' ,
					lastName: 'Polson' ,
					memberSid: 'Jilbert Polson' ,
					job: { _id: jobId }
				} ,
				{
					_id: id2 ,
					firstName: 'Tony' ,
					lastName: 'P.' ,
					memberSid: 'Tony P.' ,
					job: { _id: jobId }
				}
			] ,
			schools: []
		} ) ;

		expect( job.$ ).to.be.like( {
			_id: jobId ,
			title: 'developer' ,
			salary: 60000 ,
			users: [] ,
			schools: []
		} ) ;
	} ) ;

	it( "back-link of multi-link" , async () => {
		var map , batch ;

		var school1 = schools.createDocument( {
			title: 'Computer Science'
		} ) ;

		var school1Id = school1.getId() ;

		var school2 = schools.createDocument( {
			title: 'Web Academy'
		} ) ;

		var school2Id = school2.getId() ;

		var job1 = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var job1Id = job1.getId() ;

		var job2 = jobs.createDocument( {
			title: 'sysadmin' ,
			salary: 55000
		} ) ;

		var job2Id = job2.getId() ;

		var job3 = jobs.createDocument( {
			title: 'front-end developer' ,
			salary: 54000
		} ) ;

		var job3Id = job3.getId() ;

		var job4 = jobs.createDocument( {
			title: 'designer' ,
			salary: 56000
		} ) ;

		var job4Id = job4.getId() ;

		// Link the documents!
		school1.setLink( 'jobs' , [ job1 , job2 , job3 ] ) ;
		school2.setLink( 'jobs' , [ job1 , job3 , job4 ] ) ;

		await Promise.all( [ job1.save() , job2.save() , job3.save() , job4.save() , school1.save() , school2.save() ] ) ;

		var dbJob = await jobs.get( job1Id ) ;
		expect( dbJob ).to.equal( {
			_id: job1Id , title: 'developer' , salary: 60000 , users: [] , schools: []
		} ) ;

		batch = await dbJob.getLink( 'schools' ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.title ] = doc ; } ) ;

		expect( map ).to.equal( {
			'Computer Science': { _id: school1Id , title: 'Computer Science' , jobs: [ { _id: job1Id } , { _id: job2Id } , { _id: job3Id } ] } ,
			'Web Academy': { _id: school2Id , title: 'Web Academy' , jobs: [ { _id: job1Id } , { _id: job3Id } , { _id: job4Id } ] }
		} ) ;

		// Sort it first
		dbJob.schools.sort( ascendingSortFn( 'title' ) ) ;

		expect( dbJob ).to.be.like( {
			_id: job1Id ,
			title: 'developer' ,
			salary: 60000 ,
			users: [] ,
			schools: [
				{ _id: school1Id , title: 'Computer Science' , jobs: [ { _id: job1Id } , { _id: job2Id } , { _id: job3Id } ] } ,
				{ _id: school2Id , title: 'Web Academy' , jobs: [ { _id: job1Id } , { _id: job3Id } , { _id: job4Id } ] }
			]
		} ) ;

		expect( dbJob.$ ).to.be.like( {
			_id: job1Id ,
			title: 'developer' ,
			salary: 60000 ,
			users: [] ,
			schools: []
		} ) ;

		dbJob = await jobs.get( job4Id ) ;
		expect( dbJob ).to.equal( {
			_id: job4Id , title: 'designer' , salary: 56000 , users: [] , schools: []
		} ) ;

		batch = await dbJob.getLink( 'schools' ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.title ] = doc ; } ) ;

		expect( batch ).to.be.like( [
			{ _id: school2Id , title: 'Web Academy' , jobs: [ { _id: job1Id } , { _id: job3Id } , { _id: job4Id } ] }
		] ) ;
	} ) ;

	it( "deep (nested) back-link of single link" , async () => {
		var map , batch ;

		var rootDoc = nestedLinks.createDocument( { name: 'root' } ) ;
		var id = rootDoc.getId() ;

		expect( rootDoc.getLinkDetails( "nested.backLinkOfLink" ) ).to.equal( {
			type: 'backLink' ,
			foreignCollection: 'nestedLinks' ,
			foreignAnyCollection: false ,
			hostPath: 'nested.backLinkOfLink' ,
			foreignPath: 'nested.link' ,
			schema: {
				collection: 'nestedLinks' ,
				type: 'backLink' ,
				opaque: true ,
				default: [] ,
				sanitize: [ 'toBackLink' ] ,
				tags: [ 'content' ] ,
				path: 'nested.link' ,
				inputHint: "embedded"
			}
		} ) ;

		var childDoc1 = nestedLinks.createDocument( { name: 'child1' } ) ;
		var childDoc2 = nestedLinks.createDocument( { name: 'child2' } ) ;
		var childDoc3 = nestedLinks.createDocument( { name: 'child3' } ) ;

		// First test

		childDoc1.setLink( 'nested.link' , rootDoc ) ;
		childDoc2.setLink( 'nested.link' , rootDoc ) ;

		await Promise.all( [ rootDoc.save() , childDoc1.save() , childDoc2.save() , childDoc3.save() ] ) ;
		batch = await rootDoc.getLink( "nested.backLinkOfLink" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.name ] = doc ; } ) ;

		expect( map ).to.equal( {
			child1: { _id: childDoc1.getId() ,
				name: "child1" ,
				nested: {
					backLinkOfLink: [] , backLinkOfMultiLink: [] , link: { _id: id } , multiLink: []
				} } ,
			child2: { _id: childDoc2.getId() ,
				name: "child2" ,
				nested: {
					backLinkOfLink: [] , backLinkOfMultiLink: [] , link: { _id: id } , multiLink: []
				} }
		} ) ;

		// Second test

		childDoc3.setLink( 'nested.link' , rootDoc ) ;
		await childDoc3.save() ;
		batch = await rootDoc.getLink( "nested.backLinkOfLink" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.name ] = doc ; } ) ;

		expect( map ).to.equal( {
			child1: { _id: childDoc1.getId() ,
				name: "child1" ,
				nested: {
					backLinkOfLink: [] , backLinkOfMultiLink: [] , link: { _id: id } , multiLink: []
				} } ,
			child2: { _id: childDoc2.getId() ,
				name: "child2" ,
				nested: {
					backLinkOfLink: [] , backLinkOfMultiLink: [] , link: { _id: id } , multiLink: []
				} } ,
			child3: { _id: childDoc3.getId() ,
				name: "child3" ,
				nested: {
					backLinkOfLink: [] , backLinkOfMultiLink: [] , link: { _id: id } , multiLink: []
				} }
		} ) ;

		// Third test

		childDoc2.removeLink( 'nested.link' ) ;
		await childDoc2.save() ;
		batch = await rootDoc.getLink( "nested.backLinkOfLink" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.name ] = doc ; } ) ;

		expect( map ).to.equal( {
			child1: { _id: childDoc1.getId() ,
				name: "child1" ,
				nested: {
					backLinkOfLink: [] , backLinkOfMultiLink: [] , link: { _id: id } , multiLink: []
				} } ,
			child3: { _id: childDoc3.getId() ,
				name: "child3" ,
				nested: {
					backLinkOfLink: [] , backLinkOfMultiLink: [] , link: { _id: id } , multiLink: []
				} }
		} ) ;

		expect( rootDoc ).to.be.like( {
			_id: id ,
			name: "root" ,
			nested: {
				backLinkOfLink: [
					{ _id: childDoc1.getId() ,
						name: "child1" ,
						nested: {
							backLinkOfLink: [] , backLinkOfMultiLink: [] , link: { _id: id } , multiLink: []
						} } ,
					{ _id: childDoc3.getId() ,
						name: "child3" ,
						nested: {
							backLinkOfLink: [] , backLinkOfMultiLink: [] , link: { _id: id } , multiLink: []
						} }
				]
			}
		} ) ;

		expect( rootDoc.$ ).to.be.like( {
			_id: id ,
			name: "root" ,
			nested: { backLinkOfLink: [] }
		} ) ;
	} ) ;

	// This test is not fully written
	it( "deep (nested) back-link of multi-link" , async () => {
		var map , batch ;

		var rootDoc = nestedLinks.createDocument( { name: 'root' } ) ;
		var id = rootDoc.getId() ;

		expect( rootDoc.getLinkDetails( "nested.backLinkOfMultiLink" ) ).to.equal( {
			type: 'backLink' ,
			foreignCollection: 'nestedLinks' ,
			foreignAnyCollection: false ,
			hostPath: 'nested.backLinkOfMultiLink' ,
			foreignPath: 'nested.multiLink' ,
			schema: {
				collection: 'nestedLinks' ,
				type: 'backLink' ,
				opaque: true ,
				default: [] ,
				tags: [ 'content' ] ,
				sanitize: [ 'toBackLink' ] ,
				path: 'nested.multiLink' ,
				inputHint: "embedded"
			}
		} ) ;

		var otherDoc1 = nestedLinks.createDocument( { name: 'otherDoc1' } ) ;
		var otherDoc2 = nestedLinks.createDocument( { name: 'otherDoc2' } ) ;

		var childDoc1 = nestedLinks.createDocument( { name: 'child1' } ) ;
		var childDoc2 = nestedLinks.createDocument( { name: 'child2' } ) ;
		var childDoc3 = nestedLinks.createDocument( { name: 'child3' } ) ;
		var childDoc4 = nestedLinks.createDocument( { name: 'child4' } ) ;

		// First test

		childDoc1.setLink( 'nested.multiLink' , [ rootDoc ] ) ;
		childDoc2.setLink( 'nested.multiLink' , [ rootDoc , otherDoc1 , otherDoc2 ] ) ;
		childDoc3.setLink( 'nested.multiLink' , [ otherDoc1 , otherDoc2 ] ) ;

		await Promise.all( [ rootDoc.save() , otherDoc1.save() , otherDoc2.save() , childDoc1.save() , childDoc2.save() , childDoc3.save() , childDoc4.save() ] ) ;
		batch = await rootDoc.getLink( "nested.backLinkOfMultiLink" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.name ] = doc ; } ) ;

		expect( map ).to.equal( {
			child1: { _id: childDoc1.getId() , name: "child1" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , multiLink: [ { _id: rootDoc.getId() } ] } } ,
			child2: { _id: childDoc2.getId() , name: "child2" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , multiLink: [ { _id: rootDoc.getId() } , { _id: otherDoc1.getId() } , { _id: otherDoc2.getId() } ] } }
		} ) ;

		// Second test

		childDoc3.addLink( 'nested.multiLink' , rootDoc ) ;
		await childDoc3.save() ;
		batch = await rootDoc.getLink( "nested.backLinkOfMultiLink" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.name ] = doc ; } ) ;

		expect( map ).to.equal( {
			child1: { _id: childDoc1.getId() , name: "child1" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , multiLink: [ { _id: rootDoc.getId() } ] } } ,
			child2: { _id: childDoc2.getId() , name: "child2" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , multiLink: [ { _id: rootDoc.getId() } , { _id: otherDoc1.getId() } , { _id: otherDoc2.getId() } ] } } ,
			child3: { _id: childDoc3.getId() , name: "child3" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , multiLink: [ { _id: otherDoc1.getId() } , { _id: otherDoc2.getId() } , { _id: rootDoc.getId() } ] } }
		} ) ;

		// Third test

		childDoc2.removeLink( 'nested.multiLink' , rootDoc ) ;
		await childDoc2.save() ;
		batch = await rootDoc.getLink( "nested.backLinkOfMultiLink" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.name ] = doc ; } ) ;

		expect( map ).to.equal( {
			child1: { _id: childDoc1.getId() , name: "child1" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , multiLink: [ { _id: rootDoc.getId() } ] } } ,
			child3: { _id: childDoc3.getId() , name: "child3" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , multiLink: [ { _id: otherDoc1.getId() } , { _id: otherDoc2.getId() } , { _id: rootDoc.getId() } ] } }
		} ) ;

		expect( rootDoc ).to.be.like( {
			_id: id ,
			name: "root" ,
			nested: {
				backLinkOfMultiLink: [
					{ _id: childDoc1.getId() , name: "child1" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , multiLink: [ { _id: rootDoc.getId() } ] } } ,
					{ _id: childDoc3.getId() , name: "child3" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , multiLink: [ { _id: otherDoc1.getId() } , { _id: otherDoc2.getId() } , { _id: rootDoc.getId() } ] } }
				]
			}
		} ) ;

		expect( rootDoc.$ ).to.be.like( {
			_id: id ,
			name: "root" ,
			nested: { backLinkOfMultiLink: [] }
		} ) ;
	} ) ;
} ) ;



describe( "Any-collection links" , () => {

	beforeEach( clearDB ) ;

	it( "should retrieve/populate an any-collection link" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var userId = user.getId() ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var jobId = job.getId() ;

		var doc = anyCollectionLinks.createDocument( {
			name: 'docname'
		} ) ;

		var docId = doc.getId() ;

		doc.setLink( 'link' , user ) ;

		expect( doc ).to.equal( {
			_id: docId ,
			name: 'docname' ,
			link: user ,
			backLink: []
		} ) ;

		expect( doc.$ ).to.equal( {
			_id: docId ,
			name: 'docname' ,
			link: { _id: userId , _collection: 'users' } ,
			backLink: []
		} ) ;

		await user.save() ;
		await job.save() ;
		await doc.save() ;

		var dbDoc = await anyCollectionLinks.get( docId ) ;

		expect( dbDoc.link._id ).to.equal( userId ) ;
		expect( dbDoc.link._collection ).to.be( 'users' ) ;
		await expect( dbDoc.getLink( 'link' ) ).to.eventually.equal( {
			_id: userId ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson'
		} ) ;

		doc.setLink( 'link' , job ) ;

		expect( doc ).to.equal( {
			_id: docId ,
			name: 'docname' ,
			link: job ,
			backLink: []
		} ) ;

		expect( doc.$ ).to.equal( {
			_id: docId ,
			name: 'docname' ,
			link: { _id: jobId , _collection: 'jobs' } ,
			backLink: []
		} ) ;

		await doc.save() ;
		dbDoc = await anyCollectionLinks.get( docId ) ;

		expect( dbDoc.link._id ).to.equal( jobId ) ;
		expect( dbDoc.link._collection ).to.be( 'jobs' ) ;
		await expect( dbDoc.getLink( 'link' ) ).to.eventually.equal( {
			_id: jobId ,
			title: 'developer' ,
			salary: 60000 ,
			schools: [] ,
			users: []
		} ) ;

		dbDoc = await anyCollectionLinks.get( docId , { populate: 'link' } ) ;

		expect( dbDoc ).to.equal( {
			_id: docId ,
			name: 'docname' ,
			link: {
				_id: jobId ,
				title: 'developer' ,
				salary: 60000 ,
				schools: [] ,
				users: []
			} ,
			backLink: []
		} ) ;
	} ) ;

	it( "should retrieve/populate back-link from any-collection links" , async () => {
		var masterDoc = anyCollectionLinks.createDocument( { name: 'masterDoc' } ) ;
		var masterDocId = masterDoc.getId() ;

		var doc1 = anyCollectionLinks.createDocument( { name: 'doc1' } ) ;
		var doc1Id = doc1.getId() ;

		var doc2 = anyCollectionLinks.createDocument( { name: 'doc2' } ) ;
		var doc2Id = doc2.getId() ;

		var doc3 = anyCollectionLinks.createDocument( { name: 'doc3' } ) ;
		var doc3Id = doc3.getId() ;

		doc1.setLink( 'link' , masterDoc ) ;
		doc2.setLink( 'link' , masterDoc ) ;
		doc3.setLink( 'link' , masterDoc ) ;

		await masterDoc.save() ;
		await doc1.save() ;
		await doc2.save() ;
		await doc3.save() ;

		var dbMasterDoc = await anyCollectionLinks.get( masterDocId ) ;

		await expect( dbMasterDoc.getLink( 'backLink' ) ).to.eventually.be.partially.like( [
			{ name: "doc1" } ,
			{ name: "doc2" } ,
			{ name: "doc3" }
		] ) ;

		// We create a user, we force re-using the same ID to try to mess up with the back-link foreign-collection filtering
		var user = users.createDocument( {
			_id: masterDocId ,
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var userId = user.getId() ;
		expect( userId ).to.be( masterDocId ) ;

		doc3.setLink( 'link' , user ) ;

		await user.save() ;
		await doc3.save() ;

		await expect( users.get( userId ) ).to.eventually.be.partially.like( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		dbMasterDoc = await anyCollectionLinks.get( masterDocId ) ;

		await expect( dbMasterDoc.getLink( 'backLink' ) ).to.eventually.be.partially.like( [
			{ name: "doc1" } ,
			{ name: "doc2" }
		] ) ;

		dbMasterDoc = await anyCollectionLinks.get( masterDocId , { populate: 'backLink' } ) ;

		expect( dbMasterDoc ).to.be.partially.like( {
			_id: masterDocId ,
			name: 'masterDoc' ,
			backLink: [
				{ name: "doc1" } ,
				{ name: "doc2" }
			]
		} ) ;
	} ) ;
} ) ;



describe( "Attachment links (driver: " + ATTACHMENT_MODE + ")" , () => {

	beforeEach( clearDB ) ;
	beforeEach( () => {
		users.attachmentDriver.appendExtension = users.attachmentAppendExtension ;
	} ) ;

	it( "should create, save, and load an attachment" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		//var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		//user.setAttachment( 'file' , attachment ) ;
		var attachment = user.setAttachment( 'file' , { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		//log.error( user.file ) ;

		// Raw DB data
		expect( user.$.file ).not.to.be.a( rootsDb.Attachment ) ;
		expect( user.$.file ).to.equal( {
			filename: 'joke.txt' ,
			extension: 'txt' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'text/plain' ,
			fileSize: 24 ,
			hash: null ,
			hashType: null ,
			metadata: {}
		} ) ;

		//console.error( "\n\n>>> Unit attachment >>>" , user.file , '\n' ) ;
		expect( user.file ).to.be.a( rootsDb.Attachment ) ;
		expect( user.file ).to.be.partially.like( {
			filename: 'joke.txt' ,
			extension: 'txt' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'text/plain' ,
			fileSize: 24 ,
			hash: null ,
			hashType: null ,
			metadata: {}
		} ) ;

		await user.save() ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.be.partially.like( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'joke.txt' ,
				extension: 'txt' ,
				id: user.file.id ,	// Unpredictable
				contentType: 'text/plain' ,
				fileSize: 24 ,
				hash: null ,
				hashType: null ,
				metadata: {}
			}
		} ) ;

		var details = dbUser.getAttachmentDetails( 'file' ) ;
		expect( details ).to.be.partially.like( {
			type: 'attachment' ,
			hostPath: 'file' ,
			schema: {
				optional: true ,
				type: 'attachment' ,
				tags: [ 'content' ] ,
				inputHint: "file"
			} ,
			attachment: {
				id: dbUser.file.id ,
				filename: 'joke.txt' ,
				extension: 'txt' ,
				contentType: 'text/plain' ,
				fileSize: 24 ,
				hash: null ,
				hashType: null ,
				metadata: {} ,
				collectionName: 'users' ,
				documentId: id.toString() ,
				driver: users.attachmentDriver ,
				path: ( ATTACHMENT_MODE === 'file' ? USERS_ATTACHMENT_DIR : '' ) + dbUser.getId() + '/' + details.attachment.id ,
				publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + details.attachment.id
			}
		} ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.partially.like( {
			id: user.file.id ,
			filename: 'joke.txt' ,
			extension: 'txt' ,
			contentType: 'text/plain' ,
			fileSize: 24 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? USERS_ATTACHMENT_DIR : '' ) + dbUser.getId() + '/' + details.attachment.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + details.attachment.id
		} ) ;

		expect( path.extname( dbAttachment.id ) ).not.to.be( '.txt' ) ;
		expect( path.extname( dbAttachment.path ) ).not.to.be( '.txt' ) ;
		expect( path.extname( dbAttachment.publicUrl ) ).not.to.be( '.txt' ) ;

		var content = await dbAttachment.load() ;
		expect( content.toString() ).to.be( "grigrigredin menufretin\n" ) ;
	} ) ;

	it( "with the 'attachmentAppendExtension' option, it should be stored with the extension" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag
		// We force append extension here
		users.attachmentDriver.appendExtension = true ;

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		//var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		//user.setAttachment( 'file' , attachment ) ;
		var attachment = user.setAttachment( 'file' , { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		//log.error( user.file ) ;

		// Raw DB data
		expect( user.$.file ).not.to.be.a( rootsDb.Attachment ) ;
		expect( user.$.file ).to.equal( {
			filename: 'joke.txt' ,
			extension: 'txt' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'text/plain' ,
			fileSize: 24 ,
			hash: null ,
			hashType: null ,
			metadata: {}
		} ) ;

		await user.save() ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).not.to.throw() ;
		}
		
		expect( path.extname( attachment.path ) ).to.be( '.txt' ) ;

		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.be.partially.like( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'joke.txt' ,
				extension: 'txt' ,
				id: user.file.id ,	// Unpredictable
				contentType: 'text/plain' ,
				fileSize: 24 ,
				hash: null ,
				hashType: null ,
				metadata: {}
			}
		} ) ;

		expect( path.extname( dbUser.file.id ) ).to.be( '.txt' ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.partially.like( {
			id: user.file.id ,
			filename: 'joke.txt' ,
			extension: 'txt' ,
			contentType: 'text/plain' ,
			fileSize: 24 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? USERS_ATTACHMENT_DIR : '' ) + dbUser.getId() + '/' + user.file.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + user.file.id
		} ) ;

		expect( path.extname( dbAttachment.id ) ).to.be( '.txt' ) ;
		expect( path.extname( dbAttachment.path ) ).to.be( '.txt' ) ;
		expect( path.extname( dbAttachment.publicUrl ) ).to.be( '.txt' ) ;

		var content = await dbAttachment.load() ;
		expect( content.toString() ).to.be( "grigrigredin menufretin\n" ) ;
	} ) ;

	it( "should use metadata of an attachment and alter it" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		//var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		//user.setAttachment( 'file' , attachment ) ;
		var attachment = user.setAttachment( 'file' , { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;

		//await attachment.save() ;
		await user.save() ;

		var dbUser = await users.get( id ) ;
		
		// There was a bug with proxy/restoreAttachment, so we check staging right now
		expect( dbUser._.localChanges ).to.equal( null ) ;
		dbUser.file ;
		expect( dbUser._.localChanges ).to.equal( null ) ;
		
		dbUser.file.set( { filename: 'lol.txt' , contentType: 'text/joke' , metadata: { width: 100 } } ) ;
		// Check if staging is correct
		expect( dbUser._.localChanges ).to.equal( { file: { filename: null , extension: null , contentType: null , metadata: { width: null } } } ) ;
		await dbUser.save() ;

		dbUser = await users.get( id ) ;

		expect( dbUser ).to.be.partially.like( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'lol.txt' ,
				extension: 'txt' ,
				id: user.file.id ,	// Unpredictable
				contentType: 'text/joke' ,
				fileSize: 24 ,
				hash: null ,
				hashType: null ,
				metadata: { width: 100 }
			}
		} ) ;

		dbUser = await users.get( id ) ;
		dbUser.file.set( { metadata: { height: 120 } } ) ;
		// Check if staging is correct
		expect( dbUser._.localChanges ).to.equal( { file: { metadata: { height: null } } } ) ;
		await dbUser.save() ;

		dbUser = await users.get( id ) ;

		expect( dbUser ).to.be.partially.like( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'lol.txt' ,
				extension: 'txt' ,
				id: user.file.id ,	// Unpredictable
				contentType: 'text/joke' ,
				fileSize: 24 ,
				hash: null ,
				hashType: null ,
				metadata: { width: 100 , height: 120 }
			}
		} ) ;

		var details = dbUser.getAttachmentDetails( 'file' ) ;
		expect( details ).to.be.partially.like( {
			type: 'attachment' ,
			hostPath: 'file' ,
			schema: {
				optional: true ,
				type: 'attachment' ,
				tags: [ 'content' ] ,
				inputHint: "file"
			} ,
			attachment: {
				id: dbUser.file.id ,
				filename: 'lol.txt' ,
				extension: 'txt' ,
				contentType: 'text/joke' ,
				fileSize: 24 ,
				hash: null ,
				hashType: null ,
				metadata: { width: 100 , height: 120 } ,
				collectionName: 'users' ,
				documentId: id.toString() ,
				driver: users.attachmentDriver ,
				path: ( ATTACHMENT_MODE === 'file' ? USERS_ATTACHMENT_DIR : '' ) + dbUser.getId() + '/' + details.attachment.id ,
				publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + details.attachment.id
			}
		} ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.partially.like( {
			id: dbUser.file.id ,
			filename: 'lol.txt' ,
			extension: 'txt' ,
			contentType: 'text/joke' ,
			fileSize: 24 ,
			hash: null ,
			hashType: null ,
			metadata: { width: 100 , height: 120 } ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? USERS_ATTACHMENT_DIR : '' ) + dbUser.getId() + '/' + details.attachment.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + details.attachment.id
		} ) ;

		expect( path.extname( dbAttachment.id ) ).not.to.be( '.txt' ) ;
		expect( path.extname( dbAttachment.path ) ).not.to.be( '.txt' ) ;
		expect( path.extname( dbAttachment.publicUrl ) ).not.to.be( '.txt' ) ;

		var content = await dbAttachment.load() ;
		expect( content.toString() ).to.be( "grigrigredin menufretin\n" ) ;
	} ) ;

	it( "should replace an attachment" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		//var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		//user.setAttachment( 'file' , attachment ) ;
		var attachment = user.setAttachment( 'file' , { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;

		//await attachment.save() ;
		await user.save() ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		var dbUser = await users.get( id ) ;

		await expect( dbUser.getAttachment( 'file' ).load().then( v => v.toString() ) ).to.eventually.be( "grigrigredin menufretin\n" ) ;

		/*
		var attachment2 = user.createAttachment(
			{ filename: 'hello-world.html' , contentType: 'text/html' } ,
			"<html><head></head><body>Hello world!</body></html>\n"
		) ;
		dbUser.setAttachment( 'file' , attachment2 ) ;
		*/

		var attachment2 = dbUser.setAttachment(
			'file' ,
			{ filename: 'hello-world.html' , contentType: 'text/html' } ,
			"<html><head></head><body>Hello world!</body></html>\n"
		) ;

		// Check that the previous file has NOT been deleted YET
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		//await attachment2.save() ;
		await dbUser.save() ;

		// Check that the previous file has been deleted -- Should be done AFTER .save()
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).to.throw( Error , { code: 'ENOENT' } ) ;
		}

		dbUser = await users.get( id ) ;

		expect( dbUser ).to.be.partially.like( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'hello-world.html' ,
				extension: 'html' ,
				id: dbUser.file.id ,	// Unpredictable
				contentType: 'text/html' ,
				fileSize: 52 ,
				hash: null ,
				hashType: null ,
				metadata: {}
			}
		} ) ;

		var details = dbUser.getAttachmentDetails( 'file' ) ;
		expect( details ).to.be.partially.like( {
			type: 'attachment' ,
			hostPath: 'file' ,
			schema: {
				optional: true ,
				tags: [ 'content' ] ,
				type: 'attachment' ,
				inputHint: "file"
			} ,
			attachment: {
				id: dbUser.file.id ,
				filename: 'hello-world.html' ,
				extension: 'html' ,
				contentType: 'text/html' ,
				fileSize: 52 ,
				hash: null ,
				hashType: null ,
				metadata: {} ,
				collectionName: 'users' ,
				documentId: id.toString() ,
				driver: users.attachmentDriver ,
				path: ( ATTACHMENT_MODE === 'file' ? USERS_ATTACHMENT_DIR : '' ) + dbUser.getId() + '/' + details.attachment.id ,
				publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + details.attachment.id
			}
		} ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.a( rootsDb.Attachment ) ;
		expect( dbAttachment ).to.be.partially.like( {
			id: dbUser.file.id ,
			filename: 'hello-world.html' ,
			extension: 'html' ,
			contentType: 'text/html' ,
			fileSize: 52 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? USERS_ATTACHMENT_DIR : '' ) + dbUser.getId() + '/' + details.attachment.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + details.attachment.id
		} ) ;

		await expect( dbAttachment.load().then( v => v.toString() ) ).to.eventually.be( "<html><head></head><body>Hello world!</body></html>\n" ) ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( dbAttachment.path , fs.R_OK ) ; } ).not.to.throw() ;
		}
	} ) ;

	it( "should delete an attachment" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		//var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		//user.setAttachment( 'file' , attachment ) ;
		var attachment = user.setAttachment( 'file' , { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;

		//await attachment.save() ;
		await user.save() ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		var dbUser = await users.get( id ) ;

		await expect( dbUser.getAttachment( 'file' ).load().then( v => v.toString() ) ).to.eventually.be( "grigrigredin menufretin\n" ) ;

		dbUser.removeAttachment( 'file' ) ;

		// Check that the previous file has NOT been deleted YET
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		await dbUser.save() ;

		// Check that the previous file has been deleted
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).to.throw( Error , { code: 'ENOENT' } ) ;
		}

		expect( dbUser ).to.equal( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: null
		} ) ;

		var details = dbUser.getAttachmentDetails( 'file' ) ;
		expect( details ).to.be.like( {
			type: 'attachment' ,
			attachment: null
		} ) ;

		expect( () => dbUser.getAttachment( 'file' ) ).to.throw( ErrorStatus , { type: 'notFound' } ) ;
	} ) ;

	it( "should create, save and replace attachments as stream, and load as stream" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;
		var stream = new streamKit.FakeReadable( {
			timeout: 50 , chunkSize: 10 , chunkCount: 4 , filler: 'a'.charCodeAt( 0 )
		} ) ;

		//var attachment = user.createAttachment( { filename: 'random.bin' , contentType: 'bin/random' } , stream ) ;
		//user.setAttachment( 'file' , attachment ) ;
		var attachment = user.setAttachment( 'file' , { filename: 'random.bin' , contentType: 'bin/random' } , stream ) ;
		//log.error( user.file ) ;

		expect( user.file ).to.be.a( rootsDb.Attachment ) ;
		expect( user.file ).to.be.partially.like( {
			filename: 'random.bin' ,
			extension: 'bin' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'bin/random' ,
			fileSize: null ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
		} ) ;

		//await attachment.save() ;
		await user.save() ;
		
		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.be.partially.like( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'random.bin' ,
				extension: 'bin' ,
				id: user.file.id ,	// Unpredictable
				contentType: 'bin/random' ,
				fileSize: 40 ,
				hash: null ,
				hashType: null ,
				metadata: {} ,
			}
		} ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.partially.like( {
			id: dbUser.file.id ,
			filename: 'random.bin' ,
			extension: 'bin' ,
			contentType: 'bin/random' ,
			fileSize: 40 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? USERS_ATTACHMENT_DIR : '' ) + dbUser.getId() + '/' + attachment.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + attachment.id
		} ) ;

		await expect( dbAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'a'.repeat( 40 ) ) ;

		stream = new streamKit.FakeReadable( {
			timeout: 50 , chunkSize: 10 , chunkCount: 3 , filler: 'b'.charCodeAt( 0 )
		} ) ;

		//var attachment2 = user.createAttachment( { filename: 'more-random.bin' , contentType: 'bin/random' } , stream ) ;
		//dbUser.setAttachment( 'file' , attachment2 ) ;
		var attachment2 = dbUser.setAttachment( 'file' , { filename: 'more-random.bin' , contentType: 'bin/random' } , stream ) ;

		// Check that the previous file has NOT been deleted YET
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		//await attachment2.save() ;
		await dbUser.save() ;

		// Check that the previous file has been deleted -- SHOULD BE AFTER .save()
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).to.throw( Error , { code: 'ENOENT' } ) ;
		}

		dbUser = await users.get( id ) ;

		expect( dbUser ).to.be.partially.like( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'more-random.bin' ,
				extension: 'bin' ,
				id: dbUser.file.id ,	// Unpredictable
				contentType: 'bin/random' ,
				fileSize: 30 ,
				hash: null ,
				hashType: null ,
				metadata: {} ,
			}
		} ) ;

		dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.partially.like( {
			id: dbUser.file.id ,
			filename: 'more-random.bin' ,
			extension: 'bin' ,
			contentType: 'bin/random' ,
			fileSize: 30 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? USERS_ATTACHMENT_DIR : '' ) + dbUser.getId() + '/' + attachment2.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + attachment2.id
		} ) ;

		await expect( dbAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'b'.repeat( 30 ) ) ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( dbAttachment.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		// Now load as a stream
		var readStream = await dbAttachment.getReadStream() ;
		var fakeWritable = new streamKit.WritableToBuffer() ;
		readStream.pipe( fakeWritable ) ;
		await Promise.onceEvent( fakeWritable , "finish" ) ;

		expect( fakeWritable.get().toString() ).to.be( 'b'.repeat( 30 ) ) ;
	} ) ;

	it( "should .save() a document with the 'attachmentStreams' option" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;
		var attachmentStreams = new rootsDb.AttachmentStreams() ;

		attachmentStreams.addStream(
			new streamKit.FakeReadable( {
				timeout: 20 , chunkSize: 10 , chunkCount: 4 , filler: 'a'.charCodeAt( 0 )
			} ) ,
			'file' ,
			{ filename: 'random.bin' , contentType: 'bin/random' }
		) ;

		setTimeout( () => {
			attachmentStreams.addStream(
				new streamKit.FakeReadable( {
					timeout: 20 , chunkSize: 7 , chunkCount: 4 , filler: 'b'.charCodeAt( 0 )
				} ) ,
				'avatar' ,
				{ filename: 'face.jpg' , contentType: 'image/jpeg' }
			) ;
		} , 100 ) ;

		setTimeout( () => {
			attachmentStreams.addStream(
				new streamKit.FakeReadable( {
					timeout: 20 , chunkSize: 7 , chunkCount: 3 , filler: 'c'.charCodeAt( 0 )
				} ) ,
				'publicKey' ,
				{ filename: 'rsa.pub' , contentType: 'application/x-pem-file' }
			) ;
		} , 200 ) ;

		setTimeout( () => attachmentStreams.end() , 300 ) ;

		await user.save( { attachmentStreams: attachmentStreams } ) ;

		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.be.partially.like( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'random.bin' ,
				extension: 'bin' ,
				id: dbUser.file.id ,	// Unpredictable
				contentType: 'bin/random' ,
				fileSize: 40 ,
				hash: null ,
				hashType: null ,
				metadata: {} ,
			} ,
			avatar: {
				filename: 'face.jpg' ,
				extension: 'jpg' ,
				id: dbUser.avatar.id ,	// Unpredictable
				contentType: 'image/jpeg' ,
				fileSize: 28 ,
				hash: null ,
				hashType: null ,
				metadata: {} ,
			} ,
			publicKey: {
				filename: 'rsa.pub' ,
				extension: 'pub' ,
				id: dbUser.publicKey.id ,	// Unpredictable
				contentType: 'application/x-pem-file' ,
				fileSize: 21 ,
				hash: null ,
				hashType: null ,
				metadata: {} ,
			}
		} ) ;

		var fileAttachment = dbUser.getAttachment( 'file' ) ;
		expect( fileAttachment ).to.be.partially.like( {
			filename: 'random.bin' ,
			extension: 'bin' ,
			contentType: 'bin/random' ,
			fileSize: 40 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
		} ) ;

		await expect( fileAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'a'.repeat( 40 ) ) ;

		var avatarAttachment = dbUser.getAttachment( 'avatar' ) ;

		expect( avatarAttachment ).to.be.partially.like( {
			filename: 'face.jpg' ,
			extension: 'jpg' ,
			contentType: 'image/jpeg' ,
			fileSize: 28 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
		} ) ;

		await expect( avatarAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'b'.repeat( 28 ) ) ;

		var publicKeyAttachment = dbUser.getAttachment( 'publicKey' ) ;
		expect( publicKeyAttachment ).to.be.partially.like( {
			filename: 'rsa.pub' ,
			extension: 'pub' ,
			contentType: 'application/x-pem-file' ,
			fileSize: 21 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
		} ) ;

		await expect( publicKeyAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'c'.repeat( 21 ) ) ;
	} ) ;
} ) ;



describe( "Attachment links and checksum/hash (driver: "  + ATTACHMENT_MODE + ")" , () => {

	// Here we change the 'users' collection before performing the test, so it forces hash computation
	beforeEach( clearDB ) ;
	beforeEach( () => {
		users.attachmentHashType = 'sha256' ;
	} ) ;

	afterEach( () => {
		users.attachmentHashType = null ;
	} ) ;

	it( "should save attachment and compute its checksum/hash then load it" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		var contentHash = crypto.createHash( 'sha256' ).update( 'grigrigredin menufretin\n' ).digest( 'base64' ) ;

		//var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		//user.setAttachment( 'file' , attachment ) ;
		var attachment = user.setAttachment( 'file' , { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		//log.error( user.file ) ;

		expect( user.file ).to.be.partially.like( {
			filename: 'joke.txt' ,
			extension: 'txt' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'text/plain' ,
			fileSize: 24 ,
			hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		//await attachment.save() ;
		await user.save() ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.be.partially.like( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'joke.txt' ,
				extension: 'txt' ,
				id: user.file.id ,	// Unpredictable
				contentType: 'text/plain' ,
				fileSize: 24 ,
				hash: contentHash ,
				hashType: 'sha256' ,
				metadata: {} ,
			}
		} ) ;

		var details = dbUser.getAttachmentDetails( 'file' ) ;
		expect( details ).to.be.partially.like( {
			type: 'attachment' ,
			hostPath: 'file' ,
			schema: {
				optional: true ,
				type: 'attachment' ,
				tags: [ 'content' ] ,
				inputHint: "file"
			} ,
			attachment: {
				id: dbUser.file.id ,
				filename: 'joke.txt' ,
				extension: 'txt' ,
				contentType: 'text/plain' ,
				fileSize: 24 ,
				hash: contentHash ,
				hashType: 'sha256' ,
				metadata: {} ,
				collectionName: 'users' ,
				documentId: id.toString() ,
				driver: users.attachmentDriver ,
				path: ( ATTACHMENT_MODE === 'file' ? USERS_ATTACHMENT_DIR : '' ) + dbUser.getId() + '/' + details.attachment.id ,
				publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + details.attachment.id
			}
		} ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.partially.like( {
			id: user.file.id ,
			filename: 'joke.txt' ,
			extension: 'txt' ,
			contentType: 'text/plain' ,
			fileSize: 24 ,
			hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? USERS_ATTACHMENT_DIR : '' ) + dbUser.getId() + '/' + details.attachment.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + details.attachment.id
		} ) ;

		var content = await dbAttachment.load() ;
		expect( content.toString() ).to.be( "grigrigredin menufretin\n" ) ;
	} ) ;

	it( "should save attachment and expect a given checksum/hash + fileSize" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		var attachment ,
			contentHash = crypto.createHash( 'sha256' ).update( 'grigrigredin menufretin\n' ).digest( 'base64' ) ,
			badContentHash = contentHash.slice( 0 , -3 ) + 'bad' ;

		// With an incorrect hash
		//expect( () => user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' , hash: badContentHash , fileSize: 24 } , "grigrigredin menufretin\n" ) ).to.throw( Error , { code: 'badHash' } ) ;
		expect( () => user.setAttachment( 'file' , { filename: 'joke.txt' , contentType: 'text/plain' , hash: badContentHash , fileSize: 24 } , "grigrigredin menufretin\n" ) ).to.eventually.throw( Error , { code: 'badHash' } ) ;
		// With an incorrect file size
		//expect( () => user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' , hash: contentHash , fileSize: 21 } , "grigrigredin menufretin\n" ) ).to.throw( Error , { code: 'badFileSize' } ) ;
		expect( () => user.setAttachment( 'file' , { filename: 'joke.txt' , contentType: 'text/plain' , hash: contentHash , fileSize: 21 } , "grigrigredin menufretin\n" ) ).to.eventually.throw( Error , { code: 'badFileSize' } ) ;

		// With the correct hash
		//attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' , hash: contentHash , fileSize: 24 } , "grigrigredin menufretin\n" ) ;
		//user.setAttachment( 'file' , attachment ) ;
		attachment = user.setAttachment( 'file' , { filename: 'joke.txt' , contentType: 'text/plain' , hash: contentHash , fileSize: 24 } , "grigrigredin menufretin\n" ) ;
		//log.error( user.file ) ;

		expect( user.file ).to.be.partially.like( {
			filename: 'joke.txt' ,
			extension: 'txt' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'text/plain' ,
			fileSize: 24 ,
			hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		//await attachment.save() ;
		await user.save() ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.be.partially.like( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'joke.txt' ,
				extension: 'txt' ,
				id: user.file.id ,	// Unpredictable
				contentType: 'text/plain' ,
				fileSize: 24 ,
				hash: contentHash ,
				hashType: 'sha256' ,
				metadata: {} ,
			}
		} ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.partially.like( {
			id: user.file.id ,
			filename: 'joke.txt' ,
			extension: 'txt' ,
			contentType: 'text/plain' ,
			fileSize: 24 ,
			hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? USERS_ATTACHMENT_DIR : '' ) + dbUser.getId() + '/' + dbAttachment.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + dbAttachment.id
		} ) ;

		var content = await dbAttachment.load() ;
		expect( content.toString() ).to.be( "grigrigredin menufretin\n" ) ;
	} ) ;

	it( "should save attachment as stream and compute its checksum/hash" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		var attachment , stream ,
			contentHash = crypto.createHash( 'sha256' ).update( 'a'.repeat( 40 ) ).digest( 'base64' ) ;

		stream = new streamKit.FakeReadable( {
			timeout: 50 , chunkSize: 10 , chunkCount: 4 , filler: 'a'.charCodeAt( 0 )
		} ) ;

		//attachment = user.createAttachment( { filename: 'random.bin' , contentType: 'bin/random' } , stream ) ;
		//user.setAttachment( 'file' , attachment ) ;
		attachment = user.setAttachment( 'file' , { filename: 'random.bin' , contentType: 'bin/random' } , stream ) ;
		//log.error( user.file ) ;

		expect( user.file ).to.be.partially.like( {
			filename: 'random.bin' ,
			extension: 'bin' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'bin/random' ,
			fileSize: null ,	// The size is not yet computed since it is a stream!
			hash: null ,	// The hash is not yet computed since it is a stream!
			//hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		// It should be ok here
		//await attachment.save() ;
		await user.save() ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.be.partially.like( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'random.bin' ,
				extension: 'bin' ,
				id: user.file.id ,	// Unpredictable
				contentType: 'bin/random' ,
				fileSize: 40 ,
				hash: contentHash ,
				hashType: 'sha256' ,
				metadata: {} ,
			}
		} ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.partially.like( {
			id: dbUser.file.id ,
			filename: 'random.bin' ,
			extension: 'bin' ,
			contentType: 'bin/random' ,
			fileSize: 40 ,
			hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? USERS_ATTACHMENT_DIR : '' ) + dbUser.getId() + '/' + attachment.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + attachment.id
		} ) ;

		await expect( dbAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'a'.repeat( 40 ) ) ;
	} ) ;

	it( "should save attachment as stream and expect a given checksum/hash + fileSize" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		var attachment , stream ,
			contentHash = crypto.createHash( 'sha256' ).update( 'a'.repeat( 40 ) ).digest( 'base64' ) ,
			badContentHash = contentHash.slice( 0 , -3 ) + 'bad' ;

		
		// Start with a bad hash

		stream = new streamKit.FakeReadable( {
			timeout: 50 , chunkSize: 10 , chunkCount: 4 , filler: 'a'.charCodeAt( 0 )
		} ) ;

		//attachment = user.createAttachment( { filename: 'random.bin' , contentType: 'bin/random' , hash: badContentHash , fileSize: 40 } , stream ) ;
		//user.setAttachment( 'file' , attachment ) ;
		attachment = user.setAttachment( 'file' , { filename: 'random.bin' , contentType: 'bin/random' , hash: badContentHash , fileSize: 40 } , stream ) ;
		//log.error( user.file ) ;

		expect( user.file ).to.be.partially.like( {
			filename: 'random.bin' ,
			extension: 'bin' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'bin/random' ,
			fileSize: 40 ,
			hash: badContentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		// It should throw here
		//await expect( () => attachment.save() ).to.eventually.throw( Error , { code: 'badHash' } ) ;
		await expect( () => user.save() ).to.eventually.throw( Error , { code: 'badHash' } ) ;
		
		
		// Then with a bad file size

		stream = new streamKit.FakeReadable( {
			timeout: 50 , chunkSize: 10 , chunkCount: 4 , filler: 'a'.charCodeAt( 0 )
		} ) ;

		//attachment = user.createAttachment( { filename: 'random.bin' , contentType: 'bin/random' , hash: contentHash , fileSize: 35 } , stream ) ;
		//user.setAttachment( 'file' , attachment ) ;
		attachment = user.setAttachment( 'file' , { filename: 'random.bin' , contentType: 'bin/random' , hash: contentHash , fileSize: 35 } , stream ) ;
		//log.error( user.file ) ;

		expect( user.file ).to.be.partially.like( {
			filename: 'random.bin' ,
			extension: 'bin' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'bin/random' ,
			fileSize: 35 ,
			hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		// It should throw here
		//await expect( () => attachment.save() ).to.eventually.throw( Error , { code: 'badFileSize' } ) ;
		await expect( () => user.save() ).to.eventually.throw( Error , { code: 'badFileSize' } ) ;
		
		
		// Now start over with the correct one
		
		stream = new streamKit.FakeReadable( {
			timeout: 50 , chunkSize: 10 , chunkCount: 4 , filler: 'a'.charCodeAt( 0 )
		} ) ;

		//attachment = user.createAttachment( { filename: 'random.bin' , contentType: 'bin/random' , hash: contentHash , fileSize: 40 } , stream ) ;
		//user.setAttachment( 'file' , attachment ) ;
		attachment = user.setAttachment( 'file' , { filename: 'random.bin' , contentType: 'bin/random' , hash: contentHash , fileSize: 40 } , stream ) ;
		//log.error( user.file ) ;

		expect( user.file ).to.be.partially.like( {
			filename: 'random.bin' ,
			extension: 'bin' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'bin/random' ,
			fileSize: 40 ,
			hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		// It should be ok here
		//await attachment.save() ;
		await user.save() ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.be.partially.like( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'random.bin' ,
				extension: 'bin' ,
				id: user.file.id ,	// Unpredictable
				contentType: 'bin/random' ,
				fileSize: 40 ,
				hash: contentHash ,
				hashType: 'sha256' ,
				metadata: {} ,
			}
		} ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.partially.like( {
			id: dbUser.file.id ,
			filename: 'random.bin' ,
			extension: 'bin' ,
			contentType: 'bin/random' ,
			fileSize: 40 ,
			hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? USERS_ATTACHMENT_DIR : '' ) + dbUser.getId() + '/' + attachment.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + attachment.id
		} ) ;

		await expect( dbAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'a'.repeat( 40 ) ) ;
	} ) ;

	it( "should .save() a document with the 'attachmentStreams' option and compute its checksum/hash" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		var contentHash = [
				crypto.createHash( 'sha256' ).update( 'a'.repeat( 40 ) ).digest( 'base64' ) ,
				crypto.createHash( 'sha256' ).update( 'b'.repeat( 28 ) ).digest( 'base64' ) ,
				crypto.createHash( 'sha256' ).update( 'c'.repeat( 21 ) ).digest( 'base64' )
			] ;

		var attachmentStreams = new rootsDb.AttachmentStreams() ;

		attachmentStreams.addStream(
			new streamKit.FakeReadable( {
				timeout: 20 , chunkSize: 10 , chunkCount: 4 , filler: 'a'.charCodeAt( 0 )
			} ) ,
			'file' ,
			{ filename: 'random.bin' , contentType: 'bin/random' }
		) ;

		setTimeout( () => {
			attachmentStreams.addStream(
				new streamKit.FakeReadable( {
					timeout: 20 , chunkSize: 7 , chunkCount: 4 , filler: 'b'.charCodeAt( 0 )
				} ) ,
				'avatar' ,
				{ filename: 'face.jpg' , contentType: 'image/jpeg' }
			) ;
		} , 100 ) ;

		setTimeout( () => {
			attachmentStreams.addStream(
				new streamKit.FakeReadable( {
					timeout: 20 , chunkSize: 7 , chunkCount: 3 , filler: 'c'.charCodeAt( 0 )
				} ) ,
				'publicKey' ,
				{ filename: 'rsa.pub' , contentType: 'application/x-pem-file' }
			) ;
		} , 200 ) ;

		setTimeout( () => attachmentStreams.end() , 300 ) ;

		// It should pass
		await user.save( { attachmentStreams: attachmentStreams } ) ;
		

		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.be.partially.like( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'random.bin' ,
				extension: 'bin' ,
				id: dbUser.file.id ,	// Unpredictable
				contentType: 'bin/random' ,
				fileSize: 40 ,
				hash: contentHash[ 0 ] ,
				hashType: 'sha256' ,
				metadata: {} ,
			} ,
			avatar: {
				filename: 'face.jpg' ,
				extension: 'jpg' ,
				id: dbUser.avatar.id ,	// Unpredictable
				contentType: 'image/jpeg' ,
				fileSize: 28 ,
				hash: contentHash[ 1 ] ,
				hashType: 'sha256' ,
				metadata: {} ,
			} ,
			publicKey: {
				filename: 'rsa.pub' ,
				extension: 'pub' ,
				id: dbUser.publicKey.id ,	// Unpredictable
				contentType: 'application/x-pem-file' ,
				fileSize: 21 ,
				hash: contentHash[ 2 ] ,
				hashType: 'sha256' ,
				metadata: {} ,
			}
		} ) ;

		var fileAttachment = dbUser.getAttachment( 'file' ) ;
		expect( fileAttachment ).to.be.partially.like( {
			filename: 'random.bin' ,
			extension: 'bin' ,
			contentType: 'bin/random' ,
			fileSize: 40 ,
			hash: contentHash[ 0 ] ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		await expect( fileAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'a'.repeat( 40 ) ) ;

		var avatarAttachment = dbUser.getAttachment( 'avatar' ) ;

		expect( avatarAttachment ).to.be.partially.like( {
			filename: 'face.jpg' ,
			extension: 'jpg' ,
			contentType: 'image/jpeg' ,
			fileSize: 28 ,
			hash: contentHash[ 1 ] ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		await expect( avatarAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'b'.repeat( 28 ) ) ;

		var publicKeyAttachment = dbUser.getAttachment( 'publicKey' ) ;
		expect( publicKeyAttachment ).to.be.partially.like( {
			filename: 'rsa.pub' ,
			extension: 'pub' ,
			contentType: 'application/x-pem-file' ,
			fileSize: 21 ,
			hash: contentHash[ 2 ] ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		await expect( publicKeyAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'c'.repeat( 21 ) ) ;
	} ) ;

	it( "should .save() a document with the 'attachmentStreams' option and expect given checksum/hash + fileSize" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		var contentHash = [
				crypto.createHash( 'sha256' ).update( 'a'.repeat( 40 ) ).digest( 'base64' ) ,
				crypto.createHash( 'sha256' ).update( 'b'.repeat( 28 ) ).digest( 'base64' ) ,
				crypto.createHash( 'sha256' ).update( 'c'.repeat( 21 ) ).digest( 'base64' )
			] ,
			badContentHash = contentHash.map( str => str.slice( 0 , -3 ) + 'bad' ) ;

		
		// Start with a bad hash

		var badAttachmentStreams = new rootsDb.AttachmentStreams() ;

		badAttachmentStreams.addStream(
			new streamKit.FakeReadable( {
				timeout: 20 , chunkSize: 10 , chunkCount: 4 , filler: 'a'.charCodeAt( 0 )
			} ) ,
			'file' ,
			{ filename: 'random.bin' , contentType: 'bin/random' , hash: badContentHash[ 0 ] , fileSize: 40 }
		) ;

		setTimeout( () => {
			badAttachmentStreams.addStream(
				new streamKit.FakeReadable( {
					timeout: 20 , chunkSize: 7 , chunkCount: 4 , filler: 'b'.charCodeAt( 0 )
				} ) ,
				'avatar' ,
				{ filename: 'face.jpg' , contentType: 'image/jpeg' , hash: badContentHash[ 1 ] , fileSize: 28 }
			) ;
		} , 100 ) ;

		setTimeout( () => {
			badAttachmentStreams.addStream(
				new streamKit.FakeReadable( {
					timeout: 20 , chunkSize: 7 , chunkCount: 3 , filler: 'c'.charCodeAt( 0 )
				} ) ,
				'publicKey' ,
				{ filename: 'rsa.pub' , contentType: 'application/x-pem-file' , hash: badContentHash[ 2 ] , fileSize: 21 }
			) ;
		} , 200 ) ;

		setTimeout( () => badAttachmentStreams.end() , 300 ) ;

		await expect( () => user.save( { attachmentStreams: badAttachmentStreams } ) ).to.eventually.throw( Error , { code: 'badHash' } ) ;
		
		
		// Then with a bad file size

		badAttachmentStreams = new rootsDb.AttachmentStreams() ;

		badAttachmentStreams.addStream(
			new streamKit.FakeReadable( {
				timeout: 20 , chunkSize: 10 , chunkCount: 4 , filler: 'a'.charCodeAt( 0 )
			} ) ,
			'file' ,
			{ filename: 'random.bin' , contentType: 'bin/random' , hash: contentHash[ 0 ] , fileSize: 41 }
		) ;

		setTimeout( () => {
			badAttachmentStreams.addStream(
				new streamKit.FakeReadable( {
					timeout: 20 , chunkSize: 7 , chunkCount: 4 , filler: 'b'.charCodeAt( 0 )
				} ) ,
				'avatar' ,
				{ filename: 'face.jpg' , contentType: 'image/jpeg' , hash: contentHash[ 1 ] , fileSize: 14 }
			) ;
		} , 100 ) ;

		setTimeout( () => {
			badAttachmentStreams.addStream(
				new streamKit.FakeReadable( {
					timeout: 20 , chunkSize: 7 , chunkCount: 3 , filler: 'c'.charCodeAt( 0 )
				} ) ,
				'publicKey' ,
				{ filename: 'rsa.pub' , contentType: 'application/x-pem-file' , hash: contentHash[ 2 ] , fileSize: 17 }
			) ;
		} , 200 ) ;

		setTimeout( () => badAttachmentStreams.end() , 300 ) ;

		await expect( () => user.save( { attachmentStreams: badAttachmentStreams } ) ).to.eventually.throw( Error , { code: 'badFileSize' } ) ;
		
		
		// Now start over with the correct one
		
		var attachmentStreams = new rootsDb.AttachmentStreams() ;

		attachmentStreams.addStream(
			new streamKit.FakeReadable( {
				timeout: 20 , chunkSize: 10 , chunkCount: 4 , filler: 'a'.charCodeAt( 0 )
			} ) ,
			'file' ,
			{ filename: 'random.bin' , contentType: 'bin/random' , hash: contentHash[ 0 ] , fileSize: 40 }
		) ;

		setTimeout( () => {
			attachmentStreams.addStream(
				new streamKit.FakeReadable( {
					timeout: 20 , chunkSize: 7 , chunkCount: 4 , filler: 'b'.charCodeAt( 0 )
				} ) ,
				'avatar' ,
				{ filename: 'face.jpg' , contentType: 'image/jpeg' , hash: contentHash[ 1 ] , fileSize: 28 }
			) ;
		} , 100 ) ;

		setTimeout( () => {
			attachmentStreams.addStream(
				new streamKit.FakeReadable( {
					timeout: 20 , chunkSize: 7 , chunkCount: 3 , filler: 'c'.charCodeAt( 0 )
				} ) ,
				'publicKey' ,
				{ filename: 'rsa.pub' , contentType: 'application/x-pem-file' , hash: contentHash[ 2 ] , fileSize: 21 }
			) ;
		} , 200 ) ;

		setTimeout( () => attachmentStreams.end() , 300 ) ;

		// It should pass
		await user.save( { attachmentStreams: attachmentStreams } ) ;
		

		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.be.partially.like( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'random.bin' ,
				extension: 'bin' ,
				id: dbUser.file.id ,	// Unpredictable
				contentType: 'bin/random' ,
				fileSize: 40 ,
				hash: contentHash[ 0 ] ,
				hashType: 'sha256' ,
				metadata: {} ,
			} ,
			avatar: {
				filename: 'face.jpg' ,
				extension: 'jpg' ,
				id: dbUser.avatar.id ,	// Unpredictable
				contentType: 'image/jpeg' ,
				fileSize: 28 ,
				hash: contentHash[ 1 ] ,
				hashType: 'sha256' ,
				metadata: {} ,
			} ,
			publicKey: {
				filename: 'rsa.pub' ,
				extension: 'pub' ,
				id: dbUser.publicKey.id ,	// Unpredictable
				contentType: 'application/x-pem-file' ,
				fileSize: 21 ,
				hash: contentHash[ 2 ] ,
				hashType: 'sha256' ,
				metadata: {} ,
			}
		} ) ;

		var fileAttachment = dbUser.getAttachment( 'file' ) ;
		expect( fileAttachment ).to.be.partially.like( {
			filename: 'random.bin' ,
			extension: 'bin' ,
			contentType: 'bin/random' ,
			fileSize: 40 ,
			hash: contentHash[ 0 ] ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		await expect( fileAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'a'.repeat( 40 ) ) ;

		var avatarAttachment = dbUser.getAttachment( 'avatar' ) ;

		expect( avatarAttachment ).to.be.partially.like( {
			filename: 'face.jpg' ,
			extension: 'jpg' ,
			contentType: 'image/jpeg' ,
			fileSize: 28 ,
			hash: contentHash[ 1 ] ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		await expect( avatarAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'b'.repeat( 28 ) ) ;

		var publicKeyAttachment = dbUser.getAttachment( 'publicKey' ) ;
		expect( publicKeyAttachment ).to.be.partially.like( {
			filename: 'rsa.pub' ,
			extension: 'pub' ,
			contentType: 'application/x-pem-file' ,
			fileSize: 21 ,
			hash: contentHash[ 2 ] ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		await expect( publicKeyAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'c'.repeat( 21 ) ) ;
	} ) ;
} ) ;



describe( "AttachmentSet links (driver: " + ATTACHMENT_MODE + ")" , () => {

	beforeEach( clearDB ) ;

	it( "should create, save, load, and delete attachments from an attachmentSet" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var image = images.createDocument( { name: 'selfie' } ) ;
		var id = image.getId() ;

		// Exists by default
		expect( image.$.fileSet ).not.to.be.a( rootsDb.AttachmentSet ) ;
		expect( image.$.fileSet ).to.equal( {} ) ;

		// Is always auto-populated
		expect( image.fileSet ).to.be.a( rootsDb.AttachmentSet ) ;

		var source = image.fileSet.set( 'source' , { filename: 'source.png' , contentType: 'image/png' } , "not a png" ) ;

		// Raw DB data
		expect( image.$.fileSet ).not.to.be.a( rootsDb.AttachmentSet ) ;
		expect( image.$.fileSet.attachments.source.id ).to.be.a( 'string' ) ;
		expect( image.$.fileSet ).to.be.like( {
			metadata: {} ,
			attachments: {
				source: {
					id: image.$.fileSet.attachments.source.id ,	// Unpredictable
					filename: 'source.png' ,
					extension: 'png' ,
					contentType: 'image/png' ,
					fileSize: 9 ,
					hash: null ,
					hashType: null ,
					metadata: {}
				}
			}
		} ) ;

		//console.error( "\n\n>>> Unit attachment >>>" , image.fileSet , '\n' ) ;
		expect( image.fileSet ).to.be.a( rootsDb.AttachmentSet ) ;
		expect( image.fileSet ).to.be.partially.like( {
			metadata: {} ,
			attachments: {
				source: {
					id: image.fileSet.attachments.source.id ,	// Unpredictable
					filename: 'source.png' ,
					extension: 'png' ,
					contentType: 'image/png' ,
					fileSize: 9 ,
					hash: null ,
					hashType: null ,
					metadata: {}
				}
			}
		} ) ;

		await image.save() ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( source.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		var dbImage = await images.get( id ) ;
		expect( dbImage ).to.be.partially.like( {
			_id: id ,
			name: 'selfie' ,
			fileSet: {
				metadata: {} ,
				attachments: {
					source: {
						id: image.fileSet.attachments.source.id ,	// Unpredictable
						filename: 'source.png' ,
						extension: 'png' ,
						contentType: 'image/png' ,
						fileSize: 9 ,
						hash: null ,
						hashType: null ,
						metadata: {}
					}
				}
			}
		} ) ;

		//var dbAttachment = dbImage.getAttachment( 'fileSet' ) ;
		expect( dbImage.fileSet ).to.be.a( rootsDb.AttachmentSet ) ;
		expect( dbImage.fileSet ).to.be.partially.like( {
			metadata: {} ,
			attachments: {
				source: {
					id: dbImage.fileSet.id ,
					filename: 'source.png' ,
					extension: 'png' ,
					contentType: 'image/png' ,
					fileSize: 9 ,
					hash: null ,
					hashType: null ,
					metadata: {} ,
					collectionName: 'images' ,
					documentId: id.toString() ,
					driver: images.attachmentDriver ,
					path: ( ATTACHMENT_MODE === 'file' ? IMAGES_ATTACHMENT_DIR : '' ) + dbImage.getId() + '/' + dbImage.fileSet.attachments.source.id ,
					publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbImage.getId() + '/' + dbImage.fileSet.attachments.source.id
				}
			}
		} ) ;

		var content = await dbImage.fileSet.get( 'source' ).load() ;
		expect( content.toString() ).to.be( "not a png" ) ;



		// Now add 2 variants

		var thumbnail = dbImage.fileSet.set( 'thumbnail' , { filename: 'thumbnail.png' , contentType: 'image/png' } , "not a thumbnail png" ) ;
		var small = dbImage.fileSet.set( 'small' , { filename: 'small.png' , contentType: 'image/png' } , "not a small png" ) ;

		await dbImage.save() ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( source.path , fs.R_OK ) ; } ).not.to.throw() ;
			expect( () => { fs.accessSync( thumbnail.path , fs.R_OK ) ; } ).not.to.throw() ;
			expect( () => { fs.accessSync( small.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		dbImage = await images.get( id ) ;
		expect( dbImage ).to.be.partially.like( {
			_id: id ,
			name: 'selfie' ,
			fileSet: {
				metadata: {} ,
				attachments: {
					source: {
						id: dbImage.fileSet.attachments.source.id ,	// Unpredictable
						filename: 'source.png' ,
						extension: 'png' ,
						contentType: 'image/png' ,
						fileSize: 9 ,
						hash: null ,
						hashType: null ,
						metadata: {}
					} ,
					thumbnail: {
						id: dbImage.fileSet.attachments.thumbnail.id ,	// Unpredictable
						filename: 'thumbnail.png' ,
						extension: 'png' ,
						contentType: 'image/png' ,
						fileSize: 19 ,
						hash: null ,
						hashType: null ,
						metadata: {}
					} ,
					small: {
						id: dbImage.fileSet.attachments.small.id ,	// Unpredictable
						filename: 'small.png' ,
						extension: 'png' ,
						contentType: 'image/png' ,
						fileSize: 15 ,
						hash: null ,
						hashType: null ,
						metadata: {}
					} ,
				}
			}
		} ) ;

		//var dbAttachment = dbImage.getAttachment( 'fileSet' ) ;
		expect( dbImage.fileSet ).to.be.a( rootsDb.AttachmentSet ) ;
		expect( dbImage.fileSet ).to.be.partially.like( {
			metadata: {} ,
			attachments: {
				source: {
					id: dbImage.fileSet.id ,
					filename: 'source.png' ,
					extension: 'png' ,
					contentType: 'image/png' ,
					fileSize: 9 ,
					hash: null ,
					hashType: null ,
					metadata: {} ,
					collectionName: 'images' ,
					documentId: id.toString() ,
					driver: images.attachmentDriver ,
					path: ( ATTACHMENT_MODE === 'file' ? IMAGES_ATTACHMENT_DIR : '' ) + dbImage.getId() + '/' + dbImage.fileSet.attachments.source.id ,
					publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbImage.getId() + '/' + dbImage.fileSet.attachments.source.id
				} ,
				thumbnail: {
					id: dbImage.fileSet.id ,
					filename: 'thumbnail.png' ,
					extension: 'png' ,
					contentType: 'image/png' ,
					fileSize: 19 ,
					hash: null ,
					hashType: null ,
					metadata: {} ,
					collectionName: 'images' ,
					documentId: id.toString() ,
					driver: images.attachmentDriver ,
					path: ( ATTACHMENT_MODE === 'file' ? IMAGES_ATTACHMENT_DIR : '' ) + dbImage.getId() + '/' + dbImage.fileSet.attachments.thumbnail.id ,
					publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbImage.getId() + '/' + dbImage.fileSet.attachments.thumbnail.id
				} ,
				small: {
					id: dbImage.fileSet.id ,
					filename: 'small.png' ,
					extension: 'png' ,
					contentType: 'image/png' ,
					fileSize: 15 ,
					hash: null ,
					hashType: null ,
					metadata: {} ,
					collectionName: 'images' ,
					documentId: id.toString() ,
					driver: images.attachmentDriver ,
					path: ( ATTACHMENT_MODE === 'file' ? IMAGES_ATTACHMENT_DIR : '' ) + dbImage.getId() + '/' + dbImage.fileSet.attachments.small.id ,
					publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbImage.getId() + '/' + dbImage.fileSet.attachments.small.id
				}
			}
		} ) ;

		content = await dbImage.fileSet.get( 'source' ).load() ;
		expect( content.toString() ).to.be( "not a png" ) ;

		content = await dbImage.fileSet.get( 'thumbnail' ).load() ;
		expect( content.toString() ).to.be( "not a thumbnail png" ) ;

		content = await dbImage.fileSet.get( 'small' ).load() ;
		expect( content.toString() ).to.be( "not a small png" ) ;



		// Now remove 2 variants

		dbImage.fileSet.delete( 'source' ) ;
		dbImage.fileSet.delete( 'thumbnail' ) ;

		await dbImage.save() ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( source.path , fs.R_OK ) ; } ).to.throw( Error , { code: 'ENOENT' } ) ;
			expect( () => { fs.accessSync( thumbnail.path , fs.R_OK ) ; } ).to.throw( Error , { code: 'ENOENT' } ) ;
			expect( () => { fs.accessSync( small.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		dbImage = await images.get( id ) ;
		expect( dbImage.fileSet.attachments.source ).to.be.undefined() ;
		expect( dbImage.fileSet.attachments.thumbnail ).to.be.undefined() ;
		expect( dbImage ).to.be.partially.like( {
			_id: id ,
			name: 'selfie' ,
			fileSet: {
				metadata: {} ,
				attachments: {
					small: {
						id: dbImage.fileSet.attachments.small.id ,	// Unpredictable
						filename: 'small.png' ,
						extension: 'png' ,
						contentType: 'image/png' ,
						fileSize: 15 ,
						hash: null ,
						hashType: null ,
						metadata: {}
					} ,
				}
			}
		} ) ;

		//var dbAttachment = dbImage.getAttachment( 'fileSet' ) ;
		expect( dbImage.fileSet ).to.be.a( rootsDb.AttachmentSet ) ;
		expect( dbImage.fileSet ).to.be.partially.like( {
			metadata: {} ,
			attachments: {
				small: {
					id: dbImage.fileSet.id ,
					filename: 'small.png' ,
					extension: 'png' ,
					contentType: 'image/png' ,
					fileSize: 15 ,
					hash: null ,
					hashType: null ,
					metadata: {} ,
					collectionName: 'images' ,
					documentId: id.toString() ,
					driver: images.attachmentDriver ,
					path: ( ATTACHMENT_MODE === 'file' ? IMAGES_ATTACHMENT_DIR : '' ) + dbImage.getId() + '/' + dbImage.fileSet.attachments.small.id ,
					publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbImage.getId() + '/' + dbImage.fileSet.attachments.small.id
				}
			}
		} ) ;

		expect( dbImage.fileSet.get( 'source' ) ).to.be( undefined ) ;
		expect( dbImage.fileSet.get( 'thumbnail' ) ).to.be( undefined ) ;

		content = await dbImage.fileSet.get( 'small' ).load() ;
		expect( content.toString() ).to.be( "not a small png" ) ;
	} ) ;

	it( "using .getAttachment()/.setAttachment()/.removeAttachment() API with attachmentSet" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var image = images.createDocument( { name: 'selfie' } ) ;
		var id = image.getId() ;

		var source = image.setAttachment( 'fileSet' , 'source' , { filename: 'source.png' , contentType: 'image/png' } , "not a png" ) ;

		expect( image.getAttachment( 'fileSet' ) ).to.be.a( rootsDb.AttachmentSet ) ;
		expect( image.getAttachment( 'fileSet' ) ).to.be.partially.like( {
			metadata: {} ,
			attachments: {
				source: {
					id: image.fileSet.attachments.source.id ,	// Unpredictable
					filename: 'source.png' ,
					extension: 'png' ,
					contentType: 'image/png' ,
					fileSize: 9 ,
					hash: null ,
					hashType: null ,
					metadata: {}
				}
			}
		} ) ;
		expect( image.getAttachment( 'fileSet' , 'source' ) ).to.be.partially.like( {
			id: image.fileSet.attachments.source.id ,	// Unpredictable
			filename: 'source.png' ,
			extension: 'png' ,
			contentType: 'image/png' ,
			fileSize: 9 ,
			hash: null ,
			hashType: null ,
			metadata: {}
		} ) ;

		await image.save() ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( source.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		var dbImage = await images.get( id ) ;
		expect( dbImage ).to.be.partially.like( {
			_id: id ,
			name: 'selfie' ,
			fileSet: {
				metadata: {} ,
				attachments: {
					source: {
						id: image.fileSet.attachments.source.id ,	// Unpredictable
						filename: 'source.png' ,
						extension: 'png' ,
						contentType: 'image/png' ,
						fileSize: 9 ,
						hash: null ,
						hashType: null ,
						metadata: {}
					}
				}
			}
		} ) ;

		expect( dbImage.getAttachment( 'fileSet' ) ).to.be.a( rootsDb.AttachmentSet ) ;
		expect( dbImage.getAttachment( 'fileSet' ) ).to.be.partially.like( {
			metadata: {} ,
			attachments: {
				source: {
					id: dbImage.fileSet.id ,
					filename: 'source.png' ,
					extension: 'png' ,
					contentType: 'image/png' ,
					fileSize: 9 ,
					hash: null ,
					hashType: null ,
					metadata: {} ,
					collectionName: 'images' ,
					documentId: id.toString() ,
					driver: images.attachmentDriver ,
					path: ( ATTACHMENT_MODE === 'file' ? IMAGES_ATTACHMENT_DIR : '' ) + dbImage.getId() + '/' + dbImage.fileSet.attachments.source.id ,
					publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbImage.getId() + '/' + dbImage.fileSet.attachments.source.id
				}
			}
		} ) ;
		expect( dbImage.getAttachment( 'fileSet' , 'source' ) ).to.be.partially.like( {
			id: dbImage.fileSet.id ,
			filename: 'source.png' ,
			extension: 'png' ,
			contentType: 'image/png' ,
			fileSize: 9 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
			collectionName: 'images' ,
			documentId: id.toString() ,
			driver: images.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? IMAGES_ATTACHMENT_DIR : '' ) + dbImage.getId() + '/' + dbImage.fileSet.attachments.source.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbImage.getId() + '/' + dbImage.fileSet.attachments.source.id
		} ) ;

		var content = await dbImage.getAttachment( 'fileSet' , 'source' ).load() ;
		expect( content.toString() ).to.be( "not a png" ) ;
		content = await dbImage.getAttachment( 'fileSet' ).get( 'source' ).load() ;
		expect( content.toString() ).to.be( "not a png" ) ;



		// Now add 2 variants

		var thumbnail = dbImage.setAttachment( 'fileSet' , 'thumbnail' , { filename: 'thumbnail.png' , contentType: 'image/png' } , "not a thumbnail png" ) ;
		var small = dbImage.setAttachment( 'fileSet' , 'small' , { filename: 'small.png' , contentType: 'image/png' } , "not a small png" ) ;

		await dbImage.save() ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( source.path , fs.R_OK ) ; } ).not.to.throw() ;
			expect( () => { fs.accessSync( thumbnail.path , fs.R_OK ) ; } ).not.to.throw() ;
			expect( () => { fs.accessSync( small.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		dbImage = await images.get( id ) ;
		expect( dbImage ).to.be.partially.like( {
			_id: id ,
			name: 'selfie' ,
			fileSet: {
				metadata: {} ,
				attachments: {
					source: {
						id: dbImage.fileSet.attachments.source.id ,	// Unpredictable
						filename: 'source.png' ,
						extension: 'png' ,
						contentType: 'image/png' ,
						fileSize: 9 ,
						hash: null ,
						hashType: null ,
						metadata: {}
					} ,
					thumbnail: {
						id: dbImage.fileSet.attachments.thumbnail.id ,	// Unpredictable
						filename: 'thumbnail.png' ,
						extension: 'png' ,
						contentType: 'image/png' ,
						fileSize: 19 ,
						hash: null ,
						hashType: null ,
						metadata: {}
					} ,
					small: {
						id: dbImage.fileSet.attachments.small.id ,	// Unpredictable
						filename: 'small.png' ,
						extension: 'png' ,
						contentType: 'image/png' ,
						fileSize: 15 ,
						hash: null ,
						hashType: null ,
						metadata: {}
					}
				}
			}
		} ) ;

		expect( dbImage.getAttachment( 'fileSet' ) ).to.be.a( rootsDb.AttachmentSet ) ;
		expect( dbImage.getAttachment( 'fileSet' , 'source' ) ).to.be.partially.like( {
			id: dbImage.fileSet.id ,
			filename: 'source.png' ,
			extension: 'png' ,
			contentType: 'image/png' ,
			fileSize: 9 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
			collectionName: 'images' ,
			documentId: id.toString() ,
			driver: images.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? IMAGES_ATTACHMENT_DIR : '' ) + dbImage.getId() + '/' + dbImage.fileSet.attachments.source.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbImage.getId() + '/' + dbImage.fileSet.attachments.source.id
		} ) ;
		expect( dbImage.getAttachment( 'fileSet' , 'thumbnail' ) ).to.be.partially.like( {
			id: dbImage.fileSet.id ,
			filename: 'thumbnail.png' ,
			extension: 'png' ,
			contentType: 'image/png' ,
			fileSize: 19 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
			collectionName: 'images' ,
			documentId: id.toString() ,
			driver: images.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? IMAGES_ATTACHMENT_DIR : '' ) + dbImage.getId() + '/' + dbImage.fileSet.attachments.thumbnail.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbImage.getId() + '/' + dbImage.fileSet.attachments.thumbnail.id
		} ) ;
		expect( dbImage.getAttachment( 'fileSet' , 'small' ) ).to.be.partially.like( {
			id: dbImage.fileSet.id ,
			filename: 'small.png' ,
			extension: 'png' ,
			contentType: 'image/png' ,
			fileSize: 15 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
			collectionName: 'images' ,
			documentId: id.toString() ,
			driver: images.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? IMAGES_ATTACHMENT_DIR : '' ) + dbImage.getId() + '/' + dbImage.fileSet.attachments.small.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbImage.getId() + '/' + dbImage.fileSet.attachments.small.id
		} ) ;

		content = await dbImage.getAttachment( 'fileSet' , 'source' ).load() ;
		expect( content.toString() ).to.be( "not a png" ) ;

		content = await dbImage.getAttachment( 'fileSet' , 'thumbnail' ).load() ;
		expect( content.toString() ).to.be( "not a thumbnail png" ) ;

		content = await dbImage.getAttachment( 'fileSet' , 'small' ).load() ;
		expect( content.toString() ).to.be( "not a small png" ) ;



		// Now remove 2 variants

		dbImage.removeAttachment( 'fileSet' , 'source' ) ;
		dbImage.removeAttachment( 'fileSet' , 'thumbnail' ) ;

		await dbImage.save() ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( source.path , fs.R_OK ) ; } ).to.throw( Error , { code: 'ENOENT' } ) ;
			expect( () => { fs.accessSync( thumbnail.path , fs.R_OK ) ; } ).to.throw( Error , { code: 'ENOENT' } ) ;
			expect( () => { fs.accessSync( small.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		dbImage = await images.get( id ) ;
		expect( dbImage.fileSet.attachments.source ).to.be.undefined() ;
		expect( dbImage.fileSet.attachments.thumbnail ).to.be.undefined() ;
		expect( dbImage ).to.be.partially.like( {
			_id: id ,
			name: 'selfie' ,
			fileSet: {
				metadata: {} ,
				attachments: {
					small: {
						id: dbImage.fileSet.attachments.small.id ,	// Unpredictable
						filename: 'small.png' ,
						extension: 'png' ,
						contentType: 'image/png' ,
						fileSize: 15 ,
						hash: null ,
						hashType: null ,
						metadata: {}
					} ,
				}
			}
		} ) ;

		expect( dbImage.getAttachment( 'fileSet' ) ).to.be.a( rootsDb.AttachmentSet ) ;
		expect( dbImage.getAttachment( 'fileSet' , 'small' ) ).to.be.partially.like( {
			id: dbImage.fileSet.id ,
			filename: 'small.png' ,
			extension: 'png' ,
			contentType: 'image/png' ,
			fileSize: 15 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
			collectionName: 'images' ,
			documentId: id.toString() ,
			driver: images.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? IMAGES_ATTACHMENT_DIR : '' ) + dbImage.getId() + '/' + dbImage.fileSet.attachments.small.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbImage.getId() + '/' + dbImage.fileSet.attachments.small.id
		} ) ;

		expect( () => dbImage.getAttachment( 'fileSet' , 'source' ) ).to.throw( ErrorStatus , { type: 'notFound' } ) ;
		expect( () => dbImage.getAttachment( 'fileSet' , 'thumbnail' ) ).to.throw( ErrorStatus , { type: 'notFound' } ) ;

		content = await dbImage.getAttachment( 'fileSet' , 'small' ).load() ;
		expect( content.toString() ).to.be( "not a small png" ) ;
	} ) ;

	it( "should .save() a document with the 'attachmentStreams' option targeting an attachmentSet" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var image = images.createDocument( { name: 'selfie' } ) ;

		var id = image.getId() ;
		var attachmentStreams = new rootsDb.AttachmentStreams() ;

		attachmentStreams.addStream(
			new streamKit.FakeReadable( {
				timeout: 20 , chunkSize: 10 , chunkCount: 4 , filler: 'a'.charCodeAt( 0 )
			} ) ,
			'fileSet' , 'source' ,
			{ filename: 'source.png' , contentType: 'image/png' }
		) ;

		setTimeout( () => {
			attachmentStreams.addStream(
				new streamKit.FakeReadable( {
					timeout: 20 , chunkSize: 7 , chunkCount: 4 , filler: 'b'.charCodeAt( 0 )
				} ) ,
				'fileSet' , 'small' ,
				{ filename: 'small.jpg' , contentType: 'image/jpeg' }
			) ;
		} , 100 ) ;

		setTimeout( () => {
			attachmentStreams.addStream(
				new streamKit.FakeReadable( {
					timeout: 20 , chunkSize: 7 , chunkCount: 3 , filler: 'c'.charCodeAt( 0 )
				} ) ,
				'fileSet' , 'thumbnail' ,
				{ filename: 'thumbnail.jpg' , contentType: 'image/jpeg' }
			) ;
		} , 200 ) ;

		setTimeout( () => attachmentStreams.end() , 300 ) ;

		await image.save( { attachmentStreams: attachmentStreams } ) ;

		var dbImage = await images.get( id ) ;
		expect( dbImage ).to.be.partially.like( {
			_id: id ,
			name: 'selfie' ,
			fileSet: {
				metadata: {} ,
				attachments: {
					source: {
						id: dbImage.fileSet.attachments.source.id ,	// Unpredictable
						filename: 'source.png' ,
						extension: 'png' ,
						contentType: 'image/png' ,
						fileSize: 40 ,
						hash: null ,
						hashType: null ,
						metadata: {}
					} ,
					small: {
						id: dbImage.fileSet.attachments.small.id ,	// Unpredictable
						filename: 'small.jpg' ,
						extension: 'jpg' ,
						contentType: 'image/jpeg' ,
						fileSize: 28 ,
						hash: null ,
						hashType: null ,
						metadata: {}
					} ,
					thumbnail: {
						id: dbImage.fileSet.attachments.thumbnail.id ,	// Unpredictable
						filename: 'thumbnail.jpg' ,
						extension: 'jpg' ,
						contentType: 'image/jpeg' ,
						fileSize: 21 ,
						hash: null ,
						hashType: null ,
						metadata: {}
					}
				}
			}
		} ) ;

		expect( dbImage.getAttachment( 'fileSet' , 'source' ) ).to.be.partially.like( {
			filename: 'source.png' ,
			extension: 'png' ,
			contentType: 'image/png' ,
			fileSize: 40 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
		} ) ;

		await expect( dbImage.getAttachment( 'fileSet' , 'source' ).load().then( v => v.toString() ) ).to.eventually.be( 'a'.repeat( 40 ) ) ;

		expect( dbImage.getAttachment( 'fileSet' , 'small' ) ).to.be.partially.like( {
			filename: 'small.jpg' ,
			extension: 'jpg' ,
			contentType: 'image/jpeg' ,
			fileSize: 28 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
		} ) ;

		await expect( dbImage.getAttachment( 'fileSet' , 'small' ).load().then( v => v.toString() ) ).to.eventually.be( 'b'.repeat( 28 ) ) ;

		expect( dbImage.getAttachment( 'fileSet' , 'thumbnail' ) ).to.be.partially.like( {
			filename: 'thumbnail.jpg' ,
			extension: 'jpg' ,
			contentType: 'image/jpeg' ,
			fileSize: 21 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
		} ) ;

		await expect( dbImage.getAttachment( 'fileSet' , 'thumbnail' ).load().then( v => v.toString() ) ).to.eventually.be( 'c'.repeat( 21 ) ) ;
	} ) ;
} ) ;



describe( "AttachmentSet links and checksum (driver: " + ATTACHMENT_MODE + ")" , () => {

	// Here we change the 'users' collection before performing the test, so it forces hash computation
	beforeEach( clearDB ) ;
	beforeEach( () => {
		images.attachmentHashType = 'sha256' ;
	} ) ;

	afterEach( () => {
		images.attachmentHashType = null ;
	} ) ;

	it( "should create, save, and load attachments from an attachmentSet with a checksum/hash" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var image = images.createDocument( { name: 'selfie' } ) ;
		var id = image.getId() ;

		// Exists by default
		expect( image.$.fileSet ).not.to.be.a( rootsDb.AttachmentSet ) ;
		expect( image.$.fileSet ).to.equal( {} ) ;

		// Is always auto-populated
		expect( image.fileSet ).to.be.a( rootsDb.AttachmentSet ) ;

		var source = image.fileSet.set( 'source' , { filename: 'source.png' , contentType: 'image/png' } , "not a png" ) ;
		var contentHash = crypto.createHash( 'sha256' ).update( "not a png" ).digest( 'base64' ) ;

		expect( image.fileSet ).to.be.partially.like( {
			metadata: {} ,
			attachments: {
				source: {
					id: image.fileSet.attachments.source.id ,	// Unpredictable
					filename: 'source.png' ,
					extension: 'png' ,
					contentType: 'image/png' ,
					fileSize: 9 ,
					hash: contentHash ,
					hashType: 'sha256' ,
					metadata: {}
				}
			}
		} ) ;

		await image.save() ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( source.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		var dbImage = await images.get( id ) ;
		expect( dbImage ).to.be.partially.like( {
			_id: id ,
			name: 'selfie' ,
			fileSet: {
				metadata: {} ,
				attachments: {
					source: {
						id: image.fileSet.attachments.source.id ,	// Unpredictable
						filename: 'source.png' ,
						extension: 'png' ,
						contentType: 'image/png' ,
						fileSize: 9 ,
						hash: contentHash ,
						hashType: 'sha256' ,
						metadata: {}
					}
				}
			}
		} ) ;

		//var dbAttachment = dbImage.getAttachment( 'fileSet' ) ;
		expect( dbImage.fileSet ).to.be.a( rootsDb.AttachmentSet ) ;
		expect( dbImage.fileSet ).to.be.partially.like( {
			metadata: {} ,
			attachments: {
				source: {
					id: dbImage.fileSet.id ,
					filename: 'source.png' ,
					extension: 'png' ,
					contentType: 'image/png' ,
					fileSize: 9 ,
					hash: contentHash ,
					hashType: 'sha256' ,
					metadata: {} ,
					collectionName: 'images' ,
					documentId: id.toString() ,
					driver: images.attachmentDriver ,
					path: ( ATTACHMENT_MODE === 'file' ? IMAGES_ATTACHMENT_DIR : '' ) + dbImage.getId() + '/' + dbImage.fileSet.attachments.source.id ,
					publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbImage.getId() + '/' + dbImage.fileSet.attachments.source.id
				}
			}
		} ) ;

		var content = await dbImage.fileSet.get( 'source' ).load() ;
		expect( content.toString() ).to.be( "not a png" ) ;
	} ) ;

	it( "using .getAttachment()/.setAttachment() API with attachmentSet and checksum/hash" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var image = images.createDocument( { name: 'selfie' } ) ;
		var id = image.getId() ;

		var source = image.setAttachment( 'fileSet' , 'source' , { filename: 'source.png' , contentType: 'image/png' } , "not a png" ) ;
		var contentHash = crypto.createHash( 'sha256' ).update( "not a png" ).digest( 'base64' ) ;

		expect( image.getAttachment( 'fileSet' ) ).to.be.a( rootsDb.AttachmentSet ) ;
		expect( image.getAttachment( 'fileSet' ) ).to.be.partially.like( {
			metadata: {} ,
			attachments: {
				source: {
					id: image.fileSet.attachments.source.id ,	// Unpredictable
					filename: 'source.png' ,
					extension: 'png' ,
					contentType: 'image/png' ,
					fileSize: 9 ,
					hash: contentHash ,
					hashType: 'sha256' ,
					metadata: {}
				}
			}
		} ) ;
		expect( image.getAttachment( 'fileSet' , 'source' ) ).to.be.partially.like( {
			id: image.fileSet.attachments.source.id ,	// Unpredictable
			filename: 'source.png' ,
			extension: 'png' ,
			contentType: 'image/png' ,
			fileSize: 9 ,
			hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {}
		} ) ;

		await image.save() ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( source.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		var dbImage = await images.get( id ) ;
		expect( dbImage ).to.be.partially.like( {
			_id: id ,
			name: 'selfie' ,
			fileSet: {
				metadata: {} ,
				attachments: {
					source: {
						id: image.fileSet.attachments.source.id ,	// Unpredictable
						filename: 'source.png' ,
						extension: 'png' ,
						contentType: 'image/png' ,
						fileSize: 9 ,
						hash: contentHash ,
						hashType: 'sha256' ,
						metadata: {}
					}
				}
			}
		} ) ;

		expect( dbImage.getAttachment( 'fileSet' ) ).to.be.a( rootsDb.AttachmentSet ) ;
		expect( dbImage.getAttachment( 'fileSet' ) ).to.be.partially.like( {
			metadata: {} ,
			attachments: {
				source: {
					id: dbImage.fileSet.id ,
					filename: 'source.png' ,
					extension: 'png' ,
					contentType: 'image/png' ,
					fileSize: 9 ,
					hash: contentHash ,
					hashType: 'sha256' ,
					metadata: {} ,
					collectionName: 'images' ,
					documentId: id.toString() ,
					driver: images.attachmentDriver ,
					path: ( ATTACHMENT_MODE === 'file' ? IMAGES_ATTACHMENT_DIR : '' ) + dbImage.getId() + '/' + dbImage.fileSet.attachments.source.id ,
					publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbImage.getId() + '/' + dbImage.fileSet.attachments.source.id
				}
			}
		} ) ;
		expect( dbImage.getAttachment( 'fileSet' , 'source' ) ).to.be.partially.like( {
			id: dbImage.fileSet.id ,
			filename: 'source.png' ,
			extension: 'png' ,
			contentType: 'image/png' ,
			fileSize: 9 ,
			hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
			collectionName: 'images' ,
			documentId: id.toString() ,
			driver: images.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? IMAGES_ATTACHMENT_DIR : '' ) + dbImage.getId() + '/' + dbImage.fileSet.attachments.source.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbImage.getId() + '/' + dbImage.fileSet.attachments.source.id
		} ) ;

		var content = await dbImage.getAttachment( 'fileSet' , 'source' ).load() ;
		expect( content.toString() ).to.be( "not a png" ) ;
		content = await dbImage.getAttachment( 'fileSet' ).get( 'source' ).load() ;
		expect( content.toString() ).to.be( "not a png" ) ;
	} ) ;

	it( "should .save() a document with the 'attachmentStreams' option targeting an attachmentSet" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var image = images.createDocument( { name: 'selfie' } ) ;

		var id = image.getId() ;

		var contentHash = [
				crypto.createHash( 'sha256' ).update( 'a'.repeat( 40 ) ).digest( 'base64' ) ,
				crypto.createHash( 'sha256' ).update( 'b'.repeat( 28 ) ).digest( 'base64' ) ,
				crypto.createHash( 'sha256' ).update( 'c'.repeat( 21 ) ).digest( 'base64' )
			] ;

		var attachmentStreams = new rootsDb.AttachmentStreams() ;

		attachmentStreams.addStream(
			new streamKit.FakeReadable( {
				timeout: 20 , chunkSize: 10 , chunkCount: 4 , filler: 'a'.charCodeAt( 0 )
			} ) ,
			'fileSet' , 'source' ,
			{ filename: 'source.png' , contentType: 'image/png' }
		) ;

		setTimeout( () => {
			attachmentStreams.addStream(
				new streamKit.FakeReadable( {
					timeout: 20 , chunkSize: 7 , chunkCount: 4 , filler: 'b'.charCodeAt( 0 )
				} ) ,
				'fileSet' , 'small' ,
				{ filename: 'small.jpg' , contentType: 'image/jpeg' }
			) ;
		} , 100 ) ;

		setTimeout( () => {
			attachmentStreams.addStream(
				new streamKit.FakeReadable( {
					timeout: 20 , chunkSize: 7 , chunkCount: 3 , filler: 'c'.charCodeAt( 0 )
				} ) ,
				'fileSet' , 'thumbnail' ,
				{ filename: 'thumbnail.jpg' , contentType: 'image/jpeg' }
			) ;
		} , 200 ) ;

		setTimeout( () => attachmentStreams.end() , 300 ) ;

		await image.save( { attachmentStreams: attachmentStreams } ) ;

		var dbImage = await images.get( id ) ;
		expect( dbImage ).to.be.partially.like( {
			_id: id ,
			name: 'selfie' ,
			fileSet: {
				metadata: {} ,
				attachments: {
					source: {
						id: dbImage.fileSet.attachments.source.id ,	// Unpredictable
						filename: 'source.png' ,
						extension: 'png' ,
						contentType: 'image/png' ,
						fileSize: 40 ,
						hash: contentHash[ 0 ] ,
						hashType: 'sha256' ,
						metadata: {}
					} ,
					small: {
						id: dbImage.fileSet.attachments.small.id ,	// Unpredictable
						filename: 'small.jpg' ,
						extension: 'jpg' ,
						contentType: 'image/jpeg' ,
						fileSize: 28 ,
						hash: contentHash[ 1 ] ,
						hashType: 'sha256' ,
						metadata: {}
					} ,
					thumbnail: {
						id: dbImage.fileSet.attachments.thumbnail.id ,	// Unpredictable
						filename: 'thumbnail.jpg' ,
						extension: 'jpg' ,
						contentType: 'image/jpeg' ,
						fileSize: 21 ,
						hash: contentHash[ 2 ] ,
						hashType: 'sha256' ,
						metadata: {}
					}
				}
			}
		} ) ;

		expect( dbImage.getAttachment( 'fileSet' , 'source' ) ).to.be.partially.like( {
			filename: 'source.png' ,
			extension: 'png' ,
			contentType: 'image/png' ,
			fileSize: 40 ,
			hash: contentHash[ 0 ] ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		await expect( dbImage.getAttachment( 'fileSet' , 'source' ).load().then( v => v.toString() ) ).to.eventually.be( 'a'.repeat( 40 ) ) ;

		expect( dbImage.getAttachment( 'fileSet' , 'small' ) ).to.be.partially.like( {
			filename: 'small.jpg' ,
			extension: 'jpg' ,
			contentType: 'image/jpeg' ,
			fileSize: 28 ,
			hash: contentHash[ 1 ] ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		await expect( dbImage.getAttachment( 'fileSet' , 'small' ).load().then( v => v.toString() ) ).to.eventually.be( 'b'.repeat( 28 ) ) ;

		expect( dbImage.getAttachment( 'fileSet' , 'thumbnail' ) ).to.be.partially.like( {
			filename: 'thumbnail.jpg' ,
			extension: 'jpg' ,
			contentType: 'image/jpeg' ,
			fileSize: 21 ,
			hash: contentHash[ 2 ] ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		await expect( dbImage.getAttachment( 'fileSet' , 'thumbnail' ).load().then( v => v.toString() ) ).to.eventually.be( 'c'.repeat( 21 ) ) ;
	} ) ;
} ) ;



describe( "Locks" , () => {

	beforeEach( clearDB ) ;

	it( "should lock a document (create, save, lock, retrieve, lock, retrieve)" , async () => {
		var lockable = lockables.createDocument( { data: 'something' } ) ,
			id = lockable.getId() ,
			dbLockable , lockId ;

		await lockable.save() ;
		await expect( lockables.get( id ) ).to.eventually.equal( {
			_id: id , data: 'something' , _lockedBy: null , _lockedAt: null
		} ) ;

		lockId = await lockable.lock() ;
		expect( lockId ).to.be.an( mongodb.ObjectId ) ;
		expect( lockable._.meta.lockId ).to.be.an( mongodb.ObjectId ) ;
		expect( lockable._.meta.lockId ).to.be( lockId ) ;

		dbLockable = await lockables.get( id ) ;
		expect( dbLockable ).to.equal( {
			_id: id , data: 'something' , _lockedBy: lockId , _lockedAt: dbLockable._lockedAt
		} ) ;
		expect( dbLockable._lockedAt ).to.be.a( Date ) ;

		await expect( lockable.lock() ).to.eventually.be( null ) ;
		await expect( lockable.unlock() ).to.eventually.be( true ) ;
		await expect( lockable.lock() ).to.eventually.be.a( mongodb.ObjectId ) ;
	} ) ;

	it( "Document#lock() on a local (non-upstream) document" , async () => {
		var lockable = lockables.createDocument( { data: 'something' } ) ,
			id = lockable.getId();

		var lockId = await lockable.lock() ;
		expect( lockId ).to.be.truthy() ;
		expect( lockId ).to.be.a( mongodb.ObjectId ) ;
		expect( lockable._.meta.lockId ).to.be( lockId ) ;
		await lockable.save() ;

		var dbLockable2 = await lockables.get( id ) ;
		expect( dbLockable2.data ).to.equal( 'something' ) ;
		expect( '' + dbLockable2._lockedBy ).to.equal( '' + lockId ) ;

		var lockId2 = await dbLockable2.lock() ;
		expect( lockId2 ).to.be( null ) ;
		
		await lockable.unlock() ;
		expect( lockable._.meta.lockId ).to.be( null ) ;
		
		lockId2 = await dbLockable2.lock() ;
		expect( lockId2 ).to.be.a( mongodb.ObjectId ) ;
	} ) ;

	it.opt( "should perform a Collection#lockingFind(): lock, retrieve locked document, then manually release locks" , async () => {
		var batch = lockables.createBatch( [
			{ data: 'one' } ,
			{ data: 'two' } ,
			{ data: 'three' } ,
			{ data: 'four' } ,
			{ data: 'five' } ,
			{ data: 'six' }
		] ) ;

		await batch.save() ;

		await Promise.all( [
			// First lock
			( async () => {
				let { lockId , batch: dbBatch } = await lockables.lockingFind( { data: { $in: [ 'one' , 'two' ] } } ) ;

				expect( dbBatch ).to.be.a( rootsDb.Batch ) ;
				expect( dbBatch ).to.have.length( 2 ) ;
				expect( lockId ).to.be.an( mongodb.ObjectId ) ;
				expect( dbBatch.meta.lockId ).to.be.an( mongodb.ObjectId ) ;
				expect( dbBatch.meta.lockId ).to.be( lockId ) ;

				// Check that the lockId is set on the document
				expect( dbBatch[ 0 ]._.meta.lockId ).to.be( lockId ) ;

				var map = {} ;
				dbBatch.forEach( doc => map[ doc.data ] = doc ) ;

				expect( map ).to.partially.equal( {
					one: { data: 'one' } ,
					two: { data: 'two' }
				} ) ;

				await Promise.resolveTimeout( 30 ) ;
				await dbBatch.releaseLocks() ;
			} )() ,

			// Second lock: should not lock item #1 and #2, only #3
			( async () => {
				await Promise.resolveTimeout( 0 ) ;
				let { lockId , batch: dbBatch } = await lockables.lockingFind( { data: { $in: [ 'one' , 'two' , 'three' ] } } ) ;

				expect( dbBatch ).to.have.length( 1 ) ;
				expect( dbBatch ).to.be.partially.like( [ { data: 'three' } ] ) ;
				expect( lockId ).to.be.an( mongodb.ObjectId ) ;
				expect( dbBatch.meta.lockId ).to.be( lockId ) ;
				
				// Check that the lockId is set on the document
				expect( dbBatch[ 0 ]._.meta.lockId ).to.be( lockId ) ;

				await Promise.resolveTimeout( 30 ) ;
				await dbBatch.releaseLocks() ;
			} )() ,

			// Thid lock: should not lock item #1 #2 and #3 but returns them as the 'other' batch, 'our' batch contains #4 #5
			( async () => {
				await Promise.resolveTimeout( 0 ) ;
				let { lockId , batch: dbBatch , otherBatch: otherDbBatch } = await lockables.lockingFind( { data: { $in: [ 'one' , 'two' , 'three' , 'four' , 'five' ] } } , { other: true } ) ;

				expect( dbBatch ).to.be.a( rootsDb.Batch ) ;
				expect( dbBatch ).to.have.length( 2 ) ;
				expect( dbBatch ).to.be.partially.like( [ { data: 'four' } , { data: 'five' } ] ) ;
				expect( dbBatch.meta.lockId ).to.be.an( mongodb.ObjectId ) ;
				expect( dbBatch.meta.lockId ).to.be( lockId ) ;

				// Check that the lockId is set on the document
				expect( dbBatch[ 0 ]._.meta.lockId ).to.be( lockId ) ;

				expect( otherDbBatch ).to.be.a( rootsDb.Batch ) ;
				expect( otherDbBatch ).to.have.length( 3 ) ;
				expect( otherDbBatch ).to.be.partially.like( [ { data: 'one' } , { data: 'two' } , { data: 'three' } ] ) ;
				expect( otherDbBatch.meta.lockId ).to.be( null ) ;
				expect( otherDbBatch.meta.lockId ).to.be( null ) ;
				
				await Promise.resolveTimeout( 30 ) ;
				await dbBatch.releaseLocks() ;
			} )() ,

			// Fourth lock that lock/retrieve nothing
			( async () => {
				await Promise.resolveTimeout( 10 ) ;
				let { lockId , batch: dbBatch } = await lockables.lockingFind( { data: { $in: [ 'one' , 'two' , 'three' ] } } ) ;
				
				expect( dbBatch ).to.have.length( 0 ) ;
			} )()
		] ) ;
		
		var { lockId: lockId2 , batch: dbBatch2 } = await lockables.lockingFind( { data: { $in: [ 'one' , 'two' , 'three' ] } } ) ;
		expect( dbBatch2 ).to.have.length( 3 ) ;
		expect( dbBatch2.meta.lockId ).to.be( lockId2 ) ;
		expect( dbBatch2[ 0 ]._.meta.lockId ).to.be( lockId2 ) ;

		// Check that immediately after, the data are NOT available
		var { lockId: lockId3 , batch: dbBatch3 } = await lockables.lockingFind( { data: { $in: [ 'one' , 'two' , 'three' ] } } ) ;
		expect( lockId3 ).to.be( null ) ;
		expect( dbBatch3.meta.lockId ).to.be( null ) ;
		expect( dbBatch3 ).to.have.length( 0 ) ;

		// Check that immediately after releasing the lock, the data are available
		await dbBatch2.releaseLocks() ;
		var { lockId: lockId4 , batch: dbBatch4 } = await lockables.lockingFind( { data: { $in: [ 'one' , 'two' , 'three' ] } } ) ;
		expect( dbBatch4 ).to.have.length( 3 ) ;
		expect( dbBatch4.meta.lockId ).to.be( lockId4 ) ;
		expect( dbBatch4[ 0 ]._.meta.lockId ).to.be( lockId4 ) ;
	} ) ;

	it.opt( "mixing Collection#lockingFind() and Document#unlock()" , async () => {
		var batch = lockables.createBatch( [
			{ data: 'one' } ,
			{ data: 'two' } ,
			{ data: 'three' } ,
			{ data: 'four' } ,
			{ data: 'five' } ,
			{ data: 'six' }
		] ) ;

		await batch.save() ;

		var { lockId: lockId1 , batch: dbBatch1 } = await lockables.lockingFind( { data: { $in: [ 'one' , 'two' , 'three' ] } } ) ;
		expect( dbBatch1 ).to.have.length( 3 ) ;
		expect( dbBatch1.meta.lockId ).to.be( lockId1 ) ;
		expect( dbBatch1[ 0 ]._.meta.lockId ).to.be( lockId1 ) ;
		var index = dbBatch1.findIndex( e => e.data === 'two' ) ;
		await dbBatch1[ index ].unlock() ;

		var { lockId: lockId2 , batch: dbBatch2 } = await lockables.lockingFind( { data: { $in: [ 'one' , 'two' , 'three' ] } } ) ;
		expect( dbBatch2 ).to.have.length( 1 ) ;
		expect( dbBatch2.meta.lockId ).to.be( lockId2 ) ;
		expect( dbBatch2[ 0 ]._.meta.lockId ).to.be( lockId2 ) ;
		expect( dbBatch2[ 0 ].data ).to.be( 'two' ) ;

		await dbBatch1.releaseLocks() ;
		expect( dbBatch1[ 0 ]._.meta.lockId ).to.be( null ) ;
		expect( dbBatch1[ 1 ]._.meta.lockId ).to.be( null ) ;
		expect( dbBatch1[ 2 ]._.meta.lockId ).to.be( null ) ;
		await dbBatch2.releaseLocks() ;
		expect( dbBatch2[ 0 ]._.meta.lockId ).to.be( null ) ;

		// Check that immediately after releasing the lock, the data are available
		var { lockId: lockId3 , batch: dbBatch3 } = await lockables.lockingFind( { data: { $in: [ 'one' , 'two' , 'three' ] } } ) ;
		expect( dbBatch3 ).to.have.length( 3 ) ;
		expect( dbBatch3.meta.lockId ).to.be( lockId3 ) ;
		expect( dbBatch3[ 0 ]._.meta.lockId ).to.be( lockId3 ) ;
	} ) ;

	it.opt( "should perform a Collection#lockingFind() with the action callback variant: lock, retrieve locked document, then auto release locks" , async () => {
		var batch = lockables.createBatch( [
			{ data: 'one' } ,
			{ data: 'two' } ,
			{ data: 'three' } ,
			{ data: 'four' } ,
			{ data: 'five' } ,
			{ data: 'six' }
		] ) ;

		await batch.save() ;

		await Promise.all( [
			// First lock
			lockables.lockingFind( { data: { $in: [ 'one' , 'two' ] } } , ( lockId , dbBatch ) => {
				expect( dbBatch ).to.be.a( rootsDb.Batch ) ;
				expect( dbBatch ).to.have.length( 2 ) ;
				expect( lockId ).to.be.an( mongodb.ObjectId ) ;
				expect( dbBatch.meta.lockId ).to.be.an( mongodb.ObjectId ) ;
				expect( dbBatch.meta.lockId ).to.be( lockId ) ;

				var map = {} ;
				dbBatch.forEach( doc => map[ doc.data ] = doc ) ;

				expect( map ).to.partially.equal( {
					one: { data: 'one' } ,
					two: { data: 'two' }
				} ) ;

				return Promise.resolveTimeout( 30 ) ;
			} ) ,

			// Second lock: should not lock item #1 and #2, only #3
			Promise.resolveTimeout( 0 ).then( () => lockables.lockingFind( { data: { $in: [ 'one' , 'two' , 'three' ] } } , ( lockId , dbBatch ) => {
				expect( dbBatch ).to.have.length( 1 ) ;
				expect( dbBatch ).to.be.partially.like( [ { data: 'three' } ] ) ;
				expect( lockId ).to.be.an( mongodb.ObjectId ) ;
				expect( dbBatch.meta.lockId ).to.be( lockId ) ;
				return Promise.resolveTimeout( 30 ) ;
			} ) ) ,

			// Thid lock: should not lock item #1 #2 and #3 but returns them as the 'other' batch, 'our' batch contains #4 #5
			Promise.resolveTimeout( 0 ).then( () => lockables.lockingFind( { data: { $in: [ 'one' , 'two' , 'three' , 'four' , 'five' ] } } , { other: true } , ( lockId , dbBatch , otherDbBatch ) => {
				expect( dbBatch ).to.be.a( rootsDb.Batch ) ;
				expect( dbBatch ).to.have.length( 2 ) ;
				expect( dbBatch ).to.be.partially.like( [ { data: 'four' } , { data: 'five' } ] ) ;
				expect( dbBatch.meta.lockId ).to.be.an( mongodb.ObjectId ) ;
				expect( dbBatch.meta.lockId ).to.be( lockId ) ;
				expect( otherDbBatch ).to.be.a( rootsDb.Batch ) ;
				expect( otherDbBatch ).to.have.length( 3 ) ;
				expect( otherDbBatch ).to.be.partially.like( [ { data: 'one' } , { data: 'two' } , { data: 'three' } ] ) ;
				expect( otherDbBatch.meta.lockId ).to.be( null ) ;
				expect( otherDbBatch.meta.lockId ).to.be( null ) ;
				return Promise.resolveTimeout( 30 ) ;
			} ) ) ,

			// Fourth lock that lock/retrieve nothing
			Promise.resolveTimeout( 10 ).then( () => lockables.lockingFind( { data: { $in: [ 'one' , 'two' , 'three' ] } } , ( lockId , dbBatch ) => {
				expect( dbBatch ).to.have.length( 0 ) ;
			} ) )
		] ) ;

		await lockables.lockingFind( { data: { $in: [ 'one' , 'two' , 'three' ] } } , ( lockId , dbBatch ) => {
			expect( dbBatch ).to.have.length( 3 ) ;
		} ) ;

		// Check that immediatley after 'await', the data are available
		await lockables.lockingFind( { data: { $in: [ 'one' , 'two' , 'three' ] } } , ( lockId , dbBatch ) => {
			expect( dbBatch ).to.have.length( 3 ) ;
		} ) ;
	} ) ;
} ) ;





describe( "Freeze documents" , () => {

	beforeEach( clearDB ) ;

	it( "should freeze a document (create, save, freeze, modify, unfreeze, modify)" , async () => {
		var freezable = freezables.createDocument( { name: 'Bob' , data: { a: 1 , b: 2 } } ) ,
			id = freezable.getId() ,
			dbFreezable ;

		await freezable.save() ;
		dbFreezable = await freezables.get( id ) ;
		expect( dbFreezable ).to.equal( {
			_id: id , name: 'Bob' , data: { a: 1 , b: 2 } , _frozen: false
		} ) ;

		// Check that the property cannot be set manually, it's an internal RootsDB property
		expect( () => dbFreezable._frozen = true ).to.throw() ;
		expect( () => dbFreezable.patch( { _frozen: true } ) ).to.throw() ;


		// First check when not frozen

		dbFreezable.name = 'Alice' ;
		dbFreezable.data.b = 3 ;
		dbFreezable.data.c = 4 ;
		expect( dbFreezable ).to.equal( {
			_id: id , name: 'Alice' , data: { a: 1 , b: 3 , c: 4 } , _frozen: false
		} ) ;

		// Get it back
		await dbFreezable.save() ;
		dbFreezable = await freezables.get( id ) ;
		expect( dbFreezable ).to.equal( {
			_id: id , name: 'Alice' , data: { a: 1 , b: 3 , c: 4 } , _frozen: false
		} ) ;

		
		// Now check when frozen
		
		await dbFreezable.freeze() ;

		expect( () => dbFreezable.name = 'Charly' ).to.throw() ;
		expect( () => dbFreezable.data.b = 5 ).to.throw() ;
		expect( () => dbFreezable.data.d = 6 ).to.throw() ;
		expect( dbFreezable ).to.equal( {
			_id: id , name: 'Alice' , data: { a: 1 , b: 3 , c: 4 } , _frozen: true
		} ) ;

		// Get it back
		await expect( () => dbFreezable.save() ).to.eventually.throw() ;
		dbFreezable = await freezables.get( id ) ;
		expect( dbFreezable ).to.equal( {
			_id: id , name: 'Alice' , data: { a: 1 , b: 3 , c: 4 } , _frozen: true
		} ) ;


		// Now check when unfrozen

		await dbFreezable.unfreeze() ;

		dbFreezable.name = 'Dan' ;
		dbFreezable.data.b = 7 ;
		dbFreezable.data.e = 8 ;
		expect( dbFreezable ).to.equal( {
			_id: id , name: 'Dan' , data: { a: 1 , b: 7 , c: 4 , e: 8 } , _frozen: false
		} ) ;

		// Get it back
		await dbFreezable.save() ;
		dbFreezable = await freezables.get( id ) ;
		expect( dbFreezable ).to.equal( {
			_id: id , name: 'Dan' , data: { a: 1 , b: 7 , c: 4 , e: 8 } , _frozen: false
		} ) ;


		// Modify using Document#patch()
		
		dbFreezable.patch( { name: 'Elisa' } ) ;
		expect( dbFreezable ).to.equal( {
			_id: id , name: 'Elisa' , data: { a: 1 , b: 7 , c: 4 , e: 8 } , _frozen: false
		} ) ;

		await dbFreezable.saveAndFreeze() ;

		expect( () => dbFreezable.patch( { name: 'Fanny' } ) ).to.throw() ;
		expect( dbFreezable ).to.equal( {
			_id: id , name: 'Elisa' , data: { a: 1 , b: 7 , c: 4 , e: 8 } , _frozen: true
		} ) ;

		// Get it back
		//await dbFreezable.save() ;	// Already saved by .saveAndFreeze()
		dbFreezable = await freezables.get( id ) ;
		expect( dbFreezable ).to.equal( {
			_id: id , name: 'Elisa' , data: { a: 1 , b: 7 , c: 4 , e: 8 } , _frozen: true
		} ) ;


		// Modify using direct .raw access

		// There is no proxy here, so it's possible to change it...
		dbFreezable._.raw.name = 'Garry' ;
		expect( dbFreezable ).to.equal( {
			_id: id , name: 'Garry' , data: { a: 1 , b: 7 , c: 4 , e: 8 } , _frozen: true
		} ) ;

		// ... but there is no possible way to save changes...
		await expect( () => dbFreezable.save() ).to.eventually.throw() ;
		expect( () => dbFreezable.stage( 'name' ) ).to.throw() ;
		await expect( () => dbFreezable.commit() ).to.eventually.throw() ;

		// Get it back
		dbFreezable = await freezables.get( id ) ;
		expect( dbFreezable ).to.equal( {
			_id: id , name: 'Elisa' , data: { a: 1 , b: 7 , c: 4 , e: 8 } , _frozen: true
		} ) ;
	} ) ;
} ) ;





describe( "Immutable properties" , () => {

	beforeEach( clearDB ) ;

	it( "should create a document having an immutable property and try to set, reset, delete that property" , async () => {
		var doc = immutableProperties.createDocument( { name: 'Bob' , immutableData: 'random' } ) ,
			id = doc.getId() ,
			dbDoc ;

		await doc.save() ;
		dbDoc = await immutableProperties.get( id ) ;
		expect( dbDoc ).to.equal( {
			_id: id , name: 'Bob' , immutableData: 'random'
		} ) ;


		// First check the proxy accesses

		dbDoc.name = 'Alice' ;
		expect( () => dbDoc.immutableData = 'random2' ).to.throw() ;
		expect( () => delete dbDoc.immutableData ).to.throw() ;
		expect( dbDoc ).to.equal( {
			_id: id , name: 'Alice' , immutableData: 'random'
		} ) ;

		// Get it back
		await dbDoc.save() ;
		dbDoc = await immutableProperties.get( id ) ;
		expect( dbDoc ).to.equal( {
			_id: id , name: 'Alice' , immutableData: 'random'
		} ) ;


		// Modify using Document#patch()
		
		expect( () => dbDoc.patch( { name: 'Charly' , immutableData: 'random2' } , { validate: true } ) ).to.throw() ;
		expect( () => dbDoc.patch( { immutableData: 'random2' } ) ).to.throw() ;
		expect( dbDoc ).to.equal( {
			_id: id , name: 'Alice' , immutableData: 'random'
		} ) ;
		// Since we do not validate, it fails on the proxy side, so 'name' is already changed
		expect( () => dbDoc.patch( { name: 'Charly' , immutableData: 'random2' } ) ).to.throw() ;
		expect( dbDoc ).to.equal( {
			_id: id , name: 'Charly' , immutableData: 'random'
		} ) ;
		dbDoc.patch( { name: 'Dan' } ) ;
		expect( dbDoc ).to.equal( {
			_id: id , name: 'Dan' , immutableData: 'random'
		} ) ;
	} ) ;

	// For instance, there is no way to avoid modifying an immutable property using direct raw access...
	it.opt( "should create a document having an immutable property and try to set it using direct raw access" , async () => {
		var doc = immutableProperties.createDocument( { name: 'Bob' , immutableData: 'random' } ) ,
			id = doc.getId() ,
			dbDoc ;

		await doc.save() ;
		dbDoc = await immutableProperties.get( id ) ;
		expect( dbDoc ).to.equal( {
			_id: id , name: 'Bob' , immutableData: 'random'
		} ) ;


		// Modify using direct .raw access

		// There is no proxy here, so it's possible to change it...
		dbDoc._.raw.immutableData = 'random2' ;
		expect( dbDoc ).to.equal( {
			_id: id , name: 'Bob' , immutableData: 'random2'
		} ) ;

		// ... but there is no possible way to save changes...
		await expect( () => dbDoc.save() ).to.eventually.throw() ;
		expect( () => dbDoc.stage( 'immutableData' ) ).to.throw() ;
		await expect( () => dbDoc.commit() ).to.eventually.throw() ;

		// Get it back
		dbDoc = await immutableProperties.get( id ) ;
		expect( dbDoc ).to.equal( {
			_id: id , name: 'Bob' , immutableData: 'random'
		} ) ;
	} ) ;
} ) ;





describe( "Populate links" , () => {

	beforeEach( clearDB ) ;

	it( "link population as a .get() option (create both, link, save both, get with populate option)" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		//console.log( job ) ;
		var jobId = job.getId() ;

		user.setLink( 'job' , job ) ;

		await job.save() ;
		await user.save() ;

		var stats = {} ;
		var dbUser = await users.get( id , { populate: 'job' , stats } ) ;

		expect( dbUser ).to.equal( {
			_id: id , job: job , firstName: 'Jilbert' , lastName: 'Polson' , memberSid: 'Jilbert Polson'
		} ) ;

		expect( stats.population.depth ).to.be( 1 ) ;
		expect( stats.population.dbQueries ).to.be( 1 ) ;
	} ) ;

	it( "link population on Document instances (create both, link, save both, get then populate)" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		//console.log( job ) ;
		var jobId = job.getId() ;

		user.setLink( 'job' , job ) ;

		await job.save() ;
		await user.save() ;

		var dbUser = await users.get( id ) ;
		await dbUser.populate( [ 'job' ] ) ;

		expect( dbUser ).to.equal( {
			_id: id , job: job , firstName: 'Jilbert' , lastName: 'Polson' , memberSid: 'Jilbert Polson'
		} ) ;
	} ) ;

	it( "multiple links population as a .get() option (create, link, save, get with populate option)" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var godfather = users.createDocument( {
			firstName: 'DA' ,
			lastName: 'GODFATHER'
		} ) ;

		var id = user.getId() ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		// Link the documents!
		user.setLink( 'job' , job ) ;
		user.setLink( 'godfather' , godfather ) ;

		await job.save() ;
		await godfather.save() ;
		await user.save() ;

		var stats = {} ;
		var dbUser = await users.get( id , { populate: [ 'job' , 'godfather' ] , stats } ) ;

		expect( dbUser ).to.equal( {
			_id: id , job: job , godfather: godfather , firstName: 'Jilbert' , lastName: 'Polson' , memberSid: 'Jilbert Polson'
		} ) ;

		expect( stats.population.depth ).to.be( 1 ) ;
		expect( stats.population.dbQueries ).to.be( 2 ) ;
	} ) ;

	it( "multiple link population on Document instances (create, link, save, get then populate)" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var godfather = users.createDocument( {
			firstName: 'DA' ,
			lastName: 'GODFATHER'
		} ) ;

		var id = user.getId() ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		// Link the documents!
		user.setLink( 'job' , job ) ;
		user.setLink( 'godfather' , godfather ) ;

		await job.save() ;
		await godfather.save() ;
		await user.save() ;

		var dbUser = await users.get( id ) ;
		await dbUser.populate( [ 'job' , 'godfather' ] ) ;

		expect( dbUser ).to.equal( {
			_id: id , job: job , godfather: godfather , firstName: 'Jilbert' , lastName: 'Polson' , memberSid: 'Jilbert Polson'
		} ) ;
	} ) ;

	it( "multiple link population as a .get() option, having same and circular target" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		var connection = users.createDocument( {
			firstName: 'John' ,
			lastName: 'Fergusson'
		} ) ;

		// Link the documents!
		user.setLink( 'connection.A' , connection ) ;
		user.setLink( 'connection.B' , connection ) ;
		user.setLink( 'connection.C' , user ) ;

		await connection.save() ;
		await user.save() ;

		var stats = {} ;
		var dbUser = await users.get( id , { populate: [ 'connection.A' , 'connection.B' , 'connection.C' ] , stats } ) ;

		expect( dbUser.connection.A ).to.be( dbUser.connection.B ) ;
		expect( dbUser.connection.C ).to.be( dbUser ) ;
		expect( dbUser ).to.equal( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			connection: {
				A: connection ,
				B: connection ,
				C: dbUser
			} ,
			memberSid: 'Jilbert Polson'
		} ) ;

		expect( stats.population.depth ).to.be( 1 ) ;
		expect( stats.population.dbQueries ).to.be( 1 ) ;
	} ) ;

	it( "collect batch with multiple link population as a .collect() option (create, link, save, collect with populate option)" , async () => {
		var user1 = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var user2 = users.createDocument( {
			firstName: 'Thomas' ,
			lastName: 'Campbell'
		} ) ;

		var user3 = users.createDocument( {
			firstName: 'Harry' ,
			lastName: 'Campbell'
		} ) ;

		var godfather = users.createDocument( {
			firstName: 'DA' ,
			lastName: 'GODFATHER'
		} ) ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		// Link the documents!
		user1.setLink( 'job' , job ) ;
		user1.setLink( 'godfather' , godfather ) ;
		user3.setLink( 'godfather' , godfather ) ;

		await Promise.all( [ job.save() , godfather.save() ] ) ;
		await Promise.all( [ user1.save() , user2.save() , user3.save() ] ) ;

		var stats = {} ;
		var dbUserBatch = await users.collect( {} , { populate: [ 'job' , 'godfather' ] , stats } ) ;

		// Sort that first...
		dbUserBatch.sort( ascendingSortFn( 'firstName' ) ) ;

		expect( dbUserBatch ).to.be.like( [
			{
				firstName: 'DA' ,
				lastName: 'GODFATHER' ,
				_id: dbUserBatch[ 0 ]._id ,
				memberSid: 'DA GODFATHER'
			} ,
			{
				firstName: 'Harry' ,
				lastName: 'Campbell' ,
				_id: dbUserBatch[ 1 ]._id ,
				memberSid: 'Harry Campbell' ,
				godfather: {
					firstName: 'DA' ,
					lastName: 'GODFATHER' ,
					_id: dbUserBatch[ 0 ]._id ,
					memberSid: 'DA GODFATHER'
				}
			} ,
			{
				firstName: 'Jilbert' ,
				lastName: 'Polson' ,
				_id: dbUserBatch[ 2 ]._id ,
				memberSid: 'Jilbert Polson' ,
				job: {
					title: 'developer' ,
					salary: 60000 ,
					users: [] ,
					schools: [] ,
					_id: job._id
				} ,
				godfather: {
					firstName: 'DA' ,
					lastName: 'GODFATHER' ,
					_id: dbUserBatch[ 0 ]._id ,
					memberSid: 'DA GODFATHER'
				}
			} ,
			{
				firstName: 'Thomas' ,
				lastName: 'Campbell' ,
				_id: dbUserBatch[ 3 ]._id ,
				memberSid: 'Thomas Campbell'
			}
		] ) ;

		expect( stats.population.depth ).to.be( 1 ) ;
		// Only one DB query, since the godfather is a user and all users have been collected before the populate pass
		expect( stats.population.dbQueries ).to.be( 1 ) ;
	} ) ;

	it( "collect batch with multiple link population on Batch instances (create, link, save, collect then populate)" , async () => {
		var user1 = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var user2 = users.createDocument( {
			firstName: 'Thomas' ,
			lastName: 'Campbell'
		} ) ;

		var user3 = users.createDocument( {
			firstName: 'Harry' ,
			lastName: 'Campbell'
		} ) ;

		var godfather = users.createDocument( {
			firstName: 'DA' ,
			lastName: 'GODFATHER'
		} ) ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		// Link the documents!
		user1.setLink( 'job' , job ) ;
		user1.setLink( 'godfather' , godfather ) ;
		user3.setLink( 'godfather' , godfather ) ;

		await Promise.all( [ job.save() , godfather.save() ] ) ;
		await Promise.all( [ user1.save() , user2.save() , user3.save() ] ) ;

		var dbUserBatch = await users.collect( {} ) ;
		await dbUserBatch.populate( [ 'job' , 'godfather' ] ) ;

		// Sort that first...
		dbUserBatch.sort( ascendingSortFn( 'firstName' ) ) ;

		expect( dbUserBatch ).to.be.like( [
			{
				firstName: 'DA' ,
				lastName: 'GODFATHER' ,
				_id: dbUserBatch[ 0 ]._id ,
				memberSid: 'DA GODFATHER'
			} ,
			{
				firstName: 'Harry' ,
				lastName: 'Campbell' ,
				_id: dbUserBatch[ 1 ]._id ,
				memberSid: 'Harry Campbell' ,
				godfather: {
					firstName: 'DA' ,
					lastName: 'GODFATHER' ,
					_id: dbUserBatch[ 0 ]._id ,
					memberSid: 'DA GODFATHER'
				}
			} ,
			{
				firstName: 'Jilbert' ,
				lastName: 'Polson' ,
				_id: dbUserBatch[ 2 ]._id ,
				memberSid: 'Jilbert Polson' ,
				job: {
					title: 'developer' ,
					salary: 60000 ,
					users: [] ,
					schools: [] ,
					_id: job._id
				} ,
				godfather: {
					firstName: 'DA' ,
					lastName: 'GODFATHER' ,
					_id: dbUserBatch[ 0 ]._id ,
					memberSid: 'DA GODFATHER'
				}
			} ,
			{
				firstName: 'Thomas' ,
				lastName: 'Campbell' ,
				_id: dbUserBatch[ 3 ]._id ,
				memberSid: 'Thomas Campbell'
			}
		] ) ;
	} ) ;

	it( "collect batch with multiple link population as a .collect() option, and circular references" , async () => {
		var user1 = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var user2 = users.createDocument( {
			firstName: 'Thomas' ,
			lastName: 'Campbell'
		} ) ;

		var user3 = users.createDocument( {
			firstName: 'Harry' ,
			lastName: 'Campbell'
		} ) ;

		var godfather = users.createDocument( {
			firstName: 'DA' ,
			lastName: 'GODFATHER'
		} ) ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		// Link the documents!
		user1.setLink( 'job' , job ) ;
		user1.setLink( 'godfather' , godfather ) ;
		user3.setLink( 'godfather' , godfather ) ;
		godfather.setLink( 'godfather' , godfather ) ;

		await job.save() ;
		await godfather.save() ;
		await Promise.all( [ user1.save() , user2.save() , user3.save() ] ) ;

		var stats = {} ;
		var dbUserBatch = await users.collect( {} , { populate: [ 'job' , 'godfather' ] , stats } ) ;

		// Sort that first...
		dbUserBatch.sort( ascendingSortFn( 'firstName' ) ) ;

		// References are painful to test...
		// Here we need to recreate the circular part in the 'expected' variable
		var expected = [
			{
				firstName: 'DA' ,
				lastName: 'GODFATHER' ,
				_id: dbUserBatch[ 0 ]._id ,
				memberSid: 'DA GODFATHER'
			} ,
			{
				firstName: 'Harry' ,
				lastName: 'Campbell' ,
				_id: dbUserBatch[ 1 ]._id ,
				memberSid: 'Harry Campbell' ,
				godfather: {
					firstName: 'DA' ,
					lastName: 'GODFATHER' ,
					_id: dbUserBatch[ 0 ]._id ,
					memberSid: 'DA GODFATHER'
				}
			} ,
			{
				firstName: 'Jilbert' ,
				lastName: 'Polson' ,
				_id: dbUserBatch[ 2 ]._id ,
				memberSid: 'Jilbert Polson' ,
				job: {
					title: 'developer' ,
					salary: 60000 ,
					users: [] ,
					schools: [] ,
					_id: job._id
				} ,
				godfather: {
					firstName: 'DA' ,
					lastName: 'GODFATHER' ,
					_id: dbUserBatch[ 0 ]._id ,
					memberSid: 'DA GODFATHER'
				}
			} ,
			{
				firstName: 'Thomas' ,
				lastName: 'Campbell' ,
				_id: dbUserBatch[ 3 ]._id ,
				memberSid: 'Thomas Campbell'
			}
		] ;
		expected[ 0 ].godfather = expected[ 0 ] ;
		expect( dbUserBatch ).to.be.like( expected ) ;

		// Alternative checks, to be sure to not rely on a complex doormen feature
		expect( dbUserBatch[ 0 ].godfather ).to.be( dbUserBatch[ 0 ] ) ;
		expect( dbUserBatch[ 1 ].godfather ).to.be( dbUserBatch[ 0 ] ) ;
		expect( dbUserBatch[ 2 ].godfather ).to.be( dbUserBatch[ 0 ] ) ;

		// JSON.stringify() should throw, because of circular references
		expect( () => { JSON.stringify( dbUserBatch ) ; } ).to.throw() ;

		expect( stats.population.depth ).to.be( 1 ) ;
		// Only one DB query, since the godfather is a user and all users have been collected before the populate pass
		expect( stats.population.dbQueries ).to.be( 1 ) ;
	} ) ;

	it( "collect batch with multiple link population as a .collect() option, and circular references: using noReference" , async () => {
		var user1 = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var user2 = users.createDocument( {
			firstName: 'Thomas' ,
			lastName: 'Campbell'
		} ) ;

		var user3 = users.createDocument( {
			firstName: 'Harry' ,
			lastName: 'Campbell'
		} ) ;

		var godfather = users.createDocument( {
			firstName: 'DA' ,
			lastName: 'GODFATHER'
		} ) ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		// Link the documents!
		user1.setLink( 'job' , job ) ;
		user1.setLink( 'godfather' , godfather ) ;
		user3.setLink( 'godfather' , godfather ) ;
		godfather.setLink( 'godfather' , godfather ) ;

		await job.save() ;
		await godfather.save() ;
		await Promise.all( [ user1.save() , user2.save() , user3.save() ] ) ;

		var stats = {} ;
		var dbUserBatch = await users.collect( {} , { populate: [ 'job' , 'godfather' ] , noReference: true , stats } ) ;

		// Sort that first...
		dbUserBatch.sort( ascendingSortFn( 'firstName' ) ) ;

		expect( dbUserBatch ).to.be.like( [
			{
				firstName: 'DA' ,
				lastName: 'GODFATHER' ,
				_id: dbUserBatch[ 0 ]._id ,
				memberSid: 'DA GODFATHER' ,
				godfather: {
					firstName: 'DA' ,
					lastName: 'GODFATHER' ,
					_id: dbUserBatch[ 0 ]._id ,
					memberSid: 'DA GODFATHER' ,
					godfather: { _id: dbUserBatch[ 0 ]._id }
				}
			} ,
			{
				firstName: 'Harry' ,
				lastName: 'Campbell' ,
				_id: dbUserBatch[ 1 ]._id ,
				memberSid: 'Harry Campbell' ,
				godfather: {
					firstName: 'DA' ,
					lastName: 'GODFATHER' ,
					_id: dbUserBatch[ 0 ]._id ,
					memberSid: 'DA GODFATHER' ,
					godfather: { _id: dbUserBatch[ 0 ]._id }
				}
			} ,
			{
				firstName: 'Jilbert' ,
				lastName: 'Polson' ,
				_id: dbUserBatch[ 2 ]._id ,
				memberSid: 'Jilbert Polson' ,
				job: {
					title: 'developer' ,
					salary: 60000 ,
					users: [] ,
					schools: [] ,
					_id: job._id
				} ,
				godfather: {
					firstName: 'DA' ,
					lastName: 'GODFATHER' ,
					_id: dbUserBatch[ 0 ]._id ,
					memberSid: 'DA GODFATHER' ,
					godfather: { _id: dbUserBatch[ 0 ]._id }
				}
			} ,
			{
				firstName: 'Thomas' ,
				lastName: 'Campbell' ,
				_id: dbUserBatch[ 3 ]._id ,
				memberSid: 'Thomas Campbell'
			}
		] ) ;

		// Alternative checks
		expect( dbUserBatch[ 0 ].godfather ).not.to.be( dbUserBatch[ 0 ] ) ;
		expect( dbUserBatch[ 1 ].godfather ).not.to.be( dbUserBatch[ 0 ] ) ;
		expect( dbUserBatch[ 2 ].godfather ).not.to.be( dbUserBatch[ 0 ] ) ;

		// JSON.stringify() should not throw anymore
		expect( () => { JSON.stringify( dbUserBatch ) ; } ).not.to.throw() ;

		expect( stats.population.depth ).to.be( 1 ) ;
		// Only one DB query, since the godfather is a user and all users have been collected before the populate pass
		expect( stats.population.dbQueries ).to.be( 1 ) ;
	} ) ;

	it( "'multi-link' population as a .get()/.collect() option (create both, link, save both, get with populate option)" , async () => {
		var school1 = schools.createDocument( {
			title: 'Computer Science'
		} ) ;

		var school2 = schools.createDocument( {
			title: 'Web Academy'
		} ) ;

		var job1 = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var job2 = jobs.createDocument( {
			title: 'sysadmin' ,
			salary: 55000
		} ) ;

		var job3 = jobs.createDocument( {
			title: 'front-end developer' ,
			salary: 54000
		} ) ;

		var job4 = jobs.createDocument( {
			title: 'designer' ,
			salary: 56000
		} ) ;

		// Link the documents!
		school1.setLink( 'jobs' , [ job1 , job2 , job3 ] ) ;
		school2.setLink( 'jobs' , [ job1 , job3 , job4 ] ) ;

		await Promise.all( [ job1.save() , job2.save() , job3.save() , job4.save() , school1.save() , school2.save() ] ) ;

		var stats = {} ;
		var dbSchool = await schools.get( school1._id , { populate: 'jobs' , stats } ) ;

		expect( dbSchool ).to.equal( {
			_id: school1._id ,
			title: 'Computer Science' ,
			jobs: [ job1 , job2 , job3 ]
		} ) ;

		expect( stats.population.depth ).to.be( 1 ) ;
		expect( stats.population.dbQueries ).to.be( 1 ) ;

		// Again, with batch

		stats = {} ;
		var batch = await schools.collect( {} , { populate: 'jobs' , stats } ) ;

		// Just swap in case it arrives in the wrong order
		if ( batch[ 0 ].title !== 'Computer Science' ) { batch = [ batch[ 1 ] , batch[ 0 ] ] ; }

		expect( batch ).to.be.like( [
			{
				_id: school1._id ,
				title: 'Computer Science' ,
				jobs: [ job1 , job2 , job3 ]
			} ,
			{
				_id: school2._id ,
				title: 'Web Academy' ,
				jobs: [ job1 , job3 , job4 ]
			}
		] ) ;

		expect( stats.population.depth ).to.be( 1 ) ;
		expect( stats.population.dbQueries ).to.be( 1 ) ;
	} ) ;

	it( "'back-link' population as a .get() option (create both, link, save both, get with populate option)" , async () => {
		var user1 = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var user2 = users.createDocument( {
			firstName: 'Tony' ,
			lastName: 'P.'
		} ) ;

		var user3 = users.createDocument( {
			firstName: 'John' ,
			lastName: 'C.'
		} ) ;

		var user4 = users.createDocument( {
			firstName: 'Richard' ,
			lastName: 'S.'
		} ) ;

		var job1 = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var job2 = jobs.createDocument( {
			title: 'star developer' ,
			salary: 200000
		} ) ;

		var job3 = jobs.createDocument( {
			title: 'zero' ,
			salary: 0
		} ) ;

		// Link the documents!
		user1.setLink( 'job' , job1 ) ;
		user2.setLink( 'job' , job1 ) ;
		user3.setLink( 'job' , job2 ) ;
		user4.setLink( 'job' , job2 ) ;

		await Promise.all( [ job1.save() , job2.save() , job3.save() , user1.save() , user2.save() , user3.save() , user4.save() ] ) ;

		var stats = {} ;
		var dbJob = await jobs.get( job1._id , { populate: 'users' , stats } ) ;

		// Just swap in case it arrives in the wrong order
		if ( dbJob.users[ 0 ].firstName === 'Tony' ) { dbJob.users = [ dbJob.users[ 1 ] , dbJob.users[ 0 ] ] ; }

		expect( dbJob ).to.be.like( {
			_id: job1._id ,
			title: 'developer' ,
			salary: 60000 ,
			users: [
				{
					_id: user1._id ,
					firstName: 'Jilbert' ,
					lastName: 'Polson' ,
					memberSid: 'Jilbert Polson' ,
					job: { _id: job1._id }
				} ,
				{
					_id: user2._id ,
					firstName: 'Tony' ,
					lastName: 'P.' ,
					memberSid: 'Tony P.' ,
					job: { _id: job1._id }
				}
			] ,
			schools: []
		} ) ;

		expect( stats.population.depth ).to.be( 1 ) ;
		expect( stats.population.dbQueries ).to.be( 1 ) ;

		// Again, with batch

		stats = {} ;
		var batch = await jobs.collect( {} , { populate: 'users' , stats } ) ;

		// Sort that first...
		batch.sort( ascendingSortFn( 'title' ) ) ;

		expect( batch[ 0 ].users ).to.have.length( 2 ) ;

		// Just swap in case it arrives in the wrong order
		if ( batch[ 0 ].users[ 0 ].firstName === 'Tony' ) { batch[ 0 ].users = [ batch[ 0 ].users[ 1 ] , batch[ 0 ].users[ 0 ] ] ; }

		expect( batch[ 0 ] ).to.be.like( {
			_id: job1._id ,
			title: 'developer' ,
			salary: 60000 ,
			users: [
				{
					_id: user1._id ,
					firstName: 'Jilbert' ,
					lastName: 'Polson' ,
					memberSid: 'Jilbert Polson' ,
					job: { _id: job1._id }
				} ,
				{
					_id: user2._id ,
					firstName: 'Tony' ,
					lastName: 'P.' ,
					memberSid: 'Tony P.' ,
					job: { _id: job1._id }
				}
			] ,
			schools: []
		} ) ;

		expect( batch[ 1 ].users ).to.have.length( 2 ) ;

		// Just swap in case it arrives in the wrong order
		if ( batch[ 1 ].users[ 0 ].firstName === 'Richard' ) { batch[ 1 ].users = [ batch[ 1 ].users[ 1 ] , batch[ 1 ].users[ 0 ] ] ; }

		expect( batch[ 1 ] ).to.be.like( {
			_id: job2._id ,
			title: 'star developer' ,
			salary: 200000 ,
			users: [
				{
					_id: user3._id ,
					firstName: 'John' ,
					lastName: 'C.' ,
					memberSid: 'John C.' ,
					job: { _id: job2._id }
				} ,
				{
					_id: user4._id ,
					firstName: 'Richard' ,
					lastName: 'S.' ,
					memberSid: 'Richard S.' ,
					job: { _id: job2._id }
				}
			] ,
			schools: []
		} ) ;

		expect( batch[ 2 ] ).to.be.like( {
			_id: job3._id ,
			title: 'zero' ,
			salary: 0 ,
			users: [] ,
			schools: []
		} ) ;

		expect( stats.population.depth ).to.be( 1 ) ;
		expect( stats.population.dbQueries ).to.be( 1 ) ;
	} ) ;

	it( "'back-link' of multi-link population as a .get() option" , async () => {
		var school1 = schools.createDocument( {
			title: 'Computer Science'
		} ) ;

		var school2 = schools.createDocument( {
			title: 'Web Academy'
		} ) ;

		var job1 = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var job2 = jobs.createDocument( {
			title: 'sysadmin' ,
			salary: 55000
		} ) ;

		var job3 = jobs.createDocument( {
			title: 'front-end developer' ,
			salary: 54000
		} ) ;

		var job4 = jobs.createDocument( {
			title: 'designer' ,
			salary: 56000
		} ) ;

		// Link the documents!
		school1.setLink( 'jobs' , [ job1 , job2 , job3 ] ) ;
		school2.setLink( 'jobs' , [ job1 , job3 , job4 ] ) ;

		await Promise.all( [ job1.save() , job2.save() , job3.save() , job4.save() , school1.save() , school2.save() ] ) ;

		var stats = {} ;
		var dbJob = await jobs.get( job1._id , { populate: 'schools' , stats } ) ;

		expect( dbJob.schools ).to.have.length( 2 ) ;

		dbJob.schools.sort( ascendingSortFn( 'title' ) ) ;

		// Order by id
		dbJob.schools[ 0 ].jobs.sort( ascendingSortFn( '_id' ) ) ;
		dbJob.schools[ 1 ].jobs.sort( ascendingSortFn( '_id' ) ) ;

		expect( dbJob ).to.be.like( {
			_id: job1._id ,
			title: 'developer' ,
			salary: 60000 ,
			users: [] ,
			schools: [
				{
					_id: school1._id ,
					title: 'Computer Science' ,
					jobs: [ { _id: job1._id } , { _id: job2._id } , { _id: job3._id } ]
				} ,
				{
					_id: school2._id ,
					title: 'Web Academy' ,
					jobs: [ { _id: job1._id } , { _id: job3._id } , { _id: job4._id } ]
				}
			]
		} ) ;

		expect( stats.population.depth ).to.be( 1 ) ;
		expect( stats.population.dbQueries ).to.be( 1 ) ;

		stats = {} ;
		dbJob = await jobs.get( job4._id , { populate: 'schools' , stats } ) ;

		// Order by id
		dbJob.schools[ 0 ].jobs.sort( ascendingSortFn( '_id' ) ) ;

		expect( dbJob ).to.be.like( {
			_id: job4._id ,
			title: 'designer' ,
			salary: 56000 ,
			users: [] ,
			schools: [
				{
					_id: school2._id ,
					title: 'Web Academy' ,
					jobs: [ { _id: job1._id } , { _id: job3._id } , { _id: job4._id } ]
				}
			]
		} ) ;

		expect( stats.population.depth ).to.be( 1 ) ;
		expect( stats.population.dbQueries ).to.be( 1 ) ;
	} ) ;

	it( "'back-link' of multi-link population on a Document instance" , async () => {
		var school1 = schools.createDocument( {
			title: 'Computer Science'
		} ) ;

		var school2 = schools.createDocument( {
			title: 'Web Academy'
		} ) ;

		var job1 = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var job2 = jobs.createDocument( {
			title: 'sysadmin' ,
			salary: 55000
		} ) ;

		var job3 = jobs.createDocument( {
			title: 'front-end developer' ,
			salary: 54000
		} ) ;

		var job4 = jobs.createDocument( {
			title: 'designer' ,
			salary: 56000
		} ) ;

		// Link the documents!
		school1.setLink( 'jobs' , [ job1 , job2 , job3 ] ) ;
		school2.setLink( 'jobs' , [ job1 , job3 , job4 ] ) ;

		await Promise.all( [ job1.save() , job2.save() , job3.save() , job4.save() , school1.save() , school2.save() ] ) ;

		var dbJob = await jobs.get( job1._id ) ;
		await dbJob.populate( 'schools' ) ;

		expect( dbJob.schools ).to.have.length( 2 ) ;

		dbJob.schools.sort( ascendingSortFn( 'title' ) ) ;

		// Order by id
		dbJob.schools[ 0 ].jobs.sort( ascendingSortFn( '_id' ) ) ;
		dbJob.schools[ 1 ].jobs.sort( ascendingSortFn( '_id' ) ) ;

		expect( dbJob ).to.be.like( {
			_id: job1._id ,
			title: 'developer' ,
			salary: 60000 ,
			users: [] ,
			schools: [
				{
					_id: school1._id ,
					title: 'Computer Science' ,
					jobs: [ { _id: job1._id } , { _id: job2._id } , { _id: job3._id } ]
				} ,
				{
					_id: school2._id ,
					title: 'Web Academy' ,
					jobs: [ { _id: job1._id } , { _id: job3._id } , { _id: job4._id } ]
				}
			]
		} ) ;

		dbJob = await jobs.get( job4._id ) ;
		await dbJob.populate( 'schools' ) ;

		// Order by id
		dbJob.schools[ 0 ].jobs.sort( ascendingSortFn( '_id' ) ) ;

		expect( dbJob ).to.be.like( {
			_id: job4._id ,
			title: 'designer' ,
			salary: 56000 ,
			users: [] ,
			schools: [
				{
					_id: school2._id ,
					title: 'Web Academy' ,
					jobs: [ { _id: job1._id } , { _id: job3._id } , { _id: job4._id } ]
				}
			]
		} ) ;
	} ) ;
} ) ;



describe( "Populate links using the '*' wildcard" , () => {

	beforeEach( clearDB ) ;

	it( "link population of documents and batches using the '*' wildcard" , async () => {
		var product1 = products.createDocument( {
			name: 'pencil' ,
			price: 1.20
		} ) ;

		var productId1 = product1.getId() ;

		var product2 = products.createDocument( {
			name: 'eraser' ,
			price: 1.60
		} ) ;

		var productId2 = product2.getId() ;

		var product3 = products.createDocument( {
			name: 'pen' ,
			price: 5.90
		} ) ;

		var productId3 = product3.getId() ;

		var store1 = stores.createDocument( {
			name: 'Le Grand Bozar' ,
			products: [
				{ product: product1 , quantity: 56 } ,
				{ product: product2 , quantity: 37 }
			] ,
			productBatches: [
				{ batch: [ product1 , product2 ] , quantity: 3 } ,
				{ batch: [ product1 , product3 ] , quantity: 2 }
			]
		} ) ;

		var storeId1 = store1.getId() ;
		//log.hdebug( "store1: %[5]Y" , store1 ) ;

		await product1.save() ;
		await product2.save() ;
		await product3.save() ;
		await store1.save() ;
		

		// DB get, then populate

		var dbStore1 = await stores.get( storeId1 ) ;
		/*
		var linksDetails = dbStore1.getWildLinksDetails( 'products.*.product' ) ;
		log.hdebug( "linksDetails (products): %[5]Y" , linksDetails ) ;
		linksDetails = dbStore1.getWildLinksDetails( 'productBatches.*.batch' ) ;
		log.hdebug( "linksDetails (batches): %[5]Y" , linksDetails ) ;
		*/
		
		//log.hdebug( "dbStore1: %[5]Y" , dbStore1 ) ;
		expect( dbStore1 ).to.equal( {
			_id: storeId1 ,
			name: 'Le Grand Bozar' ,
			products: [
				{ product: { _id: productId1 } , quantity: 56 } ,
				{ product: { _id: productId2 } , quantity: 37 }
			] ,
			productBatches: [
				{ batch: [ { _id: productId1 } , { _id: productId2 } ] , quantity: 3 } ,
				{ batch: [ { _id: productId1 } , { _id: productId3 } ] , quantity: 2 }
			]
		} ) ;

		await dbStore1.populate( 'products.*.product' ) ;
		//log.hdebug( "dbStore1: %[5]Y" , dbStore1 ) ;
		expect( dbStore1 ).to.equal( {
			_id: storeId1 ,
			name: 'Le Grand Bozar' ,
			products: [
				{ product: product1 , quantity: 56 } ,
				{ product: product2 , quantity: 37 }
			] ,
			productBatches: [
				{ batch: [ { _id: productId1 } , { _id: productId2 } ] , quantity: 3 } ,
				{ batch: [ { _id: productId1 } , { _id: productId3 } ] , quantity: 2 }
			]
		} ) ;

		await dbStore1.populate( 'productBatches.*.batch' ) ;
		//log.hdebug( "dbStore1: %[5]Y" , dbStore1 ) ;
		expect( dbStore1 ).to.equal( {
			_id: storeId1 ,
			name: 'Le Grand Bozar' ,
			products: [
				{ product: product1 , quantity: 56 } ,
				{ product: product2 , quantity: 37 }
			] ,
			productBatches: [
				{ batch: [ product1 , product2 ] , quantity: 3 } ,
				{ batch: [ product1 , product3 ] , quantity: 2 }
			]
		} ) ;
		

		// DB get including populate

		var stats = {} ;
		dbStore1 = await stores.get( storeId1 , { populate: [ 'products.*.product' , 'productBatches.*.batch' ] , stats } ) ;
		expect( dbStore1 ).to.equal( {
			_id: storeId1 ,
			name: 'Le Grand Bozar' ,
			products: [
				{ product: product1 , quantity: 56 } ,
				{ product: product2 , quantity: 37 }
			] ,
			productBatches: [
				{ batch: [ product1 , product2 ] , quantity: 3 } ,
				{ batch: [ product1 , product3 ] , quantity: 2 }
			]
		} ) ;
		expect( stats.population.depth ).to.be( 1 ) ;
		expect( stats.population.dbQueries ).to.be( 1 ) ;


		// DB batch get including populate

		var store2 = stores.createDocument( {
			name: 'Plume' ,
			products: [
				{ product: product3 , quantity: 12 }
			] ,
			productBatches: [
				{ batch: [ product1 , product2 , product3 ] , quantity: 7 }
			]
		} ) ;

		var storeId2 = store2.getId() ;
		//log.hdebug( "store2: %[5]Y" , store2 ) ;

		await store2.save() ;

		stats = {} ;
		var dbBatch = await stores.find( {} , { populate: [ 'products.*.product' , 'productBatches.*.batch' ] , stats } ) ;
		//log.hdebug( "dbBatch: %[8l50000]Y" , dbBatch ) ;
		expect( dbBatch ).to.be.like( [
			{
				_id: storeId1 ,
				name: 'Le Grand Bozar' ,
				products: [
					{ product: product1 , quantity: 56 } ,
					{ product: product2 , quantity: 37 }
				] ,
				productBatches: [
					{ batch: [ product1 , product2 ] , quantity: 3 } ,
					{ batch: [ product1 , product3 ] , quantity: 2 }
				]
			} ,
			{
				_id: storeId2 ,
				name: 'Plume' ,
				products: [
					{ product: product3 , quantity: 12 }
				] ,
				productBatches: [
					{ batch: [ product1 , product2 , product3 ] , quantity: 7 }
				]
			}
		] ) ;

		expect( stats.population.depth ).to.be( 1 ) ;
		expect( stats.population.dbQueries ).to.be( 1 ) ;


		// DB batch get, then populate batch

		var dbBatch = await stores.find( {} ) ;
		await dbBatch.populate( [ 'products.*.product' , 'productBatches.*.batch' ] ) ;
		//log.hdebug( "dbBatch: %[8l50000]Y" , dbBatch ) ;
		expect( dbBatch ).to.be.like( [
			{
				_id: storeId1 ,
				name: 'Le Grand Bozar' ,
				products: [
					{ product: product1 , quantity: 56 } ,
					{ product: product2 , quantity: 37 }
				] ,
				productBatches: [
					{ batch: [ product1 , product2 ] , quantity: 3 } ,
					{ batch: [ product1 , product3 ] , quantity: 2 }
				]
			} ,
			{
				_id: storeId2 ,
				name: 'Plume' ,
				products: [
					{ product: product3 , quantity: 12 }
				] ,
				productBatches: [
					{ batch: [ product1 , product2 , product3 ] , quantity: 7 }
				]
			}
		] ) ;
	} ) ;
} ) ;



describe( "Deep populate links" , () => {

	beforeEach( clearDB ) ;

	it( "deep population as a .get() option (links and back-link)" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var user2 = users.createDocument( {
			firstName: 'Robert' ,
			lastName: 'Polson'
		} ) ;

		var gfUser = users.createDocument( {
			firstName: 'The' ,
			lastName: 'Godfather'
		} ) ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var job2 = jobs.createDocument( {
			title: 'senior developer' ,
			salary: 80000
		} ) ;

		// Link the documents!
		user.setLink( 'job' , job ) ;
		user2.setLink( 'job' , job ) ;
		gfUser.setLink( 'job' , job2 ) ;
		user.setLink( 'godfather' , gfUser ) ;
		user2.setLink( 'godfather' , gfUser ) ;

		await job.save() ;
		await job2.save() ;
		await user.save() ;
		await user2.save() ;
		await gfUser.save() ;

		var stats = {} ;

		// Check that the syntax support both array and direct string
		//var dbUser = await users.get( user._id , { deepPopulate: { users: [ 'job' ] , jobs: 'users' } , stats } ) ;
		var dbUser = await users.get( user._id , { deepPopulate: { users: [ 'job' , 'godfather' ] , jobs: 'users' } , stats } ) ;

		expect( dbUser.job.users ).to.have.length( 2 ) ;

		// Just swap in case it arrives in the wrong order
		if ( dbUser.job.users[ 0 ].firstName === 'Robert' ) {
			dbUser.job.users = [ dbUser.job.users[ 1 ] , dbUser.job.users[ 0 ] ] ;
		}

		expect( dbUser.job.users[ 0 ].job ).to.be( dbUser.job ) ;
		expect( dbUser.job.users[ 1 ].job ).to.be( dbUser.job ) ;

		// Circular references... so boring to test...
		var expected = {
			_id: user._id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			job: {
				_id: job._id ,
				title: 'developer' ,
				salary: 60000 ,
				schools: [] ,
				users: []
			}
		} ;
		expected.godfather = {
			_id: gfUser._id ,
			firstName: "The" ,
			lastName: "Godfather" ,
			memberSid: "The Godfather" ,
			job: {
				_id: job2._id ,
				title: 'senior developer' ,
				salary: 80000 ,
				schools: [] ,
				users: []
			}
		} ;
		expected.godfather.job.users[ 0 ] = expected.godfather ;
		expected.job.users[ 0 ] = expected ;
		expected.job.users[ 1 ] = {
			_id: user2._id ,
			firstName: "Robert" ,
			lastName: "Polson" ,
			memberSid: "Robert Polson" ,
			godfather: expected.godfather ,
			job: expected.job
		} ;
		expect( dbUser ).to.be.like( expected ) ;

		// There is something wrong this the "like" assertion and proxy (?) ATM
		expect( dbUser.godfather ).to.be.like( expected.godfather ) ;
		expect( dbUser.job.users[ 0 ] ).to.be.like( expected ) ;
		//expect( dbUser.job.users[ 1 ] ).to.be.like( expected.job.users[ 1 ] ) ;

		//log.hdebug( "%[5l10000]Y" , dbUser ) ;

		// There are 5 queries because of job's backlink to users (we don't have IDs when back-linking)
		expect( stats.population.depth ).to.be( 3 ) ;
		expect( stats.population.dbQueries ).to.be( 5 ) ;



		// Now test the depth limit
		stats = {} ;
		dbUser = await users.get( user._id , { depth: 2 , deepPopulate: { users: [ 'job' , 'godfather' ] , jobs: 'users' } , stats } ) ;
		expect( stats.population.depth ).to.be( 2 ) ;
		expect( stats.population.dbQueries ).to.be( 4 ) ;
	} ) ;

	it( "deep population on Document instances (links and back-link)" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var user2 = users.createDocument( {
			firstName: 'Robert' ,
			lastName: 'Polson'
		} ) ;

		var gfUser = users.createDocument( {
			firstName: 'The' ,
			lastName: 'Godfather'
		} ) ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var job2 = jobs.createDocument( {
			title: 'senior developer' ,
			salary: 80000
		} ) ;

		// Link the documents!
		user.setLink( 'job' , job ) ;
		user2.setLink( 'job' , job ) ;
		gfUser.setLink( 'job' , job2 ) ;
		user.setLink( 'godfather' , gfUser ) ;
		user2.setLink( 'godfather' , gfUser ) ;

		await job.save() ;
		await job2.save() ;
		await user.save() ;
		await user2.save() ;
		await gfUser.save() ;

		// Check that the syntax support both array and direct string
		var dbUser = await users.get( user._id ) ;
		await dbUser.populate( [ 'job' , 'godfather' ] , { deepPopulate: { users: [ 'job' , 'godfather' ] , jobs: 'users' } } ) ;

		expect( dbUser.job.users ).to.have.length( 2 ) ;

		// Just swap in case it arrives in the wrong order
		if ( dbUser.job.users[ 0 ].firstName === 'Robert' ) {
			dbUser.job.users = [ dbUser.job.users[ 1 ] , dbUser.job.users[ 0 ] ] ;
		}

		expect( dbUser.job.users[ 0 ].job ).to.be( dbUser.job ) ;
		expect( dbUser.job.users[ 1 ].job ).to.be( dbUser.job ) ;

		// Circular references... so boring to test...
		var expected = {
			_id: user._id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			job: {
				_id: job._id ,
				title: 'developer' ,
				salary: 60000 ,
				schools: [] ,
				users: []
			}
		} ;
		expected.godfather = {
			_id: gfUser._id ,
			firstName: "The" ,
			lastName: "Godfather" ,
			memberSid: "The Godfather" ,
			job: {
				_id: job2._id ,
				title: 'senior developer' ,
				salary: 80000 ,
				schools: [] ,
				users: []
			}
		} ;
		expected.godfather.job.users[ 0 ] = expected.godfather ;
		expected.job.users[ 0 ] = expected ;
		expected.job.users[ 1 ] = {
			_id: user2._id ,
			firstName: "Robert" ,
			lastName: "Polson" ,
			memberSid: "Robert Polson" ,
			godfather: expected.godfather ,
			job: expected.job
		} ;
		expect( dbUser ).to.be.like( expected ) ;

		// There is something wrong this the "like" assertion and proxy (?) ATM
		expect( dbUser.godfather ).to.be.like( expected.godfather ) ;
		expect( dbUser.job.users[ 0 ] ).to.be.like( expected ) ;
	} ) ;

	it( "more deep population tests" ) ;
} ) ;



describe( "Caching with the memory model" , () => {

	beforeEach( clearDB ) ;

	it( "should get a document from a Memory Model cache" , async () => {
		var mem = world.createMemoryModel() ;

		var rawUser = {
			_id: '123456789012345678901234' ,
			firstName: 'John' ,
			lastName: 'McGregor'
		} ;

		mem.addRaw( 'users' , rawUser ) ;
		expect( mem.getRaw( 'users' , rawUser._id ) ).to.equal( { _id: '123456789012345678901234' , firstName: 'John' , lastName: 'McGregor' } ) ;

		var user = await users.get( rawUser._id , { cache: mem } ) ;
		expect( user ).to.equal( { _id: rawUser._id , firstName: 'John' , lastName: 'McGregor' } ) ;

		expect( mem.getProxy( 'users' , rawUser._id ) ).to.be.partially.like( { firstName: 'John' , lastName: 'McGregor' } ) ;
	} ) ;

	it( "should multiGet all documents from a Memory Model cache (complete cache hit)" , async () => {
		var mem = world.createMemoryModel() ;

		var someUsers = [
			{
				_id: '000000000000000000000001' ,
				firstName: 'John1' ,
				lastName: 'McGregor'
			} ,
			{
				_id: '000000000000000000000002' ,
				firstName: 'John2' ,
				lastName: 'McGregor'
			} ,
			{
				_id: '000000000000000000000003' ,
				firstName: 'John3' ,
				lastName: 'McGregor'
			}
		] ;

		mem.addRaw( 'users' , someUsers[ 0 ] ) ;
		mem.addRaw( 'users' , someUsers[ 1 ] ) ;
		mem.addRaw( 'users' , someUsers[ 2 ] ) ;

		var ids = [
			'000000000000000000000001' ,
			'000000000000000000000002' ,
			'000000000000000000000003'
		] ;

		var batch = await users.multiGet( ids , { cache: mem } ) ;

		batch.sort( ascendingSortFn( '_id' ) ) ;

		expect( batch ).to.be.like( [
			{
				_id: someUsers[ 0 ]._id ,
				firstName: 'John1' ,
				lastName: 'McGregor'
			} ,
			{
				_id: someUsers[ 1 ]._id ,
				firstName: 'John2' ,
				lastName: 'McGregor'
			} ,
			{
				_id: someUsers[ 2 ]._id ,
				firstName: 'John3' ,
				lastName: 'McGregor'
			}
		] ) ;
	} ) ;

	it( "should multiGet some document from a Memory Model cache (partial cache hit)" , async () => {
		var mem = world.createMemoryModel() ;

		var someUsers = [
			{
				_id: '000000000000000000000001' ,
				firstName: 'John1' ,
				lastName: 'McGregor'
			} ,
			{
				_id: '000000000000000000000002' ,
				firstName: 'John2' ,
				lastName: 'McGregor'
			} ,
			{
				_id: '000000000000000000000003' ,
				firstName: 'John3' ,
				lastName: 'McGregor'
			}
		] ;

		mem.addRaw( 'users' , someUsers[ 0 ] ) ;
		mem.addRaw( 'users' , someUsers[ 1 ] ) ;
		mem.addRaw( 'users' , someUsers[ 2 ] ) ;

		var anotherOne = users.createDocument( {
			_id: '000000000000000000000004' ,
			firstName: 'John4' ,
			lastName: 'McGregor'
		} ) ;

		await anotherOne.save() ;

		var ids = [
			'000000000000000000000001' ,
			'000000000000000000000002' ,
			'000000000000000000000004'
		] ;

		var batch = await users.multiGet( ids , { cache: mem } ) ;

		batch.sort( ascendingSortFn( '_id' ) ) ;

		expect( batch ).to.be.like( [
			{
				_id: someUsers[ 0 ]._id ,
				firstName: 'John1' ,
				lastName: 'McGregor'
			} ,
			{
				_id: someUsers[ 1 ]._id ,
				firstName: 'John2' ,
				lastName: 'McGregor'
			} ,
			{
				_id: anotherOne._id ,
				firstName: 'John4' ,
				lastName: 'McGregor' ,
				memberSid: 'John4 McGregor'
			}
		] ) ;
	} ) ;

} ) ;



describe( "Memory model" , () => {

	beforeEach( clearDB ) ;

	it( "should create a memoryModel, retrieve documents with 'populate' on 'link' and 'back-link', with the 'memory' options and effectively save them in the memoryModel" , async () => {
		var memory = world.createMemoryModel() ;

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var user2 = users.createDocument( {
			firstName: 'Pat' ,
			lastName: 'Mulligan'
		} ) ;

		var user3 = users.createDocument( {
			firstName: 'Bill' ,
			lastName: 'Baroud'
		} ) ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var job2 = jobs.createDocument( {
			title: 'adventurer' ,
			salary: 200000
		} ) ;

		// Link the documents!
		user.setLink( 'job' , job ) ;
		user2.setLink( 'job' , job ) ;
		user3.setLink( 'job' , job2 ) ;

		await Promise.all( [ job.save() , job2.save() ] ) ;
		await Promise.all( [ user.save() , user2.save() , user3.save() ] ) ;

		var stats = {} ;
		var dbUser = await users.collect( {} , { cache: memory , populate: 'job' , stats } ) ;

		expect( memory.collections ).to.have.keys( 'users' , 'jobs' ) ;
		expect( memory.collections.users.rawDocuments ).to.have.keys( '' + user._id , '' + user2._id , '' + user3._id ) ;
		expect( memory.collections.jobs.rawDocuments ).to.have.keys( '' + job._id , '' + job2._id ) ;

		expect( memory.collections.users.rawDocuments[ user._id ] ).to.equal( {
			_id: user._id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			job: { _id: job._id }
		} ) ;

		expect( memory.collections.users.rawDocuments[ user2._id ] ).to.equal( {
			_id: user2._id ,
			firstName: 'Pat' ,
			lastName: 'Mulligan' ,
			memberSid: 'Pat Mulligan' ,
			job: { _id: job._id }
		} ) ;

		expect( memory.collections.users.rawDocuments[ user3._id ] ).to.equal( {
			_id: user3._id ,
			firstName: 'Bill' ,
			lastName: 'Baroud' ,
			memberSid: 'Bill Baroud' ,
			job: { _id: job2._id }
		} ) ;

		expect( memory.collections.jobs.rawDocuments[ job._id ] ).to.equal( {
			_id: job._id ,
			title: 'developer' ,
			salary: 60000 ,
			users: [] ,
			schools: []
		} ) ;

		expect( memory.collections.jobs.rawDocuments[ job2._id ] ).to.equal( {
			_id: job2._id ,
			title: 'adventurer' ,
			salary: 200000 ,
			users: [] ,
			schools: []
		} ) ;

		//console.error( memory.collections.users.rawDocuments ) ;
		//console.error( memory.collections.jobs.rawDocuments ) ;

		expect( stats.population.depth ).to.be( 1 ) ;
		expect( stats.population.dbQueries ).to.be( 1 ) ;


		stats = {} ;
		var dbJob = await jobs.collect( {} , { cache: memory , populate: 'users' , stats } ) ;

		expect( memory.collections ).to.have.keys( 'users' , 'jobs' ) ;

		expect( memory.collections.users.rawDocuments ).to.have.keys( '' + user._id , user2._id , user3._id ) ;
		expect( memory.collections.jobs.rawDocuments ).to.have.keys( '' + job._id , job2._id ) ;

		expect( memory.collections.users.rawDocuments[ user._id ] ).to.equal( {
			_id: user._id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			job: { _id: job._id }
		} ) ;

		expect( memory.collections.users.rawDocuments[ user2._id ] ).to.equal( {
			_id: user2._id ,
			firstName: 'Pat' ,
			lastName: 'Mulligan' ,
			memberSid: 'Pat Mulligan' ,
			job: { _id: job._id }
		} ) ;

		expect( memory.collections.users.rawDocuments[ user3._id ] ).to.equal( {
			_id: user3._id ,
			firstName: 'Bill' ,
			lastName: 'Baroud' ,
			memberSid: 'Bill Baroud' ,
			job: { _id: job2._id }
		} ) ;

		expect( memory.collections.jobs.rawDocuments[ job._id ] ).to.equal( {
			_id: job._id ,
			title: 'developer' ,
			salary: 60000 ,
			schools: [] ,
			users: []
		} ) ;

		expect( memory.collections.jobs.rawDocuments[ job2._id ] ).to.equal( {
			_id: job2._id ,
			title: 'adventurer' ,
			salary: 200000 ,
			schools: [] ,
			users: []
		} ) ;

		//console.error( memory.collections.users.rawDocuments ) ;
		//console.error( memory.collections.jobs.rawDocuments ) ;

		// This is a back-link, so a DB query is mandatory here
		expect( stats.population.depth ).to.be( 1 ) ;
		expect( stats.population.dbQueries ).to.be( 1 ) ;


		stats = {} ;
		dbUser = await users.collect( {} , { cache: memory , populate: 'job' , stats } ) ;

		// This is the same query already performed on user.
		// We just check populate Depth and Queries here: a total cache hit should happen!
		expect( stats.population.depth ).to.be( 0 ) ;
		expect( stats.population.dbQueries ).to.be( 0 ) ;
	} ) ;

	it( "should also works with multi-link" , async () => {
		var memory = world.createMemoryModel() ;

		var school1 = schools.createDocument( { title: 'Computer Science' } ) ;
		var school2 = schools.createDocument( { title: 'Web Academy' } ) ;
		var job1 = jobs.createDocument( { title: 'developer' , salary: 60000 } ) ;
		var job2 = jobs.createDocument( { title: 'sysadmin' , salary: 55000 } ) ;
		var job3 = jobs.createDocument( { title: 'front-end developer' , salary: 54000 } ) ;
		var job4 = jobs.createDocument( { title: 'designer' , salary: 56000 } ) ;

		// Link the documents!
		school1.setLink( 'jobs' , [ job1 , job2 , job3 ] ) ;
		school2.setLink( 'jobs' , [ job1 , job3 , job4 ] ) ;

		await Promise.all( [ job1.save() , job2.save() , job3.save() , job4.save() ] ) ;
		await Promise.all( [ school1.save() , school2.save() ] ) ;


		var stats = {} ;
		var dbSchool = await schools.collect( {} , { populate: 'jobs' , cache: memory , stats } ) ;
		expect( memory.collections ).to.have.keys( 'schools' , 'jobs' ) ;

		expect( memory.collections.schools.rawDocuments ).to.have.keys( '' + school1._id , '' + school2._id ) ;
		expect( memory.collections.jobs.rawDocuments ).to.have.keys( '' + job1._id , job2._id , job3._id , job4._id ) ;

		expect( memory.collections.schools.rawDocuments[ school1._id ] ).to.equal( {
			_id: school1._id ,
			title: 'Computer Science' ,
			jobs: [ { _id: job1._id } , { _id: job2._id } , { _id: job3._id } ]
		} ) ;

		expect( memory.collections.schools.rawDocuments[ school2._id ] ).to.equal( {
			_id: school2._id ,
			title: 'Web Academy' ,
			jobs: [ { _id: job1._id } , { _id: job3._id } , { _id: job4._id } ]
		} ) ;

		expect( memory.collections.jobs.rawDocuments[ job1._id ] ).to.equal( {
			_id: job1._id ,
			title: 'developer' ,
			salary: 60000 ,
			users: [] ,
			schools: []
		} ) ;

		expect( memory.collections.jobs.rawDocuments[ job2._id ] ).to.equal( {
			_id: job2._id ,
			title: 'sysadmin' ,
			salary: 55000 ,
			users: [] ,
			schools: []
		} ) ;

		expect( memory.collections.jobs.rawDocuments[ job3._id ] ).to.equal( {
			_id: job3._id ,
			title: 'front-end developer' ,
			salary: 54000 ,
			users: [] ,
			schools: []
		} ) ;

		expect( memory.collections.jobs.rawDocuments[ job4._id ] ).to.equal( {
			_id: job4._id ,
			title: 'designer' ,
			salary: 56000 ,
			users: [] ,
			schools: []
		} ) ;

		expect( stats.population.depth ).to.be( 1 ) ;
		expect( stats.population.dbQueries ).to.be( 1 ) ;


		stats = {} ;
		dbSchool = await schools.collect( {} , { populate: 'jobs' , cache: memory , stats } ) ;

		expect( stats.population.depth ).to.be( 0 ) ;
		expect( stats.population.dbQueries ).to.be( 0 ) ;
	} ) ;

	it( "incremental population should work as expected" , async () => {
		var memory = world.createMemoryModel() ;

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var user2 = users.createDocument( {
			firstName: 'Robert' ,
			lastName: 'Polson'
		} ) ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var deepPopulate = {
			users: 'job' ,
			jobs: 'users'
		} ;

		// Link the documents!
		user.setLink( 'job' , job ) ;
		user2.setLink( 'job' , job ) ;

		await job.save() ;
		await user.save() ;
		await user2.save() ;

		var stats = {} ;
		var dbUser = await users.get( user._id , { cache: memory , stats } ) ;

		expect( dbUser ).to.equal( {
			_id: user._id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			job: { _id: job._id }
		} ) ;


		stats = {} ;
		dbUser = await users.get( user._id , { cache: memory , populate: 'job' , stats } ) ;
		expect( dbUser ).to.equal( {
			_id: user._id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			job: {
				_id: job._id ,
				title: 'developer' ,
				salary: 60000 ,
				users: [] ,
				schools: []
			}
		} ) ;

		expect( memory.collections.jobs.rawDocuments[ job._id ] ).to.equal( {
			_id: job._id ,
			title: 'developer' ,
			salary: 60000 ,
			users: [] ,
			schools: []
		} ) ;
		expect( stats.population.depth ).to.be( 1 ) ;
		expect( stats.population.dbQueries ).to.be( 1 ) ;


		dbUser = await users.get( user._id , { cache: memory , deepPopulate: deepPopulate , stats } ) ;

		expect( dbUser.job.users ).to.have.length( 2 ) ;

		if ( dbUser.job.users[ 0 ].firstName === 'Robert' ) {
			dbUser.job.users = [ dbUser.job.users[ 1 ] , dbUser.job.users[ 0 ] ] ;
		}

		expect( dbUser.job.users[ 0 ].job ).to.be( dbUser.job ) ;
		expect( dbUser.job.users[ 1 ].job ).to.be( dbUser.job ) ;

		var expected = {
			_id: user._id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			job: {
				_id: job._id ,
				title: 'developer' ,
				salary: 60000 ,
				schools: [] ,
				users: []
			}
		} ;
		expected.job.users[ 0 ] = expected ;
		expected.job.users[ 1 ] = {
			_id: user2._id ,
			firstName: 'Robert' ,
			lastName: 'Polson' ,
			memberSid: 'Robert Polson' ,
			job: expected.job
		} ;
		expect( dbUser ).to.be.like( expected ) ;

		expect( stats.population.depth ).to.be( 1 ) ;
		expect( stats.population.dbQueries ).to.be( 1 ) ;
	} ) ;

	it( "should also works with back-multi-link" ) ;
} ) ;



describe( "Counters" , () => {

	beforeEach( clearDB ) ;

	it( "get/set counters" , async () => {
		var c ;

		c = await counters.getNextCounterFor( 'counter' ) ;
		expect( c ).to.be( 1 ) ;

		c = await counters.getNextCounterFor( 'counter' ) ;
		expect( c ).to.be( 2 ) ;

		c = await counters.getNextCounterFor( 'n' ) ;
		expect( c ).to.be( 1 ) ;

		c = await counters.getNextCounterFor( 'counter' ) ;
		expect( c ).to.be( 3 ) ;

		c = await counters.getNextCounterFor( 'n' ) ;
		expect( c ).to.be( 2 ) ;

		await counters.setNextCounterFor( 'counter' , 1 ) ;
		c = await counters.getNextCounterFor( 'counter' ) ;
		expect( c ).to.be( 1 ) ;

		c = await counters.getNextCounterFor( 'counter' ) ;
		expect( c ).to.be( 2 ) ;

		c = await counters.getNextCounterFor( 'n' ) ;
		expect( c ).to.be( 3 ) ;
	} ) ;
} ) ;



describe( "Versioning" , () => {

	beforeEach( clearDB ) ;

	it( "versioned collection should save every modifications in the versions collection" , async () => {
		expect( versionedItems.versioning ).to.be( true ) ;

		var versionedItem = versionedItems.createDocument( {
			name: 'item#1' ,
			p1: 'value1a'
		} ) ;

		var versionedItemId = versionedItem.getId() ;

		expect( versionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 1 ,
			_lastModified: versionedItem._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value1a' ,
			versions: []
		} ) ;

		versionedItem.p1 = 'value1b' ;
		// Version should not be incremented, because it was not even saved once
		expect( versionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 1 ,
			_lastModified: versionedItem._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value1b' ,
			versions: []
		} ) ;

		await versionedItem.save() ;

		versionedItem.p1 = 'value1c' ;

		// Version should not be incremented, because changed was not saved yet
		expect( versionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 1 ,
			_lastModified: versionedItem._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value1c' ,
			versions: []
		} ) ;

		await versionedItem.save() ;

		// Version should be incremented now we have save it
		expect( versionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 2 ,
			_lastModified: versionedItem._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value1c' ,
			versions: []
		} ) ;

		var batch = await versions.find( { '_activeVersion._id': versionedItemId , '_activeVersion._collection': 'versionedItems' } ) ;
		expect( '' + batch[ 0 ]._id ).not.to.be( '' + versionedItemId ) ;
		expect( batch ).to.be.like( [
			{
				_id: batch[ 0 ]._id ,	// unpredictable
				_version: 1 ,
				_lastModified: batch[ 0 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1b' ,
				versions: []
			}
		] ) ;

		var dbVersionedItem = await versionedItems.get( versionedItemId ) ;

		expect( dbVersionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 2 ,
			_lastModified: dbVersionedItem._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value1c' ,
			versions: []
		} ) ;

		dbVersionedItem.p2 = 'value2a' ;
		dbVersionedItem.p2 = 'value2b' ;

		// Still no version change so far, because not saved
		expect( dbVersionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 2 ,
			_lastModified: dbVersionedItem._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value1c' ,
			p2: 'value2b' ,
			versions: []
		} ) ;

		await dbVersionedItem.save() ;

		// Still no change so far, because not saved
		expect( dbVersionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 3 ,
			_lastModified: dbVersionedItem._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value1c' ,
			p2: 'value2b' ,
			versions: []
		} ) ;

		batch = await versions.find( { '_activeVersion._id': versionedItemId , '_activeVersion._collection': 'versionedItems' } ) ;
		expect( '' + batch[ 0 ]._id ).not.to.be( '' + versionedItemId ) ;
		expect( '' + batch[ 1 ]._id ).not.to.be( '' + versionedItemId ) ;
		expect( '' + batch[ 0 ]._id ).not.to.be( '' + batch[ 1 ]._id ) ;
		expect( batch ).to.be.like( [
			{
				_id: batch[ 0 ]._id ,	// unpredictable
				_version: 1 ,
				_lastModified: batch[ 0 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1b' ,
				versions: []
			} ,
			{
				_id: batch[ 1 ]._id ,	// unpredictable
				_version: 2 ,
				_lastModified: batch[ 1 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1c' ,
				versions: []
			}
		] ) ;

		dbVersionedItem.p2 = 'value2c' ;
		await dbVersionedItem.save() ;

		dbVersionedItem = await versionedItems.get( versionedItemId ) ;

		expect( dbVersionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 4 ,
			_lastModified: dbVersionedItem._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value1c' ,
			p2: 'value2c' ,
			versions: []
		} ) ;

		batch = await versions.find( { '_activeVersion._id': versionedItemId , '_activeVersion._collection': 'versionedItems' } ) ;
		expect( batch ).to.be.like( [
			{
				_id: batch[ 0 ]._id ,	// unpredictable
				_version: 1 ,
				_lastModified: batch[ 0 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1b' ,
				versions: []
			} ,
			{
				_id: batch[ 1 ]._id ,	// unpredictable
				_version: 2 ,
				_lastModified: batch[ 1 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1c' ,
				versions: []
			} ,
			{
				_id: batch[ 2 ]._id ,	// unpredictable
				_version: 3 ,
				_lastModified: batch[ 2 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1c' ,
				p2: 'value2b' ,
				versions: []
			}
		] ) ;

		var dbItemVersions = await dbVersionedItem.getLink( 'versions' ) ;
		expect( dbItemVersions ).to.be.like( [
			{
				_id: dbItemVersions[ 0 ]._id ,	// unpredictable
				_version: 1 ,
				_lastModified: dbItemVersions[ 0 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1b' ,
				versions: []
			} ,
			{
				_id: dbItemVersions[ 1 ]._id ,	// unpredictable
				_version: 2 ,
				_lastModified: dbItemVersions[ 1 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1c' ,
				versions: []
			} ,
			{
				_id: dbItemVersions[ 2 ]._id ,	// unpredictable
				_version: 3 ,
				_lastModified: dbItemVersions[ 2 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1c' ,
				p2: 'value2b' ,
				versions: []
			}
		] ) ;


		// Test the overwrite feature

		var versionedItemReplacement = versionedItems.createDocument( {
			_id: versionedItemId ,
			name: 'item#1' ,
			p1: 'value1-over' ,
			p2: 'value2-over'
		} ) ;

		await versionedItemReplacement.save( { overwrite: true } ) ;

		dbVersionedItem = await versionedItems.get( versionedItemId ) ;

		expect( dbVersionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 5 ,
			_lastModified: dbVersionedItem._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value1-over' ,
			p2: 'value2-over' ,
			versions: []
		} ) ;

		batch = await versions.find( { '_activeVersion._id': versionedItemId , '_activeVersion._collection': 'versionedItems' } ) ;
		expect( batch ).to.be.like( [
			{
				_id: batch[ 0 ]._id ,	// unpredictable
				_version: 1 ,
				_lastModified: batch[ 0 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1b' ,
				versions: []
			} ,
			{
				_id: batch[ 1 ]._id ,	// unpredictable
				_version: 2 ,
				_lastModified: batch[ 1 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1c' ,
				versions: []
			} ,
			{
				_id: batch[ 2 ]._id ,	// unpredictable
				_version: 3 ,
				_lastModified: batch[ 2 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1c' ,
				p2: 'value2b' ,
				versions: []
			} ,
			{
				_id: batch[ 3 ]._id ,	// unpredictable
				_version: 4 ,
				_lastModified: batch[ 3 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1c' ,
				p2: 'value2c' ,
				versions: []
			}
		] ) ;


		// Test the commit feature

		dbVersionedItem.name = 'ITEM#1' ;
		await dbVersionedItem.commit() ;

		expect( dbVersionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 6 ,
			_lastModified: dbVersionedItem._lastModified ,	// unpredictable
			name: 'ITEM#1' ,
			p1: 'value1-over' ,
			p2: 'value2-over' ,
			versions: []
		} ) ;

		dbVersionedItem = await versionedItems.get( versionedItemId ) ;

		expect( dbVersionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 6 ,
			_lastModified: dbVersionedItem._lastModified ,	// unpredictable
			name: 'ITEM#1' ,
			p1: 'value1-over' ,
			p2: 'value2-over' ,
			versions: []
		} ) ;

		batch = await versions.find( { '_activeVersion._id': versionedItemId , '_activeVersion._collection': 'versionedItems' } ) ;
		expect( batch ).to.be.like( [
			{
				_id: batch[ 0 ]._id ,	// unpredictable
				_version: 1 ,
				_lastModified: batch[ 0 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1b' ,
				versions: []
			} ,
			{
				_id: batch[ 1 ]._id ,	// unpredictable
				_version: 2 ,
				_lastModified: batch[ 1 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1c' ,
				versions: []
			} ,
			{
				_id: batch[ 2 ]._id ,	// unpredictable
				_version: 3 ,
				_lastModified: batch[ 2 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1c' ,
				p2: 'value2b' ,
				versions: []
			} ,
			{
				_id: batch[ 3 ]._id ,	// unpredictable
				_version: 4 ,
				_lastModified: batch[ 3 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1c' ,
				p2: 'value2c' ,
				versions: []
			} ,
			{
				_id: batch[ 4 ]._id ,  // unpredictable
				_version: 5 ,
				_lastModified: batch[ 4 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1-over' ,
				p2: 'value2-over' ,
				versions: []
			}
		] ) ;


		// Test delete

		await dbVersionedItem.delete() ;
		await expect( () => versionedItems.get( versionedItemId ) ).to.reject.with.an( ErrorStatus , { type: 'notFound' } ) ;

		batch = await versions.find( { '_activeVersion._id': versionedItemId , '_activeVersion._collection': 'versionedItems' } ) ;
		expect( batch ).to.be.like( [
			{
				_id: batch[ 0 ]._id ,	// unpredictable
				_version: 1 ,
				_lastModified: batch[ 0 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1b' ,
				versions: []
			} ,
			{
				_id: batch[ 1 ]._id ,	// unpredictable
				_version: 2 ,
				_lastModified: batch[ 1 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1c' ,
				versions: []
			} ,
			{
				_id: batch[ 2 ]._id ,	// unpredictable
				_version: 3 ,
				_lastModified: batch[ 2 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1c' ,
				p2: 'value2b' ,
				versions: []
			} ,
			{
				_id: batch[ 3 ]._id ,	// unpredictable
				_version: 4 ,
				_lastModified: batch[ 3 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1c' ,
				p2: 'value2c' ,
				versions: []
			} ,
			{
				_id: batch[ 4 ]._id ,  // unpredictable
				_version: 5 ,
				_lastModified: batch[ 4 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'item#1' ,
				p1: 'value1-over' ,
				p2: 'value2-over' ,
				versions: []
			} ,
			{
				_id: batch[ 5 ]._id ,  // unpredictable
				_version: 6 ,
				_lastModified: batch[ 5 ]._lastModified ,	// unpredictable
				_activeVersion: {
					_id: versionedItemId ,
					_collection: 'versionedItems'
				} ,
				name: 'ITEM#1' ,
				p1: 'value1-over' ,
				p2: 'value2-over' ,
				versions: []
			}
		] ) ;
	} ) ;

	it( "setting a property to another value/object which is equal (in the 'doormen sens') should not create a new version" , async () => {
		var date1 = new Date() , date2 = new Date() ;

		var versionedItem = versionedItems.createDocument( {
			name: 'item#1' ,
			p1: 'value1a' ,
			p3: { a: 1 } ,
			p4: date1
		} ) ;

		var versionedItemId = versionedItem.getId() ;

		expect( versionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 1 ,
			_lastModified: versionedItem._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value1a' ,
			p3: { a: 1 } ,
			p4: date1 ,
			versions: []
		} ) ;

		await versionedItem.save() ;

		versionedItem.p1 = 'value1a' ;
		await versionedItem.save() ;

		// Version should be incremented now we have save it
		expect( versionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 1 ,
			_lastModified: versionedItem._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value1a' ,
			p3: { a: 1 } ,
			p4: date1 ,
			versions: []
		} ) ;

		var batch = await versions.find( { '_activeVersion._id': versionedItemId , '_activeVersion._collection': 'versionedItems' } ) ;
		expect( batch ).to.have.length( 0 ) ;

		versionedItem.p3 = { a: 1 } ;
		await versionedItem.save() ;

		// Version should be incremented now we have save it
		expect( versionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 1 ,
			_lastModified: versionedItem._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value1a' ,
			p3: { a: 1 } ,
			p4: date1 ,
			versions: []
		} ) ;

		batch = await versions.find( { '_activeVersion._id': versionedItemId , '_activeVersion._collection': 'versionedItems' } ) ;
		expect( batch ).to.have.length( 0 ) ;

		versionedItem.p4 = date2 ;
		await versionedItem.save() ;

		// Version should be incremented now we have save it
		expect( versionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 1 ,
			_lastModified: versionedItem._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value1a' ,
			p3: { a: 1 } ,
			p4: date1 ,
			versions: []
		} ) ;

		batch = await versions.find( { '_activeVersion._id': versionedItemId , '_activeVersion._collection': 'versionedItems' } ) ;
		expect( batch ).to.have.length( 0 ) ;
	} ) ;

	it( "race conditions" , async () => {
		expect( versionedItems.versioning ).to.be( true ) ;

		var versionedItem = versionedItems.createDocument( {
			name: 'item#1' ,
			p1: 'value1a'
		} ) ;

		var versionedItemId = versionedItem.getId() ;

		expect( versionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 1 ,
			_lastModified: versionedItem._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value1a' ,
			versions: []
		} ) ;

		await versionedItem.save() ;

		var dbVersionedItem1 = await versionedItems.get( versionedItemId ) ;
		var dbVersionedItem2 = await versionedItems.get( versionedItemId ) ;

		expect( dbVersionedItem1 ).to.equal( {
			_id: versionedItemId ,
			_version: 1 ,
			_lastModified: dbVersionedItem1._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value1a' ,
			versions: []
		} ) ;

		expect( dbVersionedItem1 ).to.equal( dbVersionedItem2 ) ;

		dbVersionedItem1.p1 = 'value2a' ;
		dbVersionedItem2.p1 = 'value2b' ;
		await dbVersionedItem1.commit() ;
		await dbVersionedItem2.commit() ;

		dbVersionedItem1 = await versionedItems.get( versionedItemId ) ;

		expect( dbVersionedItem1 ).to.equal( {
			_id: versionedItemId ,
			_version: 3 ,
			_lastModified: dbVersionedItem1._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value2b' ,
			versions: []
		} ) ;

	} ) ;
} ) ;



describe( "Extended Document" , () => {

	beforeEach( clearDB ) ;

	it( "should call a method of the extended Document wrapper at creation and after retrieving it from DB" , async () => {
		var ext = extendables.createDocument( {
			data: 'sOmeDaTa'
		} ) ;

		expect( ext._ ).to.be.an( rootsDb.Document ) ;
		expect( ext._ ).to.be.an( Extended ) ;

		expect( ext._.getNormalized() ).to.be( 'somedata' ) ;

		await ext.save() ;
		var dbExt = await extendables.get( ext._id ) ;

		expect( dbExt._ ).to.be.an( rootsDb.Document ) ;
		expect( dbExt._ ).to.be.an( Extended ) ;

		expect( dbExt ).to.equal( { _id: dbExt._id , data: 'sOmeDaTa' } ) ;
		expect( dbExt._.getNormalized() ).to.be( 'somedata' ) ;

		expect( dbExt.getNormalized() ).to.be( 'somedata' ) ;

		dbExt.data = 'mOreVespEnEGaS' ;

		expect( dbExt._.getNormalized() ).to.be( 'morevespenegas' ) ;

		// Check direct method usage (through proxy)
		expect( dbExt.getNormalized() ).to.be( 'morevespenegas' ) ;
	} ) ;

	it( "should call a method of the extended Batch wrapper at creation and after retrieving it from DB" , async () => {
		var ext1 = extendables.createDocument( { data: 'oNe' } ) ;
		var ext2 = extendables.createDocument( { data: 'tWo' } ) ;
		var ext3 = extendables.createDocument( { data: 'thRee' } ) ;

		//await Promise.all( [ ext1.save() , ext2.save() , ext3.save() ] ) ;
		await Promise.all( [ ext3.save() , ext2.save() , ext1.save() ] ) ;
		var dbBatch = await extendables.collect( {} ) ;

		// Sort that first...
		dbBatch.sort( ( a , b ) => ( a.data.charCodeAt( 0 ) * 1000 + a.data.charCodeAt( 1 ) ) - ( b.data.charCodeAt( 0 ) * 1000 + b.data.charCodeAt( 1 ) ) ) ;

		expect( dbBatch ).to.be.an( rootsDb.Batch ) ;
		expect( dbBatch ).to.be.an( ExtendedBatch ) ;
		expect( dbBatch ).to.be.an( Array ) ;
		expect( dbBatch.foo() ).to.be( 'oNetWothRee' ) ;
	} ) ;
} ) ;



describe( "Hooks" , () => {
	it( "'beforeCreateDocument'" ) ;
	it( "'afterCreateDocument'" ) ;
} ) ;



describe( "Dead-links behavior" , () => {
	it( "Test link and multi-link 'populate' behavior when encoutering dead-links" ) ;
	it( "Test Document#getLink()" ) ;
	it( "Test Document#getLink() and its dead-link repairing behavior for links and multi-links" ) ;
} ) ;



describe( "Historical bugs" , () => {

	beforeEach( clearDB ) ;

	it( "collect on empty collection with populate (was throwing uncaught error)" , async () => {
		var batch = await users.collect( {} , { populate: [ 'job' , 'godfather' ] } ) ;
		expect( batch ).to.be.like( [] ) ;
	} ) ;

	it.opt( "'keyTooLargeToIndex' should provide enough information to be debugged" , async () => {
		// This creates an index of 1119 bytes:
		var town = towns.createDocument( {
			name: 'Paris'.repeat( 100 ) ,
			meta: {
				country: 'France'.repeat( 100 )
			}
		} ) ;

		await expect( () => town.save() ).to.reject.with( ErrorStatus , { type: 'badRequest' , code: 'keyTooLargeToIndex' } ) ;
	} ) ;

	it( "validation featuring sanitizers should update both locally and remotely after a document's commit()" , async () => {
		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: "60000"
		} ) ;

		var town = towns.createDocument( {
			name: 'Paris' ,
			meta: {
				rank: "7" ,
				population: '2200K' ,
				country: 'France'
			}
		} ) ;

		expect( job.salary ).to.be( 60000 ) ;	// toInteger at document's creation
		expect( town.meta.rank ).to.be( 7 ) ;	// toInteger at document's creation

		await job.save() ;
		await town.save() ;

		var dbJob = await jobs.get( job._id ) ;

		expect( dbJob ).to.equal( {
			_id: job._id , title: 'developer' , salary: 60000 , users: [] , schools: []
		} ) ;

		job.patch( { salary: "65000" } ) ;
		// Before sanitizing: it's a string
		expect( job ).to.equal( {
			_id: job._id , title: 'developer' , salary: "65000" , users: [] , schools: []
		} ) ;

		await job.commit() ;
		// After commit/sanitizing: now a number
		expect( job ).to.equal( {
			_id: job._id , title: 'developer' , salary: 65000 , users: [] , schools: []
		} ) ;

		dbJob = await jobs.get( job._id ) ;
		expect( dbJob ).to.equal( {
			_id: job._id , title: 'developer' , salary: 65000 , users: [] , schools: []
		} ) ;

		var dbTown = await towns.get( town._id ) ;
		expect( dbTown ).to.equal( { _id: town._id , name: 'Paris' , meta: { rank: 7 , population: '2200K' , country: 'France' } } ) ;

		town.patch( { "meta.rank": "8" } ) ;
		// Before sanitizing: it's a string
		expect( town ).to.equal( { _id: town._id , name: 'Paris' , meta: { rank: "8" , population: '2200K' , country: 'France' } } ) ;
		await town.commit() ;

		// After commit/sanitizing: now a number
		expect( town ).to.equal( { _id: town._id , name: 'Paris' , meta: { rank: 8 , population: '2200K' , country: 'France' } } ) ;

		dbTown = await towns.get( town._id ) ;
		expect( dbTown ).to.equal( { _id: town._id , name: 'Paris' , meta: { rank: 8 , population: '2200K' , country: 'France' } } ) ;
	} ) ;

	it( "setting garbage to an attachment property should abort with an error" , async () => {
		var user ;

		// First try: at object creation
		expect( () => {
			user = users.createDocument( {
				firstName: 'Jilbert' ,
				lastName: 'Polson' ,
				file: 'garbage'
			} ) ;
		} ).to.throw() ;

		user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;


		// Second try: using setLink
		expect( () => { user.$.setLink( 'file' , 'garbage' ) ; } ).to.throw() ;
		expect( user.file ).to.be( undefined ) ;

		// third try: by setting the property directly
		user.file = 'garbage' ;
		expect( () => { user.validate() ; } ).to.throw() ;

		// By default, a collection has the validateOnSave option, so we have to .save()
		await expect( () => user.save() ).to.reject() ;
		await expect( () => users.get( user._id ) ).to.reject() ;
	} ) ;

	it( "the special field _id should be taken as indexed by default, allowing queries on _id" , async () => {
		expect( users.indexedProperties._id ).to.be.ok() ;
		expect( users.uniques[ 0 ] ).to.equal( [ '_id' ] ) ;
	} ) ;

	it( "direct assignment to a (multi)link (without using .setLink()) should at least transform to proper link (as document) and populate the document proxy" , async () => {
		var map , batch , dbSchool ;

		var school = schools.createDocument( {
			title: 'Computer Science'
		} ) ;

		var id = school.getId() ;

		var job1 = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var job1Id = job1.getId() ;

		var job2 = jobs.createDocument( {
			title: 'sysadmin' ,
			salary: 55000
		} ) ;

		var job2Id = job2.getId() ;

		var job3 = jobs.createDocument( {
			title: 'front-end developer' ,
			salary: 54000
		} ) ;

		var job3Id = job3.getId() ;

		// direct access
		school.jobs = [ job1 , job2 , job1 ] ;
		expect( school.jobs ).to.equal( [ job1 , job2 , job1 ] ) ;
		expect( school._.raw.jobs ).to.equal( [ { _id: job1Id } , { _id: job2Id } , { _id: job1Id } ] ) ;
		expect( school._.populatedDocumentProxies.get( school._.raw.jobs[ 0 ] ) ).to.be( job1 ) ;

		await Promise.all( [ job1.save() , job2.save() , job3.save() , school.save() ] ) ;

		// Duplicated links should be removed now, proxy Set did not trigger the validator, but .save() does...

		dbSchool = await schools.get( id ) ;
		expect( dbSchool ).to.equal( { _id: id , title: 'Computer Science' , jobs: [ { _id: job1Id } , { _id: job2Id } ] } ) ;
		expect( dbSchool._.raw.jobs ).to.equal( [ { _id: job1Id } , { _id: job2Id } ] ) ;
		expect( dbSchool._.populatedDocumentProxies.get( dbSchool._.raw.jobs[ 0 ] ) ).to.be.undefined() ;

		batch = await school.getLink( "jobs" ) ;
		//log.hdebug( "after .save(), using .getLink() %Y" , batch ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.title ] = doc ; } ) ;

		expect( map ).to.equal( {
			developer: {
				_id: job1Id , title: 'developer' , salary: 60000 , users: [] , schools: []
			} ,
			sysadmin: {
				_id: job2Id , title: 'sysadmin' , salary: 55000 , users: [] , schools: []
			}
		} ) ;
	} ) ;

	it( "direct assignment to a (multi)link (without using .setLink()) should at least transform to proper link (as document-like object) and populate the document proxy" , async () => {
		var map , batch , dbSchool ;

		var school = schools.createDocument( {
			title: 'Computer Science'
		} ) ;

		var id = school.getId() ;

		var job1 = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var job1Id = job1.getId() ;

		var job2 = jobs.createDocument( {
			title: 'sysadmin' ,
			salary: 55000
		} ) ;

		var job2Id = job2.getId() ;

		var job3 = jobs.createDocument( {
			title: 'front-end developer' ,
			salary: 54000
		} ) ;

		var job3Id = job3.getId() ;

		// direct access
		var multiLink = JSON.parse( JSON.stringify( [ job1 , job2 , job1 ] ) ) ;
		//log.hdebug( "%Y" , multiLink ) ;
		school.jobs = multiLink ;
		expect( school.jobs ).to.equal( multiLink ) ;
		expect( school._.raw.jobs ).to.equal( multiLink ) ;
		expect( school._.populatedDocumentProxies.get( school._.raw.jobs[ 0 ] ) ).to.be.undefined() ;

		await Promise.all( [ job1.save() , job2.save() , job3.save() , school.save() ] ) ;

		// Duplicated links should be removed now, proxy Set did not trigger the validator, but .save() does...

		dbSchool = await schools.get( id ) ;
		expect( dbSchool ).to.equal( { _id: id , title: 'Computer Science' , jobs: [ { _id: job1Id } , { _id: job2Id } ] } ) ;
		expect( dbSchool._.raw.jobs ).to.equal( [ { _id: job1Id } , { _id: job2Id } ] ) ;
		expect( dbSchool._.populatedDocumentProxies.get( dbSchool._.raw.jobs[ 0 ] ) ).to.be.undefined() ;

		batch = await school.getLink( "jobs" ) ;
		//log.hdebug( "after .save(), using .getLink() %Y" , batch ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.title ] = doc ; } ) ;

		expect( map ).to.equal( {
			developer: {
				_id: job1Id , title: 'developer' , salary: 60000 , users: [] , schools: []
			} ,
			sysadmin: {
				_id: job2Id , title: 'sysadmin' , salary: 55000 , users: [] , schools: []
			}
		} ) ;
	} ) ;

	it( "patch on link providing non-ID" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var userId = user.getId() ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var jobId = job.getId() ;

		await Promise.all( [ user.save() , job.save() ] ) ;
		var dbUser = await users.get( userId ) ;

		// Forbid access to internal properties of a link: link are "opaque"
		expect( () => dbUser.patch( { "job._id": jobId } , { validate: true } ) ).to.throw.a( doormen.ValidatorError ) ;
		dbUser.patch( { job: { _id: jobId } } , { validate: true } ) ;
		//log.hdebug( "%Y" , dbUser._.raw ) ;
		expect( dbUser.job._id ).to.equal( jobId ) ;
		await expect( dbUser.getLink( 'job' ) ).to.eventually.equal( {
			_id: jobId ,
			title: "developer" ,
			salary: 60000 ,
			users: [] ,
			schools: []
		} ) ;
	} ) ;

	it( "patch with validate option off on multi-link providing non-ID" , async () => {
		var map , batch , dbSchool ;

		var school = schools.createDocument( {
			title: 'Computer Science'
		} ) ;

		var id = school.getId() ;

		var job1 = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var job1Id = job1.getId() ;

		var job2 = jobs.createDocument( {
			title: 'sysadmin' ,
			salary: 55000
		} ) ;

		var job2Id = job2.getId() ;

		var job3 = jobs.createDocument( {
			title: 'front-end developer' ,
			salary: 54000
		} ) ;

		var job3Id = job3.getId() ;

		await Promise.all( [ job1.save() , job2.save() , job3.save() , school.save() ] ) ;
		await expect( schools.get( id ) ).to.eventually.equal( { _id: id , title: 'Computer Science' , jobs: [] } ) ;

		// No validate: so it is stored as it is
		school.patch( { jobs: [ { _id: '' + job1._id , title: 'developer' , salary: 60000 } , { _id: '' + job2._id } ] } ) ;
		expect( school._.raw ).to.equal( { _id: id , title: 'Computer Science' , jobs: [ { _id: '' + job1._id , title: 'developer' , salary: 60000 } , { _id: '' + job2._id } ] } ) ;

		// No we save it, so validation happens NOW!
		await school.save() ;
		expect( school._.raw ).to.equal( { _id: id , title: 'Computer Science' , jobs: [ { _id: job1Id } , { _id: job2Id } ] } ) ;

		await Promise.all( [ job1.save() , job2.save() , job3.save() , school.save() ] ) ;
		await expect( schools.get( id ) ).to.eventually.equal( { _id: id , title: 'Computer Science' , jobs: [ { _id: job1Id } , { _id: job2Id } ] } ) ;
	} ) ;

	it( "patch with validate option on on multi-link providing non-ID" , async () => {
		var map , batch , dbSchool ;

		var school = schools.createDocument( {
			title: 'Computer Science'
		} ) ;

		var id = school.getId() ;

		var job1 = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var job1Id = job1.getId() ;

		var job2 = jobs.createDocument( {
			title: 'sysadmin' ,
			salary: 55000
		} ) ;

		var job2Id = job2.getId() ;

		var job3 = jobs.createDocument( {
			title: 'front-end developer' ,
			salary: 54000
		} ) ;

		var job3Id = job3.getId() ;

		await Promise.all( [ job1.save() , job2.save() , job3.save() , school.save() ] ) ;
		await expect( schools.get( id ) ).to.eventually.equal( { _id: id , title: 'Computer Science' , jobs: [] } ) ;

		// It's validated NOW: so it is stored the way it should be
		school.patch( { jobs: [ { _id: '' + job1._id , title: 'developer' , salary: 60000 } , { _id: '' + job2._id } ] } , { validate: true } ) ;
		expect( school._.raw ).to.equal( { _id: id , title: 'Computer Science' , jobs: [ { _id: job1Id } , { _id: job2Id } ] } ) ;

		// No changes...
		await school.save() ;
		expect( school._.raw ).to.equal( { _id: id , title: 'Computer Science' , jobs: [ { _id: job1Id } , { _id: job2Id } ] } ) ;

		await Promise.all( [ job1.save() , job2.save() , job3.save() , school.save() ] ) ;
		await expect( schools.get( id ) ).to.eventually.equal( { _id: id , title: 'Computer Science' , jobs: [ { _id: job1Id } , { _id: job2Id } ] } ) ;
	} ) ;

	it( "should fix the bug where .commit() auto-sanitize on array creates bad top-level key with brackets (bug in doormen's patch)" , async () => {
		var product1 = products.createDocument( {
			name: 'pencil' ,
			price: 1.20
		} ) ;

		var productId1 = product1.getId() ;

		var product2 = products.createDocument( {
			name: 'eraser' ,
			price: 1.60
		} ) ;

		var productId2 = product2.getId() ;

		var store1 = stores.createDocument( {
			name: 'Le Grand Bozar' ,
			products: [
				{ product: product1 , quantity: 56 } ,
				{ product: product2 , quantity: 37 }
			]
		} ) ;

		var storeId1 = store1.getId() ;
		//log.hdebug( "store1: %[5]Y" , store1 ) ;

		await product1.save() ;
		await product2.save() ;
		await store1.save() ;
		

		var dbStore1 = await stores.get( storeId1 ) ;
		
		//log.hdebug( "dbStore1: %[5]Y" , dbStore1 ) ;
		expect( dbStore1 ).to.equal( {
			_id: storeId1 ,
			name: 'Le Grand Bozar' ,
			products: [
				{ product: { _id: productId1 } , quantity: 56 } ,
				{ product: { _id: productId2 } , quantity: 37 }
			] ,
			productBatches: []
		} ) ;

		// Force creating a patch that would have to sanitize the array
		dbStore1.products[ 0 ].quantity = "21" ;
		// use options validate: true, specifically because it's how it works in RestQuery's .patchDocument()
		dbStore1.patch( { "products.1.quantity": 12 } , { validate: true } ) ;

		expect( dbStore1 ).to.equal( {
			_id: storeId1 ,
			name: 'Le Grand Bozar' ,
			products: [
				{ product: { _id: productId1 } , quantity: "21" } ,
				{ product: { _id: productId2 } , quantity: 12 }
			] ,
			productBatches: []
		} ) ;

		await dbStore1.commit() ;

		var rawDbStore1 = await stores.get( storeId1 , { raw: true } ) ;
		//log.hdebug( "rawDbStore1: %[5]Y" , rawDbStore1 ) ;

		// THIS IS THE BUG, because .commit() creates the patch: { "products[0].quantity": 21 }
		// instead of: { "products.0.quantity": 21 } 
		expect( rawDbStore1 ).not.to.have.key( 'products[0]' ) ;

		expect( rawDbStore1 ).to.equal( {
			_id: storeId1 ,
			name: 'Le Grand Bozar' ,
			products: [
				{ product: { _id: productId1 } , quantity: 21 } ,
				{ product: { _id: productId2 } , quantity: 12 }
			] ,
			productBatches: []
		} ) ;
	} ) ;

	it( "yyy should fix the bug where the .commit()'s patch overlaps itself" , async () => {
		var product1 = products.createDocument( {
			name: 'pencil' ,
			price: 1.20
		} ) ;

		var productId1 = product1.getId() ;

		var product2 = products.createDocument( {
			name: 'eraser' ,
			price: 1.60
		} ) ;

		var productId2 = product2.getId() ;

		var store1 = stores.createDocument( {
			name: 'Le Grand Bozar' ,
			products: [
				{ product: product1 , quantity: 56 } ,
				{ product: product2 , quantity: 37 }
			]
		} ) ;

		var storeId1 = store1.getId() ;
		//log.hdebug( "store1: %[5]Y" , store1 ) ;

		await product1.save() ;
		await product2.save() ;
		await store1.save() ;
		

		var dbStore1 = await stores.get( storeId1 ) ;
		
		//log.hdebug( "dbStore1: %[5]Y" , dbStore1 ) ;
		expect( dbStore1 ).to.equal( {
			_id: storeId1 ,
			name: 'Le Grand Bozar' ,
			products: [
				{ product: { _id: productId1 } , quantity: 56 } ,
				{ product: { _id: productId2 } , quantity: 37 }
			] ,
			productBatches: []
		} ) ;

		// Force creating a patch that would have to sanitize the array
		dbStore1.products[ 0 ].quantity = "21" ;
		dbStore1.patch( { "products.0": { product: { _id: productId1 } , quantity: "22" } } ) ;

		expect( dbStore1 ).to.equal( {
			_id: storeId1 ,
			name: 'Le Grand Bozar' ,
			products: [
				{ product: { _id: productId1 } , quantity: "22" } ,
				{ product: { _id: productId2 } , quantity: 37 }
			] ,
			productBatches: []
		} ) ;

		
		
		// Testing internal machinery
		// (see Document#commit() source-code, if something needs to be changed)

		var dbPatch = dbStore1._.buildDbPatch() ;
		//log.hdebug( ".buildDbPatch(): %Y" , dbPatch ) ;
		expect( dbPatch ).to.equal( {
			set: {
				"products.0": {
					product: { _id: productId1 } ,
					quantity: "22"
				}
			}
		} ) ;

		dbStore1._.collection.validateAndUpdatePatch( dbStore1._.raw , dbPatch.set ) ;
		//log.hdebug( "after .validateAndUpdatePatch(): %Y" , dbPatch ) ;
		expect( dbPatch ).to.equal( {
			set: {
				"products.0": {
					product: { _id: productId1 } ,
					quantity: 22
				}
			}
		} ) ;
		
		

		// Here MongoDB would raise an error if the patch overlap itself
		await expect( dbStore1.commit() ).to.eventually.not.throw() ;

		var rawDbStore1 = await stores.get( storeId1 , { raw: true } ) ;
		
		expect( rawDbStore1 ).to.equal( {
			_id: storeId1 ,
			name: 'Le Grand Bozar' ,
			products: [
				{ product: { _id: productId1 } , quantity: 22 } ,
				{ product: { _id: productId2 } , quantity: 37 }
			] ,
			productBatches: []
		} ) ;
	} ) ;
} ) ;



describe( "Exporter" , () => {

	beforeEach( clearDB ) ;

	it( "should export db" , async () => {
		var user , job , userList = [] , jobList = [] ;

		job = jobs.createDocument( { title: 'developer' , salary: 60000 } ) ;
		jobList.push( job ) ;
		await job.save() ;

		job = jobs.createDocument( { title: 'sysadmin' , salary: 55000 } ) ;
		jobList.push( job ) ;
		await job.save() ;

		user = users.createDocument( { firstName: 'Joe' , lastName: 'Doe' } )
		user.job = jobList[ 0 ] ;
		userList.push( user ) ;
		await user.save() ;

		user = users.createDocument( { firstName: 'Jack' , lastName: 'Wilson' } )
		userList.push( user ) ;
		await user.save() ;

		user = users.createDocument( { firstName: 'Tony' , lastName: 'Stark' } )
		userList.push( user ) ;
		await user.save() ;

		await world.export( path.join( __dirname , 'exporter' ) ) ;
		
		var usersContent = await fs.promises.readFile( path.join( __dirname , 'exporter' , "users.jsonstream" ) , 'utf8' ) ;
		expect( usersContent ).to.be(
			'{"_id":"' + userList[ 0 ].getId() + '","firstName":"Joe","lastName":"Doe","memberSid":"Joe Doe","job":{"_id":"' + jobList[ 0 ].getId() + '"}}\n'
			+ '{"_id":"' + userList[ 1 ].getId() + '","firstName":"Jack","lastName":"Wilson","memberSid":"Jack Wilson"}\n'
			+ '{"_id":"' + userList[ 2 ].getId() + '","firstName":"Tony","lastName":"Stark","memberSid":"Tony Stark"}\n'
		) ;

		var jobsContent = await fs.promises.readFile( path.join( __dirname , 'exporter' , "jobs.jsonstream" ) , 'utf8' ) ;
		expect( jobsContent ).to.be(
			'{"_id":"' + jobList[ 0 ].getId() + '","title":"developer","salary":60000,"users":[],"schools":[]}\n'
			+ '{"_id":"' + jobList[ 1 ].getId() + '","title":"sysadmin","salary":55000,"users":[],"schools":[]}\n'
		) ;
	} ) ;
} ) ;



if ( IMPORTER ) {
	describe( "Importer" , () => {

		beforeEach( clearDB ) ;

		it( "should import data" , async () => {
			await world.import( path.join( __dirname , 'importer' , 'mapping.json' ) ) ;
			
			var batch = await jobs.collect( {} ) ;

			//log.info( "Jobs: %I" , [ ... batch ] ) ;

			// MongoDB may shuffle things up, so we don't use an array here
			var map = {} ;
			batch.forEach( doc => map[ doc.title ] = doc ) ;

			expect( map ).to.only.have.own.keys( 'dev' , 'devops' ) ;
			expect( map ).to.partially.equal( {
				dev: {
					_id: map.dev.getId() , title: 'dev' , salary: 3500
				} ,
				devops: {
					_id: map.devops.getId() , title: 'devops' , salary: 3200
				}
			} ) ;
			
		} ) ;
	} ) ;
}



if ( FAKE_DATA_GENERATOR ) {
	describe( "Fake data generation" , () => {

		beforeEach( clearDB ) ;

		it( "should generate fake document on a collection" , async () => {
			var user = users.createFakeDocument() ;
			log.info( "User: %I" , user ) ;
			expect( user ).to.be.an( Object ) ;
			expect( user.$ ).to.be.an( Object ) ;
			expect( user._ ).to.be.a( rootsDb.Document ) ;
			expect( user._id ).to.be.an( mongodb.ObjectId ) ;
			expect( user.getId() ).to.be.an( mongodb.ObjectId ) ;
			expect( user._id ).to.be( user.getId() ) ;

			expect( user.firstName ).not.to.be.empty() ;
			expect( user.firstName ).to.be.a( 'string' ) ;
			expect( user.lastName ).not.to.be.empty() ;
			expect( user.lastName ).to.be.a( 'string' ) ;

			await user.save() ;
			
			var dbUser = await users.get( user._id ) ;
			log.info( "DB User: %I" , dbUser ) ;
			expect( dbUser ).to.be.an( Object ) ;
			expect( dbUser.$ ).to.be.an( Object ) ;
			expect( dbUser._ ).to.be.a( rootsDb.Document ) ;
			expect( dbUser._id ).to.be.an( mongodb.ObjectId ) ;
			expect( dbUser.getId() ).to.be.an( mongodb.ObjectId ) ;
			expect( dbUser._id ).to.be( dbUser.getId() ) ;

			expect( dbUser.firstName ).not.to.be.empty() ;
			expect( dbUser.firstName ).to.be.a( 'string' ) ;
			expect( dbUser.lastName ).not.to.be.empty() ;
			expect( dbUser.lastName ).to.be.a( 'string' ) ;


			var job = jobs.createFakeDocument() ;
			log.info( "Job: %I" , job ) ;
			expect( job ).to.be.an( Object ) ;
			expect( job._ ).to.be.a( rootsDb.Document ) ;
			expect( job.title ).not.to.be.empty() ;
			expect( job.title ).to.be.a( 'string' ) ;
		} ) ;

		it( "should generate fake batch of documents on a collection" , async () => {
			var userBatch = users.createFakeBatch( 3 ) ;
			log.info( "User batch: %I" , userBatch ) ;

			expect( Array.isArray( userBatch ) ).to.be.ok() ;
			expect( userBatch ).to.be.an( Array ) ;
			expect( userBatch ).to.be.a( rootsDb.Batch ) ;
			expect( userBatch ).to.have.length( 3 ) ;

			for ( let index = 0 ; index < 3 ; index ++ ) {
				expect( userBatch[ index ] ).to.be.an( Object ) ;
				expect( userBatch[ index ].$ ).to.be.an( Object ) ;
				expect( userBatch[ index ]._ ).to.be.a( rootsDb.Document ) ;
				expect( userBatch[ index ]._id ).to.be.an( mongodb.ObjectId ) ;
				expect( userBatch[ index ].getId() ).to.be.an( mongodb.ObjectId ) ;
				expect( userBatch[ index ]._id ).to.be( userBatch[ index ].getId() ) ;
				expect( userBatch[ index ].firstName ).not.to.be.empty() ;
				expect( userBatch[ index ].firstName ).to.be.a( 'string' ) ;
				expect( userBatch[ index ].lastName ).not.to.be.empty() ;
				expect( userBatch[ index ].lastName ).to.be.a( 'string' ) ;
			}

			await userBatch.save() ;
			
			var dbUserBatch = await users.find( {} ) ;
			log.info( "DB User Batch: %I" , dbUserBatch ) ;
			expect( Array.isArray( dbUserBatch ) ).to.be.ok() ;
			expect( dbUserBatch ).to.be.an( Array ) ;
			expect( dbUserBatch ).to.be.a( rootsDb.Batch ) ;
			expect( dbUserBatch ).to.have.length( 3 ) ;

			for ( let index = 0 ; index < 3 ; index ++ ) {
				expect( dbUserBatch[ index ] ).to.be.an( Object ) ;
				expect( dbUserBatch[ index ].$ ).to.be.an( Object ) ;
				expect( dbUserBatch[ index ]._ ).to.be.a( rootsDb.Document ) ;
				expect( dbUserBatch[ index ]._id ).to.be.an( mongodb.ObjectId ) ;
				expect( dbUserBatch[ index ].getId() ).to.be.an( mongodb.ObjectId ) ;
				expect( dbUserBatch[ index ]._id ).to.be( dbUserBatch[ index ].getId() ) ;
				expect( dbUserBatch[ index ].firstName ).not.to.be.empty() ;
				expect( dbUserBatch[ index ].firstName ).to.be.a( 'string' ) ;
				expect( dbUserBatch[ index ].lastName ).not.to.be.empty() ;
				expect( dbUserBatch[ index ].lastName ).to.be.a( 'string' ) ;
			}
		} ) ;
	} ) ;
}



// Move slow tests at the end
describe( "Slow tests" , () => {
	describe( "Build collections' indexes" , () => {

		beforeEach( clearDBIndexes ) ;
		//beforeEach( dropDBCollections ) ;

		it.opt( "should build indexes" , async function() {
			//console.log( "start test" ) ;
			this.timeout( 15000 ) ;
			expect( users.uniques ).to.equal( [ [ '_id' ] , [ 'memberSid' , 'job._id' ] ] ) ;
			expect( jobs.uniques ).to.equal( [ [ '_id' ] ] ) ;

			return Promise.map( Object.keys( world.collections ) , async ( name ) => {
				var collection = world.collections[ name ] ;

				try {
					await collection.buildIndexes() ;
					var indexes = await collection.driver.getIndexes() ;
					//log.hdebug( "Index built for collection %s\nDB indexes: %Y\nSchema indexes: %Y" , name , indexes , collection.indexes ) ;

					// Should be reversed: indexes has less key than collection.indexes...
					// Also it's not an optimal test, it should be more detailed.
					//expect( collection.indexes ).to.be.partially.like( indexes ) ;

					expect( Object.keys( indexes ) ).to.have.length.of( Object.keys( collection.indexes ).length ) ;
					//log.hdebug( ">>>>>>>>>>>>>>>>> %s\nDB: %Y\nframework: %Y" , name , indexes , collection.indexes ) ;

					Object.keys( indexes ).forEach( indexName => {
						var index = indexes[ indexName ] ;
						var cIndex = collection.indexes[ indexName ] ;
						expect( index.properties ).to.equal( cIndex.properties ) ;
						expect( index.unique || false ).to.be( cIndex.unique ) ;

						if ( cIndex.partial ) { expect( index.partialFilterExpression ).to.be.ok() ; }
						else { expect( index.partialFilterExpression ).not.to.be.ok() ; }

						if ( cIndex.collation ) { expect( index.collation ).to.partially.equal( cIndex.collation ) ; }
						else { expect( index.collation ).not.to.be.ok() ; }
					} ) ;
				}
				catch ( error ) {
					log.error( "Failed for %s: %E" , collection.name , error ) ;
					throw error ;
				}
			} ) ;
		} ) ;
	} ) ;

	it.opt( "Test user/password in the connection string" , async () => {
		/*
			First, create a user in the mongo-shell with the command:
			db.createUser( { user: 'restricted' , pwd: 'restricted-pw' , roles: [ { role: "readWrite", db: "rootsDb-restricted" } ] } )
		*/

		var world_ = new rootsDb.World() ;
		var descriptor = {
			url: 'mongodb://restricted:restricted-pw@localhost:27017/rootsDb-restricted/restrictedCollection' ,
			properties: {
				prop1: {
					type: 'string'
				} ,
				prop2: {
					type: 'string'
				}
			}
		} ;

		var restrictedCollection = await world_.createAndInitCollection( 'restrictedCollection' , descriptor ) ;

		//console.log( "restrictedCollection.url:" , restrictedCollection.url ) ;
		//console.log( "restrictedCollection.config:" , restrictedCollection.config ) ;
		//console.log( "restrictedCollection.driver.url:" , restrictedCollection.driver.url ) ;

		var doc = restrictedCollection.createDocument( {
			prop1: 'v1' ,
			prop2: 'v2'
		} ) ;

		var id = doc.getId() ;

		await doc.save() ;
		var dbDoc = await restrictedCollection.get( id ) ;

		expect( dbDoc ).to.be.an( Object ) ;
		expect( dbDoc._ ).to.be.a( rootsDb.Document ) ;
		expect( dbDoc._id ).to.be.an( mongodb.ObjectId ) ;
		expect( dbDoc._id ).to.equal( id ) ;
		expect( dbDoc ).to.equal( { _id: doc._id , prop1: 'v1' , prop2: 'v2' } ) ;


		// Check failure
		var descriptor2 = {
			url: 'mongodb://restricted:badpwé@localhost:27017/rootsDb-restricted/restrictedCollection2' ,
			properties: {
				prop1: {
					type: 'string'
				} ,
				prop2: {
					type: 'string'
				}
			}
		} ;

		var restrictedCollection2 = await world_.createAndInitCollection( 'restrictedCollection2' , descriptor2 ) ;
		var doc2 = restrictedCollection2.createDocument( {
			prop1: 'v3' ,
			prop2: 'v4'
		} ) ;
		id = doc2.getId() ;

		await expect( () => doc2.save() ).to.reject() ;
	} ) ;
} ) ;

