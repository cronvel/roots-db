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
testOption( 'attachment-driver' ) ;

var ATTACHMENT_MODE , USERS_ATTACHMENT_URL ;
const ATTACHMENT_PUBLIC_BASE_URL = 'http://example.cdn.net/example' ;

switch ( getTestOption( 'attachment-driver' ) ) {
	case 's3' :
		ATTACHMENT_MODE = 's3' ;
		USERS_ATTACHMENT_URL = require( './s3-config.local.json' ).attachmentUrl ;
		break ;
	case 'file' :
	default :
		ATTACHMENT_MODE = 'file' ;
		USERS_ATTACHMENT_URL = 'file://' + __dirname + '/tmp/' ;
		break ;
}

// Init extensions
rootsDb.initExtensions() ;

// Create the world...
const world = new rootsDb.World() ;

// Collections...
var versions , users , jobs , schools , towns , lockables , nestedLinks , anyCollectionLinks , versionedItems , extendables ;

const versionsDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/versions' ,
	attachmentUrl: 'file://' + __dirname + '/tmp/'
} ;

const usersDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/users' ,
	attachmentUrl: USERS_ATTACHMENT_URL ,
	attachmentPublicBaseUrl: ATTACHMENT_PUBLIC_BASE_URL ,
	properties: {
		firstName: {
			type: 'string' ,
			maxLength: 30 ,
			default: 'Joe'
		} ,
		lastName: {
			type: 'string' ,
			maxLength: 30 ,
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
	properties: {
		title: {
			type: 'string' ,
			maxLength: 50 ,
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
			// Ti use with the collation unit test
			collation: {
				locale: 'en' ,
				caseLevel: true ,
				numericOrdering: true
			}
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

const lockablesDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/lockables' ,
	canLock: true ,
	lockTimeout: 40 ,
	properties: {
		data: { type: 'string' }
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
		dropCollection( users ) ,
		dropCollection( jobs ) ,
		dropCollection( schools ) ,
		dropCollection( towns ) ,
		dropCollection( lockables ) ,
		dropCollection( nestedLinks ) ,
		dropCollection( anyCollectionLinks ) ,
		dropCollection( versionedItems ) ,
		dropCollection( extendables )
	] ) ;
}



// clear DB: remove every item, so we can safely test
function clearDB() {
	return Promise.all( [
		clearCollection( versions ) ,
		clearCollection( users ) ,
		clearCollection( jobs ) ,
		clearCollection( schools ) ,
		clearCollection( towns ) ,
		clearCollection( lockables ) ,
		clearCollection( nestedLinks ) ,
		clearCollection( anyCollectionLinks ) ,
		clearCollection( versionedItems ) ,
		clearCollection( extendables )
	] ) ;
}



// clear DB indexes: remove all indexes
function clearDBIndexes() {
	return Promise.all( [
		clearCollectionIndexes( versions ) ,
		clearCollectionIndexes( users ) ,
		clearCollectionIndexes( jobs ) ,
		clearCollectionIndexes( schools ) ,
		clearCollectionIndexes( towns ) ,
		clearCollectionIndexes( lockables ) ,
		clearCollectionIndexes( nestedLinks ) ,
		clearCollectionIndexes( anyCollectionLinks ) ,
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



function clearCollection( collection ) {
	return collection.driver.rawInit()
		.then( () => collection.driver.raw.deleteMany() )
		.catch( error => {
			if ( error.code === 26 ) { return ; }	// NS not found, nothing to clear!
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



/* Tests */



// Force creating the collection
before( async () => {
	versions = await world.createAndInitVersionCollection( 'versions' , versionsDescriptor ) ;
	expect( versions ).to.be.a( rootsDb.VersionCollection ) ;

	users = await world.createAndInitCollection( 'users' , usersDescriptor ) ;
	expect( users ).to.be.a( rootsDb.Collection ) ;

	jobs = await world.createAndInitCollection( 'jobs' , jobsDescriptor ) ;
	expect( jobs ).to.be.a( rootsDb.Collection ) ;

	schools = await world.createAndInitCollection( 'schools' , schoolsDescriptor ) ;
	expect( schools ).to.be.a( rootsDb.Collection ) ;

	towns = await world.createAndInitCollection( 'towns' , townsDescriptor ) ;
	expect( towns ).to.be.a( rootsDb.Collection ) ;

	lockables = await world.createAndInitCollection( 'lockables' , lockablesDescriptor ) ;
	expect( lockables ).to.be.a( rootsDb.Collection ) ;

	nestedLinks = await world.createAndInitCollection( 'nestedLinks' , nestedLinksDescriptor ) ;
	expect( nestedLinks ).to.be.a( rootsDb.Collection ) ;

	anyCollectionLinks = await world.createAndInitCollection( 'anyCollectionLinks' , anyCollectionLinksDescriptor ) ;
	expect( anyCollectionLinks ).to.be.a( rootsDb.Collection ) ;

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
		expect( users.documentSchema ).to.equal(
			{
				url: "mongodb://localhost:27017/rootsDb/users" ,
				attachmentUrl: USERS_ATTACHMENT_URL ,
				attachmentPublicBaseUrl: ATTACHMENT_PUBLIC_BASE_URL ,
				properties: {
					firstName: {
						type: "string" , maxLength: 30 , default: "Joe" , tags: [ "content" ] , inputHint: "text"
					} ,
					lastName: {
						type: "string" , maxLength: 30 , default: "Doe" , tags: [ "content" ] , inputHint: "text"
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
						type: "attachment" , optional: true , tags: [ "content" ] , inputHint: "file"
					} ,
					publicKey: {
						type: "attachment" , optional: true , tags: [ "content" ] , inputHint: "file"
					} ,
					file: {
						type: "attachment" , optional: true , tags: [ "content" ] , inputHint: "file"
					} ,
					_id: {
						type: "objectId" , sanitize: "toObjectId" , optional: true , system: true , tags: [ "id" ]
					}
				} ,
				indexes: [
					{
						name: 'Hzz2C_5P5AOjVdPSG_7URlrUcAk' , properties: { "job._id": 1 } , links: { job: 1 } , unique: false , partial: false
					} ,
					{
						name: 'EyFhPyID5m2EHyOTgAvC5I8AOeY' , properties: { "job._id": 1 , memberSid: 1 } , links: { job: 1 } , unique: true , partial: false
					}
				] ,
				hooks: users.documentSchema.hooks ,
				versioning: false ,
				canLock: false ,
				lockTimeout: 1000 ,
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
						type: "objectId" , sanitize: "toObjectId" , optional: true , system: true , tags: [ "id" ]
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
						unique: true ,
						partial: false ,
						collation: {
							locale: 'en' ,
							caseLevel: true ,
							numericOrdering: true
						}
					}
				] ,
				hooks: schools.documentSchema.hooks ,
				versioning: false ,
				canLock: false ,
				lockTimeout: 1000 ,
				refreshTimeout: 1000 ,
				Batch: schools.documentSchema.Batch ,
				Collection: schools.documentSchema.Collection ,
				Document: schools.documentSchema.Document
			}
		) ;

		expect( versions.documentSchema ).to.equal(
			{
				url: "mongodb://localhost:27017/rootsDb/versions" ,
				attachmentUrl: 'file://' + __dirname + '/tmp/' ,
				extraProperties: true ,
				properties: {
					_activeVersion: {
						type: "link" , anyCollection: true , inputHint: "embedded" , opaque: true , sanitize: [ "toLink" ] , system: true , tags: [ "system-content" ]
					} ,
					_id: {
						type: "objectId" , sanitize: "toObjectId" , optional: true , system: true , tags: [ "id" ]
					} ,
					_lastModified: {
						defaultFn: "now" , inputHint: "date" , sanitize: [ "toDate" ] , system: true , tags: [ "system-content" ] , type: "date"
					} ,
					_version: {
						default: 1 , inputHint: "text" , sanitize: [ "toInteger" ] , system: true , tags: [ "system-content" ] , type: "integer"
					}
				} ,
				indexes: [
					{
						name: "OgLcSH3E_bMZrjYG5DZG08RSJtc" ,
						properties: {
							"_activeVersion._collection": 1 ,
							"_activeVersion._id": 1
						} ,
						links: { _activeVersion: 1 } ,
						unique: false ,
						partial: false
					} ,
					{
						name: "Jqlr2UkS886iHcOUoIDbKoR_PEg" ,
						properties: {
							"_activeVersion._collection": 1 ,
							"_activeVersion._id": 1 ,
							_version: 1
						} ,
						links: { _activeVersion: 1 } ,
						unique: true ,
						partial: false
					}
				] ,
				hooks: versions.documentSchema.hooks ,
				versioning: false ,
				canLock: false ,
				lockTimeout: 1000 ,
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
		expect( user._id ).to.be.an( mongodb.ObjectID ) ;
		expect( user.getId() ).to.be.an( mongodb.ObjectID ) ;
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
		expect( user._id ).to.be.an( mongodb.ObjectID ) ;
		expect( user.getId() ).to.be.an( mongodb.ObjectID ) ;
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
		expect( user.__enumerate__() ).to.only.contain( '_id' , 'firstName' , 'lastName' , 'memberSid' ) ;

		user.setTagMask( [ 'id' ] ) ;
		expect( user.__enumerate__() ).to.only.contain( '_id' , 'memberSid' ) ;

		// Directly on creation
		user = users.createDocument( {
			firstName: 'Bobby' ,
			lastName: 'Fischer'
		} , {
			tagMask: [ 'id' ]
		} ) ;
		expect( user.__enumerate__() ).to.only.contain( '_id' , 'memberSid' ) ;

		town = towns.createDocument( {
			name: 'Paris' ,
			meta: {
				rank: 1 ,
				population: '2200K' ,
				country: 'France'
			}
		} ) ;
		expect( town.__enumerate__() ).to.only.contain( '_id' , 'name' , 'meta' ) ;
		expect( town.meta.__enumerate__() ).to.only.contain( 'rank' , 'population' , 'country' ) ;

		town.setTagMask( [ 'meta' , 'rank' ] ) ;
		expect( town.__enumerate__() ).to.only.contain( 'meta' ) ;
		expect( town.meta.__enumerate__() ).to.only.contain( 'rank' , 'population' , 'country' ) ;

		town.setTagMask( [ 'meta' ] ) ;
		expect( town.__enumerate__() ).to.only.contain( 'meta' ) ;
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

		expect( user.getId() ).to.be.an( mongodb.ObjectID ) ;
		expect( user._id ).to.be( user.getId() ) ;

		expect( user ).to.equal( {
			_id: user._id ,
			firstName: 'Bobby' ,
			lastName: 'Fischer' ,
			memberSid: 'Bobby Fischer'
		} ) ;

		var clone = user.clone() ;

		expect( clone.getId() ).to.be.an( mongodb.ObjectID ) ;
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
		expect( dbUser._id ).to.be.an( mongodb.ObjectID ) ;
		expect( dbUser._id ).to.equal( id ) ;
		expect( dbUser ).to.equal( {
			_id: dbUser._id , firstName: 'John' , lastName: 'McGregor' , memberSid: 'John McGregor'
		} ) ;
	} ) ;

	it( "when trying to get an unexistant document, an ErrorStatus (type: notFound) should be issued" , async () => {
		// Unexistant ID
		var id = new mongodb.ObjectID() ;

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
		expect( rawDbUser._id ).to.be.an( mongodb.ObjectID ) ;
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
		town._.addLocalChange( [ 'meta' , 'country' ] ) ;
		expect( town._.localChanges ).to.equal( { meta: { country: null } } ) ;
		expect( town._.buildDbPatch().set ).to.equal( { "meta.country": "France" } ) ;
		town._.addLocalChange( [ 'meta' ] ) ;
		expect( town._.localChanges ).to.equal( { meta: null } ) ;
		expect( town._.buildDbPatch().set ).to.equal( { meta: { population: "2200K" , country: "France" } } ) ;

		town._.localChanges = null ;
		town._.addLocalChange( [ 'meta' ] ) ;
		expect( town._.localChanges ).to.equal( { meta: null } ) ;
		expect( town._.buildDbPatch().set ).to.equal( { meta: { population: "2200K" , country: "France" } } ) ;
		town._.addLocalChange( [ 'meta' , 'country' ] ) ;
		expect( town._.localChanges ).to.equal( { meta: null } ) ;
		expect( town._.buildDbPatch().set ).to.equal( { meta: { population: "2200K" , country: "France" } } ) ;
		town._.addLocalChange( [ 'meta' , 'population' ] ) ;
		expect( town._.localChanges ).to.equal( { meta: null } ) ;
		expect( town._.buildDbPatch().set ).to.equal( { meta: { population: "2200K" , country: "France" } } ) ;

		town._.localChanges = null ;
		town._.addLocalChange( [ 'meta' , 'population' ] ) ;
		town._.addLocalChange( [ 'meta' , 'country' ] ) ;
		expect( town._.localChanges ).to.equal( { meta: { population: null , country: null } } ) ;
		expect( town._.buildDbPatch().set ).to.equal( { "meta.population": "2200K" , "meta.country": "France" } ) ;
		town._.addLocalChange( [ 'meta' ] ) ;
		expect( town._.localChanges ).to.equal( { meta: null } ) ;
		expect( town._.buildDbPatch().set ).to.equal( { meta: { population: "2200K" , country: "France" } } ) ;

		town._.localChanges = null ;
		town._.addLocalChange( [ 'meta' ] ) ;
		town._.addLocalChange( [ 'meta' ] ) ;
		expect( town._.localChanges ).to.equal( { meta: null } ) ;

		town._.localChanges = null ;
		town._.addLocalChange( [ 'meta' , 'population' ] ) ;
		town._.addLocalChange( [ 'meta' , 'population' ] ) ;
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
		expect( userBatch[ 0 ]._id ).to.be.an( mongodb.ObjectID ) ;
		expect( userBatch[ 0 ].getId() ).to.be.an( mongodb.ObjectID ) ;
		expect( userBatch[ 0 ]._id ).to.be( userBatch[ 0 ].getId() ) ;
		expect( userBatch[ 0 ] ).to.partially.equal( expectedDefaultUser ) ;
		expect( userBatch[ 0 ].$ ).to.partially.equal( expectedDefaultUser ) ;

		expect( userBatch[ 1 ] ).to.be.an( Object ) ;
		expect( userBatch[ 1 ].$ ).to.be.an( Object ) ;
		expect( userBatch[ 1 ]._ ).to.be.a( rootsDb.Document ) ;
		expect( userBatch[ 1 ]._id ).to.be.an( mongodb.ObjectID ) ;
		expect( userBatch[ 1 ].getId() ).to.be.an( mongodb.ObjectID ) ;
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
		expect( userBatch[ 0 ]._id ).to.be.an( mongodb.ObjectID ) ;
		expect( userBatch[ 0 ].getId() ).to.be.an( mongodb.ObjectID ) ;
		expect( userBatch[ 0 ]._id ).to.be( userBatch[ 0 ].getId() ) ;
		expect( userBatch[ 0 ] ).to.equal( {
			_id: userBatch[ 0 ].getId() , firstName: 'Bobby' , lastName: 'Fischer' , memberSid: 'Bobby Fischer'
		} ) ;

		expect( userBatch[ 1 ] ).to.be.an( Object ) ;
		expect( userBatch[ 1 ].$ ).to.be.an( Object ) ;
		expect( userBatch[ 1 ]._ ).to.be.a( rootsDb.Document ) ;
		expect( userBatch[ 1 ]._id ).to.be.an( mongodb.ObjectID ) ;
		expect( userBatch[ 1 ].getId() ).to.be.an( mongodb.ObjectID ) ;
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
		expect( userBatch[ 2 ]._id ).to.be.an( mongodb.ObjectID ) ;
		expect( userBatch[ 2 ].getId() ).to.be.an( mongodb.ObjectID ) ;
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
			schools.createDocument( { title: 'A school' } )
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
			{ title: "that school" } ,
			{ title: "That school" }
		] ) ;
	} ) ;
} ) ;



describe( "Find each document with a query object (serialized)" , () => {

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

		await users.findEach( { firstName: { $regex: /^[thomasepnbo]+$/ , $options: 'i' } } , doc => {
			expect( doc._ ).to.be.a( rootsDb.Document ) ;
			docs.push( doc ) ;
			// Force a timeout
			return Promise.resolveTimeout( 50 ) ;
		} ) ;

		// Check serialization time
		expect( Date.now() - startDate ).to.be.greater.than( 150 ) ;

		expect( docs ).to.be.an( Array ) ;
		expect( docs ).to.have.length( 3 ) ;

		expect( docs ).to.be.partially.like( [
			{ firstName: 'Bob' , lastName: 'Marley' , memberSid: 'Bob Marley' } ,
			{ firstName: 'Stephen' , lastName: 'Marley' , memberSid: 'Stephen Marley' } ,
			{ firstName: 'Thomas' , lastName: 'Jefferson' , memberSid: 'Thomas Jefferson' }
		] ) ;
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
			users: {} ,
			schools: {}
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
		expect( JSON.stringify( user ) ).to.be( '{"firstName":"Jilbert","lastName":"Polson","_id":"' + userId.toString() + '","memberSid":"Jilbert Polson","job":{"title":"developer","salary":60000,"users":{},"schools":{},"_id":"' + jobId.toString() + '"}}' ) ;
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
				_id: job1Id , title: 'developer' , salary: 60000 , users: {} , schools: {}
			} ,
			sysadmin: {
				_id: job2Id , title: 'sysadmin' , salary: 55000 , users: {} , schools: {}
			}
		} ) ;

		// Test auto-populate on .getLink()
		dbSchool = await schools.get( id ) ;

		batch = await dbSchool.getLink( "jobs" ) ;
		expect( dbSchool.$.jobs ).to.equal( [ { _id: job1Id } , { _id: job2Id } ] ) ;
		expect( dbSchool.jobs ).to.equal( [ job1 , job2 ] ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.title ] = doc ; } ) ;

		expect( map ).to.equal( {
			developer: {
				_id: job1Id , title: 'developer' , salary: 60000 , users: {} , schools: {}
			} ,
			sysadmin: {
				_id: job2Id , title: 'sysadmin' , salary: 55000 , users: {} , schools: {}
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
				_id: job1Id , title: 'developer' , salary: 60000 , users: {} , schools: {}
			} ,
			sysadmin: {
				_id: job2Id , title: 'sysadmin' , salary: 55000 , users: {} , schools: {}
			} ,
			"front-end developer": {
				_id: job3Id , title: 'front-end developer' , salary: 54000 , users: {} , schools: {}
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
				_id: job1Id , title: 'developer' , salary: 60000 , users: {} , schools: {}
			} ,
			"front-end developer": {
				_id: job3Id , title: 'front-end developer' , salary: 54000 , users: {} , schools: {}
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
				backLinkOfLink: {} ,
				backLinkOfMultiLink: {} ,
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
				_id: job1Id , title: 'developer' , salary: 60000 , users: {} , schools: {}
			} ,
			sysadmin: {
				_id: job2Id , title: 'sysadmin' , salary: 55000 , users: {} , schools: {}
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
			_id: jobId , title: 'developer' , salary: 60000 , users: {} , schools: {}
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
			schools: {}
		} ) ;

		expect( job.$ ).to.be.like( {
			_id: jobId ,
			title: 'developer' ,
			salary: 60000 ,
			users: {} ,
			schools: {}
		} ) ;

		expect( job.$.users ).to.equal( {} ) ;

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
			schools: {}
		} ) ;

		expect( job.$ ).to.be.like( {
			_id: jobId ,
			title: 'developer' ,
			salary: 60000 ,
			users: {} ,
			schools: {}
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
			_id: job1Id , title: 'developer' , salary: 60000 , users: {} , schools: {}
		} ) ;

		batch = await dbJob.getLink( 'schools' ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.title ] = doc ; } ) ;

		expect( map ).to.equal( {
			'Computer Science': { _id: school1Id , title: 'Computer Science' , jobs: [ { _id: job1Id } , { _id: job2Id } , { _id: job3Id } ] } ,
			'Web Academy': { _id: school2Id , title: 'Web Academy' , jobs: [ { _id: job1Id } , { _id: job3Id } , { _id: job4Id } ] }
		} ) ;

		expect( dbJob ).to.be.like( {
			_id: job1Id ,
			title: 'developer' ,
			salary: 60000 ,
			users: {} ,
			schools: [
				{ _id: school1Id , title: 'Computer Science' , jobs: [ { _id: job1Id } , { _id: job2Id } , { _id: job3Id } ] } ,
				{ _id: school2Id , title: 'Web Academy' , jobs: [ { _id: job1Id } , { _id: job3Id } , { _id: job4Id } ] }
			]
		} ) ;

		expect( dbJob.$ ).to.be.like( {
			_id: job1Id ,
			title: 'developer' ,
			salary: 60000 ,
			users: {} ,
			schools: {}
		} ) ;

		dbJob = await jobs.get( job4Id ) ;
		expect( dbJob ).to.equal( {
			_id: job4Id , title: 'designer' , salary: 56000 , users: {} , schools: {}
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
					backLinkOfLink: {} , backLinkOfMultiLink: {} , link: { _id: id } , multiLink: []
				} } ,
			child2: { _id: childDoc2.getId() ,
				name: "child2" ,
				nested: {
					backLinkOfLink: {} , backLinkOfMultiLink: {} , link: { _id: id } , multiLink: []
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
					backLinkOfLink: {} , backLinkOfMultiLink: {} , link: { _id: id } , multiLink: []
				} } ,
			child2: { _id: childDoc2.getId() ,
				name: "child2" ,
				nested: {
					backLinkOfLink: {} , backLinkOfMultiLink: {} , link: { _id: id } , multiLink: []
				} } ,
			child3: { _id: childDoc3.getId() ,
				name: "child3" ,
				nested: {
					backLinkOfLink: {} , backLinkOfMultiLink: {} , link: { _id: id } , multiLink: []
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
					backLinkOfLink: {} , backLinkOfMultiLink: {} , link: { _id: id } , multiLink: []
				} } ,
			child3: { _id: childDoc3.getId() ,
				name: "child3" ,
				nested: {
					backLinkOfLink: {} , backLinkOfMultiLink: {} , link: { _id: id } , multiLink: []
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
							backLinkOfLink: {} , backLinkOfMultiLink: {} , link: { _id: id } , multiLink: []
						} } ,
					{ _id: childDoc3.getId() ,
						name: "child3" ,
						nested: {
							backLinkOfLink: {} , backLinkOfMultiLink: {} , link: { _id: id } , multiLink: []
						} }
				]
			}
		} ) ;

		expect( rootDoc.$ ).to.be.like( {
			_id: id ,
			name: "root" ,
			nested: { backLinkOfLink: {} }
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
			child1: { _id: childDoc1.getId() , name: "child1" , nested: { backLinkOfLink: {} , backLinkOfMultiLink: {} , multiLink: [ { _id: rootDoc.getId() } ] } } ,
			child2: { _id: childDoc2.getId() , name: "child2" , nested: { backLinkOfLink: {} , backLinkOfMultiLink: {} , multiLink: [ { _id: rootDoc.getId() } , { _id: otherDoc1.getId() } , { _id: otherDoc2.getId() } ] } }
		} ) ;

		// Second test

		childDoc3.addLink( 'nested.multiLink' , rootDoc ) ;
		await childDoc3.save() ;
		batch = await rootDoc.getLink( "nested.backLinkOfMultiLink" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.name ] = doc ; } ) ;

		expect( map ).to.equal( {
			child1: { _id: childDoc1.getId() , name: "child1" , nested: { backLinkOfLink: {} , backLinkOfMultiLink: {} , multiLink: [ { _id: rootDoc.getId() } ] } } ,
			child2: { _id: childDoc2.getId() , name: "child2" , nested: { backLinkOfLink: {} , backLinkOfMultiLink: {} , multiLink: [ { _id: rootDoc.getId() } , { _id: otherDoc1.getId() } , { _id: otherDoc2.getId() } ] } } ,
			child3: { _id: childDoc3.getId() , name: "child3" , nested: { backLinkOfLink: {} , backLinkOfMultiLink: {} , multiLink: [ { _id: otherDoc1.getId() } , { _id: otherDoc2.getId() } , { _id: rootDoc.getId() } ] } }
		} ) ;

		// Third test

		childDoc2.removeLink( 'nested.multiLink' , rootDoc ) ;
		await childDoc2.save() ;
		batch = await rootDoc.getLink( "nested.backLinkOfMultiLink" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.name ] = doc ; } ) ;

		expect( map ).to.equal( {
			child1: { _id: childDoc1.getId() , name: "child1" , nested: { backLinkOfLink: {} , backLinkOfMultiLink: {} , multiLink: [ { _id: rootDoc.getId() } ] } } ,
			child3: { _id: childDoc3.getId() , name: "child3" , nested: { backLinkOfLink: {} , backLinkOfMultiLink: {} , multiLink: [ { _id: otherDoc1.getId() } , { _id: otherDoc2.getId() } , { _id: rootDoc.getId() } ] } }
		} ) ;

		expect( rootDoc ).to.be.like( {
			_id: id ,
			name: "root" ,
			nested: {
				backLinkOfMultiLink: [
					{ _id: childDoc1.getId() , name: "child1" , nested: { backLinkOfLink: {} , backLinkOfMultiLink: {} , multiLink: [ { _id: rootDoc.getId() } ] } } ,
					{ _id: childDoc3.getId() , name: "child3" , nested: { backLinkOfLink: {} , backLinkOfMultiLink: {} , multiLink: [ { _id: otherDoc1.getId() } , { _id: otherDoc2.getId() } , { _id: rootDoc.getId() } ] } }
				]
			}
		} ) ;

		expect( rootDoc.$ ).to.be.like( {
			_id: id ,
			name: "root" ,
			nested: { backLinkOfMultiLink: {} }
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
			backLink: {}
		} ) ;

		expect( doc.$ ).to.equal( {
			_id: docId ,
			name: 'docname' ,
			link: { _id: userId , _collection: 'users' } ,
			backLink: {}
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
			backLink: {}
		} ) ;

		expect( doc.$ ).to.equal( {
			_id: docId ,
			name: 'docname' ,
			link: { _id: jobId , _collection: 'jobs' } ,
			backLink: {}
		} ) ;

		await doc.save() ;
		dbDoc = await anyCollectionLinks.get( docId ) ;

		expect( dbDoc.link._id ).to.equal( jobId ) ;
		expect( dbDoc.link._collection ).to.be( 'jobs' ) ;
		await expect( dbDoc.getLink( 'link' ) ).to.eventually.equal( {
			_id: jobId ,
			title: 'developer' ,
			salary: 60000 ,
			schools: {} ,
			users: {}
		} ) ;

		dbDoc = await anyCollectionLinks.get( docId , { populate: 'link' } ) ;

		expect( dbDoc ).to.equal( {
			_id: docId ,
			name: 'docname' ,
			link: {
				_id: jobId ,
				title: 'developer' ,
				salary: 60000 ,
				schools: {} ,
				users: {}
			} ,
			backLink: {}
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

	it( "should create, save, and load an attachment" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		await user.setAttachment( 'file' , attachment ) ;
		//log.error( user.file ) ;

		// Raw DB data
		expect( user.$.file ).not.to.be.a( rootsDb.Attachment ) ;
		expect( user.$.file ).to.equal( {
			filename: 'joke.txt' ,
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
			id: user.file.id ,	// Unpredictable
			contentType: 'text/plain' ,
			fileSize: 24 ,
			hash: null ,
			hashType: null ,
			metadata: {}
		} ) ;

		await attachment.save() ;
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
				contentType: 'text/plain' ,
				fileSize: 24 ,
				hash: null ,
				hashType: null ,
				metadata: {} ,
				collectionName: 'users' ,
				documentId: id.toString() ,
				driver: users.attachmentDriver ,
				path: ( ATTACHMENT_MODE === 'file' ? __dirname + '/tmp/' : '' ) + dbUser.getId() + '/' + details.attachment.id ,
				publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + details.attachment.id
			}
		} ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.partially.like( {
			id: user.file.id ,
			filename: 'joke.txt' ,
			contentType: 'text/plain' ,
			fileSize: 24 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? __dirname + '/tmp/' : '' ) + dbUser.getId() + '/' + details.attachment.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + details.attachment.id
		} ) ;

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

		var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		await user.setAttachment( 'file' , attachment ) ;

		await attachment.save() ;
		await user.save() ;

		var dbUser = await users.get( id ) ;
		
		// There was a bug with proxy/restoreAttachment, so we check staging right now
		expect( dbUser._.localChanges ).to.equal( null ) ;
		dbUser.file ;
		expect( dbUser._.localChanges ).to.equal( null ) ;
		
		dbUser.file.set( { filename: 'lol.txt' , contentType: 'text/joke' , metadata: { width: 100 } } ) ;
		// Check if staging is correct
		expect( dbUser._.localChanges ).to.equal( { file: { filename: null , contentType: null , metadata: { width: null } } } ) ;
		await dbUser.save() ;

		dbUser = await users.get( id ) ;

		expect( dbUser ).to.be.partially.like( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'lol.txt' ,
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
				contentType: 'text/joke' ,
				fileSize: 24 ,
				hash: null ,
				hashType: null ,
				metadata: { width: 100 , height: 120 } ,
				collectionName: 'users' ,
				documentId: id.toString() ,
				driver: users.attachmentDriver ,
				path: ( ATTACHMENT_MODE === 'file' ? __dirname + '/tmp/' : '' ) + dbUser.getId() + '/' + details.attachment.id ,
				publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + details.attachment.id
			}
		} ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.partially.like( {
			id: dbUser.file.id ,
			filename: 'lol.txt' ,
			contentType: 'text/joke' ,
			fileSize: 24 ,
			hash: null ,
			hashType: null ,
			metadata: { width: 100 , height: 120 } ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? __dirname + '/tmp/' : '' ) + dbUser.getId() + '/' + details.attachment.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + details.attachment.id
		} ) ;

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

		var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		await user.setAttachment( 'file' , attachment ) ;

		await attachment.save() ;
		await user.save() ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		var dbUser = await users.get( id ) ;

		await expect( dbUser.getAttachment( 'file' ).load()
			.then( v => v.toString() ) ).to.eventually.be( "grigrigredin menufretin\n" ) ;

		var attachment2 = user.createAttachment(
			{ filename: 'hello-world.html' , contentType: 'text/html' } ,
			"<html><head></head><body>Hello world!</body></html>\n"
		) ;

		await dbUser.setAttachment( 'file' , attachment2 ) ;

		// Check that the previous file has been deleted
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).to.throw( Error , { code: 'ENOENT' } ) ;
		}

		await attachment2.save() ;
		await dbUser.save() ;

		dbUser = await users.get( id ) ;

		expect( dbUser ).to.be.partially.like( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'hello-world.html' ,
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
				contentType: 'text/html' ,
				fileSize: 52 ,
				hash: null ,
				hashType: null ,
				metadata: {} ,
				collectionName: 'users' ,
				documentId: id.toString() ,
				driver: users.attachmentDriver ,
				path: ( ATTACHMENT_MODE === 'file' ? __dirname + '/tmp/' : '' ) + dbUser.getId() + '/' + details.attachment.id ,
				publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + details.attachment.id
			}
		} ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.a( rootsDb.Attachment ) ;
		expect( dbAttachment ).to.be.partially.like( {
			id: dbUser.file.id ,
			filename: 'hello-world.html' ,
			contentType: 'text/html' ,
			fileSize: 52 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? __dirname + '/tmp/' : '' ) + dbUser.getId() + '/' + details.attachment.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + details.attachment.id
		} ) ;

		await expect( dbAttachment.load().then( v => v.toString() ) ).to.eventually.be( "<html><head></head><body>Hello world!</body></html>\n" ) ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( dbAttachment.path , fs.R_OK ) ; } ).not.to.throw() ;
		}
	} ) ;

	it( "Delete an attachment" , async function() {
		this.timeout( 4000 ) ;	// High timeout because some driver like S3 have a huge lag

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		await user.setAttachment( 'file' , attachment ) ;

		await attachment.save() ;
		await user.save() ;

		// Check that the file exists
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).not.to.throw() ;
		}

		var dbUser = await users.get( id ) ;

		await expect( dbUser.getAttachment( 'file' ).load()
			.then( v => v.toString() ) ).to.eventually.be( "grigrigredin menufretin\n" ) ;

		await dbUser.removeAttachment( 'file' ) ;

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

		var attachment = user.createAttachment( { filename: 'random.bin' , contentType: 'bin/random' } , stream ) ;
		await user.setAttachment( 'file' , attachment ) ;
		//log.error( user.file ) ;

		expect( user.file ).to.be.a( rootsDb.Attachment ) ;
		expect( user.file ).to.be.partially.like( {
			filename: 'random.bin' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'bin/random' ,
			fileSize: null ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
		} ) ;

		await attachment.save() ;
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
			contentType: 'bin/random' ,
			fileSize: 40 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? __dirname + '/tmp/' : '' ) + dbUser.getId() + '/' + attachment.id ,
			publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + attachment.id
		} ) ;

		await expect( dbAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'a'.repeat( 40 ) ) ;

		stream = new streamKit.FakeReadable( {
			timeout: 50 , chunkSize: 10 , chunkCount: 3 , filler: 'b'.charCodeAt( 0 )
		} ) ;
		var attachment2 = user.createAttachment( { filename: 'more-random.bin' , contentType: 'bin/random' } , stream ) ;

		await dbUser.setAttachment( 'file' , attachment2 ) ;

		// Check that the previous file has been deleted
		if ( ATTACHMENT_MODE === 'file' ) {
			expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).to.throw( Error , { code: 'ENOENT' } ) ;
		}

		await attachment2.save() ;
		await dbUser.save() ;

		dbUser = await users.get( id ) ;

		expect( dbUser ).to.be.partially.like( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'more-random.bin' ,
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
			contentType: 'bin/random' ,
			fileSize: 30 ,
			hash: null ,
			hashType: null ,
			metadata: {} ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? __dirname + '/tmp/' : '' ) + dbUser.getId() + '/' + attachment2.id ,
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
				id: dbUser.file.id ,	// Unpredictable
				contentType: 'bin/random' ,
				fileSize: 40 ,
				hash: null ,
				hashType: null ,
				metadata: {} ,
			} ,
			avatar: {
				filename: 'face.jpg' ,
				id: dbUser.avatar.id ,	// Unpredictable
				contentType: 'image/jpeg' ,
				fileSize: 28 ,
				hash: null ,
				hashType: null ,
				metadata: {} ,
			} ,
			publicKey: {
				filename: 'rsa.pub' ,
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
	beforeEach( () => {
		clearDB() ;
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
		var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		await user.setAttachment( 'file' , attachment ) ;
		//log.error( user.file ) ;

		expect( user.file ).to.be.partially.like( {
			filename: 'joke.txt' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'text/plain' ,
			fileSize: 24 ,
			hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		await attachment.save() ;
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
				contentType: 'text/plain' ,
				fileSize: 24 ,
				hash: contentHash ,
				hashType: 'sha256' ,
				metadata: {} ,
				collectionName: 'users' ,
				documentId: id.toString() ,
				driver: users.attachmentDriver ,
				path: ( ATTACHMENT_MODE === 'file' ? __dirname + '/tmp/' : '' ) + dbUser.getId() + '/' + details.attachment.id ,
				publicUrl: ATTACHMENT_PUBLIC_BASE_URL + '/' + dbUser.getId() + '/' + details.attachment.id
			}
		} ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.partially.like( {
			id: user.file.id ,
			filename: 'joke.txt' ,
			contentType: 'text/plain' ,
			fileSize: 24 ,
			hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? __dirname + '/tmp/' : '' ) + dbUser.getId() + '/' + details.attachment.id ,
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
		expect( () => user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' , hash: badContentHash , fileSize: 24 } , "grigrigredin menufretin\n" ) ).to.throw( Error , { code: 'badHash' } ) ;
		// With an incorrect file size
		expect( () => user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' , hash: contentHash , fileSize: 21 } , "grigrigredin menufretin\n" ) ).to.throw( Error , { code: 'badFileSize' } ) ;

		// With the correct hash
		attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' , hash: contentHash , fileSize: 24 } , "grigrigredin menufretin\n" ) ;
		await user.setAttachment( 'file' , attachment ) ;
		//log.error( user.file ) ;

		expect( user.file ).to.be.partially.like( {
			filename: 'joke.txt' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'text/plain' ,
			fileSize: 24 ,
			hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		await attachment.save() ;
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
			contentType: 'text/plain' ,
			fileSize: 24 ,
			hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? __dirname + '/tmp/' : '' ) + dbUser.getId() + '/' + dbAttachment.id ,
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

		attachment = user.createAttachment( { filename: 'random.bin' , contentType: 'bin/random' } , stream ) ;
		await user.setAttachment( 'file' , attachment ) ;
		//log.error( user.file ) ;

		expect( user.file ).to.be.partially.like( {
			filename: 'random.bin' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'bin/random' ,
			fileSize: null ,	// The size is not yet computed since it is a stream!
			hash: null ,	// The hash is not yet computed since it is a stream!
			//hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		// It should be ok here
		await attachment.save() ;
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
			contentType: 'bin/random' ,
			fileSize: 40 ,
			hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? __dirname + '/tmp/' : '' ) + dbUser.getId() + '/' + attachment.id ,
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

		attachment = user.createAttachment( { filename: 'random.bin' , contentType: 'bin/random' , hash: badContentHash , fileSize: 40 } , stream ) ;
		await user.setAttachment( 'file' , attachment ) ;
		//log.error( user.file ) ;

		expect( user.file ).to.be.partially.like( {
			filename: 'random.bin' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'bin/random' ,
			fileSize: 40 ,
			hash: badContentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		// It should throw here
		await expect( () => attachment.save() ).to.eventually.throw( Error , { code: 'badHash' } ) ;
		
		
		// Then with a bad file size

		stream = new streamKit.FakeReadable( {
			timeout: 50 , chunkSize: 10 , chunkCount: 4 , filler: 'a'.charCodeAt( 0 )
		} ) ;

		attachment = user.createAttachment( { filename: 'random.bin' , contentType: 'bin/random' , hash: contentHash , fileSize: 35 } , stream ) ;
		await user.setAttachment( 'file' , attachment ) ;
		//log.error( user.file ) ;

		expect( user.file ).to.be.partially.like( {
			filename: 'random.bin' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'bin/random' ,
			fileSize: 35 ,
			hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		// It should throw here
		await expect( () => attachment.save() ).to.eventually.throw( Error , { code: 'badFileSize' } ) ;
		
		
		// Now start over with the correct one
		
		stream = new streamKit.FakeReadable( {
			timeout: 50 , chunkSize: 10 , chunkCount: 4 , filler: 'a'.charCodeAt( 0 )
		} ) ;

		attachment = user.createAttachment( { filename: 'random.bin' , contentType: 'bin/random' , hash: contentHash , fileSize: 40 } , stream ) ;
		await user.setAttachment( 'file' , attachment ) ;
		//log.error( user.file ) ;

		expect( user.file ).to.be.partially.like( {
			filename: 'random.bin' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'bin/random' ,
			fileSize: 40 ,
			hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		// It should be ok here
		await attachment.save() ;
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
			contentType: 'bin/random' ,
			fileSize: 40 ,
			hash: contentHash ,
			hashType: 'sha256' ,
			metadata: {} ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			driver: users.attachmentDriver ,
			path: ( ATTACHMENT_MODE === 'file' ? __dirname + '/tmp/' : '' ) + dbUser.getId() + '/' + attachment.id ,
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
				id: dbUser.file.id ,	// Unpredictable
				contentType: 'bin/random' ,
				fileSize: 40 ,
				hash: contentHash[ 0 ] ,
				hashType: 'sha256' ,
				metadata: {} ,
			} ,
			avatar: {
				filename: 'face.jpg' ,
				id: dbUser.avatar.id ,	// Unpredictable
				contentType: 'image/jpeg' ,
				fileSize: 28 ,
				hash: contentHash[ 1 ] ,
				hashType: 'sha256' ,
				metadata: {} ,
			} ,
			publicKey: {
				filename: 'rsa.pub' ,
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

		var badAttachmentStreams = new rootsDb.AttachmentStreams() ;

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
				id: dbUser.file.id ,	// Unpredictable
				contentType: 'bin/random' ,
				fileSize: 40 ,
				hash: contentHash[ 0 ] ,
				hashType: 'sha256' ,
				metadata: {} ,
			} ,
			avatar: {
				filename: 'face.jpg' ,
				id: dbUser.avatar.id ,	// Unpredictable
				contentType: 'image/jpeg' ,
				fileSize: 28 ,
				hash: contentHash[ 1 ] ,
				hashType: 'sha256' ,
				metadata: {} ,
			} ,
			publicKey: {
				filename: 'rsa.pub' ,
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
			contentType: 'application/x-pem-file' ,
			fileSize: 21 ,
			hash: contentHash[ 2 ] ,
			hashType: 'sha256' ,
			metadata: {} ,
		} ) ;

		await expect( publicKeyAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'c'.repeat( 21 ) ) ;
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
		expect( lockId ).to.be.an( mongodb.ObjectID ) ;
		expect( lockable._.meta.lockId ).to.be.an( mongodb.ObjectID ) ;
		expect( lockable._.meta.lockId ).to.be( lockId ) ;

		dbLockable = await lockables.get( id ) ;
		expect( dbLockable ).to.equal( {
			_id: id , data: 'something' , _lockedBy: lockId , _lockedAt: dbLockable._lockedAt
		} ) ;
		expect( dbLockable._lockedAt ).to.be.a( Date ) ;

		await expect( lockable.lock() ).to.eventually.be( null ) ;
		await expect( lockable.unlock() ).to.eventually.be( true ) ;
		await expect( lockable.lock() ).to.eventually.be.a( mongodb.ObjectID ) ;
	} ) ;

	it( "should perform a .lockedPartialFind(): lock, retrieve locked document, then release locks" , async () => {
		var lockId ;

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
			lockables.lockedPartialFind( { data: { $in: [ 'one' , 'two' ] } } , dbBatch => {
				expect( dbBatch ).to.have.length( 2 ) ;

				var map = {} ;
				dbBatch.forEach( doc => {
					map[ doc.data ] = doc ;
				} ) ;

				expect( map ).to.partially.equal( {
					one: { data: 'one' } ,
					two: { data: 'two' }
				} ) ;

				return Promise.resolveTimeout( 30 ) ;
			} ) ,
			Promise.resolveTimeout( 0 , () => lockables.lockedPartialFind( { data: { $in: [ 'one' , 'two' , 'three' ] } } , dbBatch => {
				expect( dbBatch ).to.have.length( 1 ) ;
				expect( dbBatch ).to.be.partially.like( [ { data: 'three' } ] ) ;
				return Promise.resolveTimeout( 30 ) ;
			} ) ) ,
			Promise.resolveTimeout( 10 , () => lockables.lockedPartialFind( { data: { $in: [ 'one' , 'two' , 'three' ] } } , dbBatch => {
				expect( dbBatch ).to.have.length( 0 ) ;
			} ) )
		] ) ;

		await lockables.lockedPartialFind( { data: { $in: [ 'one' , 'two' , 'three' ] } } , dbBatch => {
			expect( dbBatch ).to.have.length( 3 ) ;
		} ) ;

		// Check that immediatley after 'await', the data are available
		await lockables.lockedPartialFind( { data: { $in: [ 'one' , 'two' , 'three' ] } } , dbBatch => {
			expect( dbBatch ).to.have.length( 3 ) ;
		} ) ;
	} ) ;
} ) ;





describe( "Populate links" , () => {

	beforeEach( clearDB ) ;

	it( "link population (create both, link, save both, get with populate option)" , async () => {
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

	it( "multiple link population (create, link, save, get with populate option)" , async () => {
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

	it( "multiple link population having same and circular target" , async () => {
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

	it( "collect batch with multiple link population (create, link, save, collect with populate option)" , async () => {
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
		dbUserBatch.sort( ( a , b ) => a.firstName.charCodeAt( 0 ) - b.firstName.charCodeAt( 0 ) ) ;

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
					users: {} ,
					schools: {} ,
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

	it( "collect batch with multiple link population and circular references" , async () => {
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
		dbUserBatch.sort( ( a , b ) => a.firstName.charCodeAt( 0 ) - b.firstName.charCodeAt( 0 ) ) ;

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
					users: {} ,
					schools: {} ,
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

	it( "collect batch with multiple link population and circular references: using noReference" , async () => {
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
		dbUserBatch.sort( ( a , b ) => a.firstName.charCodeAt( 0 ) - b.firstName.charCodeAt( 0 ) ) ;

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
					users: {} ,
					schools: {} ,
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

	it( "'multi-link' population (create both, link, save both, get with populate option)" , async () => {
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

	it( "'back-link' population (create both, link, save both, get with populate option)" , async () => {
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
			schools: {}
		} ) ;

		expect( stats.population.depth ).to.be( 1 ) ;
		expect( stats.population.dbQueries ).to.be( 1 ) ;

		// Again, with batch

		stats = {} ;
		var batch = await jobs.collect( {} , { populate: 'users' , stats } ) ;

		// Sort that first...
		batch.sort( ( a , b ) => a.title.charCodeAt( 0 ) - b.title.charCodeAt( 0 ) ) ;

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
			schools: {}
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
			schools: {}
		} ) ;

		expect( batch[ 2 ] ).to.be.like( {
			_id: job3._id ,
			title: 'zero' ,
			salary: 0 ,
			users: {} ,
			schools: {}
		} ) ;

		expect( stats.population.depth ).to.be( 1 ) ;
		expect( stats.population.dbQueries ).to.be( 1 ) ;
	} ) ;

	it( "'back-link' of multi-link population" , async () => {
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

		dbJob.schools.sort( ( a , b ) => b.title - a.title ) ;

		// Order by id
		dbJob.schools[ 0 ].jobs.sort( ( a , b ) => '' + a._id > '' + b._id ? 1 : -1 ) ;
		dbJob.schools[ 1 ].jobs.sort( ( a , b ) => '' + a._id > '' + b._id ? 1 : -1 ) ;

		expect( dbJob ).to.be.like( {
			_id: job1._id ,
			title: 'developer' ,
			salary: 60000 ,
			users: {} ,
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
		dbJob.schools[ 0 ].jobs.sort( ( a , b ) => '' + a._id > '' + b._id ? 1 : -1 ) ;

		expect( dbJob ).to.be.like( {
			_id: job4._id ,
			title: 'designer' ,
			salary: 56000 ,
			users: {} ,
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
} ) ;



describe( "Deep populate links" , () => {

	beforeEach( clearDB ) ;

	it( "deep population (links and back-link)" , async () => {
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
				schools: {} ,
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
				schools: {} ,
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

		batch.sort( ( a , b ) => {
			return parseInt( a._id.toString() , 10 ) - parseInt( b._id.toString() , 10 ) ;
		} ) ;

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

		batch.sort( ( a , b ) => {
			return parseInt( a._id.toString() , 10 ) - parseInt( b._id.toString() , 10 ) ;
		} ) ;

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
			users: {} ,
			schools: {}
		} ) ;

		expect( memory.collections.jobs.rawDocuments[ job2._id ] ).to.equal( {
			_id: job2._id ,
			title: 'adventurer' ,
			salary: 200000 ,
			users: {} ,
			schools: {}
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
			schools: {} ,
			users: {}
		} ) ;

		expect( memory.collections.jobs.rawDocuments[ job2._id ] ).to.equal( {
			_id: job2._id ,
			title: 'adventurer' ,
			salary: 200000 ,
			schools: {} ,
			users: {}
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
			users: {} ,
			schools: {}
		} ) ;

		expect( memory.collections.jobs.rawDocuments[ job2._id ] ).to.equal( {
			_id: job2._id ,
			title: 'sysadmin' ,
			salary: 55000 ,
			users: {} ,
			schools: {}
		} ) ;

		expect( memory.collections.jobs.rawDocuments[ job3._id ] ).to.equal( {
			_id: job3._id ,
			title: 'front-end developer' ,
			salary: 54000 ,
			users: {} ,
			schools: {}
		} ) ;

		expect( memory.collections.jobs.rawDocuments[ job4._id ] ).to.equal( {
			_id: job4._id ,
			title: 'designer' ,
			salary: 56000 ,
			users: {} ,
			schools: {}
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
				users: {} ,
				schools: {}
			}
		} ) ;

		expect( memory.collections.jobs.rawDocuments[ job._id ] ).to.equal( {
			_id: job._id ,
			title: 'developer' ,
			salary: 60000 ,
			users: {} ,
			schools: {}
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
				schools: {} ,
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
			versions: {}
		} ) ;

		versionedItem.p1 = 'value1b' ;
		// Version should not be incremented, because it was not even saved once
		expect( versionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 1 ,
			_lastModified: versionedItem._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value1b' ,
			versions: {}
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
			versions: {}
		} ) ;

		await versionedItem.save() ;

		// Version should be incremented now we have save it
		expect( versionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 2 ,
			_lastModified: versionedItem._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value1c' ,
			versions: {}
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
				versions: {}
			}
		] ) ;

		var dbVersionedItem = await versionedItems.get( versionedItemId ) ;

		expect( dbVersionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 2 ,
			_lastModified: dbVersionedItem._lastModified ,	// unpredictable
			name: 'item#1' ,
			p1: 'value1c' ,
			versions: {}
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
			versions: {}
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
			versions: {}
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
				versions: {}
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
				versions: {}
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
			versions: {}
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
				versions: {}
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
				versions: {}
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
				versions: {}
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
				versions: {}
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
				versions: {}
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
				versions: {}
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
			versions: {}
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
				versions: {}
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
				versions: {}
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
				versions: {}
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
				versions: {}
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
			versions: {}
		} ) ;

		dbVersionedItem = await versionedItems.get( versionedItemId ) ;

		expect( dbVersionedItem ).to.equal( {
			_id: versionedItemId ,
			_version: 6 ,
			_lastModified: dbVersionedItem._lastModified ,	// unpredictable
			name: 'ITEM#1' ,
			p1: 'value1-over' ,
			p2: 'value2-over' ,
			versions: {}
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
				versions: {}
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
				versions: {}
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
				versions: {}
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
				versions: {}
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
				versions: {}
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
				versions: {}
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
				versions: {}
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
				versions: {}
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
				versions: {}
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
				versions: {}
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
				versions: {}
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
			versions: {}
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
			versions: {}
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
			versions: {}
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
			versions: {}
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
			versions: {}
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
			versions: {}
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
			versions: {}
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
			_id: job._id , title: 'developer' , salary: 60000 , users: {} , schools: {}
		} ) ;

		job.patch( { salary: "65000" } ) ;
		// Before sanitizing: it's a string
		expect( job ).to.equal( {
			_id: job._id , title: 'developer' , salary: "65000" , users: {} , schools: {}
		} ) ;

		await job.commit() ;
		// After commit/sanitizing: now a number
		expect( job ).to.equal( {
			_id: job._id , title: 'developer' , salary: 65000 , users: {} , schools: {}
		} ) ;

		dbJob = await jobs.get( job._id ) ;
		expect( dbJob ).to.equal( {
			_id: job._id , title: 'developer' , salary: 65000 , users: {} , schools: {}
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
				_id: job1Id , title: 'developer' , salary: 60000 , users: {} , schools: {}
			} ,
			sysadmin: {
				_id: job2Id , title: 'sysadmin' , salary: 55000 , users: {} , schools: {}
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
				_id: job1Id , title: 'developer' , salary: 60000 , users: {} , schools: {}
			} ,
			sysadmin: {
				_id: job2Id , title: 'sysadmin' , salary: 55000 , users: {} , schools: {}
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
		expect( () => dbUser.patch( { "job._id": '' + jobId } , { validate: true } ) ).to.throw.a( doormen.ValidatorError ) ;
		dbUser.patch( { job: { _id: '' + jobId } } , { validate: true } ) ;
		//log.hdebug( "%Y" , dbUser._.raw ) ;
		expect( dbUser.job._id ).to.equal( jobId ) ;
		await expect( dbUser.getLink( 'job' ) ).to.eventually.equal( {
			_id: jobId ,
			title: "developer" ,
			salary: 60000 ,
			users: {} ,
			schools: {}
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
} ) ;



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
		expect( dbDoc._id ).to.be.an( mongodb.ObjectID ) ;
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

