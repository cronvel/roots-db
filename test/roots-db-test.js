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

/* global describe, it, before, after, beforeEach, expect */

"use strict" ;



var rootsDb = require( '..' ) ;
var util = require( 'util' ) ;
var mongodb = require( 'mongodb' ) ;
var fs = require( 'fs' ) ;

var hash = require( 'hash-kit' ) ;
var string = require( 'string-kit' ) ;
var tree = require( 'tree-kit' ) ;
var streamKit = require( 'stream-kit' ) ;

var Promise = require( 'seventh' ) ;

var ErrorStatus = require( 'error-status' ) ;
var doormen = require( 'doormen' ) ;

var logfella = require( 'logfella' ) ;
logfella.global.setGlobalConfig( { minLevel: process.argv.includes( '--debug' ) ? 'debug' : 'warning' } ) ;
var log = logfella.global.use( 'unit-test' ) ;



// Create the world...
var world = new rootsDb.World() ;

// Collections...
var users , jobs , schools , towns , lockables , nestedLinks , extendables ;

var usersDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/users' ,
	attachmentUrl: __dirname + '/tmp/' ,
	properties: {
		firstName: {
			type: 'string' ,
			maxLength: 30 ,
			default: 'Joe' ,
			tier: 2
		} ,
		lastName: {
			type: 'string' ,
			maxLength: 30 ,
			default: 'Doe' ,
			tier: 2
		} ,
		godfather: {
			type: 'link' ,
			optional: true ,
			collection: 'users' ,
			tier: 3
		} ,
		file: {
			type: 'attachment' ,
			optional: true ,
			tier: 3
		} ,
		connection: {
			type: 'strictObject' ,
			optional: true ,
			of: { type: 'link' , collection: 'users' } ,
			tier: 3
		} ,
		job: {
			type: 'link' ,
			optional: true ,
			collection: 'jobs' ,
			tier: 3
		} ,
		memberSid: {
			optional: true ,
			type: 'string' ,
			maxLength: 30 ,
			tier: 2
		}
	} ,
	indexes: [
		{ properties: { job: 1 } } ,
		{ properties: { job: 1 , memberSid: 1 } , unique: true }
	] ,
	hooks: {
		afterCreateDocument: //[
			function( document ) {
				//console.log( "- Users afterCreateDocument 'after' hook -" ) ;
				document.memberSid = '' + document.firstName + ' ' + document.lastName ;
			}
		//]
	}
} ;

var expectedDefaultUser = { firstName: 'Joe' , lastName: 'Doe' , memberSid: 'Joe Doe' } ;

var jobsDescriptor = {
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

var schoolsDescriptor = {
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
	}
} ;

var townsDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/towns' ,
	properties: {
		name: { type: 'string' } ,
		meta: {
			type: 'strictObject' ,
			default: {} ,
			extraProperties: true ,
			properties: {
				rank: {
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

var lockablesDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/lockables' ,
	canLock: true ,
	lockTimeout: 40 ,
	properties: {
		data: { type: 'string' }
	} ,
	indexes: []
} ;

var nestedLinksDescriptor = {
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



function Extended( collection , rawDoc , options ) {
	rootsDb.DocumentWrapper.call( this , collection , rawDoc , options ) ;
}

Extended.prototype = Object.create( rootsDb.DocumentWrapper.prototype ) ;
Extended.prototype.constructor = Extended ;

Extended.prototype.getNormalized = function() {
	return this.document.data.toLowerCase() ;
} ;

function ExtendedBatch( collection , rawDoc , options ) {
	rootsDb.BatchWrapper.call( this , collection , rawDoc , options ) ;
}

ExtendedBatch.prototype = Object.create( rootsDb.BatchWrapper.prototype ) ;
ExtendedBatch.prototype.constructor = ExtendedBatch ;

ExtendedBatch.prototype.concat = function() {
	var i , iMax , str = '' ;
	for ( i = 0 , iMax = this.batch.length ; i < iMax ; i ++ ) { str += this.batch[ i ].data ; }
	return str ;
} ;



var extendablesDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/extendables' ,
	DocumentWrapper: Extended ,
	BatchWrapper: ExtendedBatch ,
	properties: {
		data: { type: 'string' }
	} ,
	meta: {} ,
	indexes: []
} ;



/* Utils */



// clear DB: remove every item, so we can safely test
function clearDB() {
	return Promise.all( [
		clearCollection( users ) ,
		clearCollection( jobs ) ,
		clearCollection( schools ) ,
		clearCollection( towns ) ,
		clearCollection( lockables ) ,
		clearCollection( nestedLinks ) ,
		clearCollection( extendables )
	] ) ;
}



// clear DB: remove every item, so we can safely test
function clearDBIndexes() {
	return Promise.all( [
		clearCollectionIndexes( users ) ,
		clearCollectionIndexes( jobs ) ,
		clearCollectionIndexes( schools ) ,
		clearCollectionIndexes( towns ) ,
		clearCollectionIndexes( lockables ) ,
		clearCollectionIndexes( nestedLinks ) ,
		clearCollectionIndexes( extendables )
	] ).then( () => { log.verbose( "All indexes cleared" ) ; } ) ;
}



function clearCollection( collection ) {
	return collection.driver.rawInit()
		.then( () => collection.driver.raw.deleteMany() ) ;
}



function clearCollectionIndexes( collection ) {
	return collection.driver.rawInit()
		.then( () => collection.driver.raw.dropIndexes() ) ;
}



/* Tests */



// Force creating the collection
before( () => {
	users = world.createCollection( 'users' , usersDescriptor ) ;
	expect( users ).to.be.a( rootsDb.Collection ) ;

	jobs = world.createCollection( 'jobs' , jobsDescriptor ) ;
	expect( jobs ).to.be.a( rootsDb.Collection ) ;

	schools = world.createCollection( 'schools' , schoolsDescriptor ) ;
	expect( schools ).to.be.a( rootsDb.Collection ) ;

	towns = world.createCollection( 'towns' , townsDescriptor ) ;
	expect( towns ).to.be.a( rootsDb.Collection ) ;

	lockables = world.createCollection( 'lockables' , lockablesDescriptor ) ;
	expect( lockables ).to.be.a( rootsDb.Collection ) ;

	nestedLinks = world.createCollection( 'nestedLinks' , nestedLinksDescriptor ) ;
	expect( nestedLinks ).to.be.a( rootsDb.Collection ) ;

	extendables = world.createCollection( 'extendables' , extendablesDescriptor ) ;
	expect( extendables ).to.be.a( rootsDb.Collection ) ;
} ) ;



describe( "Collection" , () => {

	it( "Tier masks" , () => {
		expect( users.tierPropertyMasks ).to.equal( [
			{} ,
			{ _id: true } ,
			{
				firstName: true , lastName: true , memberSid: true , _id: true
			} ,
			{
				firstName: true , lastName: true , godfather: true , file: true , connection: true , job: true , memberSid: true , _id: true
			} ,
			{
				firstName: true , lastName: true , godfather: true , file: true , connection: true , job: true , memberSid: true , _id: true
			} ,
			{
				firstName: true , lastName: true , godfather: true , file: true , connection: true , job: true , memberSid: true , _id: true
			}
		] ) ;
	} ) ;
} ) ;



describe( "Build collections' indexes" , () => {

	beforeEach( clearDBIndexes ) ;

	it.skip( "should build indexes" , async () => {
		expect( users.uniques ).to.equal( [ [ '_id' ] , [ 'job' , 'memberSid' ] ] ) ;
		expect( jobs.uniques ).to.equal( [ [ '_id' ] ] ) ;

		return Promise.forEach( Object.keys( world.collections ) , async ( name ) => {
			var collection = world.collections[ name ] ;
			await collection.buildIndexes() ;
			log.verbose( 'Index built for collection %s' , name ) ;
			expect( await collection.driver.getIndexes() ).to.equal( collection.indexes ) ;
		} ) ;
	} ) ;
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
		
		var rawDbUser = await users.get( id , { raw: true } ) ;
		
		expect( rawDbUser._ ).not.to.be.a( rootsDb.Document ) ;
		expect( rawDbUser._id ).to.be.an( mongodb.ObjectID ) ;
		expect( rawDbUser._id ).to.equal( id ) ;
		expect( rawDbUser ).to.equal( {
			_id: rawDbUser._id , firstName: 'John' , lastName: 'McGregor' , memberSid: 'John McGregor'
		} ) ;
	} ) ;

	it( "when trying to get an unexistant document, an ErrorStatus (type: notFound) should be issued" , async () => {
		// Unexistant ID
		var id = new mongodb.ObjectID() ;

		await expect( () => users.get( id ) ).to.reject.with.an( ErrorStatus , { type: 'notFound' } ) ;
		await expect( () => users.get( id , { raw: true } ) ).to.reject.with.an( ErrorStatus , { type: 'notFound' } ) ;
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
		expect( dbUser._.localPatch ).to.equal( { firstName: 'Joey' } ) ;
		
		await dbUser.commit() ;
		await expect( users.get( id ) ).to.eventually.equal( {
			_id: id , firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny Starks'
		} ) ;
		
		dbUser.firstName = 'Jack' ;
		dbUser.lastName = 'Smith' ;
		expect( dbUser ).to.equal( {
			_id: id , firstName: 'Jack' , lastName: 'Smith' , memberSid: 'Johnny Starks'
		} ) ;
		expect( dbUser._.localPatch ).to.equal( { firstName: 'Jack' , lastName: 'Smith' } ) ;
		
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
		expect( dbUser._.localPatch ).to.be( null ) ;
		
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
		expect( dbUser._.localPatch ).to.equal( { firstName: 'Joey' } ) ;
		
		await dbUser.commit() ;
		await expect( users.get( id ) ).to.eventually.equal( {
			_id: id , firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny Starks'
		} ) ;
		
		dbUser.patch( { firstName: 'Jack' , lastName: 'Smith' } ) ;
		expect( dbUser ).to.equal( {
			_id: id , firstName: 'Jack' , lastName: 'Smith' , memberSid: 'Johnny Starks'
		} ) ;
		expect( dbUser._.localPatch ).to.equal( { firstName: 'Jack' , lastName: 'Smith' } ) ;
		
		await dbUser.commit() ;
		await expect( users.get( id ) ).to.eventually.equal( {
			_id: id , firstName: 'Jack' , lastName: 'Smith' , memberSid: 'Johnny Starks'
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
		expect( dbTown._.localPatch ).to.equal( { "meta.population": "2500K" } ) ;
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
		expect( user ).to.equal( { _id: id , firstName: 'Johnny' , lastName: 'Smith' , memberSid: 'Johnny Starks' } ) ;
		expect( dbUser ).to.equal( { _id: id , firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny Starks' } ) ;
		
		await Promise.all( [
			user.commit() ,
			dbUser.commit()
		] ) ;
		
		await expect( users.get( id ) ).to.eventually.equal( {
			_id: id , firstName: 'Joey' , lastName: 'Smith' , memberSid: 'Johnny Starks'
		} ) ;
	} ) ;

	it( "overwrite and depth mixing" ) ;
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
		expect( users.createFingerprint( { job: '123456789012345678901234' } ).unique ).to.be( false ) ;
		expect( users.createFingerprint( { memberSid: 'terry-bogard' } ).unique ).to.be( false ) ;
		expect( users.createFingerprint( { job: '123456789012345678901234' , memberSid: 'terry-bogard' } ).unique ).to.be( true ) ;
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
		
		await expect( users.getUnique( { memberSid: memberSid , job: jobId } ) ).to.eventually.equal( {
			_id: userId , job: jobId , firstName: 'Bill' , lastName: "Cut'throat" , memberSid: "Bill Cut'throat"
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
		expect( userBatch[ 0 ] ).to.equal( { _id: userBatch[ 0 ].getId() , firstName: 'Bobby' , lastName: 'Fischer' , memberSid: 'Bobby Fischer' } ) ;
		
		expect( userBatch[ 1 ] ).to.be.an( Object ) ;
		expect( userBatch[ 1 ].$ ).to.be.an( Object ) ;
		expect( userBatch[ 1 ]._ ).to.be.a( rootsDb.Document ) ;
		expect( userBatch[ 1 ]._id ).to.be.an( mongodb.ObjectID ) ;
		expect( userBatch[ 1 ].getId() ).to.be.an( mongodb.ObjectID ) ;
		expect( userBatch[ 1 ]._id ).to.be( userBatch[ 1 ].getId() ) ;
		expect( userBatch[ 1 ] ).to.partially.equal( { _id: userBatch[ 1 ].getId() , firstName: 'John' , lastName: 'Smith' , memberSid: 'John Smith' } ) ;
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
		expect( userBatch[ 2 ] ).to.partially.equal( { _id: userBatch[ 2 ].getId() , firstName: 'Kurisu' , lastName: 'Makise' , memberSid: 'Kurisu Makise' } ) ;
		
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
			{ _id: userBatch[ 0 ].getId() , firstName: 'Bobby' , lastName: 'Fischer' , memberSid: 'Bobby Fischer' }
		) ;

		await expect( users.get( userBatch[ 1 ].getId() ) ).to.eventually.equal(
			{ _id: userBatch[ 1 ].getId() , firstName: 'John' , lastName: 'Smith' , memberSid: 'John Smith' }
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
			Bob: { _id: marleys[ 0 ].getId() , firstName: 'Bob' , lastName: 'Marley' , memberSid: 'Bob Marley' } ,
			Julian: { _id: marleys[ 1 ].getId() , firstName: 'Julian' , lastName: 'Marley' , memberSid: 'Julian Marley' } ,
			Stephen: { _id: marleys[ 2 ].getId() , firstName: 'Stephen' , lastName: 'Marley' , memberSid: 'Stephen Marley' } ,
			Ziggy: { _id: marleys[ 3 ].getId() , firstName: 'Ziggy' , lastName: 'Marley' , memberSid: 'Ziggy Marley' } ,
			Rita: { _id: marleys[ 4 ].getId() , firstName: 'Rita' , lastName: 'Marley' , memberSid: 'Rita Marley' }
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
			Stephen: { _id: marleys[ 2 ].getId() , firstName: 'Stephen' , lastName: 'Marley' , memberSid: 'Stephen Marley' } ,
			Rita: { _id: marleys[ 4 ].getId() , firstName: 'Rita' , lastName: 'Marley' , memberSid: 'Rita Marley' }
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
			Bob: { _id: localBatch[ 0 ].getId() , firstName: 'Bob' , lastName: 'Marley' , memberSid: 'Bob Marley' } ,
			Julian: { _id: localBatch[ 1 ].getId() , firstName: 'Julian' , lastName: 'Marley' , memberSid: 'Julian Marley' } ,
			Stephen: { _id: localBatch[ 3 ].getId() , firstName: 'Stephen' , lastName: 'Marley' , memberSid: 'Stephen Marley' } ,
			Ziggy: { _id: localBatch[ 4 ].getId() , firstName: 'Ziggy' , lastName: 'Marley' , memberSid: 'Ziggy Marley' } ,
			Rita: { _id: localBatch[ 6 ].getId() , firstName: 'Rita' , lastName: 'Marley' , memberSid: 'Rita Marley' }
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
			Stephen: { _id: localBatch[ 3 ].getId() , firstName: 'Stephen' , lastName: 'Marley' , memberSid: 'Stephen Marley' } ,
			Thomas: { _id: localBatch[ 5 ].getId() , firstName: 'Thomas' , lastName: 'Jefferson' , memberSid: 'Thomas Jefferson' }
		} ) ;
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
			foreignCollection: 'jobs' ,
			//foreignId:  ,
			hostPath: 'job' ,
			schema: {
				collection: 'jobs' ,
				optional: true ,
				type: 'link' ,
				sanitize: [ 'toLink' ] ,
				tier: 3
			}
		} ) ;
		
		// Same on saved documents...
		await user.save() ;
		var dbUser = await users.get( userId ) ;

		expect( dbUser.getLinkDetails( 'job' ) ).to.equal( {
			type: 'link' ,
			foreignCollection: 'jobs' ,
			//foreignId:  ,
			hostPath: 'job' ,
			schema: {
				collection: 'jobs' ,
				optional: true ,
				type: 'link' ,
				sanitize: [ 'toLink' ] ,
				tier: 3
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

		expect( user.job ).to.equal( jobId ) ;
		expect( user.getLinkDetails( 'job' ) ).to.equal( {
			type: 'link' ,
			foreignCollection: 'jobs' ,
			foreignId: jobId ,
			hostPath: 'job' ,
			schema: {
				collection: 'jobs' ,
				optional: true ,
				type: 'link' ,
				sanitize: [ 'toLink' ] ,
				tier: 3
			}
		} ) ;
		
		// Same on saved documents...
		await user.save() ;
		await job.save() ;
		var dbUser = await users.get( userId ) ;
		var dbJob = await jobs.get( jobId ) ;

		expect( dbUser.job ).to.equal( jobId ) ;
		expect( user.getLinkDetails( 'job' ) ).to.equal( {
			type: 'link' ,
			foreignCollection: 'jobs' ,
			foreignId: jobId ,
			hostPath: 'job' ,
			schema: {
				collection: 'jobs' ,
				optional: true ,
				type: 'link' ,
				sanitize: [ 'toLink' ] ,
				tier: 3
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

		await user.save() ;
		await job.save() ;
		var dbUser = await users.get( userId ) ;

		expect( dbUser.job ).to.equal( jobId ) ;
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

		expect( user.connection.A ).to.equal( connectionAId ) ;
		expect( user.connection.B ).to.equal( connectionBId ) ;
		
		await Promise.all( [ connectionA.save() , connectionB.save() , user.save() ] ) ;
		
		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.equal( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			connection: {
				A: connectionAId ,
				B: connectionBId
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
				A: connectionAId
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
} ) ;



describe( "Multi-links" , () => {

	beforeEach( clearDB ) ;

	it( "should create, save, retrieve, add and remove multi-links" , async () => {
		var map , batch ;

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
		expect( school.jobs ).to.equal( [ job1Id , job2Id ] ) ;

		await Promise.all( [ job1.save() , job2.save() , job3.save() , school.save() ] ) ;
		await expect( schools.get( id ) ).to.eventually.equal( { _id: id , title: 'Computer Science' , jobs: [ job1Id , job2Id ] } ) ;
		
		batch = await school.getLink( "jobs" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.title ] = doc ; } ) ;
		
		expect( map ).to.equal( {
			developer: { _id: job1Id , title: 'developer' , salary: 60000 , users: [] , schools: [] } ,
			sysadmin: { _id: job2Id , title: 'sysadmin' , salary: 55000 , users: [] , schools: [] }
		} ) ;
		
		// Second test
		
		school.addLink( 'jobs' , job3 ) ;
		expect( school.jobs ).to.equal( [ job1Id , job2Id , job3Id ] ) ;
		await school.save() ;

		batch = await school.getLink( "jobs" ) ;
		
		map = {} ;
		batch.forEach( doc => { map[ doc.title ] = doc ; } ) ;
		
		expect( map ).to.equal( {
			developer: { _id: job1Id , title: 'developer' , salary: 60000 , users: [] , schools: [] } ,
			sysadmin: { _id: job2Id , title: 'sysadmin' , salary: 55000 , users: [] , schools: [] } ,
			"front-end developer": { _id: job3Id , title: 'front-end developer' , salary: 54000 , users: [] , schools: [] }
		} ) ;
		
		// Third test
		
		school.removeLink( 'jobs' , job2 ) ;
		expect( school.jobs ).to.equal( [ job1Id , job3Id ] ) ;
		await school.save() ;
		
		batch = await school.getLink( "jobs" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.title ] = doc ; } ) ;
		
		expect( map ).to.equal( {
			developer: { _id: job1Id , title: 'developer' , salary: 60000 , users: [] , schools: [] } ,
			"front-end developer": { _id: job3Id , title: 'front-end developer' , salary: 54000 , users: [] , schools: [] }
		} ) ;
	} ) ;

	it( "should create, save, retrieve, add and remove deep (nested) multi-links" , async () => {
		var map , batch ;

		var rootDoc = nestedLinks.createDocument( { name: 'root' } ) ;
		var id = rootDoc.getId() ;

		var childDoc1 = nestedLinks.createDocument( { name: 'child1' } ) ;
		var childDoc2 = nestedLinks.createDocument( { name: 'child2' } ) ;
		var childDoc3 = nestedLinks.createDocument( { name: 'child3' } ) ;
		
		// First test

		rootDoc.setLink( 'nested.multiLink' , [ childDoc1 , childDoc2 ] ) ;
		expect( rootDoc.nested.multiLink ).to.equal( [ childDoc1.getId() , childDoc2.getId() ] ) ;

		await Promise.all( [ rootDoc.save() , childDoc1.save() , childDoc2.save() , childDoc3.save() ] ) ;
		await expect( nestedLinks.get( id ) ).to.eventually.equal( {
			_id: id ,
			name: 'root' ,
			nested: {
				backLinkOfLink: [] ,
				backLinkOfMultiLink: [] ,
				multiLink: [ childDoc1.getId() , childDoc2.getId() ]
			}
		} ) ;
		
		batch = await rootDoc.getLink( "nested.multiLink" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.name ] = doc ; } ) ;
		
		expect( map ).to.equal( {
			child1: { _id: childDoc1.getId() , name: "child1" , nested: {} } ,
			child2: { _id: childDoc2.getId() , name: "child2" , nested: {} }
		} ) ;
		
		// Second test
		
		rootDoc.addLink( 'nested.multiLink' , childDoc3 ) ;
		expect( rootDoc.nested.multiLink ).to.equal( [ childDoc1.getId() , childDoc2.getId() , childDoc3.getId() ] ) ;
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
		expect( rootDoc.nested.multiLink ).to.equal( [ childDoc1.getId() , childDoc3.getId() ] ) ;
		await rootDoc.save() ;
		
		batch = await rootDoc.getLink( "nested.multiLink" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.name ] = doc ; } ) ;
		
		expect( map ).to.equal( {
			child1: { _id: childDoc1.getId() , name: "child1" , nested: {} } ,
			child3: { _id: childDoc3.getId() , name: "child3" , nested: {} }
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
			hostPath: 'users' ,
			foreignPath: 'job' ,
			schema: {
				collection: 'users' ,
				//optional: true ,
				type: 'backLink' ,
				sanitize: [ 'toBackLink' ] ,
				path: 'job' ,
				tier: 3
			}
		} ) ;
		
		batch = await job.getLink( "users" ) ;
		
		expect( batch.slice() ).to.equal( [
			{
				_id: id ,
				firstName: 'Jilbert' ,
				lastName: 'Polson' ,
				memberSid: 'Jilbert Polson' ,
				job: jobId
			}
		] ) ;
		
		
		user2.setLink( 'job' , job ) ;
		await user2.save() ;
		
		batch = await job.getLink( "users" ) ;
		
		map = {} ;
		batch.forEach( doc => { map[ doc.firstName ] = doc ; } ) ;
		
		expect( map ).to.equal( {
			Jilbert: { _id: id , firstName: 'Jilbert' , lastName: 'Polson' , memberSid: 'Jilbert Polson' , job: jobId } ,
			Tony: { _id: id2 , firstName: 'Tony' , lastName: 'P.' , memberSid: 'Tony P.' , job: jobId }
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
		expect( dbJob ).to.equal( { _id: job1Id , title: 'developer' , salary: 60000 , users: [] , schools: [] } ) ;
		
		batch = await dbJob.getLink( 'schools' ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.title ] = doc ; } ) ;
		
		expect( map ).to.equal( {
			'Computer Science': { _id: school1Id , title: 'Computer Science' , jobs: [ job1Id , job2Id , job3Id ] } ,
			'Web Academy': { _id: school2Id , title: 'Web Academy' , jobs: [ job1Id , job3Id , job4Id ] }
		} ) ;
		
		dbJob = await jobs.get( job4Id ) ;
		expect( dbJob ).to.equal( { _id: job4Id , title: 'designer' , salary: 56000 , users: [] , schools: [] } ) ;
		
		batch = await dbJob.getLink( 'schools' ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.title ] = doc ; } ) ;
		
		expect( batch.slice() ).to.equal( [
			{ _id: school2Id , title: 'Web Academy' , jobs: [ job1Id , job3Id , job4Id ] }
		] ) ;
	} ) ;

	it( "deep (nested) back-link of single link" , async () => {
		var map , batch ;
		
		var rootDoc = nestedLinks.createDocument( { name: 'root' } ) ;
		var id = rootDoc.getId() ;

		expect( rootDoc.getLinkDetails( "nested.backLinkOfLink" ) ).to.equal( {
			type: 'backLink' ,
			foreignCollection: 'nestedLinks' ,
			hostPath: 'nested.backLinkOfLink' ,
			foreignPath: 'nested.link' ,
			schema: {
				collection: 'nestedLinks' ,
				type: 'backLink' ,
				sanitize: [ 'toBackLink' ] ,
				path: 'nested.link' ,
				tier: 3
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
			child1: { _id: childDoc1.getId() , name: "child1" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , link: id , multiLink: [] } } ,
			child2: { _id: childDoc2.getId() , name: "child2" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , link: id , multiLink: [] } }
		} ) ;
		
		// Second test
		
		childDoc3.setLink( 'nested.link' , rootDoc ) ;
		await childDoc3.save() ;
		batch = await rootDoc.getLink( "nested.backLinkOfLink" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.name ] = doc ; } ) ;
		
		expect( map ).to.equal( {
			child1: { _id: childDoc1.getId() , name: "child1" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , link: id , multiLink: [] } } ,
			child2: { _id: childDoc2.getId() , name: "child2" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , link: id , multiLink: [] } } ,
			child3: { _id: childDoc3.getId() , name: "child3" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , link: id , multiLink: [] } }
		} ) ;
		
		// Third test
		
		childDoc2.removeLink( 'nested.link' ) ;
		await childDoc2.save() ;
		batch = await rootDoc.getLink( "nested.backLinkOfLink" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.name ] = doc ; } ) ;
		
		expect( map ).to.equal( {
			child1: { _id: childDoc1.getId() , name: "child1" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , link: id , multiLink: [] } } ,
			child3: { _id: childDoc3.getId() , name: "child3" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , link: id , multiLink: [] } }
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
			hostPath: 'nested.backLinkOfMultiLink' ,
			foreignPath: 'nested.multiLink' ,
			schema: {
				collection: 'nestedLinks' ,
				type: 'backLink' ,
				sanitize: [ 'toBackLink' ] ,
				path: 'nested.multiLink' ,
				tier: 3
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
			child1: { _id: childDoc1.getId() , name: "child1" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , multiLink: [ rootDoc.getId() ] } } ,
			child2: { _id: childDoc2.getId() , name: "child2" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , multiLink: [ rootDoc.getId() , otherDoc1.getId() , otherDoc2.getId() ] } }
		} ) ;
		
		// Second test
		
		childDoc3.addLink( 'nested.multiLink' , rootDoc ) ;
		await childDoc3.save() ;
		batch = await rootDoc.getLink( "nested.backLinkOfMultiLink" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.name ] = doc ; } ) ;
		
		expect( map ).to.equal( {
			child1: { _id: childDoc1.getId() , name: "child1" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , multiLink: [ rootDoc.getId() ] } } ,
			child2: { _id: childDoc2.getId() , name: "child2" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , multiLink: [ rootDoc.getId() , otherDoc1.getId() , otherDoc2.getId() ] } } ,
			child3: { _id: childDoc3.getId() , name: "child3" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , multiLink: [ otherDoc1.getId() , otherDoc2.getId() , rootDoc.getId() ] } }
		} ) ;
		
		// Third test
		
		childDoc2.removeLink( 'nested.multiLink' , rootDoc ) ;
		await childDoc2.save() ;
		batch = await rootDoc.getLink( "nested.backLinkOfMultiLink" ) ;

		map = {} ;
		batch.forEach( doc => { map[ doc.name ] = doc ; } ) ;
		
		expect( map ).to.equal( {
			child1: { _id: childDoc1.getId() , name: "child1" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , multiLink: [ rootDoc.getId() ] } } ,
			child3: { _id: childDoc3.getId() , name: "child3" , nested: { backLinkOfLink: [] , backLinkOfMultiLink: [] , multiLink: [ otherDoc1.getId() , otherDoc2.getId() , rootDoc.getId() ] } }
		} ) ;
	} ) ;
} ) ;



describe( "Attachment links" , () => {

	beforeEach( clearDB ) ;

	it( "should create, save, and load an attachment" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;
		
		var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		var fullUrl = attachment.fullUrl ;
		user.setAttachment( 'file' , attachment ) ;
		//log.error( user.file ) ;

		expect( user.file ).to.equal( {
			filename: 'joke.txt' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'text/plain'
		} ) ;
		
		await attachment.save() ;
		await user.save() ;
		
		// Check that the file exists
		expect( () => { fs.accessSync( fullUrl , fs.R_OK ) ; } ).not.to.throw() ;
		
		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.equal( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'joke.txt' ,
				id: user.file.id ,	// Unpredictable
				contentType: 'text/plain'
			}
		} ) ;
		
		var details = dbUser.getAttachmentDetails( 'file' ) ;
		expect( details ).to.be.like( {
			type: 'attachment' ,
			hostPath: 'file' ,
			schema: {
				optional: true ,
				type: 'attachment' ,
				tier: 3
			} ,
			attachment: {
				id: user.file.id ,
				filename: 'joke.txt' ,
				contentType: 'text/plain' ,
				collectionName: 'users' ,
				documentId: id.toString() ,
				incoming: undefined ,
				baseUrl: details.attachment.baseUrl ,
				fullUrl: details.attachment.baseUrl +
					details.attachment.documentId.toString() +
					'/' + details.attachment.id.toString()
			}
		} ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.like( {
			id: dbUser.file.id ,
			filename: 'joke.txt' ,
			contentType: 'text/plain' ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			incoming: undefined ,
			baseUrl: dbAttachment.baseUrl ,
			fullUrl: dbAttachment.baseUrl + dbAttachment.documentId.toString() + '/' + dbAttachment.id.toString()
		} ) ;
		
		var content = await dbAttachment.load() ;
		expect( content.toString() ).to.be( "grigrigredin menufretin\n" ) ;
	} ) ;
	
	it( "should alter meta-data of an attachment" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;
		
		var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		var fullUrl = attachment.fullUrl ;
		user.setAttachment( 'file' , attachment ) ;
		
		await attachment.save() ;
		await user.save() ;
		
		var dbUser = await users.get( id ) ;
		dbUser.file.filename = 'lol.txt' ;
		dbUser.file.contentType = 'text/joke' ;
		await dbUser.save() ;
		
		dbUser = await users.get( id ) ;
		
		expect( dbUser ).to.equal( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'lol.txt' ,
				id: user.file.id ,	// Unpredictable
				contentType: 'text/joke'
			}
		} ) ;
		
		var details = dbUser.getAttachmentDetails( 'file' ) ;
		expect( details ).to.be.like( {
			type: 'attachment' ,
			hostPath: 'file' ,
			schema: {
				optional: true ,
				type: 'attachment' ,
				tier: 3
			} ,
			attachment: {
				id: user.file.id ,
				filename: 'lol.txt' ,
				contentType: 'text/joke' ,
				collectionName: 'users' ,
				documentId: id.toString() ,
				incoming: undefined ,
				baseUrl: details.attachment.baseUrl ,
				fullUrl: details.attachment.baseUrl +
					details.attachment.documentId.toString() +
					'/' + details.attachment.id.toString()
			}
		} ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.like( {
			id: dbUser.file.id ,
			filename: 'lol.txt' ,
			contentType: 'text/joke' ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			incoming: undefined ,
			baseUrl: dbAttachment.baseUrl ,
			fullUrl: dbAttachment.baseUrl + dbAttachment.documentId.toString() + '/' + dbAttachment.id.toString()
		} ) ;
		
		var content = await dbAttachment.load() ;
		expect( content.toString() ).to.be( "grigrigredin menufretin\n" ) ;
	} ) ;

	it( "should replace an attachment" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;
		
		var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		var fullUrl = attachment.fullUrl ;
		await user.setAttachment( 'file' , attachment ) ;
		
		await attachment.save() ;
		await user.save() ;
		
		// Check that the file exists
		expect( () => { fs.accessSync( fullUrl , fs.R_OK ) ; } ).not.to.throw() ;
		
		var dbUser = await users.get( id ) ;
		
		await expect( dbUser.getAttachment( 'file' ).load().then( v => v.toString() ) ).to.eventually.be( "grigrigredin menufretin\n" ) ;
		
		var attachment2 = user.createAttachment(
			{ filename: 'hello-world.html' , contentType: 'text/html' } ,
			"<html><head></head><body>Hello world!</body></html>\n"
		) ;

		await dbUser.setAttachment( 'file' , attachment2 ) ;

		// Check that the previous file has been deleted
		expect( () => { fs.accessSync( fullUrl , fs.R_OK ) ; } ).to.throw( Error , { code: 'ENOENT' } ) ;
		
		await attachment2.save() ;
		await dbUser.save() ;

		dbUser = await users.get( id ) ;
		
		expect( dbUser ).to.equal( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'hello-world.html' ,
				id: dbUser.file.id ,	// Unpredictable
				contentType: 'text/html'
			}
		} ) ;
		
		var details = dbUser.getAttachmentDetails( 'file' ) ;
		expect( details ).to.be.like( {
			type: 'attachment' ,
			hostPath: 'file' ,
			schema: {
				optional: true ,
				type: 'attachment' ,
				tier: 3
			} ,
			attachment: {
				id: dbUser.file.id ,
				filename: 'hello-world.html' ,
				contentType: 'text/html' ,
				collectionName: 'users' ,
				documentId: id.toString() ,
				incoming: undefined ,
				baseUrl: details.attachment.baseUrl ,
				fullUrl: details.attachment.baseUrl +
					details.attachment.documentId.toString() +
					'/' + details.attachment.id.toString()
			}
		} ) ;
		
		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.like( {
			id: dbUser.file.id ,
			filename: 'hello-world.html' ,
			contentType: 'text/html' ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			incoming: undefined ,
			baseUrl: details.attachment.baseUrl ,
			fullUrl: details.attachment.baseUrl +
				details.attachment.documentId.toString() +
				'/' + details.attachment.id.toString()
		} ) ;

		await expect( dbAttachment.load().then( v => v.toString() ) ).to.eventually.be( "<html><head></head><body>Hello world!</body></html>\n" ) ;
		
		// Check that the file exists
		expect( () => { fs.accessSync( dbAttachment.fullUrl , fs.R_OK ) ; } ).not.to.throw() ;
	} ) ;

	it( "Delete an attachment" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;
		
		var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		var fullUrl = attachment.fullUrl ;
		await user.setAttachment( 'file' , attachment ) ;
		
		await attachment.save() ;
		await user.save() ;
		
		// Check that the file exists
		expect( () => { fs.accessSync( fullUrl , fs.R_OK ) ; } ).not.to.throw() ;
		
		var dbUser = await users.get( id ) ;
		
		await expect( dbUser.getAttachment( 'file' ).load().then( v => v.toString() ) ).to.eventually.be( "grigrigredin menufretin\n" ) ;
		
		await dbUser.removeAttachment( 'file' ) ;

		// Check that the previous file has been deleted
		expect( () => { fs.accessSync( fullUrl , fs.R_OK ) ; } ).to.throw( Error , { code: 'ENOENT' } ) ;
		
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
	
	it( "should create, save and replace attachments as streams" , async () => {
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;
		var stream = new streamKit.FakeReadable( { timeout: 100 , chunkSize: 10 , chunkCount: 4 , filler: 'a'.charCodeAt(0) } ) ;
		
		var attachment = user.createAttachment( { filename: 'random.bin' , contentType: 'bin/random' } , stream ) ;
		var fullUrl = attachment.fullUrl ;
		user.setAttachment( 'file' , attachment ) ;
		//log.error( user.file ) ;

		expect( user.file ).to.equal( {
			filename: 'random.bin' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'bin/random'
		} ) ;
		
		await attachment.save() ;
		await user.save() ;
		
		// Check that the file exists
		expect( () => { fs.accessSync( fullUrl , fs.R_OK ) ; } ).not.to.throw() ;
		
		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.equal( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'random.bin' ,
				id: user.file.id ,	// Unpredictable
				contentType: 'bin/random'
			}
		} ) ;
		
		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.like( {
			id: dbUser.file.id ,
			filename: 'random.bin' ,
			contentType: 'bin/random' ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			incoming: undefined ,
			baseUrl: dbAttachment.baseUrl ,
			fullUrl: dbAttachment.baseUrl + dbAttachment.documentId.toString() + '/' + dbAttachment.id.toString()
		} ) ;
		
		await expect( dbAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'a'.repeat( 40 ) ) ;

		
		stream = new streamKit.FakeReadable( { timeout: 100 , chunkSize: 10 , chunkCount: 3 , filler: 'b'.charCodeAt(0) } ) ;
		var attachment2 = user.createAttachment( { filename: 'more-random.bin' , contentType: 'bin/random' } , stream ) ;

		await dbUser.setAttachment( 'file' , attachment2 ) ;

		// Check that the previous file has been deleted
		expect( () => { fs.accessSync( fullUrl , fs.R_OK ) ; } ).to.throw( Error , { code: 'ENOENT' } ) ;
		
		await attachment2.save() ;
		await dbUser.save() ;

		dbUser = await users.get( id ) ;
		
		expect( dbUser ).to.equal( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'more-random.bin' ,
				id: dbUser.file.id ,	// Unpredictable
				contentType: 'bin/random'
			}
		} ) ;
		
		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.like( {
			id: dbUser.file.id ,
			filename: 'more-random.bin' ,
			contentType: 'bin/random' ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			incoming: undefined ,
			baseUrl: dbAttachment.baseUrl ,
			fullUrl: dbAttachment.baseUrl +
				dbAttachment.documentId.toString() +
				'/' + dbAttachment.id.toString()
		} ) ;

		await expect( dbAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'b'.repeat( 30 ) ) ;
		
		// Check that the file exists
		expect( () => { fs.accessSync( dbAttachment.fullUrl , fs.R_OK ) ; } ).not.to.throw() ;
	} ) ;
	
	it( "AttachmentStreams objects" ) ;
} ) ;

return ;


describe( "Populate links" , () => {

	beforeEach( clearDB ) ;

	it( "link population (create both, link, save both, get with populate option)" , ( done ) => {

		var options ;

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user._id ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		//console.log( job ) ;
		var jobId = job.$.id ;

		// Link the documents!
		user.$.setLink( 'job' , job ) ;

		expect( user.job ).to.equal( jobId ) ;

		async.series( [
			function( callback ) {
				job.$.save( callback ) ;
			} ,
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				options = { populate: 'job' } ;
				users.get( id , options , ( error , user_ ) => {
					user = user_ ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ;
					expect( error ).not.to.be.ok() ;
					expect( user.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.equal( id ) ;
					expect( user ).to.equal( {
						_id: user._id , job: job , firstName: 'Jilbert' , lastName: 'Polson' , memberSid: 'Jilbert Polson'
					} ) ;

					expect( options.populateDepth ).to.be( 1 ) ;
					expect( options.populateDbQueries ).to.be( 1 ) ;

					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

	it( "multiple link population (create, link, save, get with populate option)" , ( done ) => {

		var options ;

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var godfather = users.createDocument( {
			firstName: 'DA' ,
			lastName: 'GODFATHER'
		} ) ;

		var id = user._id ;

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		// Link the documents!
		user.$.setLink( 'job' , job ) ;
		user.$.setLink( 'godfather' , godfather ) ;

		async.series( [
			function( callback ) {
				job.$.save( callback ) ;
			} ,
			function( callback ) {
				godfather.$.save( callback ) ;
			} ,
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				options = { populate: [ 'job' , 'godfather' ] } ;
				users.get( id , options , ( error , user_ ) => {
					user = user_ ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ;
					expect( error ).not.to.be.ok() ;
					expect( user.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.equal( id ) ;
					expect( user ).to.equal( {
						_id: user._id ,
						job: job ,
						godfather: godfather ,
						firstName: 'Jilbert' ,
						lastName: 'Polson' ,
						memberSid: 'Jilbert Polson'
					} ) ;

					expect( options.populateDepth ).to.be( 1 ) ;
					expect( options.populateDbQueries ).to.be( 2 ) ;

					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

	it( "multiple link population having same and circular target" , ( done ) => {

		var options ;

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user._id ;

		var connection = users.createDocument( {
			firstName: 'John' ,
			lastName: 'Fergusson'
		} ) ;

		// Link the documents!
		user.$.setLink( 'connection.A' , connection ) ;
		user.$.setLink( 'connection.B' , connection ) ;
		user.$.setLink( 'connection.C' , user ) ;

		expect( user.connection.A ).to.equal( connection.$.id ) ;
		expect( user.connection.B ).to.equal( connection.$.id ) ;
		expect( user.connection.C ).to.equal( user.$.id ) ;

		async.series( [
			function( callback ) {
				connection.$.save( callback ) ;
			} ,
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				options = { populate: [ 'connection.A' , 'connection.B' , 'connection.C' ] } ;
				users.get( id , options , ( error , user ) => {
					expect( user.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.equal( id ) ;
					expect( user.connection.A ).to.be( user.connection.B ) ;
					expect( user ).to.equal( {
						_id: user._id ,
						firstName: 'Jilbert' ,
						lastName: 'Polson' ,
						connection: {
							A: connection ,
							B: connection ,
							C: user
						} ,
						memberSid: 'Jilbert Polson'
					} ) ;

					expect( options.populateDepth ).to.be( 1 ) ;
					expect( options.populateDbQueries ).to.be( 1 ) ;

					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

	it( "collect batch with multiple link population (create, link, save, collect with populate option)" , ( done ) => {

		var options ;

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
		user1.$.setLink( 'job' , job ) ;
		user1.$.setLink( 'godfather' , godfather ) ;
		user3.$.setLink( 'godfather' , godfather ) ;

		async.series( [
			function( callback ) {
				job.$.save( callback ) ;
			} ,
			function( callback ) {
				godfather.$.save( callback ) ;
			} ,
			function( callback ) {
				user1.$.save( callback ) ;
			} ,
			function( callback ) {
				user2.$.save( callback ) ;
			} ,
			function( callback ) {
				user3.$.save( callback ) ;
			} ,
			function( callback ) {
				options = { populate: [ 'job' , 'godfather' ] } ;
				users.collect( {} , options , ( error , batch ) => {
					expect( error ).not.to.be.ok() ;

					// Sort that first...
					batch.sort( ( a , b ) => {
						return a.firstName.charCodeAt( 0 ) - b.firstName.charCodeAt( 0 ) ;
					} ) ;

					expect( batch ).to.equal( [
						{
							firstName: 'DA' ,
							lastName: 'GODFATHER' ,
							_id: batch[ 0 ]._id ,
							memberSid: 'DA GODFATHER'
							//, job: null, godfather: null
							//, job: undefined, godfather: undefined
						} ,
						{
							firstName: 'Harry' ,
							lastName: 'Campbell' ,
							_id: batch[ 1 ]._id ,
							memberSid: 'Harry Campbell' ,
							godfather: {
								firstName: 'DA' ,
								lastName: 'GODFATHER' ,
								_id: batch[ 0 ]._id ,
								memberSid: 'DA GODFATHER'
							}
							//, job: null
							//, job: undefined
						} ,
						{
							firstName: 'Jilbert' ,
							lastName: 'Polson' ,
							_id: batch[ 2 ]._id ,
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
								_id: batch[ 0 ]._id ,
								memberSid: 'DA GODFATHER'
							}
						} ,
						{
							firstName: 'Thomas' ,
							lastName: 'Campbell' ,
							_id: batch[ 3 ]._id ,
							memberSid: 'Thomas Campbell'
							//, job: null, godfather: null
							//, job: undefined, godfather: undefined
						}
					] ) ;

					expect( options.populateDepth ).to.be( 1 ) ;
					// Only one DB query, since the godfather is a user and all users have been collected before the populate pass
					expect( options.populateDbQueries ).to.be( 1 ) ;

					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

	it( "collect batch with multiple link population and circular references" , ( done ) => {

		var options ;

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
		user1.$.setLink( 'job' , job ) ;
		user1.$.setLink( 'godfather' , godfather ) ;
		user3.$.setLink( 'godfather' , godfather ) ;
		godfather.$.setLink( 'godfather' , godfather ) ;

		async.series( [
			function( callback ) {
				job.$.save( callback ) ;
			} ,
			function( callback ) {
				godfather.$.save( callback ) ;
			} ,
			function( callback ) {
				user1.$.save( callback ) ;
			} ,
			function( callback ) {
				user2.$.save( callback ) ;
			} ,
			function( callback ) {
				user3.$.save( callback ) ;
			} ,
			function( callback ) {
				options = { populate: [ 'job' , 'godfather' ] } ;
				users.collect( {} , options , ( error , batch ) => {
					expect( error ).not.to.be.ok() ;

					// Sort that first...
					batch.sort( ( a , b ) => {
						return a.firstName.charCodeAt( 0 ) - b.firstName.charCodeAt( 0 ) ;
					} ) ;

					// References are painful to test...
					// More tests covering references are done in the memory model section
					//log.warning( 'incomplete test for populate + reference' ) ;

					expect( batch[ 0 ].godfather ).to.be( batch[ 0 ] ) ;
					//expect( batch[ 1 ].godfather ).to.be( batch[ 0 ] ) ;
					expect( batch[ 2 ].godfather ).to.be( batch[ 0 ] ) ;

					// JSON.stringify() should throw
					expect( () => { JSON.stringify( batch ) ; } ).to.throwException() ;

					expect( options.populateDepth ).to.be( 1 ) ;
					// Only one DB query, since the godfather is a user and all users have been collected before the populate pass
					expect( options.populateDbQueries ).to.be( 1 ) ;

					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

	it( "collect batch with multiple link population and circular references: using noReference" , ( done ) => {

		var options ;

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
		user1.$.setLink( 'job' , job ) ;
		user1.$.setLink( 'godfather' , godfather ) ;
		user3.$.setLink( 'godfather' , godfather ) ;
		godfather.$.setLink( 'godfather' , godfather ) ;

		async.series( [
			function( callback ) {
				job.$.save( callback ) ;
			} ,
			function( callback ) {
				godfather.$.save( callback ) ;
			} ,
			function( callback ) {
				user1.$.save( callback ) ;
			} ,
			function( callback ) {
				user2.$.save( callback ) ;
			} ,
			function( callback ) {
				user3.$.save( callback ) ;
			} ,
			function( callback ) {
				options = { populate: [ 'job' , 'godfather' ] , noReference: true } ;
				users.collect( {} , options , ( error , batch ) => {
					expect( error ).not.to.be.ok() ;

					// Sort that first...
					batch.sort( ( a , b ) => {
						return a.firstName.charCodeAt( 0 ) - b.firstName.charCodeAt( 0 ) ;
					} ) ;

					expect( batch ).to.equal( [
						{
							firstName: 'DA' ,
							lastName: 'GODFATHER' ,
							_id: batch[ 0 ]._id ,
							memberSid: 'DA GODFATHER' ,
							godfather: {
								firstName: 'DA' ,
								lastName: 'GODFATHER' ,
								_id: batch[ 0 ]._id ,
								memberSid: 'DA GODFATHER' ,
								godfather: batch[ 0 ]._id
							}
						} ,
						{
							firstName: 'Harry' ,
							lastName: 'Campbell' ,
							_id: batch[ 1 ]._id ,
							memberSid: 'Harry Campbell' ,
							godfather: {
								firstName: 'DA' ,
								lastName: 'GODFATHER' ,
								_id: batch[ 0 ]._id ,
								memberSid: 'DA GODFATHER' ,
								godfather: batch[ 0 ]._id
							}
							//, job: null
							//, job: undefined
						} ,
						{
							firstName: 'Jilbert' ,
							lastName: 'Polson' ,
							_id: batch[ 2 ]._id ,
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
								_id: batch[ 0 ]._id ,
								memberSid: 'DA GODFATHER' ,
								godfather: batch[ 0 ]._id
							}
						} ,
						{
							firstName: 'Thomas' ,
							lastName: 'Campbell' ,
							_id: batch[ 3 ]._id ,
							memberSid: 'Thomas Campbell'
							//, job: null, godfather: null
							//, job: undefined, godfather: undefined
						}
					] ) ;

					//console.log( batch ) ;

					// JSON.stringify() should not throw
					expect( () => { JSON.stringify( batch ) ; } ).not.to.throwException() ;

					expect( options.populateDepth ).to.be( 1 ) ;
					// Only one DB query, since the godfather is a user and all users have been collected before the populate pass
					expect( options.populateDbQueries ).to.be( 1 ) ;

					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

	it( "'multi-link' population (create both, link, save both, get with populate option)" , ( done ) => {

		var options ;

		var school ;

		var school1 = schools.createDocument( {
			title: 'Computer Science'
		} ) ;

		var school1Id = school1._id ;

		var school2 = schools.createDocument( {
			title: 'Web Academy'
		} ) ;

		var school2Id = school2._id ;

		var job1 = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var job1Id = job1.$.id ;

		var job2 = jobs.createDocument( {
			title: 'sysadmin' ,
			salary: 55000
		} ) ;

		var job2Id = job2.$.id ;

		var job3 = jobs.createDocument( {
			title: 'front-end developer' ,
			salary: 54000
		} ) ;

		var job4 = jobs.createDocument( {
			title: 'designer' ,
			salary: 56000
		} ) ;

		var job4Id = job4.$.id ;

		// Link the documents!
		school1.$.setLink( 'jobs' , [ job1 , job2 , job3 ] ) ;
		school2.$.setLink( 'jobs' , [ job1 , job3 , job4 ] ) ;

		async.series( [
			function( callback ) {
				job1.$.save( callback ) ;
			} ,
			function( callback ) {
				job2.$.save( callback ) ;
			} ,
			function( callback ) {
				job3.$.save( callback ) ;
			} ,
			function( callback ) {
				job4.$.save( callback ) ;
			} ,
			function( callback ) {
				school1.$.save( callback ) ;
			} ,
			function( callback ) {
				school2.$.save( callback ) ;
			} ,
			function( callback ) {
				options = { populate: 'jobs' } ;
				schools.get( school1Id , options , ( error , school_ ) => {
					school = school_ ;
					//console.log( '>>>>>>>>>>>\nSchool:' , school ) ;
					expect( error ).not.to.be.ok() ;
					expect( school.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( school._id ).to.be.an( mongodb.ObjectID ) ;
					expect( school._id ).to.equal( school1Id ) ;
					expect( school ).to.equal( {
						_id: school1._id ,
						title: 'Computer Science' ,
						jobs: [ job1 , job2 , job3 ]
					} ) ;

					expect( options.populateDepth ).to.be( 1 ) ;
					expect( options.populateDbQueries ).to.be( 1 ) ;

					callback() ;
				} ) ;
			} ,
			function( callback ) {
				options = { populate: 'jobs' } ;
				schools.collect( {} , options , ( error , schools_ ) => {

					expect( error ).not.to.be.ok() ;

					if ( schools_[ 0 ].title !== 'Computer Science' ) { schools_ = [ schools_[ 1 ] , schools_[ 0 ] ] ; }

					expect( schools_ ).to.equal( [
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

					expect( options.populateDepth ).to.be( 1 ) ;
					expect( options.populateDbQueries ).to.be( 1 ) ;

					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

	it( "'back-link' population (create both, link, save both, get with populate option)" , ( done ) => {

		var options ;

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

		//console.log( job1 ) ;
		var job1Id = job1.$.id ;

		// Link the documents!
		user1.$.setLink( 'job' , job1 ) ;
		user2.$.setLink( 'job' , job1 ) ;
		user3.$.setLink( 'job' , job2 ) ;
		user4.$.setLink( 'job' , job2 ) ;

		async.series( [
			function( callback ) {
				job1.$.save( callback ) ;
			} ,
			function( callback ) {
				job2.$.save( callback ) ;
			} ,
			function( callback ) {
				user1.$.save( callback ) ;
			} ,
			function( callback ) {
				user2.$.save( callback ) ;
			} ,
			function( callback ) {
				user3.$.save( callback ) ;
			} ,
			function( callback ) {
				user4.$.save( callback ) ;
			} ,
			function( callback ) {
				options = { populate: 'users' } ;
				jobs.get( job1Id , options , ( error , job_ ) => {
					//console.error( job_.users ) ;
					expect( error ).not.to.be.ok() ;
					expect( job_.users ).to.have.length( 2 ) ;

					if ( job_.users[ 0 ].firstName === 'Tony' ) { job_.users = [ job_.users[ 1 ] , job_.users[ 0 ] ] ; }

					expect( job_ ).to.equal( {
						_id: job1._id ,
						title: 'developer' ,
						salary: 60000 ,
						users: [ user1 , user2 ] ,
						schools: []
					} ) ;

					expect( options.populateDepth ).to.be( 1 ) ;
					expect( options.populateDbQueries ).to.be( 1 ) ;

					callback() ;
				} ) ;
			} ,
			function( callback ) {
				options = { populate: 'users' } ;
				jobs.collect( {} , options , ( error , jobs_ ) => {
					expect( error ).not.to.be.ok() ;
					expect( jobs_ ).to.have.length( 2 ) ;

					//console.error( "\n\n\n\njobs:" , jobs_ ) ;
					if ( jobs_[ 0 ].title === 'star developer' ) { jobs_ = [ jobs_[ 1 ] , jobs_[ 0 ] ] ; }

					expect( jobs_[ 0 ].users ).to.have.length( 2 ) ;

					if ( jobs_[ 0 ].users[ 0 ].firstName === 'Tony' ) { jobs_[ 0 ].users = [ jobs_[ 0 ].users[ 1 ] , jobs_[ 0 ].users[ 0 ] ] ; }

					expect( jobs_[ 0 ] ).to.equal( {
						_id: job1._id ,
						title: 'developer' ,
						salary: 60000 ,
						users: [ user1 , user2 ] ,
						schools: []
					} ) ;

					expect( jobs_[ 1 ].users ).to.have.length( 2 ) ;

					if ( jobs_[ 1 ].users[ 0 ].firstName === 'Richard' ) { jobs_[ 1 ].users = [ jobs_[ 1 ].users[ 1 ] , jobs_[ 1 ].users[ 0 ] ] ; }

					expect( jobs_[ 1 ] ).to.equal( {
						_id: job2._id ,
						title: 'star developer' ,
						salary: 200000 ,
						users: [ user3 , user4 ] ,
						schools: []
					} ) ;

					expect( options.populateDepth ).to.be( 1 ) ;
					expect( options.populateDbQueries ).to.be( 1 ) ;

					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

	it( "'back-link' of multi-link population" , ( done ) => {

		var options ;

		var school1 = schools.createDocument( {
			title: 'Computer Science'
		} ) ;

		var school1Id = school1._id ;

		var school2 = schools.createDocument( {
			title: 'Web Academy'
		} ) ;

		var school2Id = school2._id ;

		var job1 = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var job1Id = job1.$.id ;

		var job2 = jobs.createDocument( {
			title: 'sysadmin' ,
			salary: 55000
		} ) ;

		var job2Id = job2.$.id ;

		var job3 = jobs.createDocument( {
			title: 'front-end developer' ,
			salary: 54000
		} ) ;

		var job3Id = job3.$.id ;

		var job4 = jobs.createDocument( {
			title: 'designer' ,
			salary: 56000
		} ) ;

		var job4Id = job4.$.id ;

		// Link the documents!
		school1.$.setLink( 'jobs' , [ job1 , job2 , job3 ] ) ;
		school2.$.setLink( 'jobs' , [ job1 , job3 , job4 ] ) ;

		async.series( [
			function( callback ) {
				job1.$.save( callback ) ;
			} ,
			function( callback ) {
				job2.$.save( callback ) ;
			} ,
			function( callback ) {
				job3.$.save( callback ) ;
			} ,
			function( callback ) {
				job4.$.save( callback ) ;
			} ,
			function( callback ) {
				school1.$.save( callback ) ;
			} ,
			function( callback ) {
				school2.$.save( callback ) ;
			} ,
			function( callback ) {
				options = { populate: 'schools' } ;
				jobs.get( job1Id , options , ( error , job ) => {
					expect( error ).not.to.be.ok() ;
					expect( job._id ).to.equal( job1Id ) ;

					expect( job.schools ).to.have.length( 2 ) ;

					job.schools.sort( ( a , b ) => { return b.title - a.title ; } ) ;

					// Order by id
					job.schools[ 0 ].jobs.sort( ( a , b ) => { return a.toString() > b.toString() ? 1 : -1 ; } ) ;
					job.schools[ 1 ].jobs.sort( ( a , b ) => { return a.toString() > b.toString() ? 1 : -1 ; } ) ;

					expect( job ).to.equal( {
						_id: job1._id ,
						title: 'developer' ,
						salary: 60000 ,
						users: [] ,
						schools: [
							{
								_id: school1._id ,
								title: 'Computer Science' ,
								jobs: [ job1Id , job2Id , job3Id ]
							} ,
							{
								_id: school2._id ,
								title: 'Web Academy' ,
								jobs: [ job1Id , job3Id , job4Id ]
							}
						]
					} ) ;

					expect( options.populateDepth ).to.be( 1 ) ;
					expect( options.populateDbQueries ).to.be( 1 ) ;

					callback() ;
				} ) ;
			} ,
			function( callback ) {
				options = { populate: 'schools' } ;
				jobs.get( job4Id , options , ( error , job ) => {
					expect( error ).not.to.be.ok() ;
					expect( job._id ).to.equal( job4Id ) ;

					expect( job.schools ).to.have.length( 1 ) ;

					// Order by id
					job.schools[ 0 ].jobs.sort( ( a , b ) => { return a.toString() > b.toString() ? 1 : -1 ; } ) ;

					expect( job ).to.equal( {
						_id: job4._id ,
						title: 'designer' ,
						salary: 56000 ,
						users: [] ,
						schools: [
							{
								_id: school2._id ,
								title: 'Web Academy' ,
								jobs: [ job1Id , job3Id , job4Id ]
							}
						]
					} ) ;

					expect( options.populateDepth ).to.be( 1 ) ;
					expect( options.populateDbQueries ).to.be( 1 ) ;

					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

} ) ;



describe( "Deep populate links" , () => {

	beforeEach( clearDB ) ;

	it( "deep population (links then back-link)" , ( done ) => {

		var options ;

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
		user.$.setLink( 'job' , job ) ;
		user2.$.setLink( 'job' , job ) ;

		async.series( [
			function( callback ) {
				job.$.save( callback ) ;
			} ,
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				user2.$.save( callback ) ;
			} ,
			function( callback ) {
				options = { deepPopulate: deepPopulate } ;
				users.get( user._id , options , ( error , user_ ) => {
					expect( error ).not.to.be.ok() ;
					expect( user_.$.populated.job ).to.be( true ) ;

					expect( user_.job.users ).to.have.length( 2 ) ;

					if ( user_.job.users[ 0 ].firstName === 'Robert' ) {
						user_.job.users = [ user_.job.users[ 1 ] , user_.job.users[ 0 ] ] ;
					}

					expect( user_.job.users[ 0 ].job ).to.be( user_.job ) ;
					expect( user_.job.users[ 1 ].job ).to.be( user_.job ) ;

					expect( user_ ).to.equal( {
						_id: user._id ,
						firstName: 'Jilbert' ,
						lastName: 'Polson' ,
						memberSid: 'Jilbert Polson' ,
						job: {
							_id: job._id ,
							title: 'developer' ,
							salary: 60000 ,
							schools: [] ,
							users: [
								user_ ,
								// We cannot use 'user2', expect.js is too confused with Circular references
								// We have to perform a second check for that
								user_.job.users[ 1 ]
							]
						}
					} ) ;

					expect( user_.job.users[ 1 ] ).to.equal( {
						_id: user2._id ,
						firstName: 'Robert' ,
						lastName: 'Polson' ,
						memberSid: 'Robert Polson' ,
						job: {
							_id: job._id ,
							title: 'developer' ,
							salary: 60000 ,
							schools: [] ,
							users: [
								user_ ,
								user_.job.users[ 1 ]
							]
						}
					} ) ;

					expect( options.populateDepth ).to.be( 2 ) ;
					expect( options.populateDbQueries ).to.be( 2 ) ;

					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

	it( "more deep population tests" ) ;
} ) ;



describe( "Caching with the memory model" , () => {

	beforeEach( clearDB ) ;

	it( "should get a document from a Memory Model cache" , ( done ) => {

		var mem = world.createMemoryModel() ;

		var rawUser = {
			_id: '123456789012345678901234' ,
			firstName: 'John' ,
			lastName: 'McGregor'
		} ;

		mem.add( 'users' , rawUser ) ;

		async.series( [
			function( callback ) {
				users.get( rawUser._id , { cache: mem } , ( error , user ) => {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ;
					expect( error ).not.to.be.ok() ;
					expect( user.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user ).to.equal( { _id: rawUser._id , firstName: 'John' , lastName: 'McGregor' } ) ;
					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

	it( "should multiGet all documents from a Memory Model cache (complete cache hit)" , ( done ) => {

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

		mem.add( 'users' , someUsers[ 0 ] ) ;
		mem.add( 'users' , someUsers[ 1 ] ) ;
		mem.add( 'users' , someUsers[ 2 ] ) ;

		async.series( [
			function( callback ) {
				var ids = [
					'000000000000000000000001' ,
					'000000000000000000000002' ,
					'000000000000000000000003'
				] ;

				users.multiGet( ids , { cache: mem } , ( error , batch ) => {
					var i , map = {} ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'Batch:' , batch ) ;
					expect( error ).not.to.be.ok() ;
					expect( batch.$ ).to.be.a( rootsDb.BatchWrapper ) ;

					batch.sort( ( a , b ) => {
						return parseInt( a._id.toString() , 10 ) - parseInt( b._id.toString() , 10 ) ;
					} ) ;

					expect( batch ).to.equal( [
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

					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

	it( "should multiGet some document from a Memory Model cache (partial cache hit)" , ( done ) => {

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

		mem.add( 'users' , someUsers[ 0 ] ) ;
		mem.add( 'users' , someUsers[ 1 ] ) ;
		mem.add( 'users' , someUsers[ 2 ] ) ;

		var anotherOne = users.createDocument( {
			_id: '000000000000000000000004' ,
			firstName: 'John4' ,
			lastName: 'McGregor'
		} ) ;

		async.series( [
			function( callback ) {
				anotherOne.$.save( callback ) ;
			} ,
			function( callback ) {
				var ids = [
					'000000000000000000000001' ,
					'000000000000000000000002' ,
					'000000000000000000000004'
				] ;

				users.multiGet( ids , { cache: mem } , ( error , batch ) => {
					var i , map = {} ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'Batch:' , batch ) ;
					expect( error ).not.to.be.ok() ;
					expect( batch.$ ).to.be.a( rootsDb.BatchWrapper ) ;

					batch.sort( ( a , b ) => {
						return parseInt( a._id.toString() , 10 ) - parseInt( b._id.toString() , 10 ) ;
					} ) ;

					expect( batch ).to.equal( [
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

					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

} ) ;



describe( "Locks" , () => {

	beforeEach( clearDB ) ;

	it( "should lock a document (create, save, lock, retrieve, lock, retrieve)" , ( done ) => {

		var lockable = lockables.createDocument( {
			data: 'something'
		} ) ;

		var id = lockable._id ;
		var lockId ;

		async.series( [
			function( callback ) {
				lockable.$.save( callback ) ;
			} ,
			function( callback ) {
				lockables.get( id , ( error , lockable ) => {
					expect( error ).not.to.be.ok() ;
					expect( lockable ).to.equal( {
						_id: lockable._id , data: 'something' , _lockedBy: null , _lockedAt: null
					} ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				lockable.$.lock( ( error , lockId_ ) => {
					expect( error ).not.to.be.ok() ;
					expect( lockId_ ).to.be.an( mongodb.ObjectID ) ;
					lockId = lockId_ ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				lockables.get( id , ( error , lockable ) => {
					expect( error ).not.to.be.ok() ;
					//log.warning( 'lockable: %J' , lockable ) ;
					expect( lockable._lockedBy ).to.equal( lockId ) ;
					expect( lockable._lockedAt ).to.be.ok() ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				lockable.$.lock( ( error , lockId_ ) => {
					expect( error ).not.to.be.ok() ;
					expect( lockId_ ).not.to.be.ok() ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				lockables.get( id , ( error , lockable ) => {
					expect( error ).not.to.be.ok() ;
					//log.warning( 'lockable: %J' , lockable ) ;
					expect( lockable._lockedBy ).to.equal( lockId ) ;
					expect( lockable._lockedAt ).to.be.ok() ;
					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

	it( "should perform a 'lockRetrieveRelease': lock, retrieve locked document, then release locks" , ( done ) => {

		var lockId ;

		var docs = [
			lockables.createDocument( { data: 'one' } ) ,
			lockables.createDocument( { data: 'two' } ) ,
			lockables.createDocument( { data: 'three' } ) ,
			lockables.createDocument( { data: 'four' } ) ,
			lockables.createDocument( { data: 'five' } ) ,
			lockables.createDocument( { data: 'six' } )
		] ;

		var mapper = function( element ) {
			return element.data ;
		} ;

		async.series( [
			function( callback ) {
				rootsDb.bulk( 'save' , docs , callback ) ;
			} ,
			function( callback ) {
				lockables.lockRetrieveRelease( { data: { $in: [ 'one' , 'two' ] } } , ( error , batch ) => {
					expect( error ).not.to.be.ok() ;
					//console.log( batch ) ;
					expect( batch ).to.have.length( 2 ) ;
					var keys = batch.map( mapper ) ;
					expect( keys ).to.contain( 'one' ) ;
					expect( keys ).to.contain( 'two' ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				lockables.lockRetrieveRelease( { data: { $in: [ 'one' , 'two' , 'three' ] } } , ( error , batch ) => {
					expect( error ).not.to.be.ok() ;
					//console.log( batch ) ;
					expect( batch ).to.have.length( 1 ) ;
					expect( batch[ 0 ].data ).to.be( 'three' ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				lockables.lockRetrieveRelease( { data: { $in: [ 'one' , 'two' , 'three' ] } } , ( error , batch ) => {
					expect( error ).not.to.be.ok() ;
					//console.log( batch ) ;
					expect( batch ).to.have.length( 0 ) ;
					setTimeout( callback , 50 ) ;
				} ) ;
			} ,
			function( callback ) {
				lockables.lockRetrieveRelease( { data: { $in: [ 'one' , 'two' , 'three' ] } } , ( error , batch , releaseFn ) => {
					expect( error ).not.to.be.ok() ;
					//console.log( batch ) ;
					expect( batch ).to.have.length( 3 ) ;
					var keys = batch.map( mapper ) ;
					expect( keys ).to.contain( 'one' ) ;
					expect( keys ).to.contain( 'two' ) ;
					expect( keys ).to.contain( 'three' ) ;
					releaseFn().callback( callback ) ;
				} ) ;
			} ,
			function( callback ) {
				lockables.lockRetrieveRelease( { data: { $in: [ 'one' , 'two' , 'three' ] } } , ( error , batch , releaseFn ) => {
					expect( error ).not.to.be.ok() ;
					//console.log( batch ) ;
					expect( batch.length ).to.be( 3 ) ;
					var keys = batch.map( mapper ) ;
					expect( keys ).to.contain( 'one' ) ;
					expect( keys ).to.contain( 'two' ) ;
					expect( keys ).to.contain( 'three' ) ;
					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;
} ) ;



describe( "Extended DocumentWrapper" , () => {

	beforeEach( clearDB ) ;

	it( "should call a method of the extended Document wrapper at creation and after retrieving it from DB" , ( done ) => {

		var ext = extendables.createDocument( {
			data: 'sOmeDaTa'
		} ) ;

		expect( ext.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
		expect( ext.$ ).to.be.an( Extended ) ;

		expect( ext.$.getNormalized() ).to.be( 'somedata' ) ;

		var id = ext._id ;

		async.series( [
			function( callback ) {
				ext.$.save( callback ) ;
			} ,
			function( callback ) {
				extendables.get( id , ( error , ext ) => {
					expect( error ).not.to.be.ok() ;
					expect( ext.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( ext.$ ).to.be.an( Extended ) ;
					expect( ext ).to.equal( { _id: ext._id , data: 'sOmeDaTa' } ) ;
					expect( ext.$.getNormalized() ).to.be( 'somedata' ) ;
					ext.data = 'mOreVespEnEGaS' ;
					expect( ext.$.getNormalized() ).to.be( 'morevespenegas' ) ;
					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

	it( "should call a method of the extended Batch wrapper at creation and after retrieving it from DB" , ( done ) => {

		var ext1 = extendables.createDocument( { data: 'oNe' } ) ;
		var ext2 = extendables.createDocument( { data: 'twO' } ) ;
		var ext3 = extendables.createDocument( { data: 'THRee' } ) ;

		var id1 = ext1._id ;
		var id2 = ext2._id ;
		var id3 = ext3._id ;

		async.series( [
			function( callback ) { ext1.$.save( callback ) ; } ,
			function( callback ) { ext2.$.save( callback ) ; } ,
			function( callback ) { ext3.$.save( callback ) ; } ,
			function( callback ) {
				extendables.collect( {} , ( error , exts ) => {
					expect( error ).not.to.be.ok() ;
					expect( exts.$ ).to.be.an( rootsDb.BatchWrapper ) ;
					expect( exts.$ ).to.be.an( ExtendedBatch ) ;
					expect( exts.$.concat() ).to.be( 'oNetwOTHRee' ) ;
					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;
} ) ;



describe( "Memory model" , () => {

	beforeEach( clearDB ) ;

	it( "should create a memoryModel, retrieve documents with 'populate' on 'link' and 'back-link', with the 'memory' options and effectively save them in the memoryModel" , ( done ) => {

		var options ;

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
		user.$.setLink( 'job' , job ) ;
		user2.$.setLink( 'job' , job ) ;
		user3.$.setLink( 'job' , job2 ) ;

		async.series( [
			function( callback ) {
				job.$.save( callback ) ;
			} ,
			function( callback ) {
				job2.$.save( callback ) ;
			} ,
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				user2.$.save( callback ) ;
			} ,
			function( callback ) {
				user3.$.save( callback ) ;
			} ,
			function( callback ) {
				options = { memory: memory , populate: 'job' } ;

				users.collect( {} , options , ( error , users_ ) => {

					var doc ;

					expect( memory.collections ).to.have.keys( 'users' , 'jobs' ) ;

					expect( memory.collections.users.documents ).to.have.keys(
						user._id.toString() ,
						user2._id.toString() ,
						user3._id.toString()
					) ;

					expect( memory.collections.jobs.documents ).to.have.keys(
						job._id.toString() ,
						job2._id.toString()
					) ;

					doc = memory.collections.users.documents[ user._id.toString() ] ;
					expect( doc ).to.equal( {
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

					doc = memory.collections.users.documents[ user2._id.toString() ] ;
					expect( doc ).to.equal( {
						_id: user2._id ,
						firstName: 'Pat' ,
						lastName: 'Mulligan' ,
						memberSid: 'Pat Mulligan' ,
						job: {
							_id: job._id ,
							title: 'developer' ,
							salary: 60000 ,
							users: [] ,
							schools: []
						}
					} ) ;

					doc = memory.collections.users.documents[ user3._id.toString() ] ;
					expect( doc ).to.equal( {
						_id: user3._id ,
						firstName: 'Bill' ,
						lastName: 'Baroud' ,
						memberSid: 'Bill Baroud' ,
						job: {
							_id: job2._id ,
							title: 'adventurer' ,
							salary: 200000 ,
							users: [] ,
							schools: []
						}
					} ) ;

					doc = memory.collections.jobs.documents[ job._id.toString() ] ;
					expect( doc ).to.equal( {
						_id: job._id ,
						title: 'developer' ,
						salary: 60000 ,
						users: [] ,
						schools: []
					} ) ;

					doc = memory.collections.jobs.documents[ job2._id.toString() ] ;
					expect( doc ).to.equal( {
						_id: job2._id ,
						title: 'adventurer' ,
						salary: 200000 ,
						users: [] ,
						schools: []
					} ) ;

					//console.error( memory.collections.users.documents ) ;
					//console.error( memory.collections.jobs.documents ) ;

					expect( options.populateDepth ).to.be( 1 ) ;
					expect( options.populateDbQueries ).to.be( 1 ) ;

					callback() ;
				} ) ;
			} ,
			function( callback ) {
				options = { memory: memory , populate: 'users' } ;
				jobs.collect( {} , options , ( error , jobs_ ) => {

					var doc ;

					expect( memory.collections ).to.have.keys( 'users' , 'jobs' ) ;

					expect( memory.collections.users.documents ).to.have.keys(
						user._id.toString() ,
						user2._id.toString() ,
						user3._id.toString()
					) ;

					expect( memory.collections.jobs.documents ).to.have.keys(
						job._id.toString() ,
						job2._id.toString()
					) ;

					doc = memory.collections.users.documents[ user._id.toString() ] ;
					expect( doc ).to.equal( {
						_id: user._id ,
						firstName: 'Jilbert' ,
						lastName: 'Polson' ,
						memberSid: 'Jilbert Polson' ,
						job: memory.collections.jobs.documents[ job._id.toString() ]
					} ) ;

					doc = memory.collections.users.documents[ user2._id.toString() ] ;
					expect( doc ).to.equal( {
						_id: user2._id ,
						firstName: 'Pat' ,
						lastName: 'Mulligan' ,
						memberSid: 'Pat Mulligan' ,
						job: memory.collections.jobs.documents[ job._id.toString() ]
					} ) ;

					doc = memory.collections.users.documents[ user3._id.toString() ] ;
					expect( doc ).to.equal( {
						_id: user3._id ,
						firstName: 'Bill' ,
						lastName: 'Baroud' ,
						memberSid: 'Bill Baroud' ,
						job: memory.collections.jobs.documents[ job2._id.toString() ]
					} ) ;

					doc = memory.collections.jobs.documents[ job._id.toString() ] ;
					if ( doc.users[ 0 ].firstName === 'Pat' ) { doc.users = [ doc.users[ 1 ] , doc.users[ 0 ] ] ; }
					expect( doc ).to.equal( {
						_id: job._id ,
						title: 'developer' ,
						salary: 60000 ,
						schools: [] ,
						users: [
							memory.collections.users.documents[ user._id.toString() ] ,
							memory.collections.users.documents[ user2._id.toString() ]
						]
					} ) ;

					doc = memory.collections.jobs.documents[ job2._id.toString() ] ;
					expect( doc ).to.equal( {
						_id: job2._id ,
						title: 'adventurer' ,
						salary: 200000 ,
						schools: [] ,
						users: [
							memory.collections.users.documents[ user3._id.toString() ]
						]
					} ) ;

					//console.error( memory.collections.users.documents ) ;
					//console.error( memory.collections.jobs.documents ) ;

					// This is a back-link, so a DB query is mandatory here
					expect( options.populateDepth ).to.be( 1 ) ;
					expect( options.populateDbQueries ).to.be( 1 ) ;

					callback() ;
				} ) ;
			} ,
			function( callback ) {
				options = { memory: memory , populate: 'job' } ;

				users.collect( {} , options , ( error , users_ ) => {
					// This is the same query already performed on user.
					// We just check populate Depth and Queries here: a total cache hit should happen!
					expect( options.populateDepth ).not.to.be.ok() ;
					expect( options.populateDbQueries ).not.to.be.ok() ;

					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

	it( "should also works with multi-link" , ( done ) => {

		var options ;

		var memory = world.createMemoryModel() ;

		var school1 = schools.createDocument( {
			title: 'Computer Science'
		} ) ;

		var school1Id = school1._id ;

		var school2 = schools.createDocument( {
			title: 'Web Academy'
		} ) ;

		var school2Id = school2._id ;

		var job1 = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;

		var job1Id = job1.$.id ;

		var job2 = jobs.createDocument( {
			title: 'sysadmin' ,
			salary: 55000
		} ) ;

		var job2Id = job2.$.id ;

		var job3 = jobs.createDocument( {
			title: 'front-end developer' ,
			salary: 54000
		} ) ;

		var job4 = jobs.createDocument( {
			title: 'designer' ,
			salary: 56000
		} ) ;

		var job4Id = job4.$.id ;

		// Link the documents!
		school1.$.setLink( 'jobs' , [ job1 , job2 , job3 ] ) ;
		school2.$.setLink( 'jobs' , [ job1 , job3 , job4 ] ) ;

		async.series( [
			function( callback ) {
				job1.$.save( callback ) ;
			} ,
			function( callback ) {
				job2.$.save( callback ) ;
			} ,
			function( callback ) {
				job3.$.save( callback ) ;
			} ,
			function( callback ) {
				job4.$.save( callback ) ;
			} ,
			function( callback ) {
				school1.$.save( callback ) ;
			} ,
			function( callback ) {
				school2.$.save( callback ) ;
			} ,
			function( callback ) {
				options = { populate: 'jobs' , memory: memory } ;

				schools.collect( {} , options , ( error , schools_ ) => {

					var doc ;

					expect( error ).not.to.be.ok() ;
					expect( memory.collections ).to.have.keys( 'schools' , 'jobs' ) ;

					expect( memory.collections.schools.documents ).to.have.keys(
						school1._id.toString() ,
						school2._id.toString()
					) ;

					expect( memory.collections.jobs.documents ).to.have.keys(
						job1._id.toString() ,
						job2._id.toString() ,
						job3._id.toString() ,
						job4._id.toString()
					) ;

					doc = memory.collections.schools.documents[ school1._id.toString() ] ;
					expect( doc ).to.equal( {
						_id: school1._id ,
						title: 'Computer Science' ,
						jobs: [
							{
								_id: job1._id ,
								title: 'developer' ,
								salary: 60000 ,
								users: [] ,
								schools: []
							} ,
							{
								_id: job2._id ,
								title: 'sysadmin' ,
								salary: 55000 ,
								users: [] ,
								schools: []
							} ,
							{
								_id: job3._id ,
								title: 'front-end developer' ,
								salary: 54000 ,
								users: [] ,
								schools: []
							}
						]
					} ) ;

					doc = memory.collections.schools.documents[ school2._id.toString() ] ;
					expect( doc ).to.equal( {
						_id: school2._id ,
						title: 'Web Academy' ,
						jobs: [
							{
								_id: job1._id ,
								title: 'developer' ,
								salary: 60000 ,
								users: [] ,
								schools: []
							} ,
							{
								_id: job3._id ,
								title: 'front-end developer' ,
								salary: 54000 ,
								users: [] ,
								schools: []
							} ,
							{
								_id: job4._id ,
								title: 'designer' ,
								salary: 56000 ,
								users: [] ,
								schools: []
							}
						]
					} ) ;

					doc = memory.collections.jobs.documents[ job1._id.toString() ] ;
					expect( doc ).to.equal( {
						_id: job1._id ,
						title: 'developer' ,
						salary: 60000 ,
						users: [] ,
						schools: []
					} ) ;

					doc = memory.collections.jobs.documents[ job2._id.toString() ] ;
					expect( doc ).to.equal( {
						_id: job2._id ,
						title: 'sysadmin' ,
						salary: 55000 ,
						users: [] ,
						schools: []
					} ) ;

					doc = memory.collections.jobs.documents[ job3._id.toString() ] ;
					expect( doc ).to.equal( {
						_id: job3._id ,
						title: 'front-end developer' ,
						salary: 54000 ,
						users: [] ,
						schools: []
					} ) ;

					doc = memory.collections.jobs.documents[ job4._id.toString() ] ;
					expect( doc ).to.equal( {
						_id: job4._id ,
						title: 'designer' ,
						salary: 56000 ,
						users: [] ,
						schools: []
					} ) ;

					expect( options.populateDepth ).to.be( 1 ) ;
					expect( options.populateDbQueries ).to.be( 1 ) ;

					callback() ;
				} ) ;
			} ,
			function( callback ) {
				options = { populate: 'jobs' , memory: memory } ;

				schools.collect( {} , options , ( error , schools_ ) => {

					// This is the same query already performed.
					// We just check populate Depth and Queries here: a total cache hit should happen!
					expect( options.populateDepth ).not.to.be.ok() ;
					expect( options.populateDbQueries ).not.to.be.ok() ;

					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

	it( "incremental population should work as expected" , ( done ) => {

		var options ;

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
		user.$.setLink( 'job' , job ) ;
		user2.$.setLink( 'job' , job ) ;

		async.series( [
			function( callback ) {
				job.$.save( callback ) ;
			} ,
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				user2.$.save( callback ) ;
			} ,
			function( callback ) {
				options = { memory: memory } ;
				users.get( user._id , options , ( error , user_ ) => {
					expect( error ).not.to.be.ok() ;
					expect( user_ ).to.equal( {
						_id: user._id ,
						firstName: 'Jilbert' ,
						lastName: 'Polson' ,
						memberSid: 'Jilbert Polson' ,
						job: job._id
					} ) ;
					expect( options.populateDepth ).not.to.be.ok() ;
					expect( options.populateDbQueries ).not.to.be.ok() ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				options = { memory: memory , populate: 'job' } ;
				users.get( user._id , options , ( error , user_ ) => {
					expect( error ).not.to.be.ok() ;
					expect( user_ ).to.equal( {
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
					expect( user_.job.$.populated.users ).not.to.be.ok() ;
					expect( memory.collections.jobs.documents[ job._id.toString() ] ).to.equal( {
						_id: job._id ,
						title: 'developer' ,
						salary: 60000 ,
						users: [] ,
						schools: []
					} ) ;
					expect( options.populateDepth ).to.be( 1 ) ;
					expect( options.populateDbQueries ).to.be( 1 ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				//console.error( '\n\n>>>>>>> Increment now!!!\n\n' ) ;
				//log.warning( 'memory users: %I' , memory.collections.users ) ;
				//log.warning( 'memory jobs: %I' , memory.collections.jobs ) ;

				options = { memory: memory , deepPopulate: deepPopulate } ;
				users.get( user._id , options , ( error , user_ ) => {
					expect( error ).not.to.be.ok() ;
					expect( user_.$.populated.job ).to.be( true ) ;

					expect( user_.job.users ).to.have.length( 2 ) ;

					if ( user_.job.users[ 0 ].firstName === 'Robert' ) {
						user_.job.users = [ user_.job.users[ 1 ] , user_.job.users[ 0 ] ] ;
					}

					expect( user_.job.users[ 0 ].job ).to.be( user_.job ) ;
					expect( user_.job.users[ 1 ].job ).to.be( user_.job ) ;

					expect( user_ ).to.equal( {
						_id: user._id ,
						firstName: 'Jilbert' ,
						lastName: 'Polson' ,
						memberSid: 'Jilbert Polson' ,
						job: {
							_id: job._id ,
							title: 'developer' ,
							salary: 60000 ,
							schools: [] ,
							users: [
								user_ ,
								// We cannot use 'user2', expect.js is too confused with Circular references
								// We have to perform a second check for that
								user_.job.users[ 1 ]
							]
						}
					} ) ;

					expect( user_.job.users[ 1 ] ).to.equal( {
						_id: user2._id ,
						firstName: 'Robert' ,
						lastName: 'Polson' ,
						memberSid: 'Robert Polson' ,
						job: {
							_id: job._id ,
							title: 'developer' ,
							salary: 60000 ,
							schools: [] ,
							users: [
								user_ ,
								user_.job.users[ 1 ]
							]
						}
					} ) ;

					expect( options.populateDepth ).to.be( 1 ) ;
					expect( options.populateDbQueries ).to.be( 1 ) ;

					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

	it( "should also works with back-multi-link" ) ;
} ) ;



describe( "Hooks" , () => {
	it( "'beforeCreateDocument'" ) ;
	it( "'afterCreateDocument'" ) ;
} ) ;



describe( "Historical bugs" , () => {

	beforeEach( clearDB ) ;

	it( "collect on empty collection with populate (was throwing uncaught error)" , ( done ) => {

		async.series( [
			function( callback ) {
				users.collect( {} , { populate: [ 'job' , 'godfather' ] } , ( error , batch ) => {
					expect( error ).not.to.be.ok() ;
					expect( batch ).to.equal( [] ) ;
					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

	it( "validation featuring sanitizers should update both locally and remotely after a document's commit()" , ( done ) => {

		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: "60000"
		} ) ;

		var jobId = job.$.id ;

		var town = towns.createDocument( {
			name: 'Paris' ,
			meta: {
				rank: "7" ,
				population: '2200K' ,
				country: 'France'
			}
		} ) ;

		var townId = town.$.id ;

		expect( job.salary ).to.be( 60000 ) ;	// toInteger at document's creation
		expect( town.meta.rank ).to.be( 7 ) ;	// toInteger at document's creation

		async.series( [
			function( callback ) {
				job.$.save( callback ) ;
			} ,
			function( callback ) {
				town.$.save( callback ) ;
			} ,
			function( callback ) {
				jobs.get( jobId , ( error , job_ ) => {
					job = job_ ;
					expect( error ).not.to.be.ok() ;
					expect( job ).to.equal( {
						_id: job._id , title: 'developer' , salary: 60000 , users: [] , schools: []
					} ) ;
					expect( job.salary ).to.be( 60000 ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				job.$.patch( { salary: "65000" } ) ;
				job.$.commit( ( error ) => {
					expect( error ).not.to.be.ok() ;
					expect( job ).to.equal( {
						_id: job._id , title: 'developer' , salary: 65000 , users: [] , schools: []
					} ) ;
					expect( job.salary ).to.be( 65000 ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				expect( job ).to.equal( {
					_id: job._id , title: 'developer' , salary: 65000 , users: [] , schools: []
				} ) ;
				jobs.get( jobId , ( error , job_ ) => {
					job = job_ ;
					expect( error ).not.to.be.ok() ;
					expect( job ).to.equal( {
						_id: job._id , title: 'developer' , salary: 65000 , users: [] , schools: []
					} ) ;
					expect( job.salary ).to.be( 65000 ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				towns.get( town._id , ( error , town_ ) => {
					town = town_ ;
					expect( error ).not.to.be.ok() ;
					expect( town ).to.equal( { _id: town._id , name: 'Paris' , meta: { rank: 7 , population: '2200K' , country: 'France' } } ) ;
					expect( town.meta.rank ).to.be( 7 ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				town.$.patch( { "meta.rank": "8" } ) ;
				town.$.commit( ( error ) => {
					expect( error ).not.to.be.ok() ;
					expect( town ).to.equal( { _id: town._id , name: 'Paris' , meta: { rank: 8 , population: '2200K' , country: 'France' } } ) ;
					expect( town.meta.rank ).to.be( 8 ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				towns.get( town._id , ( error , town_ ) => {
					town = town_ ;
					expect( error ).not.to.be.ok() ;
					expect( town ).to.equal( { _id: town._id , name: 'Paris' , meta: { rank: 8 , population: '2200K' , country: 'France' } } ) ;
					expect( town.meta.rank ).to.be( 8 ) ;
					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;

	it( "setting garbage to an attachment property should abort with an error" , ( done ) => {

		var user , id ;

		// First try: at object creation
		expect( () => {
			user = users.createDocument( {
				firstName: 'Jilbert' ,
				lastName: 'Polson' ,
				file: 'garbage'
			} ) ;
		} ).to.throwError() ;

		user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		id = user._id ;

		// Second try: using setLink
		expect( () => { user.$.setLink( 'file' , 'garbage' ) ; } ).to.throwError() ;
		expect( user.file ).to.be( undefined ) ;

		// third try: by setting the property directly
		user.file = 'garbage' ;
		expect( () => { user.$.validate() ; } ).to.throwError() ;

		// By default, a collection has the 'patchDrivenValidation' option, so we have to stage the change
		// to trigger validation on .save()
		user.$.stage( 'file' ) ;

		async.series( [
			function( callback ) {
				user.$.save( ( error ) => {
					expect( error ).to.be.ok() ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				users.get( id , ( error , user_ ) => {
					user = user_ ;
					expect( error ).to.be.ok() ;
					callback() ;
				} ) ;
			}
		] )
			.exec( done ) ;
	} ) ;
} ) ;
