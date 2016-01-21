/*
	The Cedric's Swiss Knife (CSK) - CSK Object-Document Mapping test suite

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

/* jshint unused:false */
/* global describe, it, before, after, beforeEach */



var rootsDb = require( '../lib/rootsDb.js' ) ;
var util = require( 'util' ) ;
var mongodb = require( 'mongodb' ) ;
var fs = require( 'fs' ) ;

var hash = require( 'hash-kit' ) ;
var string = require( 'string-kit' ) ;
var tree = require( 'tree-kit' ) ;
var async = require( 'async-kit' ) ;
var ErrorStatus = require( 'error-status' ) ;
var doormen = require( 'doormen' ) ;

var expect = require( 'expect.js' ) ;

var cliOptions = getCliOptions() ;
if ( ! cliOptions.log ) { cliOptions.log = { minLevel: 4 } ; }

var logfella = require( 'logfella' ) ;
logfella.global.setGlobalConfig( cliOptions.log ) ;
var log = logfella.global.use( 'mocha' ) ;



// Create the world...
var world = rootsDb.World.create() ;

// Collections...
var users , jobs , schools , towns , lockables , extendables ;

var usersDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/users' ,
	attachmentUrl: __dirname + '/tmp/' ,
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
		godfather: { type: 'link' , optional: true , collection: 'users' } ,
		file: { type: 'attachment' , optional: true } ,
		connection: {
			type: 'strictObject' ,
			optional: true ,
			of: { type: 'link' , collection: 'users' }
		} ,
		job: { type: 'link' , optional: true , collection: 'jobs' } ,
		memberSid: {
			optional: true ,
			type: 'string' ,
			maxLength: 30
		}
	} ,
	indexes: [
		{ properties: { job: 1 } } ,
		{ properties: { job: 1 , memberSid: 1 } , unique: true }
	] ,
	hooks: {
		afterCreateDocument: //[
			function( data ) {
				//console.log( "- Users afterCreateDocument 'after' hook -" ) ;
				data.memberSid = '' + data.firstName + ' ' + data.lastName ;
			}
		//]
	}
} ;

var expectedDefaultUser = { firstName: 'Joe', lastName: 'Doe' , memberSid: 'Joe Doe' } ;

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
			default: 0
		} ,
		users: { type: 'backLink' , collection: 'users' , path: 'job' } ,
		schools: { type: 'backLink' , collection: 'schools' , path: 'jobs' }
	} ,
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
		} ,
	} ,
} ;

var townsDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb/towns' ,
	properties: {
		name: { type: 'string' } ,
		meta: {
			type: 'strictObject',
			default: {}
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



function Extended() { throw new Error( 'Use Extended.create() instead' ) ; }
Extended.prototype = Object.create( rootsDb.DocumentWrapper.prototype ) ;
Extended.prototype.constructor = Extended ;

Extended.create = function extendedCreate( collection , rawDoc , options )
{
	var o = Object.create( Extended.prototype ) ;
	o.create( collection , rawDoc , options ) ;
	return o ;
} ;

Extended.prototype.create = function create( collection , rawDoc , options )
{
	rootsDb.DocumentWrapper.prototype.create.call( this , collection , rawDoc , options ) ;
} ;

Extended.prototype.getNormalized = function getNormalized()
{
	return this.document.data.toLowerCase() ;
} ;

function ExtendedBatch() { throw new Error( 'Use ExtendedBatch.create() instead' ) ; }
ExtendedBatch.prototype = Object.create( rootsDb.BatchWrapper.prototype ) ;
ExtendedBatch.prototype.constructor = ExtendedBatch ;

ExtendedBatch.create = function extendedCreate( collection , rawDoc , options )
{
	var o = Object.create( ExtendedBatch.prototype ) ;
	o.create( collection , rawDoc , options ) ;
	return o ;
} ;

ExtendedBatch.prototype.create = function create( collection , rawDoc , options )
{
	rootsDb.BatchWrapper.prototype.create.call( this , collection , rawDoc , options ) ;
} ;

ExtendedBatch.prototype.concat = function concat()
{
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



// Return options while trying to avoid mocha's parameters
function getCliOptions()
{
	var i , max = 0 ;
	
	for ( i = 2 ; i < process.argv.length ; i ++ )
	{
		if ( process.argv[ i ].match( /\*|.+\.js/ ) )
		{
			max = i ;
		}
	}
	
	return require( 'minimist' )( process.argv.slice( max + 1 ) ) ;
}



// clear DB: remove every item, so we can safely test
function clearDB( callback )
{
	async.parallel( [
		[ clearCollection , users ] ,
		[ clearCollection , jobs ] ,
		[ clearCollection , schools ] ,
		[ clearCollection , towns ] ,
		[ clearCollection , lockables ] ,
		[ clearCollection , extendables ]
	] )
	.exec( callback ) ;
}



// clear DB: remove every item, so we can safely test
function clearDBIndexes( callback )
{
	async.parallel( [
		[ clearCollectionIndexes , users ] ,
		[ clearCollectionIndexes , jobs ] ,
		[ clearCollectionIndexes , schools ] ,
		[ clearCollectionIndexes , towns ] ,
		[ clearCollectionIndexes , lockables ] ,
		[ clearCollectionIndexes , extendables ]
	] )
	.exec( callback ) ;
}



function clearCollection( collection , callback )
{
	collection.driver.rawInit( function( error ) {
		if ( error ) { callback( error ) ; return ; }
		collection.driver.raw.remove( callback ) ;
	} ) ;
}



function clearCollectionIndexes( collection , callback )
{
	collection.driver.rawInit( function( error ) {
		if ( error ) { callback( error ) ; return ; }
		collection.driver.raw.dropIndexes( function() {
			callback() ;
		} ) ;
	} ) ;
}





			/* Tests */



// Force creating the collection
before( function( done ) {
	
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
	
	extendables = world.createCollection( 'extendables' , extendablesDescriptor ) ;
	expect( extendables ).to.be.a( rootsDb.Collection ) ;
	
	done() ;
} ) ;



describe( "Build collections' indexes" , function() {
	
	beforeEach( clearDBIndexes ) ;
	
	it( "should build indexes" , function( done ) {
		
		expect( users.uniques ).to.be.eql( [ [ '_id' ], [ 'job', 'memberSid' ] ] ) ;
		expect( jobs.uniques ).to.be.eql( [ [ '_id' ] ] ) ;
		
		async.foreach( world.collections , function( collection , name , foreachCallback )
		{
			collection.buildIndexes( function( error ) {
				
				expect( error ).not.to.be.ok() ;
				
				collection.driver.getIndexes( function( error , indexes ) {
					expect( indexes ).to.be.eql( collection.indexes ) ;
					foreachCallback() ;
				} ) ;
			} ) ;
		} )
		.exec( done ) ;
	} ) ;
} ) ;



describe( "ID" , function() {
	
	it( "should create ID (like Mongo ID)" , function() {
		
		expect( users.createId() ).to.match( /^[0-9a-f]{24}$/ ) ;
		expect( users.createId() ).to.match( /^[0-9a-f]{24}$/ ) ;
		expect( users.createId() ).to.match( /^[0-9a-f]{24}$/ ) ;
		expect( users.createId() ).to.match( /^[0-9a-f]{24}$/ ) ;
		expect( users.createId() ).to.match( /^[0-9a-f]{24}$/ ) ;
	} ) ;
	
	it( "$id in document" ) ;
	it( "$id in fingerprint" ) ;
	it( "$id in criteria (queryObject)" ) ;
} ) ;



describe( "Document creation" , function() {
	
	it( "should create a document with default values" , function() {
		
		var user = users.createDocument() ;
		
		expect( user.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
		expect( user._id ).to.be.an( mongodb.ObjectID ) ;
		expect( user ).to.eql( tree.extend( null , { _id: user._id } , expectedDefaultUser ) ) ;
	} ) ;
	
	it( "should create a document using the given correct values" , function() {
		
		var user = users.createDocument( {
			firstName: 'Bobby',
			lastName: 'Fischer'
		} ) ;
		
		expect( user.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
		expect( user._id ).to.be.an( mongodb.ObjectID ) ;
		expect( user ).to.eql( {
			_id: user._id ,
			firstName: 'Bobby' ,
			lastName: 'Fischer' ,
			memberSid: 'Bobby Fischer'
		} ) ;
	} ) ;
	
	it( "should throw when trying to create a document that does not validate the schema" , function() {
		
		var user ;
		
		doormen.shouldThrow( function() {
			user = users.createDocument( {
				firstName: true,
				lastName: 3
			} ) ;
		} ) ;
		
		doormen.shouldThrow( function() {
			user = users.createDocument( {
				firstName: 'Bobby',
				lastName: 'Fischer',
				extra: 'property'
			} ) ;
		} ) ;
	} ) ;
} ) ;



describe( "Get documents" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "should get a document (create, save and retrieve)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'John' ,
			lastName: 'McGregor'
		} ) ;
		
		var id = user._id ;
		
		async.series( [
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ; 
					expect( error ).not.to.be.ok() ;
					expect( user.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
					expect( user ).to.eql( { _id: user._id , firstName: 'John' , lastName: 'McGregor' , memberSid: 'John McGregor' } ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				users.get( id , { raw: true } , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ;
					expect( error ).not.to.be.ok() ;
					expect( user.$ ).not.to.be.an( rootsDb.DocumentWrapper ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
					expect( user ).to.eql( { _id: user._id , firstName: 'John' , lastName: 'McGregor' , memberSid: 'John McGregor' } ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "when trying to get an unexistant document, an ErrorStatus (type: notFound) should be issued" , function( done ) {
		
		// Unexistant ID
		var id = new mongodb.ObjectID() ;
		
		async.parallel( [
			function( callback ) {
				users.get( id , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ; 
					expect( error ).to.be.an( ErrorStatus ) ;
					expect( error.type ).to.equal( 'notFound' ) ;
					expect( user ).to.be( undefined ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				users.get( id , { raw: true } , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ; 
					expect( error ).to.be.an( ErrorStatus ) ;
					expect( error.type ).to.equal( 'notFound' ) ;
					expect( user ).to.be( undefined ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
} ) ;



describe( "Save documents" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "should save correctly and only non-default value are registered into the upstream (create, save and retrieve)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Jack'
		} ) ;
		
		var id = user._id ;
		
		async.series( [
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ; 
					expect( error ).not.to.be.ok() ;
					expect( user ).to.eql( { _id: user._id , firstName: 'Jack' , lastName: 'Doe' , memberSid: 'Jack Doe' } ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "should save a full document so parallel save *DO* overwrite each others (create, save, retrieve, full update² and retrieve)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Johnny B.' ,
			lastName: 'Starks'
		} ) ;
		
		var id = user._id ;
		var user2 ;
		
		
		async.series( [
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , u ) {
					user2 = u ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user2 ) ; 
					expect( error ).not.to.be.ok() ;
					expect( user2._id ).to.eql( id ) ;
					expect( user2 ).to.eql( { _id: user2._id , firstName: 'Johnny B.' , lastName: 'Starks' , memberSid: 'Johnny B. Starks' } ) ;
					callback() ;
				} ) ;
			} ,
			async.parallel( [
				function( callback ) {
					user.lastName = 'Smith' ;
					user.$.save( callback ) ;
				} ,
				function( callback ) {
					user2.firstName = 'Joey' ;
					user2.$.save( callback ) ;
				}
			] ) ,
			function( callback ) {
				users.get( id , function( error , u ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ; 
					expect( error ).not.to.be.ok() ;
					expect( u._id ).to.eql( id ) ;
					expect( u ).to.eql( { _id: u._id , firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny B. Starks' } ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
} ) ;
	


describe( "Patch, stage and commit documents" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "'commit' should save staged data and do nothing on data not staged" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Johnny' ,
			lastName: 'Starks'
		} ) ;
		
		var id = user._id ;
		var user2 ;
		//id = users.createDocument()._id ;
		
		
		async.series( [
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , u ) {
					user2 = u ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user2 ) ; 
					expect( error ).not.to.be.ok() ;
					expect( user2._id ).to.eql( id ) ;
					expect( user2 ).to.eql( { _id: user._id , firstName: 'Johnny' , lastName: 'Starks' , memberSid: 'Johnny Starks' } ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				user2.firstName = 'Joey' ;
				user2.lastName = 'Smith' ;
				user2.$.stage( 'lastName' ) ;
				expect( user2 ).to.eql( { _id: user2._id , firstName: 'Joey' , lastName: 'Smith' , memberSid: 'Johnny Starks' } ) ;
				user2.$.commit( callback ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , u ) {
					expect( error ).not.to.be.ok() ;
					expect( u._id ).to.eql( id ) ;
					expect( u ).to.eql( { _id: u._id , firstName: 'Johnny' , lastName: 'Smith' , memberSid: 'Johnny Starks' } ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "'commit' should save data staged using .patch() and do nothing on data modified by .patch()" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Johnny' ,
			lastName: 'Starks'
		} ) ;
		
		var id = user._id ;
		var user2 ;
		//id = users.createDocument()._id ;
		
		
		async.series( [
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , u ) {
					user2 = u ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user2 ) ; 
					expect( error ).not.to.be.ok() ;
					expect( user2._id ).to.eql( id ) ;
					expect( user2 ).to.eql( { _id: user._id , firstName: 'Johnny' , lastName: 'Starks' , memberSid: 'Johnny Starks' } ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				user2.firstName = 'Joey' ;
				user2.$.patch( { lastName: 'Smith' } ) ;
				expect( user2 ).to.eql( { _id: user2._id , firstName: 'Joey' , lastName: 'Smith' , memberSid: 'Johnny Starks' } ) ;
				user2.$.commit( callback ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , u ) {
					expect( error ).not.to.be.ok() ;
					expect( u._id ).to.eql( id ) ;
					expect( u ).to.eql( { _id: u._id , firstName: 'Johnny' , lastName: 'Smith' , memberSid: 'Johnny Starks' } ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "should save creating a minimalistic patch so parallel save do not overwrite each others (create, save, retrieve, patch², commit² and retrieve)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Johnny' ,
			lastName: 'Starks'
		} ) ;
		
		var id = user._id ;
		var user2 ;
		//id = users.createDocument()._id ;
		
		
		async.series( [
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , u ) {
					user2 = u ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user2 ) ; 
					expect( error ).not.to.be.ok() ;
					expect( user2._id ).to.eql( id ) ;
					expect( user2 ).to.eql( { _id: user._id , firstName: 'Johnny' , lastName: 'Starks' , memberSid: 'Johnny Starks' } ) ;
					callback() ;
				} ) ;
			} ,
			async.parallel( [
				function( callback ) {
					user.$.patch( { lastName: 'Smith' } ) ;
					expect( user.lastName ).to.be( 'Smith' ) ;
					user.$.commit( callback ) ;
				} ,
				function( callback ) {
					user2.$.patch( { firstName: 'Joey' } ) ;
					expect( user2.firstName ).to.be( 'Joey' ) ;
					user2.$.commit( callback ) ;
				}
			] ) ,
			function( callback ) {
				users.get( id , function( error , u ) {
					expect( error ).not.to.be.ok() ;
					expect( u._id ).to.eql( id ) ;
					expect( u ).to.eql( { _id: u._id , firstName: 'Joey' , lastName: 'Smith' , memberSid: 'Johnny Starks' } ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "overwrite and depth mixing" ) ;
} ) ;



describe( "Delete documents" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "should delete a document (create, save, retrieve, then delete it so it cannot be retrieved again)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'John' ,
			lastName: 'McGregor'
		} ) ;
		
		//console.log( user ) ;
		var id = user._id ;
		
		async.series( [
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , u ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ; 
					expect( error ).not.to.be.ok() ;
					expect( u._id ).to.eql( id ) ;
					expect( u ).to.eql( { _id: user._id , firstName: 'John' , lastName: 'McGregor' , memberSid: "John McGregor" } ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				user.$.delete( function( error ) {
					expect( error ).not.to.be.ok() ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ; 
					expect( error ).to.be.an( ErrorStatus ) ;
					expect( error.type ).to.equal( 'notFound' ) ;
					expect( user ).to.be( undefined ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
} ) ;



describe( "Fingerprint" , function() {
	
	it( "should create a fingerprint" , function() {
		
		var f = users.createFingerprint( { firstName: 'Terry' } ) ;
		
		expect( f.$ ).to.be.an( rootsDb.FingerprintWrapper ) ;
		expect( f ).to.eql( { firstName: 'Terry' } ) ;
	} ) ;
	
	it( "should detect uniqueness correctly" , function() {
		
		expect( users.createFingerprint( { _id: '123456789012345678901234' } ).$.unique ).to.be( true ) ;
		expect( users.createFingerprint( { firstName: 'Terry' } ).$.unique ).to.be( false ) ;
		expect( users.createFingerprint( { firstName: 'Terry', lastName: 'Bogard' } ).$.unique ).to.be( false ) ;
		expect( users.createFingerprint( { _id: '123456789012345678901234', firstName: 'Terry', lastName: 'Bogard' } ).$.unique ).to.be( true ) ;
		expect( users.createFingerprint( { job: '123456789012345678901234' } ).$.unique ).to.be( false ) ;
		expect( users.createFingerprint( { memberSid: 'terry-bogard' } ).$.unique ).to.be( false ) ;
		expect( users.createFingerprint( { job: '123456789012345678901234', memberSid: 'terry-bogard' } ).$.unique ).to.be( true ) ;
	} ) ;
} ) ;



describe( "Get documents by unique fingerprint" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "should get a document (create, save and retrieve)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Bill' ,
			lastName: "Cut'throat"
		} ) ;
		
		var id = user._id ;
		var memberSid = user.memberSid ;
		
		var job = jobs.createDocument() ;
		user.job = job._id ;
		
		async.series( [
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				job.$.save( callback ) ;
			} ,
			function( callback ) {
				users.getUnique( { memberSid: memberSid , job: job._id } , function( error , u ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ;
					expect( error ).not.to.be.ok() ;
					expect( u.$ ).to.be.a( rootsDb.DocumentWrapper ) ;
					expect( u._id ).to.be.an( mongodb.ObjectID ) ;
					expect( u._id ).to.eql( id ) ;
					expect( u ).to.eql( tree.extend( null , { _id: user._id , job: job._id , firstName: 'Bill' , lastName: "Cut'throat" , memberSid: "Bill Cut'throat" } ) ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "when trying to get a document with a non-unique fingerprint, an ErrorStatus (type: badRequest) should be issued" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Bill' ,
			lastName: "Tannen"
		} ) ;
		
		async.series( [
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				users.getUnique( { firstName: 'Bill' , lastName: "Tannen" } , { raw: true } , function( error ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ;
					expect( error ).to.be.an( Error ) ;
					expect( error.type ).to.be( 'badRequest' ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				users.getUnique( { firstName: 'Bill' , lastName: "Tannen" } , function( error ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ; 
					expect( error ).to.be.an( Error ) ;
					expect( error.type ).to.be( 'badRequest' ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
} ) ;



describe( "MultiGet, Collect & find batchs" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "should get multiple document using an array of IDs (create, save and multiGet)" , function( done ) {
		
		var marleys = [
			users.createDocument( {
				firstName: 'Bob' ,
				lastName: 'Marley'
			} ) ,
			users.createDocument( {
				firstName: 'Julian' ,
				lastName: 'Marley'
			} ) ,
			users.createDocument( {
				firstName: 'Thomas' ,
				lastName: 'Jefferson'
			} ) ,
			users.createDocument( {
				firstName: 'Stephen' ,
				lastName: 'Marley'
			} ) ,
			users.createDocument( {
				firstName: 'Mr' ,
				lastName: 'X'
			} ) ,
			users.createDocument( {
				firstName: 'Ziggy' ,
				lastName: 'Marley'
			} ) ,
			users.createDocument( {
				firstName: 'Rita' ,
				lastName: 'Marley'
			} )
		] ;
		
		async.series( [
			function( callback ) {
				rootsDb.bulk( 'save' , marleys , callback ) ;
			} ,
			function( callback ) {
				var ids = [
					marleys[ 0 ]._id ,
					marleys[ 1 ]._id ,
					marleys[ 3 ]._id ,
					marleys[ 5 ]._id ,
					marleys[ 6 ]._id
				] ;
				
				users.multiGet( ids , function( error , batch ) {
					var i , map = {} ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'Batch:' , batch ) ; 
					expect( error ).not.to.be.ok() ;
					expect( batch.$ ).to.be.a( rootsDb.BatchWrapper ) ;
					expect( batch ).to.have.length( 5 ) ;
					
					for ( i = 0 ; i < batch.length ; i ++ )
					{
						//expect( batch[ i ] ).to.be.an( rootsDb.DocumentWrapper ) ;
						expect( batch[ i ].firstName ).to.be.ok() ;
						expect( batch[ i ].lastName ).to.equal( 'Marley' ) ;
						map[ batch[ i ].firstName ] = true ;
					}
					
					expect( map ).to.only.have.keys( 'Bob' , 'Julian' , 'Stephen' , 'Ziggy' , 'Rita' ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "should collect a batch using a (non-unique) fingerprint (create, save and collect batch)" , function( done ) {
		
		var marleys = [
			users.createDocument( {
				firstName: 'Bob' ,
				lastName: 'Marley'
			} ) ,
			users.createDocument( {
				firstName: 'Julian' ,
				lastName: 'Marley'
			} ) ,
			users.createDocument( {
				firstName: 'Thomas' ,
				lastName: 'Jefferson'
			} ) ,
			users.createDocument( {
				firstName: 'Stephen' ,
				lastName: 'Marley'
			} ) ,
			users.createDocument( {
				firstName: 'Mr' ,
				lastName: 'X'
			} ) ,
			users.createDocument( {
				firstName: 'Ziggy' ,
				lastName: 'Marley'
			} ) ,
			users.createDocument( {
				firstName: 'Rita' ,
				lastName: 'Marley'
			} )
		] ;
		
		async.series( [
			function( callback ) {
				rootsDb.bulk( 'save' , marleys , callback ) ;
			} ,
			function( callback ) {
				users.collect( { lastName: 'Marley' } , function( error , batch ) {
					var i , map = {} ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'Batch:' , batch ) ; 
					expect( error ).not.to.be.ok() ;
					expect( batch.$ ).to.be.a( rootsDb.BatchWrapper ) ;
					expect( batch ).to.have.length( 5 ) ;
					
					for ( i = 0 ; i < batch.length ; i ++ )
					{
						//expect( batch[ i ] ).to.be.an( rootsDb.DocumentWrapper ) ;
						expect( batch[ i ].firstName ).to.be.ok() ;
						expect( batch[ i ].lastName ).to.equal( 'Marley' ) ;
						map[ batch[ i ].firstName ] = true ;
					}
					
					expect( map ).to.only.have.keys( 'Bob' , 'Julian' , 'Stephen' , 'Ziggy' , 'Rita' ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "should find documents (in a batch) using a queryObject (create, save and find)" , function( done ) {
		
		var marleys = [
			users.createDocument( {
				firstName: 'Bob' ,
				lastName: 'Marley'
			} ) ,
			users.createDocument( {
				firstName: 'Julian' ,
				lastName: 'Marley'
			} ) ,
			users.createDocument( {
				firstName: 'Thomas' ,
				lastName: 'Jefferson'
			} ) ,
			users.createDocument( {
				firstName: 'Stephen' ,
				lastName: 'Marley'
			} ) ,
			users.createDocument( {
				firstName: 'Mr' ,
				lastName: 'X'
			} ) ,
			users.createDocument( {
				firstName: 'Ziggy' ,
				lastName: 'Marley'
			} ) ,
			users.createDocument( {
				firstName: 'Rita' ,
				lastName: 'Marley'
			} )
		] ;
		
		async.series( [
			function( callback ) {
				rootsDb.bulk( 'save' , marleys , callback ) ;
			} ,
			function( callback ) {
				users.find( { firstName: { $regex: /^[thomasstepn]+$/ , $options: 'i' } } , function( error , batch ) {
					var i , map = {} ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'Batch:' , batch ) ; 
					expect( error ).not.to.be.ok() ;
					expect( batch.$ ).to.be.a( rootsDb.BatchWrapper ) ;
					expect( batch ).to.have.length( 2 ) ;
					
					for ( i = 0 ; i < batch.length ; i ++ )
					{
						//expect( batch[ i ] ).to.be.an( rootsDb.DocumentWrapper ) ;
						expect( batch[ i ].firstName ).to.be.ok() ;
						map[ batch[ i ].firstName ] = true ;
					}
					
					expect( map ).to.only.have.keys( 'Thomas' , 'Stephen' ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
} ) ;



describe( "Embedded documents" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "should save and retrieve embedded data" , function( done ) {
		
		var town = towns.createDocument( {
			name: 'Paris' ,
			meta: {
				population: '2200K' ,
				country: 'France'
			}
		} ) ;
		
		async.series( [
			function( callback ) {
				town.$.save( callback ) ;
			} ,
			function( callback ) {
				towns.get( town._id , function( error , t ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'Town:' , string.inspect( { style: 'color' , proto: true } , town.$.meta ) ) ;
					expect( error ).not.to.be.ok() ;
					expect( t.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( t._id ).to.be.an( mongodb.ObjectID ) ;
					expect( t ).to.eql( { _id: town._id , name: 'Paris' , meta: { population: '2200K' , country: 'France' } } ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "should be able to update embedded data (patch)" , function( done ) {
		
		var town = towns.createDocument( {
			name: 'Paris' ,
			meta: {
				population: '2200K',
				country: 'France'
			}
		} ) ;
		
		async.series( [
			function( callback ) {
				town.$.save( callback ) ;
			} ,
			function( callback ) {
				towns.get( town._id , function( error , t ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'Town:' , town ) ; 
					expect( error ).not.to.be.ok() ;
					expect( t ).to.eql( { _id: town._id , name: 'Paris' , meta: { population: '2200K' , country: 'France' } } ) ;
					expect( t.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					
					t.$.patch( { "meta.population": "2300K" } ) ;
					t.$.commit( callback ) ;
				} ) ;
			} ,
			function( callback ) {
				towns.get( town._id , function( error , t ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'Town:' , town ) ; 
					expect( error ).not.to.be.ok() ;
					expect( t.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( t._id ).to.be.an( mongodb.ObjectID ) ;
					expect( t ).to.eql( { _id: town._id , name: 'Paris' , meta: { population: '2300K' , country: 'France' } } ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "should collect a batch & get unique using embedded data as fingerprint (create, save and collect batch)" , function( done ) {
		
		var townList = [
			towns.createDocument( {
				name: 'Paris' ,
				meta: {
					country: 'France' ,
					capital: true
				}
			} ) ,
			towns.createDocument( {
				name: 'Tokyo' ,
				meta: {
					country: 'Japan' ,
					capital: true
				}
			} ) ,
			towns.createDocument( {
				name: 'New York' ,
				meta: {
					country: 'USA' ,
					capital: false
				}
			} ) ,
			towns.createDocument( {
				name: 'Washington' ,
				meta: {
					country: 'USA' ,
					capital: true
				}
			} ) ,
			towns.createDocument( {
				name: 'San Francisco' ,
				meta: {
					country: 'USA' ,
					capital: false
				}
			} )
		] ;
		
		async.series( [
			function( callback ) {
				rootsDb.bulk( 'save' , townList , callback ) ;
			} ,
			function( callback ) {
				towns.collect( { "meta.country": 'USA' } , function( error , batch ) {
					var i , map = {} ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'RawBatch:' , batch ) ; 
					expect( error ).not.to.be.ok() ;
					expect( batch.$ ).to.be.an( rootsDb.BatchWrapper ) ;
					expect( batch ).to.have.length( 3 ) ;
					
					for ( i = 0 ; i < batch.length ; i ++ )
					{
						expect( batch[ i ].name ).to.be.ok() ;
						expect( batch[ i ].meta.country ).to.equal( 'USA' ) ;
						map[ batch[ i ].name ] = true ;
					}
					
					expect( map ).to.only.have.keys( 'New York' , 'Washington' , 'San Francisco' ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				towns.collect( { "meta.country": 'USA' , "meta.capital": false } , function( error , batch ) {
					var i , map = {} ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'Batch:' , batch ) ; 
					expect( error ).not.to.be.ok() ;
					expect( batch.$ ).to.be.an( rootsDb.BatchWrapper ) ;
					expect( batch ).to.have.length( 2 ) ;
					
					for ( i = 0 ; i < batch.length ; i ++ )
					{
						expect( batch[ i ].name ).to.ok() ;
						expect( batch[ i ].meta.country ).to.equal( 'USA' ) ;
						map[ batch[ i ].name ] = true ;
					}
					
					expect( map ).to.only.have.keys( 'New York' , 'San Francisco' ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				towns.getUnique( { name: 'Tokyo', "meta.country": 'Japan' } , function( error , town ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'Town:' , town ) ; 
					expect( error ).not.to.be.ok() ;
					expect( town.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( town ).to.eql( {
						_id: town._id ,
						name: 'Tokyo' ,
						meta: {
							country: 'Japan' ,
							capital: true
						}
					} ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
} ) ;



describe( "Links" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "basic link (create both, link, save both, retrieve parent, navigate to child)" , function( done ) {
		
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
		
		expect( user.job ).to.eql( jobId ) ;
		
		async.series( [
			function( callback ) {
				job.$.save( callback ) ;
			} ,
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				jobs.get( jobId , function( error , job ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'Job:' , job ) ;
					expect( error ).not.to.be.ok() ;
					expect( job.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( job._id ).to.be.an( mongodb.ObjectID ) ;
					expect( job._id ).to.eql( jobId ) ;
					expect( job ).to.eql( { _id: job._id , title: 'developer' , salary: 60000 , users: [] , schools: [] } ) ;
					
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , user_ ) {
					user = user_ ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ;
					expect( error ).not.to.be.ok() ;
					expect( user.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
					expect( user ).to.eql( { _id: user._id, job: jobId, firstName: 'Jilbert', lastName: 'Polson' , memberSid: 'Jilbert Polson' } ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				user.$.getLink( "job" , function( error , job ) {
					expect( error ).not.to.be.ok() ;
					expect( job ).to.eql( { _id: jobId , title: 'developer' , salary: 60000 , users: [] , schools: [] } ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				expect( user.$.getLinkDetails( "job" ) ).to.eql( {
					type: 'link' ,
					foreignCollection: 'jobs' ,
					foreignId: jobId ,
					hostPath: 'job' ,
					schema: {
						collection: 'jobs' ,
						optional: true ,
						type: 'link' ,
						sanitize: [ 'toLink' ] ,
					}
				} ) ;
				callback() ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "basic nested links (create both, link, save both, retrieve parent, navigate to child)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;
		
		var id = user._id ;
		
		var connectionA = users.createDocument( {
			firstName: 'John' ,
			lastName: 'Fergusson'
		} ) ;
		
		var connectionB = users.createDocument( {
			firstName: 'Andy' ,
			lastName: 'Fergusson'
		} ) ;
		
		//console.log( job ) ;
		var connectionAId = connectionA.$.id ;
		var connectionBId = connectionB.$.id ;
		
		// Link the documents!
		user.$.setLink( 'connection.A' , connectionA ) ;
		user.$.setLink( 'connection.B' , connectionB ) ;
		
		expect( user.connection.A ).to.eql( connectionAId ) ;
		expect( user.connection.B ).to.eql( connectionBId ) ;
		
		async.series( [
			function( callback ) {
				connectionA.$.save( callback ) ;
			} ,
			function( callback ) {
				connectionB.$.save( callback ) ;
			} ,
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , user ) {
					expect( user.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
					expect( user ).to.eql( {
						_id: user._id,
						firstName: 'Jilbert',
						lastName: 'Polson' ,
						connection: {
							A: connectionAId ,
							B: connectionBId
						} ,
						memberSid: 'Jilbert Polson'
					} ) ;
					
					//user.$.toto = 'toto' ;
					
					user.$.getLink( "connection.A" , function( error , userA ) {
						expect( error ).not.to.be.ok() ;
						expect( userA ).to.eql( {
							_id: connectionAId ,
							firstName: 'John' ,
							lastName: "Fergusson" ,
							memberSid: "John Fergusson"
						} ) ;
						
						user.$.getLink( "connection.B" , function( error , userB ) {
							expect( error ).not.to.be.ok() ;
							expect( userB ).to.eql( {
								_id: connectionBId ,
								firstName: 'Andy' ,
								lastName: "Fergusson" ,
								memberSid: "Andy Fergusson"
							} ) ;
							callback() ;
						} ) ;
					} ) ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "unexistant links, non-link properties" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;
		
		var id = user._id ;
		
		var connectionA = users.createDocument( {
			firstName: 'John' ,
			lastName: 'Fergusson'
		} ) ;
		
		var connectionB = users.createDocument( {
			firstName: 'Andy' ,
			lastName: 'Fergusson'
		} ) ;
		
		var connectionAId = connectionA.$.id ;
		var connectionBId = connectionB.$.id ;
		
		user.$.setLink( 'connection.A' , connectionA ) ;
		doormen.shouldThrow( function() { user.$.setLink( 'unexistant' , connectionB ) ; } ) ;
		doormen.shouldThrow( function() { user.$.setLink( 'firstName' , connectionB ) ; } ) ;
		doormen.shouldThrow( function() { user.$.setLink( 'firstName.blah' , connectionB ) ; } ) ;
		
		async.series( [
			function( callback ) {
				connectionA.$.save( callback ) ;
			} ,
			function( callback ) {
				connectionB.$.save( callback ) ;
			} ,
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , user_ ) {
					user = user_ ;
					expect( user.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
					expect( user ).to.eql( {
						_id: user._id,
						firstName: 'Jilbert',
						lastName: 'Polson' ,
						connection: {
							A: connectionAId
						} ,
						memberSid: 'Jilbert Polson'
					} ) ;
					
					//user.$.toto = 'toto' ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				user.$.getLink( "connection.A" , function( error , userA ) {
					expect( error ).not.to.be.ok() ;
					expect( userA ).to.eql( {
						_id: connectionAId ,
						firstName: 'John' ,
						lastName: "Fergusson" ,
						memberSid: "John Fergusson"
					} ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				user.$.getLink( "connection.B" , function( error , userB ) {
					expect( error ).to.be.ok() ;
					expect( error.type ).to.be( 'notFound' ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				user.$.getLink( "unexistant" , function( error , userB ) {
					expect( error ).to.be.ok() ;
					expect( error.type ).to.be( 'badRequest' ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				user.$.getLink( "unexistant.unexistant" , function( error , userB ) {
					expect( error ).to.be.ok() ;
					expect( error.type ).to.be( 'badRequest' ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				user.$.getLink( "firstName" , function( error , userB ) {
					expect( error ).to.be.ok() ;
					expect( error.type ).to.be( 'badRequest' ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				user.$.getLink( "firstName.blah" , function( error , userB ) {
					expect( error ).to.be.ok() ;
					expect( error.type ).to.be( 'badRequest' ) ;
					callback() ;
				} ) ;
			} ,
		] )
		.exec( done ) ;
	} ) ;
} ) ;



describe( "Multi-links" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "basic multi-link (create, link, save, retrieve one, retrieve multi-links, add link, check, unlink, check)" , function( done ) {
		
		var school = schools.createDocument( {
			title: 'Computer Science'
		} ) ;
		
		var id = school._id ;
		
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
		
		// Link the documents!
		school.$.setLink( 'jobs' , [ job1 , job2 ] ) ;
		
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
				school.$.save( callback ) ;
			} ,
			function( callback ) {
				schools.get( id , function( error , school_ ) {
					school = school_ ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'Job:' , job ) ;
					expect( error ).not.to.be.ok() ;
					expect( school.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( school._id ).to.be.an( mongodb.ObjectID ) ;
					expect( school._id ).to.eql( id ) ;
					expect( school ).to.eql( { _id: school._id , title: 'Computer Science' , jobs: [ job1._id , job2._id ] } ) ;
					
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				school.$.getLink( "jobs" , function( error , jobs_ ) {
					expect( error ).not.to.be.ok() ;
					expect( jobs_ ).to.have.length( 2 ) ;
					
					//console.error( jobs_ ) ;
					jobs_.sort( function( a , b ) { return b.salary - a.salary ; } ) ;
					
					expect( jobs_ ).to.eql( [
						{
							_id: jobs_[ 0 ]._id,
							title: 'developer',
							salary: 60000,
							users: [] ,
							schools: []
						} ,
						{
							_id: jobs_[ 1 ]._id,
							title: 'sysadmin',
							salary: 55000,
							users: [] ,
							schools: []
						}
					] ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				school.$.addLink( 'jobs' , job3 ) ;
				school.$.save( callback ) ;
			} ,
			function( callback ) {
				school.$.getLink( "jobs" , function( error , jobs_ ) {
					expect( error ).not.to.be.ok() ;
					expect( jobs_ ).to.have.length( 3 ) ;
					
					//console.error( jobs_ ) ;
					jobs_.sort( function( a , b ) { return b.salary - a.salary ; } ) ;
					
					expect( jobs_ ).to.eql( [
						{
							_id: jobs_[ 0 ]._id,
							title: 'developer',
							salary: 60000,
							users: [] ,
							schools: []
						} ,
						{
							_id: jobs_[ 1 ]._id,
							title: 'sysadmin',
							salary: 55000,
							users: [] ,
							schools: []
						} ,
						{
							_id: jobs_[ 2 ]._id,
							title: 'front-end developer',
							salary: 54000,
							users: [] ,
							schools: []
						} ,
					] ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				school.$.unlink( 'jobs' , job2 ) ;
				school.$.save( callback ) ;
			} ,
			function( callback ) {
				school.$.getLink( "jobs" , function( error , jobs_ ) {
					expect( error ).not.to.be.ok() ;
					expect( jobs_ ).to.have.length( 2 ) ;
					
					//console.error( jobs_ ) ;
					jobs_.sort( function( a , b ) { return b.salary - a.salary ; } ) ;
					
					expect( jobs_ ).to.eql( [
						{
							_id: jobs_[ 0 ]._id,
							title: 'developer',
							salary: 60000,
							users: [] ,
							schools: []
						} ,
						{
							_id: jobs_[ 1 ]._id,
							title: 'front-end developer',
							salary: 54000,
							users: [] ,
							schools: []
						} ,
					] ) ;
					callback() ;
				} ) ;
			} ,
		] )
		.exec( done ) ;
	} ) ;
	
	it( "basic nested multi-links" ) ;
} ) ;



describe( "Back-links" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "basic back-link (create, link, save, retrieve one, retrieve back-links)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;
		
		var id = user._id ;
		
		var user2 = users.createDocument( {
			firstName: 'Tony' ,
			lastName: 'P.'
		} ) ;
		
		var id2 = user2._id ;
		
		var job = jobs.createDocument( {
			title: 'developer' ,
			salary: 60000
		} ) ;
		
		//console.log( job ) ;
		var jobId = job.$.id ;
		
		// Link the documents!
		user.$.setLink( 'job' , job ) ;
		
		async.series( [
			function( callback ) {
				job.$.save( callback ) ;
			} ,
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				jobs.get( jobId , function( error , job_ ) {
					job = job_ ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'Job:' , job ) ;
					expect( error ).not.to.be.ok() ;
					expect( job.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( job._id ).to.be.an( mongodb.ObjectID ) ;
					expect( job._id ).to.eql( jobId ) ;
					expect( job ).to.eql( { _id: job._id , title: 'developer' , salary: 60000 , users: [] , schools: [] } ) ;
					
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , user_ ) {
					user = user_ ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ;
					expect( error ).not.to.be.ok() ;
					expect( user.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
					expect( user ).to.eql( { _id: user._id, job: jobId, firstName: 'Jilbert', lastName: 'Polson' , memberSid: 'Jilbert Polson' } ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				job.$.getLink( "users" , function( error , users_ ) {
					expect( error ).not.to.be.ok() ;
					expect( users_ ).to.be.an( Array ) ;
					expect( users_ ).to.have.length( 1 ) ;
					// Temp
					expect( users_ ).to.eql( [
						{
							_id: users_[ 0 ]._id,
							firstName: 'Jilbert',
							lastName: 'Polson',
							memberSid: 'Jilbert Polson',
							job: job._id
						}
					] ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				expect( job.$.getLinkDetails( "users" ) ).to.eql( {
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
					}
				} ) ;
				callback() ;
			} ,
			function( callback ) {
				user2.$.setLink( 'job' , job ) ;
				user2.$.save( callback ) ;
			} ,
			function( callback ) {
				job.$.getLink( "users" , function( error , users_ ) {
					expect( error ).not.to.be.ok() ;
					// Temp
					
					expect( users_ ).to.have.length( 2 ) ;
					
					//console.error( users_ ) ;
					if ( users_[ 0 ].firstName === 'Tony' )
					{
						users_ = [ users_[ 1 ] , users_[ 0 ] ] ;
					}
					
					expect( users_ ).to.eql( [
						{
							_id: users_[ 0 ]._id,
							firstName: 'Jilbert',
							lastName: 'Polson',
							memberSid: 'Jilbert Polson',
							job: job._id
						} ,
						{
							_id: users_[ 1 ]._id,
							firstName: 'Tony',
							lastName: 'P.',
							memberSid: 'Tony P.',
							job: job._id
						}
					] ) ;
					callback() ;
				} ) ;
			} ,
		] )
		.exec( done ) ;
	} ) ;
	
	it( "back-link of multi-link" , function( done ) {
		
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
				jobs.get( job1Id , function( error , job ) {
					expect( error ).not.to.be.ok() ;
					expect( job._id ).to.eql( job1Id ) ;
					expect( job ).to.eql( {
						_id: job1._id,
						title: 'developer',
						salary: 60000,
						users: [],
						schools: []
					} ) ;
					
					job.$.getLink( 'schools' , function( error , schools_ ) {
						expect( error ).not.to.be.ok() ;
						expect( schools_ ).to.have.length( 2 ) ;
						
						schools_.sort( function( a , b ) { return b.title - a.title ; } ) ;
						
						// Order by id
						schools_[ 0 ].jobs.sort( function( a , b ) { return a.toString() > b.toString() ? 1 : -1 ; } ) ;
						schools_[ 1 ].jobs.sort( function( a , b ) { return a.toString() > b.toString() ? 1 : -1 ; } ) ;
						
						expect( schools_ ).to.eql( [
							{
								_id: school1._id,
								title: 'Computer Science',
								jobs: [ job1Id , job2Id , job3Id ]
							},
							{
								_id: school2._id,
								title: 'Web Academy',
								jobs: [ job1Id , job3Id , job4Id ]
							}
						] ) ;
						
						callback() ;
					} ) ;
				} ) ;
			} ,
			function( callback ) {
				jobs.get( job4Id , function( error , job ) {
					expect( error ).not.to.be.ok() ;
					expect( job._id ).to.eql( job4Id ) ;
					expect( job ).to.eql( {
						_id: job4._id,
						title: 'designer',
						salary: 56000,
						users: [],
						schools: []
					} ) ;
					
					job.$.getLink( 'schools' , function( error , schools_ ) {
						expect( error ).not.to.be.ok() ;
						expect( schools_ ).to.have.length( 1 ) ;
						
						// Order by id
						schools_[ 0 ].jobs.sort( function( a , b ) { return a.toString() > b.toString() ? 1 : -1 ; } ) ;
						
						expect( schools_ ).to.eql( [
							{
								_id: school2._id,
								title: 'Web Academy',
								jobs: [ job1Id , job3Id , job4Id ]
							}
						] ) ;
						
						callback() ;
					} ) ;
				} ) ;
			} ,
		] )
		.exec( done ) ;
	} ) ;
	
	it( "basic nested back-links" ) ;
} ) ;



describe( "Populate links" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "link population (create both, link, save both, get with populate option)" , function( done ) {
		
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
		
		expect( user.job ).to.eql( jobId ) ;
		
		async.series( [
			function( callback ) {
				job.$.save( callback ) ;
			} ,
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				options = { populate: 'job' } ;
				users.get( id , options , function( error , user_ ) {
					user = user_ ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ;
					expect( error ).not.to.be.ok() ;
					expect( user.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
					expect( user ).to.eql( { _id: user._id, job: job, firstName: 'Jilbert', lastName: 'Polson' , memberSid: 'Jilbert Polson' } ) ;
					
					expect( options.populateDepth ).to.be( 1 ) ;
					expect( options.populateDbQueries ).to.be( 1 ) ;
					
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "multiple link population (create, link, save, get with populate option)" , function( done ) {
		
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
				users.get( id , options , function( error , user_ ) {
					user = user_ ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ;
					expect( error ).not.to.be.ok() ;
					expect( user.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
					expect( user ).to.eql( {
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
	
	it( "multiple link population having same and circular target" , function( done ) {
		
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
		
		expect( user.connection.A ).to.eql( connection.$.id ) ;
		expect( user.connection.B ).to.eql( connection.$.id ) ;
		expect( user.connection.C ).to.eql( user.$.id ) ;
		
		async.series( [
			function( callback ) {
				connection.$.save( callback ) ;
			} ,
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				options = { populate: [ 'connection.A' , 'connection.B' , 'connection.C' ] } ;
				users.get( id , options , function( error , user ) {
					expect( user.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
					expect( user.connection.A ).to.be( user.connection.B ) ;
					expect( user ).to.eql( {
						_id: user._id,
						firstName: 'Jilbert',
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
	
	it( "collect batch with multiple link population (create, link, save, collect with populate option)" , function( done ) {
		
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
				users.collect( {} , options , function( error , batch ) {
					expect( error ).not.to.be.ok() ;
					
					// Sort that first...
					batch.sort( function( a , b ) {
						return a.firstName.charCodeAt( 0 ) - b.firstName.charCodeAt( 0 ) ;
					} ) ;
					
					expect( batch ).to.eql( [
						{
							firstName: 'DA',
							lastName: 'GODFATHER',
							_id: batch[ 0 ]._id,
							memberSid: 'DA GODFATHER'
							//, job: null, godfather: null
							//, job: undefined, godfather: undefined
						},
						{
							firstName: 'Harry',
							lastName: 'Campbell',
							_id: batch[ 1 ]._id,
							memberSid: 'Harry Campbell',
							godfather: {
								firstName: 'DA',
								lastName: 'GODFATHER',
								_id: batch[ 0 ]._id,
								memberSid: 'DA GODFATHER'
							}
							//, job: null
							//, job: undefined
						},
						{
							firstName: 'Jilbert',
							lastName: 'Polson',
							_id: batch[ 2 ]._id,
							memberSid: 'Jilbert Polson',
							job: {
								title: 'developer',
								salary: 60000,
								users: [],
								schools: [],
								_id: job._id
							},
							godfather: {
								firstName: 'DA',
								lastName: 'GODFATHER',
								_id: batch[ 0 ]._id,
								memberSid: 'DA GODFATHER'
							}
						},
						{
							firstName: 'Thomas',
							lastName: 'Campbell',
							_id: batch[ 3 ]._id,
							memberSid: 'Thomas Campbell'
							//, job: null, godfather: null
							//, job: undefined, godfather: undefined
						},
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
	
	it( "collect batch with multiple link population and circular references" , function( done ) {
		
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
				users.collect( {} , options , function( error , batch ) {
					expect( error ).not.to.be.ok() ;
					
					// Sort that first...
					batch.sort( function( a , b ) {
						return a.firstName.charCodeAt( 0 ) - b.firstName.charCodeAt( 0 ) ;
					} ) ;
					
					// References are painful to test...
					// More tests covering references are done in the memory model section
					//log.warning( 'incomplete test for populate + reference' ) ;
					
					expect( batch[ 0 ].godfather ).to.be( batch[ 0 ] ) ;
					//expect( batch[ 1 ].godfather ).to.be( batch[ 0 ] ) ;
					expect( batch[ 2 ].godfather ).to.be( batch[ 0 ] ) ;
					
					// JSON.stringify() should throw
					expect( function() { JSON.stringify( batch ) ; } ).to.throwException() ;
					
					expect( options.populateDepth ).to.be( 1 ) ;
					// Only one DB query, since the godfather is a user and all users have been collected before the populate pass
					expect( options.populateDbQueries ).to.be( 1 ) ;
					
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "collect batch with multiple link population and circular references: using noReference" , function( done ) {
		
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
				users.collect( {} , options , function( error , batch ) {
					expect( error ).not.to.be.ok() ;
					
					// Sort that first...
					batch.sort( function( a , b ) {
						return a.firstName.charCodeAt( 0 ) - b.firstName.charCodeAt( 0 ) ;
					} ) ;
					
					expect( batch ).to.eql( [
						{
							firstName: 'DA',
							lastName: 'GODFATHER',
							_id: batch[ 0 ]._id,
							memberSid: 'DA GODFATHER',
							godfather: {
								firstName: 'DA',
								lastName: 'GODFATHER',
								_id: batch[ 0 ]._id,
								memberSid: 'DA GODFATHER',
								godfather: batch[ 0 ]._id
							}
						},
						{
							firstName: 'Harry',
							lastName: 'Campbell',
							_id: batch[ 1 ]._id,
							memberSid: 'Harry Campbell',
							godfather: {
								firstName: 'DA',
								lastName: 'GODFATHER',
								_id: batch[ 0 ]._id,
								memberSid: 'DA GODFATHER',
								godfather: batch[ 0 ]._id
							}
							//, job: null
							//, job: undefined
						},
						{
							firstName: 'Jilbert',
							lastName: 'Polson',
							_id: batch[ 2 ]._id,
							memberSid: 'Jilbert Polson',
							job: {
								title: 'developer',
								salary: 60000,
								users: [],
								schools: [],
								_id: job._id
							},
							godfather: {
								firstName: 'DA',
								lastName: 'GODFATHER',
								_id: batch[ 0 ]._id,
								memberSid: 'DA GODFATHER',
								godfather: batch[ 0 ]._id
							}
						},
						{
							firstName: 'Thomas',
							lastName: 'Campbell',
							_id: batch[ 3 ]._id,
							memberSid: 'Thomas Campbell'
							//, job: null, godfather: null
							//, job: undefined, godfather: undefined
						},
					] ) ;
					
					//console.log( batch ) ;
					
					// JSON.stringify() should not throw
					expect( function() { JSON.stringify( batch ) ; } ).not.to.throwException() ;
					
					expect( options.populateDepth ).to.be( 1 ) ;
					// Only one DB query, since the godfather is a user and all users have been collected before the populate pass
					expect( options.populateDbQueries ).to.be( 1 ) ;
					
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "'multi-link' population (create both, link, save both, get with populate option)" , function( done ) {
		
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
				schools.get( school1Id , options , function( error , school_ ) {
					school = school_ ;
					//console.log( '>>>>>>>>>>>\nSchool:' , school ) ;
					expect( error ).not.to.be.ok() ;
					expect( school.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( school._id ).to.be.an( mongodb.ObjectID ) ;
					expect( school._id ).to.eql( school1Id ) ;
					expect( school ).to.eql( {
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
				schools.collect( {} , options , function( error , schools_ ) {
					
					expect( error ).not.to.be.ok() ;
					
					if ( schools_[ 0 ].title !== 'Computer Science' ) { schools_ = [ schools_[ 1 ] , schools_[ 0 ] ] ; }
					
					expect( schools_ ).to.eql( [
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
			} ,
		] )
		.exec( done ) ;
	} ) ;
	
	it( "'back-link' population (create both, link, save both, get with populate option)" , function( done ) {
		
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
				jobs.get( job1Id , options , function( error , job_ ) {
					//console.error( job_.users ) ;
					expect( error ).not.to.be.ok() ;
					expect( job_.users ).to.have.length( 2 ) ;
					
					if ( job_.users[ 0 ].firstName === 'Tony' ) { job_.users = [ job_.users[ 1 ] , job_.users[ 0 ] ] ; }
					
					expect( job_ ).to.eql( {
						_id: job1._id ,
						title: 'developer' ,
						salary: 60000 ,
						users: [ user1 , user2 ],
						schools: []
					} ) ;
					
					expect( options.populateDepth ).to.be( 1 ) ;
					expect( options.populateDbQueries ).to.be( 1 ) ;
					
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				options = { populate: 'users' } ; 
				jobs.collect( {} , options , function( error , jobs_ ) {
					expect( error ).not.to.be.ok() ;
					expect( jobs_ ).to.have.length( 2 ) ;
					
					//console.error( "\n\n\n\njobs:" , jobs_ ) ;
					if ( jobs_[ 0 ].title === 'star developer' ) { jobs_ = [ jobs_[ 1 ] , jobs_[ 0 ] ] ; }
					
					expect( jobs_[ 0 ].users ).to.have.length( 2 ) ;
					
					if ( jobs_[ 0 ].users[ 0 ].firstName === 'Tony' ) { jobs_[ 0 ].users = [ jobs_[ 0 ].users[ 1 ] , jobs_[ 0 ].users[ 0 ] ] ; }
					
					expect( jobs_[ 0 ] ).to.eql( {
						_id: job1._id ,
						title: 'developer' ,
						salary: 60000 ,
						users: [ user1 , user2 ],
						schools: []
					} ) ;
					
					expect( jobs_[ 1 ].users ).to.have.length( 2 ) ;
					
					if ( jobs_[ 1 ].users[ 0 ].firstName === 'Richard' ) { jobs_[ 1 ].users = [ jobs_[ 1 ].users[ 1 ] , jobs_[ 1 ].users[ 0 ] ] ; }
					
					expect( jobs_[ 1 ] ).to.eql( {
						_id: job2._id ,
						title: 'star developer' ,
						salary: 200000 ,
						users: [ user3 , user4 ],
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
	
	it( "zzz 'back-link' of multi-link population" , function( done ) {
		
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
				jobs.get( job1Id , options , function( error , job ) {
					expect( error ).not.to.be.ok() ;
					expect( job._id ).to.eql( job1Id ) ;
					
					expect( job.schools ).to.have.length( 2 ) ;
					
					job.schools.sort( function( a , b ) { return b.title - a.title ; } ) ;
					
					// Order by id
					job.schools[ 0 ].jobs.sort( function( a , b ) { return a.toString() > b.toString() ? 1 : -1 ; } ) ;
					job.schools[ 1 ].jobs.sort( function( a , b ) { return a.toString() > b.toString() ? 1 : -1 ; } ) ;
					
					expect( job ).to.eql( {
						_id: job1._id,
						title: 'developer',
						salary: 60000,
						users: [],
						schools: [
							{
								_id: school1._id,
								title: 'Computer Science',
								jobs: [ job1Id , job2Id , job3Id ]
							},
							{
								_id: school2._id,
								title: 'Web Academy',
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
				jobs.get( job4Id , function( error , job ) {
					expect( error ).not.to.be.ok() ;
					expect( job._id ).to.eql( job4Id ) ;
					
					expect( job.schools ).to.have.length( 1 ) ;
					
					// Order by id
					job.schools[ 0 ].jobs.sort( function( a , b ) { return a.toString() > b.toString() ? 1 : -1 ; } ) ;
					
					expect( job ).to.eql( {
						_id: job4._id,
						title: 'designer',
						salary: 56000,
						users: [],
						schools: [
							{
								_id: school2._id,
								title: 'Web Academy',
								jobs: [ job1Id , job3Id , job4Id ]
							}
						]
					} ) ;
						
					expect( options.populateDepth ).to.be( 1 ) ;
					expect( options.populateDbQueries ).to.be( 1 ) ;
					
					callback() ;
				} ) ;
			} ,
		] )
		.exec( done ) ;
	} ) ;
	
} ) ;



describe( "Deep populate links" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "deep population (links then back-link)" , function( done ) {
		
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
				users.get( user._id , options , function( error , user_ ) {
					expect( error ).not.to.be.ok() ;
					expect( user_.$.populated.job ).to.be( true ) ;
					
					expect( user_.job.users ).to.have.length( 2 ) ;
					
					if ( user_.job.users[ 0 ].firstName === 'Robert' )
					{
						user_.job.users = [ user_.job.users[ 1 ] , user_.job.users[ 0 ] ] ;
					}
					
					expect( user_.job.users[ 0 ].job ).to.be( user_.job ) ;
					expect( user_.job.users[ 1 ].job ).to.be( user_.job ) ;
					
					expect( user_ ).to.eql( {
						_id: user._id,
						firstName: 'Jilbert',
						lastName: 'Polson' ,
						memberSid: 'Jilbert Polson',
						job: {
							_id: job._id ,
							title: 'developer' ,
							salary: 60000,
							schools: [],
							users: [
								user_ ,
								// We cannot use 'user2', expect.js is too confused with Circular references
								// We have to perform a second check for that
								user_.job.users[ 1 ]
							]
						}
					} ) ;
					
					expect( user_.job.users[ 1 ] ).to.eql( {
						_id: user2._id,
						firstName: 'Robert',
						lastName: 'Polson' ,
						memberSid: 'Robert Polson',
						job: {
							_id: job._id ,
							title: 'developer' ,
							salary: 60000,
							schools: [],
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
	


describe( "Attachment links" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "basic attachment (create, attach, save both, retrieve parent, navigate to child)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;
		
		var id = user._id ;
		
		// Link the documents!
		var attachment = user.$.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		var fullUrl = attachment.fullUrl ;
		user.$.setLink( 'file' , attachment ) ;
		//console.error( user.file ) ;
		
		expect( user.file ).to.eql( {
			filename: 'joke.txt' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'text/plain'
		} ) ;
		
		async.series( [
			function( callback ) {
				attachment.save( callback ) ;
			} ,
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				// Check that the file exist
				expect( function() { fs.accessSync( fullUrl , fs.R_OK ) } ).not.to.throwError() ;
				
				users.get( id , function( error , user_ ) {
					user = user_ ;
					expect( error ).not.to.be.ok() ;
					expect( user ).to.eql( {
						_id: user._id,
						firstName: 'Jilbert',
						lastName: 'Polson' ,
						memberSid: 'Jilbert Polson' ,
						file:{
							filename: 'joke.txt' ,
							id: user.file.id ,	// Unpredictable
							contentType: 'text/plain'
						}
					} ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				user.$.getLink( "file" , function( error , file ) {
					expect( error ).not.to.be.ok() ;
					expect( file ).to.eql( {
						id: user.file.id ,
						filename: 'joke.txt' ,
						contentType: 'text/plain' ,
						collectionName: 'users' ,
						documentId: user._id.toString() ,
						incoming: undefined ,
						baseUrl: file.baseUrl ,
						fullUrl: file.baseUrl + file.documentId.toString() + '/' + file.id.toString()
					} ) ;
					
					file.load( function( error , data ) {
						expect( error ).not.to.be.ok() ;
						expect( data.toString() ).to.be( "grigrigredin menufretin\n" ) ;
						callback() ;
					} ) ;
				} ) ;
			} ,
			function( callback ) {
				var details = user.$.getLinkDetails( "file" ) ;
				expect( details ).to.eql( {
					type: 'attachment' ,
					hostPath: 'file' ,
					schema: {
						optional: true ,
						type: 'attachment'
					} ,
					attachment: {
						id: user.file.id ,
						filename: 'joke.txt' ,
						contentType: 'text/plain' ,
						collectionName: 'users' ,
						documentId: user._id.toString() ,
						incoming: undefined ,
						baseUrl: details.attachment.baseUrl ,
						fullUrl: details.attachment.baseUrl +
							details.attachment.documentId.toString() +
							'/' + details.attachment.id.toString()
					}
				} ) ;
				callback() ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "Alter meta-data of an attachment" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;
		
		var id = user._id ;
		
		// Link the documents!
		var attachment = user.$.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		var fullUrl = attachment.fullUrl ;
		user.$.setLink( 'file' , attachment ) ;
		//console.error( user.file ) ;
		
		expect( user.file ).to.eql( {
			filename: 'joke.txt' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'text/plain'
		} ) ;
		
		async.series( [
			function( callback ) {
				attachment.save( callback ) ;
			} ,
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				// Check that the file exist
				expect( function() { fs.accessSync( fullUrl , fs.R_OK ) } ).not.to.throwError() ;
				
				users.get( id , function( error , user_ ) {
					user = user_ ;
					expect( error ).not.to.be.ok() ;
					expect( user ).to.eql( {
						_id: user._id,
						firstName: 'Jilbert',
						lastName: 'Polson' ,
						memberSid: 'Jilbert Polson' ,
						file:{
							filename: 'joke.txt' ,
							id: user.file.id ,	// Unpredictable
							contentType: 'text/plain'
						}
					} ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				user.$.patch( {
					"file.filename": "lol.txt" ,
					"file.contentType": "text/joke"
				} ) ;
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , user_ ) {
					user = user_ ;
					expect( error ).not.to.be.ok() ;
					expect( user ).to.eql( {
						_id: user._id,
						firstName: 'Jilbert',
						lastName: 'Polson' ,
						memberSid: 'Jilbert Polson' ,
						file:{
							filename: 'lol.txt' ,
							id: user.file.id ,	// Unpredictable
							contentType: 'text/joke'
						}
					} ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				user.$.getLink( "file" , function( error , file ) {
					expect( error ).not.to.be.ok() ;
					expect( file ).to.eql( {
						id: user.file.id ,
						filename: 'lol.txt' ,
						contentType: 'text/joke' ,
						collectionName: 'users' ,
						documentId: user._id.toString() ,
						incoming: undefined ,
						baseUrl: file.baseUrl ,
						fullUrl: file.baseUrl + file.documentId.toString() + '/' + file.id.toString()
					} ) ;
					
					file.load( function( error , data ) {
						expect( error ).not.to.be.ok() ;
						expect( data.toString() ).to.be( "grigrigredin menufretin\n" ) ;
						callback() ;
					} ) ;
				} ) ;
			} ,
			function( callback ) {
				// Check that the file exist
				expect( function() { fs.accessSync( fullUrl , fs.R_OK ) } ).not.to.throwError() ;
				
				var details = user.$.getLinkDetails( "file" ) ;
				expect( details ).to.eql( {
					type: 'attachment' ,
					hostPath: 'file' ,
					schema: {
						optional: true ,
						type: 'attachment'
					} ,
					attachment: {
						id: user.file.id ,
						filename: 'lol.txt' ,
						contentType: 'text/joke' ,
						collectionName: 'users' ,
						documentId: user._id.toString() ,
						incoming: undefined ,
						baseUrl: details.attachment.baseUrl ,
						fullUrl: details.attachment.baseUrl +
							details.attachment.documentId.toString() +
							'/' + details.attachment.id.toString()
					}
				} ) ;
				callback() ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "Replace an attachment" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;
		
		var id = user._id ;
		
		// Link the documents!
		var attachment = user.$.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		var fullUrl = attachment.fullUrl ;
		user.$.setLink( 'file' , attachment ) ;
		//console.error( user.file ) ;
		
		expect( user.file ).to.eql( {
			filename: 'joke.txt' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'text/plain'
		} ) ;
		
		async.series( [
			function( callback ) {
				attachment.save( callback ) ;
			} ,
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				// Check that the file exist
				expect( function() { fs.accessSync( fullUrl , fs.R_OK ) } ).not.to.throwError() ;
				
				users.get( id , function( error , user_ ) {
					user = user_ ;
					expect( error ).not.to.be.ok() ;
					expect( user ).to.eql( {
						_id: user._id,
						firstName: 'Jilbert',
						lastName: 'Polson' ,
						memberSid: 'Jilbert Polson' ,
						file:{
							filename: 'joke.txt' ,
							id: user.file.id ,	// Unpredictable
							contentType: 'text/plain'
						}
					} ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				user.$.getLink( "file" , function( error , file ) {
					expect( error ).not.to.be.ok() ;
					expect( file ).to.eql( {
						id: user.file.id ,
						filename: 'joke.txt' ,
						contentType: 'text/plain' ,
						collectionName: 'users' ,
						documentId: user._id.toString() ,
						incoming: undefined ,
						baseUrl: file.baseUrl ,
						fullUrl: file.baseUrl + file.documentId.toString() + '/' + file.id.toString()
					} ) ;
					
					file.load( function( error , data ) {
						expect( error ).not.to.be.ok() ;
						expect( data.toString() ).to.be( "grigrigredin menufretin\n" ) ;
						callback() ;
					} ) ;
				} ) ;
			} ,
			function( callback ) {
				var details = user.$.getLinkDetails( "file" ) ;
				expect( details ).to.eql( {
					type: 'attachment' ,
					hostPath: 'file' ,
					schema: {
						optional: true ,
						type: 'attachment'
					} ,
					attachment: {
						id: user.file.id ,
						filename: 'joke.txt' ,
						contentType: 'text/plain' ,
						collectionName: 'users' ,
						documentId: user._id.toString() ,
						incoming: undefined ,
						baseUrl: details.attachment.baseUrl ,
						fullUrl: details.attachment.baseUrl +
							details.attachment.documentId.toString() +
							'/' + details.attachment.id.toString()
					}
				} ) ;
				callback() ;
			} ,
			function( callback ) {
				var attachment = user.$.createAttachment(
					{ filename: 'hello-world.html' , contentType: 'text/html' } ,
					"<html><head></head><body>Hello world!</body></html>\n"
				) ;
				
				user.$.setLink( 'file' , attachment ) ;
				attachment.save( callback ) ;
			} ,
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				// Check that the first file has been deleted
				expect( function() { fs.accessSync( fullUrl , fs.R_OK ) } ).to.throwError() ;
				
				users.get( id , function( error , user_ ) {
					user = user_ ;
					expect( error ).not.to.be.ok() ;
					expect( user ).to.eql( {
						_id: user._id,
						firstName: 'Jilbert',
						lastName: 'Polson' ,
						memberSid: 'Jilbert Polson' ,
						file:{
							filename: 'hello-world.html' ,
							id: user.file.id ,	// Unpredictable
							contentType: 'text/html'
						}
					} ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				user.$.getLink( "file" , function( error , file ) {
					expect( error ).not.to.be.ok() ;
					expect( file ).to.eql( {
						id: user.file.id ,
						filename: 'hello-world.html' ,
						contentType: 'text/html' ,
						collectionName: 'users' ,
						documentId: user._id.toString() ,
						incoming: undefined ,
						baseUrl: file.baseUrl ,
						fullUrl: file.baseUrl + file.documentId.toString() + '/' + file.id.toString()
					} ) ;
					
					// Set the new fullUrl
					fullUrl = file.fullUrl ;
					
					file.load( function( error , data ) {
						expect( error ).not.to.be.ok() ;
						expect( data.toString() ).to.be( "<html><head></head><body>Hello world!</body></html>\n" ) ;
						callback() ;
					} ) ;
				} ) ;
			} ,
			function( callback ) {
				// Check that the new file exist
				expect( function() { fs.accessSync( fullUrl , fs.R_OK ) } ).not.to.throwError() ;
				
				var details = user.$.getLinkDetails( "file" ) ;
				expect( details ).to.eql( {
					type: 'attachment' ,
					hostPath: 'file' ,
					schema: {
						optional: true ,
						type: 'attachment'
					} ,
					attachment: {
						id: user.file.id ,
						filename: 'hello-world.html' ,
						contentType: 'text/html' ,
						collectionName: 'users' ,
						documentId: user._id.toString() ,
						incoming: undefined ,
						baseUrl: details.attachment.baseUrl ,
						fullUrl: details.attachment.baseUrl +
							details.attachment.documentId.toString() +
							'/' + details.attachment.id.toString()
					}
				} ) ;
				callback() ;
			} ,
		] )
		.exec( done ) ;
	} ) ;
	
	it( "Delete an attachment" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;
		
		var id = user._id ;
		
		// Link the documents!
		var attachment = user.$.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		var fullUrl = attachment.fullUrl ;
		user.$.setLink( 'file' , attachment ) ;
		//console.error( user.file ) ;
		
		expect( user.file ).to.eql( {
			filename: 'joke.txt' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'text/plain'
		} ) ;
		
		async.series( [
			function( callback ) {
				attachment.save( callback ) ;
			} ,
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				// Check that the file exist
				expect( function() { fs.accessSync( fullUrl , fs.R_OK ) } ).not.to.throwError() ;
				
				users.get( id , function( error , user_ ) {
					user = user_ ;
					expect( error ).not.to.be.ok() ;
					expect( user ).to.eql( {
						_id: user._id,
						firstName: 'Jilbert',
						lastName: 'Polson' ,
						memberSid: 'Jilbert Polson' ,
						file:{
							filename: 'joke.txt' ,
							id: user.file.id ,	// Unpredictable
							contentType: 'text/plain'
						}
					} ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				user.$.getLink( "file" , function( error , file ) {
					expect( error ).not.to.be.ok() ;
					expect( file ).to.eql( {
						id: user.file.id ,
						filename: 'joke.txt' ,
						contentType: 'text/plain' ,
						collectionName: 'users' ,
						documentId: user._id.toString() ,
						incoming: undefined ,
						baseUrl: file.baseUrl ,
						fullUrl: file.baseUrl + file.documentId.toString() + '/' + file.id.toString()
					} ) ;
					
					file.load( function( error , data ) {
						expect( error ).not.to.be.ok() ;
						expect( data.toString() ).to.be( "grigrigredin menufretin\n" ) ;
						callback() ;
					} ) ;
				} ) ;
			} ,
			function( callback ) {
				var details = user.$.getLinkDetails( "file" ) ;
				expect( details ).to.eql( {
					type: 'attachment' ,
					hostPath: 'file' ,
					schema: {
						optional: true ,
						type: 'attachment'
					} ,
					attachment: {
						id: user.file.id ,
						filename: 'joke.txt' ,
						contentType: 'text/plain' ,
						collectionName: 'users' ,
						documentId: user._id.toString() ,
						incoming: undefined ,
						baseUrl: details.attachment.baseUrl ,
						fullUrl: details.attachment.baseUrl +
							details.attachment.documentId.toString() +
							'/' + details.attachment.id.toString()
					}
				} ) ;
				callback() ;
			} ,
			function( callback ) {
				user.$.setLink( 'file' , null ) ;
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , user_ ) {
					user = user_ ;
					expect( error ).not.to.be.ok() ;
					expect( user ).to.eql( {
						_id: user._id,
						firstName: 'Jilbert',
						lastName: 'Polson' ,
						memberSid: 'Jilbert Polson' ,
						file: null
					} ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				user.$.getLink( "file" , function( error , file ) {
					expect( error ).to.be.ok() ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				var details = user.$.getLinkDetails( "file" ) ;
				expect( details ).to.eql( {
					type: 'attachment',
					attachment: null
				} ) ;
				
				// Finally, check that the file has been deleted
				expect( function() { fs.accessSync( fullUrl , fs.R_OK ) } ).to.throwError() ;
				callback() ;
			} ,
		] )
		.exec( done ) ;
	} ) ;
} ) ;



describe( "Caching with the memory model" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "should get a document from a Memory Model cache" , function( done ) {
		
		var mem = world.createMemoryModel() ;
		
		var rawUser = {
			_id: '123456789012345678901234' , 
			firstName: 'John' ,
			lastName: 'McGregor'
		} ;
		
		mem.add( 'users' , rawUser ) ;
		
		async.series( [
			function( callback ) {
				users.get( rawUser._id , { cache: mem } , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ; 
					expect( error ).not.to.be.ok() ;
					expect( user.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user ).to.eql( { _id: rawUser._id , firstName: 'John' , lastName: 'McGregor' } ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "should multiGet all documents from a Memory Model cache (complete cache hit)" , function( done ) {
		
		var mem = world.createMemoryModel() ;
		
		someUsers = [
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
				
				users.multiGet( ids , { cache: mem } , function( error , batch ) {
					var i , map = {} ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'Batch:' , batch ) ; 
					expect( error ).not.to.be.ok() ;
					expect( batch.$ ).to.be.a( rootsDb.BatchWrapper ) ;
					
					batch.sort( function( a , b ) {
						return parseInt( a._id.toString() , 10 ) - parseInt( b._id.toString() , 10 ) ;
					} ) ;
					
					expect( batch ).to.eql( [
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
	
	it( "should multiGet some document from a Memory Model cache (partial cache hit)" , function( done ) {
		
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
				
				users.multiGet( ids , { cache: mem } , function( error , batch ) {
					var i , map = {} ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'Batch:' , batch ) ; 
					expect( error ).not.to.be.ok() ;
					expect( batch.$ ).to.be.a( rootsDb.BatchWrapper ) ;
					
					batch.sort( function( a , b ) {
						return parseInt( a._id.toString() , 10 ) - parseInt( b._id.toString() , 10 ) ;
					} ) ;
					
					expect( batch ).to.eql( [
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

	

describe( "Locks" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "should lock a document (create, save, lock, retrieve, lock, retrieve)" , function( done ) {
		
		var lockable = lockables.createDocument( {
			data: 'something' ,
		} ) ;
		
		var id = lockable._id ;
		var lockId ;
		
		async.series( [
			function( callback ) {
				lockable.$.save( callback ) ;
			} ,
			function( callback ) {
				lockables.get( id , function( error , lockable ) {
					expect( error ).not.to.be.ok() ;
					expect( lockable ).to.eql( { _id: lockable._id , data: 'something' , _lockedBy: null , _lockedAt: null } ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				lockable.$.lock( function( error , locked , lockId_ ) {
					expect( error ).not.to.be.ok() ;
					expect( locked ).to.be.ok() ;
					expect( lockId_ ).to.be.an( mongodb.ObjectID ) ;
					lockId = lockId_ ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				lockables.get( id , function( error , lockable ) {
					expect( error ).not.to.be.ok() ;
					//log.warning( 'lockable: %J' , lockable ) ;
					expect( lockable._lockedBy ).to.eql( lockId ) ;
					expect( lockable._lockedAt ).to.be.ok() ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				lockable.$.lock( function( error , locked , lockId_ ) {
					expect( error ).not.to.be.ok() ;
					expect( locked ).not.to.be.ok() ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				lockables.get( id , function( error , lockable ) {
					expect( error ).not.to.be.ok() ;
					//log.warning( 'lockable: %J' , lockable ) ;
					expect( lockable._lockedBy ).to.eql( lockId ) ;
					expect( lockable._lockedAt ).to.be.ok() ;
					callback() ;
				} ) ;
			} ,
		] )
		.exec( done ) ;
	} ) ;
	
	it( "should perform a 'lockRetrieveRelease': lock, retrieve locked document, then release locks" , function( done ) {
		
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
				lockables.lockRetrieveRelease( { data: { $in: [ 'one' , 'two' ] } } , function( error , batch ) {
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
				lockables.lockRetrieveRelease( { data: { $in: [ 'one' , 'two' , 'three' ] } } , function( error , batch ) {
					expect( error ).not.to.be.ok() ;
					//console.log( batch ) ;
					expect( batch ).to.have.length( 1 ) ;
					expect( batch[ 0 ].data ).to.be( 'three' ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				lockables.lockRetrieveRelease( { data: { $in: [ 'one' , 'two' , 'three' ] } } , function( error , batch ) {
					expect( error ).not.to.be.ok() ;
					//console.log( batch ) ;
					expect( batch ).to.have.length( 0 ) ;
					setTimeout( callback , 50 ) ;
				} ) ;
			} ,
			function( callback ) {
				lockables.lockRetrieveRelease( { data: { $in: [ 'one' , 'two' , 'three' ] } } , function( error , batch , releaseFn ) {
					expect( error ).not.to.be.ok() ;
					//console.log( batch ) ;
					expect( batch ).to.have.length( 3 ) ;
					var keys = batch.map( mapper ) ;
					expect( keys ).to.contain( 'one' ) ;
					expect( keys ).to.contain( 'two' ) ;
					expect( keys ).to.contain( 'three' ) ;
					releaseFn( callback ) ;
				} ) ;
			} ,
			function( callback ) {
				lockables.lockRetrieveRelease( { data: { $in: [ 'one' , 'two' , 'three' ] } } , function( error , batch , releaseFn ) {
					expect( error ).not.to.be.ok() ;
					//console.log( batch ) ;
					expect( batch.length ).to.be( 3 ) ;
					var keys = batch.map( mapper ) ;
					expect( keys ).to.contain( 'one' ) ;
					expect( keys ).to.contain( 'two' ) ;
					expect( keys ).to.contain( 'three' ) ;
					callback() ;
				} ) ;
			} ,
		] )
		.exec( done ) ;
	} ) ;
} ) ;



describe( "Extended DocumentWrapper" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "should call a method of the extended Document wrapper at creation and after retrieving it from DB" , function( done ) {
		
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
				extendables.get( id , function( error , ext ) {
					expect( error ).not.to.be.ok() ;
					expect( ext.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( ext.$ ).to.be.an( Extended ) ;
					expect( ext ).to.eql( { _id: ext._id , data: 'sOmeDaTa' } ) ;
					expect( ext.$.getNormalized() ).to.be( 'somedata' ) ;
					ext.data = 'mOreVespEnEGaS' ;
					expect( ext.$.getNormalized() ).to.be( 'morevespenegas' ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "should call a method of the extended Batch wrapper at creation and after retrieving it from DB" , function( done ) {
		
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
				extendables.collect( {} , function( error , exts ) {
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



describe( "Memory model" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "should create a memoryModel, retrieve documents with 'populate' on 'link' and 'back-link', with the 'memory' options and effectively save them in the memoryModel" , function( done ) {
		
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
				
				users.collect( {} , options , function( error , users_ ) {
					
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
					expect( doc ).to.eql( {
						_id: user._id,
						firstName: 'Jilbert',
						lastName: 'Polson',
						memberSid: 'Jilbert Polson',
						job: {
							_id: job._id,
							title: 'developer',
							salary: 60000,
							users: [],
							schools: []
						}
					} ) ;
					
					doc = memory.collections.users.documents[ user2._id.toString() ] ;
					expect( doc ).to.eql( {
						_id: user2._id,
						firstName: 'Pat',
						lastName: 'Mulligan',
						memberSid: 'Pat Mulligan',
						job: {
							_id: job._id,
							title: 'developer',
							salary: 60000,
							users: [],
							schools: []
						}
					} ) ;
					
					doc = memory.collections.users.documents[ user3._id.toString() ] ;
					expect( doc ).to.eql( {
						_id: user3._id,
						firstName: 'Bill',
						lastName: 'Baroud',
						memberSid: 'Bill Baroud',
						job: {
							_id: job2._id,
							title: 'adventurer',
							salary: 200000,
							users: [],
							schools: []
						}
					} ) ;
					
					doc = memory.collections.jobs.documents[ job._id.toString() ] ;
					expect( doc ).to.eql( {
						_id: job._id,
						title: 'developer',
						salary: 60000,
						users: [],
						schools: []
					} ) ;
					
					doc = memory.collections.jobs.documents[ job2._id.toString() ] ;
					expect( doc ).to.eql( {
						_id: job2._id,
						title: 'adventurer',
						salary: 200000,
						users: [],
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
				jobs.collect( {} , options , function( error , jobs_ ) {
					
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
					expect( doc ).to.eql( {
						_id: user._id,
						firstName: 'Jilbert',
						lastName: 'Polson',
						memberSid: 'Jilbert Polson',
						job: memory.collections.jobs.documents[ job._id.toString() ]
					} ) ;
					
					doc = memory.collections.users.documents[ user2._id.toString() ] ;
					expect( doc ).to.eql( {
						_id: user2._id,
						firstName: 'Pat',
						lastName: 'Mulligan',
						memberSid: 'Pat Mulligan',
						job: memory.collections.jobs.documents[ job._id.toString() ]
					} ) ;
					
					doc = memory.collections.users.documents[ user3._id.toString() ] ;
					expect( doc ).to.eql( {
						_id: user3._id,
						firstName: 'Bill',
						lastName: 'Baroud',
						memberSid: 'Bill Baroud',
						job: memory.collections.jobs.documents[ job2._id.toString() ]
					} ) ;
					
					doc = memory.collections.jobs.documents[ job._id.toString() ] ;
					if ( doc.users[ 0 ].firstName === 'Pat' ) { doc.users = [ doc.users[ 1 ] , doc.users[ 0 ] ] ; }
					expect( doc ).to.eql( {
						_id: job._id,
						title: 'developer',
						salary: 60000,
						schools: [],
						users: [
							memory.collections.users.documents[ user._id.toString() ] ,
							memory.collections.users.documents[ user2._id.toString() ]
						]
					} ) ;
					
					doc = memory.collections.jobs.documents[ job2._id.toString() ] ;
					expect( doc ).to.eql( {
						_id: job2._id,
						title: 'adventurer',
						salary: 200000,
						schools: [],
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
				
				users.collect( {} , options , function( error , users_ ) {
					// This is the same query already performed on user.
					// We just check populate Depth and Queries here: a total cache hit should happen!
					expect( options.populateDepth ).not.to.be.ok() ;
					expect( options.populateDbQueries ).not.to.be.ok() ;
					
					callback() ;
				} ) ;
			} ,
		] )
		.exec( done ) ;
	} ) ;
	
	it( "should also works with multi-link" , function( done ) {
		
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
				
				schools.collect( {} , options , function( error , schools_ ) {
					
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
					expect( doc ).to.eql( {
						_id: school1._id,
						title: 'Computer Science' ,
						jobs: [
							{
								_id: job1._id,
								title: 'developer',
								salary: 60000,
								users: [],
								schools: []
							} ,
							{
								_id: job2._id,
								title: 'sysadmin',
								salary: 55000,
								users: [],
								schools: []
							} ,
							{
								_id: job3._id,
								title: 'front-end developer',
								salary: 54000,
								users: [],
								schools: []
							}
						]
					} ) ;
					
					doc = memory.collections.schools.documents[ school2._id.toString() ] ;
					expect( doc ).to.eql( {
						_id: school2._id,
						title: 'Web Academy' ,
						jobs: [
							{
								_id: job1._id,
								title: 'developer',
								salary: 60000,
								users: [],
								schools: []
							} ,
							{
								_id: job3._id,
								title: 'front-end developer',
								salary: 54000,
								users: [],
								schools: []
							} ,
							{
								_id: job4._id,
								title: 'designer',
								salary: 56000,
								users: [],
								schools: []
							}
						]
					} ) ;
					
					doc = memory.collections.jobs.documents[ job1._id.toString() ] ;
					expect( doc ).to.eql( {
						_id: job1._id,
						title: 'developer',
						salary: 60000,
						users: [],
						schools: []
					} ) ;
					
					doc = memory.collections.jobs.documents[ job2._id.toString() ] ;
					expect( doc ).to.eql( {
						_id: job2._id,
						title: 'sysadmin',
						salary: 55000,
						users: [],
						schools: []
					} ) ;
					
					doc = memory.collections.jobs.documents[ job3._id.toString() ] ;
					expect( doc ).to.eql( {
						_id: job3._id,
						title: 'front-end developer',
						salary: 54000,
						users: [],
						schools: []
					} ) ;
					
					doc = memory.collections.jobs.documents[ job4._id.toString() ] ;
					expect( doc ).to.eql( {
						_id: job4._id,
						title: 'designer',
						salary: 56000,
						users: [],
						schools: []
					} ) ;
					
					expect( options.populateDepth ).to.be( 1 ) ;
					expect( options.populateDbQueries ).to.be( 1 ) ;
					
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				options = { populate: 'jobs' , memory: memory } ;
				
				schools.collect( {} , options , function( error , schools_ ) {
					
					// This is the same query already performed.
					// We just check populate Depth and Queries here: a total cache hit should happen!
					expect( options.populateDepth ).not.to.be.ok() ;
					expect( options.populateDbQueries ).not.to.be.ok() ;
					
					callback() ;
				} ) ;
			} ,
		] )
		.exec( done ) ;
	} ) ;
	
	it( "incremental population should work as expected" , function( done ) {
		
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
				users.get( user._id , options , function( error , user_ ) {
					expect( error ).not.to.be.ok() ;
					expect( user_ ).to.eql( {
						_id: user._id,
						firstName: 'Jilbert',
						lastName: 'Polson' ,
						memberSid: 'Jilbert Polson',
						job: job._id 
					} ) ;
					expect( options.populateDepth ).not.to.be.ok() ;
					expect( options.populateDbQueries ).not.to.be.ok() ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				options = { memory: memory , populate: 'job' } ;
				users.get( user._id , options , function( error , user_ ) {
					expect( error ).not.to.be.ok() ;
					expect( user_ ).to.eql( {
						_id: user._id,
						firstName: 'Jilbert',
						lastName: 'Polson' ,
						memberSid: 'Jilbert Polson',
						job: {
							_id: job._id ,
							title: 'developer' ,
							salary: 60000,
							users: [],
							schools: []
						}
					} ) ;
					expect( user_.job.$.populated.users ).not.to.be.ok() ;
					expect( memory.collections.jobs.documents[ job._id.toString() ] ).to.eql( {
						_id: job._id ,
						title: 'developer' ,
						salary: 60000,
						users: [],
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
				users.get( user._id , options , function( error , user_ ) {
					expect( error ).not.to.be.ok() ;
					expect( user_.$.populated.job ).to.be( true ) ;
					
					expect( user_.job.users ).to.have.length( 2 ) ;
					
					if ( user_.job.users[ 0 ].firstName === 'Robert' )
					{
						user_.job.users = [ user_.job.users[ 1 ] , user_.job.users[ 0 ] ] ;
					}
					
					expect( user_.job.users[ 0 ].job ).to.be( user_.job ) ;
					expect( user_.job.users[ 1 ].job ).to.be( user_.job ) ;
					
					expect( user_ ).to.eql( {
						_id: user._id,
						firstName: 'Jilbert',
						lastName: 'Polson' ,
						memberSid: 'Jilbert Polson',
						job: {
							_id: job._id ,
							title: 'developer' ,
							salary: 60000,
							schools: [],
							users: [
								user_ ,
								// We cannot use 'user2', expect.js is too confused with Circular references
								// We have to perform a second check for that
								user_.job.users[ 1 ]
							]
						}
					} ) ;
					
					expect( user_.job.users[ 1 ] ).to.eql( {
						_id: user2._id,
						firstName: 'Robert',
						lastName: 'Polson' ,
						memberSid: 'Robert Polson',
						job: {
							_id: job._id ,
							title: 'developer' ,
							salary: 60000,
							schools: [],
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



describe( "Hooks" , function() {
	
	it( "'beforeCreateDocument'" ) ;
	it( "'afterCreateDocument'" ) ;
} ) ;



describe( "Historical bugs" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "collect on empty collection with populate (was throwing uncaught error)" , function( done ) {
		
		async.series( [
			function( callback ) {
				users.collect( {} , { populate: [ 'job' , 'godfather' ] } , function( error , batch ) {
					expect( error ).not.to.be.ok() ;
					expect( batch ).to.eql( [] ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "setting garbage to an attachment property should abort with an error" , function( done ) {
		
		var user , id ;
		
		// First try: at object creation
		expect( function() {
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
		expect( function() { user.$.setLink( 'file' , 'garbage' ) ; } ).to.throwError() ;
		expect( user.file ).to.be( undefined ) ;
		
		// third try: by setting the property directly
		user.file = 'garbage' ;
		expect( function() { user.$.validate() ; } ).to.throwError() ;
		
		// By default, a collection has the 'patchDrivenValidation' option, so we have to stage the change
		// to trigger validation on .save()
		user.$.stage( 'file' ) ;
		
		async.series( [
			function( callback ) {
				user.$.save( function( error ) {
					expect( error ).to.be.ok() ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , user_ ) {
					user = user_ ;
					expect( error ).to.be.ok() ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
} ) ;
