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

var hash = require( 'hash-kit' ) ;
var string = require( 'string-kit' ) ;
var tree = require( 'tree-kit' ) ;
var async = require( 'async-kit' ) ;
var ErrorStatus = require( 'error-status' ) ;
var doormen = require( 'doormen' ) ;

var expect = require( 'expect.js' ) ;



// Create the world...
var world = rootsDb.World() ;

// Collections...
var users , jobs , towns ;

var usersDescriptor = {
	url: 'mongodb://localhost:27017/test/users' ,
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
		godfatherId: { optional: true } ,
		jobId: { optional: true } ,
		memberSid: {
			optional: true ,
			type: 'string' ,
			maxLength: 30
		}
	} ,
	meta: {
		godfather: { type: 'link' , collection: 'users' , property: 'godfatherId' } ,
		job: { type: 'link' , collection: 'jobs' , property: 'jobId' }
	} ,
	indexes: [
		{ properties: { jobId: 1 } } ,
		{ properties: { jobId: 1 , memberSid: 1 } , unique: true }
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
	url: 'mongodb://localhost:27017/test/jobs' ,
	properties: {
		title: {
			type: 'string' ,
			maxLength: 50 ,
			default: 'unemployed'
		} ,
		salary: {
			type: 'integer' ,
			default: 0
		}
	} ,
	meta: {
		members: { type: 'backlink' , collection: 'users' , property: 'jobId' }
	}
} ;

var townsDescriptor = {
	url: 'mongodb://localhost:27017/test/towns' ,
	properties: {
		name: { type: 'string' } ,
		meta: {
			type: 'strictObject',
			default: {}
		}
	} ,
	meta: {
	} ,
	indexes: [
		{ properties: { name: 1 , "meta.country": 1 } , unique: true }
	]
} ;





			/* Utils */



// clear DB: remove every item, so we can safely test
function clearDB( callback )
{
	async.parallel( [
		[ clearCollection , users ] ,
		[ clearCollection , jobs ] ,
		[ clearCollection , towns ]
	] )
	.exec( callback ) ;
}



// clear DB: remove every item, so we can safely test
function clearDBIndexes( callback )
{
	async.parallel( [
		[ clearCollectionIndexes , users ] ,
		[ clearCollectionIndexes , jobs ] ,
		[ clearCollectionIndexes , towns ]
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
		collection.driver.raw.dropIndexes( callback ) ;
	} ) ;
}





			/* Tests */



// Force creating the collection
before( function( done ) {
	
	jobs = world.createCollection( 'jobs' , jobsDescriptor ) ;
	expect( jobs ).to.be.a( rootsDb.Collection ) ;
	
	users = world.createCollection( 'users' , usersDescriptor ) ;
	expect( users ).to.be.a( rootsDb.Collection ) ;
	
	towns = world.createCollection( 'towns' , townsDescriptor ) ;
	expect( towns ).to.be.a( rootsDb.Collection ) ;
	
	done() ;
} ) ;



describe( "Build collections' indexes" , function() {
	
	beforeEach( clearDBIndexes ) ;
	
	it( "should build indexes" , function( done ) {
		
		expect( users.uniques ).to.be.eql( [ [ '_id' ], [ 'jobId', 'memberSid' ] ] ) ;
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
		expect( users.createFingerprint( { jobId: '123456789012345678901234' } ).$.unique ).to.be( false ) ;
		expect( users.createFingerprint( { memberSid: 'terry-bogard' } ).$.unique ).to.be( false ) ;
		expect( users.createFingerprint( { jobId: '123456789012345678901234', memberSid: 'terry-bogard' } ).$.unique ).to.be( true ) ;
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
		user.jobId = job._id ;
		
		async.series( [
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				job.$.save( callback ) ;
			} ,
			function( callback ) {
				users.getUnique( { memberSid: memberSid , jobId: job._id } , function( error , u ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ;
					expect( error ).not.to.be.ok() ;
					expect( u.$ ).to.be.a( rootsDb.DocumentWrapper ) ;
					expect( u._id ).to.be.an( mongodb.ObjectID ) ;
					expect( u._id ).to.eql( id ) ;
					expect( u ).to.eql( tree.extend( null , { _id: user._id , jobId: job._id , firstName: 'Bill' , lastName: "Cut'throat" , memberSid: "Bill Cut'throat" } ) ) ;
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



describe( "Collect & find batchs" , function() {
	
	beforeEach( clearDB ) ;
	
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



describe( "Hooks" , function() {
	
	it( "'beforeCreateDocument'" ) ;
	it( "'afterCreateDocument'" ) ;
} ) ;




