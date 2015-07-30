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



var odm = require( '../lib/odm.js' ) ;
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
var world = odm.World() ;

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
			function( document ) {
				//console.log( "- Users afterCreateDocument 'after' hook -" ) ;
				document.$.memberSid = '' + document.$.firstName + ' ' + document.$.lastName ;
			}
		//]
	} ,
	useMemProxy: true
} ;

var expectedDefaultUser = { firstName: 'Joe', lastName: 'Doe' , godfatherId: undefined , jobId: undefined , memberSid: 'Joe Doe' } ;

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
	} ,
	useMemProxy: true
} ;

var townsDescriptor = {
	url: 'mongodb://localhost:27017/test/towns' ,
	properties: {
		name: { type: 'string' } ,
		meta: {
			type: 'object',
			default: {}
		}
	} ,
	meta: {
	} ,
	indexes: [
		{ properties: { name: 1 , "meta.country": 1 } , unique: true }
	] ,
	useMemProxy: true
} ;





			/* Utils */



// it flatten prototype chain, so a single object owns every property of its parents
var protoflatten = tree.extend.bind( undefined , { deep: true , deepFilter: { blacklist: [ mongodb.ObjectID.prototype ] } } , null ) ;



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
	expect( jobs ).to.be.a( odm.Collection ) ;
	
	users = world.createCollection( 'users' , usersDescriptor ) ;
	expect( users ).to.be.a( odm.Collection ) ;
	
	towns = world.createCollection( 'towns' , townsDescriptor ) ;
	expect( towns ).to.be.a( odm.Collection ) ;
	
	done() ;
} ) ;



describe( "Document creation" , function() {
	
	it( "should create a document with default values" , function() {
		
		var user = users.createDocument() ;
		
		expect( user ).to.be.an( odm.Document ) ;
		expect( users.useMemProxy ).to.be.ok() ;
		expect( user.useMemProxy ).to.be.ok() ;
		expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
		expect( tree.extend( null , {} , user.$ ) ).to.eql( tree.extend( null , { _id: user.$._id } , expectedDefaultUser ) ) ;
	} ) ;
	
	it( "should create a document using the given correct values" , function() {
		
		var user = users.createDocument( {
			firstName: 'Bobby',
			lastName: 'Fischer'
		} ) ;
		
		expect( user ).to.be.an( odm.Document ) ;
		expect( users.useMemProxy ).to.be.ok() ;
		expect( user.useMemProxy ).to.be.ok() ;
		expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
		expect( tree.extend( null , {} , user.$ ) ).to.eql( {
			_id: user.$._id ,
			firstName: 'Bobby' ,
			lastName: 'Fischer' ,
			memberSid: 'Bobby Fischer' ,
			godfatherId: undefined ,
			jobId: undefined
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



describe( "Fingerprint" , function() {
	
	it( "should create a fingerprint" , function() {
		
		var f = users.createFingerprint( { firstName: 'Terry' } ) ;
		
		expect( f ).to.be.an( odm.Fingerprint ) ;
		expect( tree.extend( null , {} , f.$ ) ).to.eql( { firstName: 'Terry' } ) ;
	} ) ;
	
	it( "should detect uniqueness correctly" , function() {
		
		expect( users.createFingerprint( { _id: 'somehash' } ).unique ).to.be( true ) ;
		expect( users.createFingerprint( { firstName: 'Terry' } ).unique ).to.be( false ) ;
		expect( users.createFingerprint( { firstName: 'Terry', lastName: 'Bogard' } ).unique ).to.be( false ) ;
		expect( users.createFingerprint( { _id: 'somehash', firstName: 'Terry', lastName: 'Bogard' } ).unique ).to.be( true ) ;
		expect( users.createFingerprint( { jobId: 'somehash' } ).unique ).to.be( false ) ;
		expect( users.createFingerprint( { memberSid: 'terry-bogard' } ).unique ).to.be( false ) ;
		expect( users.createFingerprint( { jobId: 'somehash', memberSid: 'terry-bogard' } ).unique ).to.be( true ) ;
	} ) ;
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



describe( "ID creation" , function() {
	
	it( "should create ID (like Mongo ID)" , function() {
		
		expect( users.createId() ).to.match( /^[0-9a-f]{24}$/ ) ;
		expect( users.createId() ).to.match( /^[0-9a-f]{24}$/ ) ;
		expect( users.createId() ).to.match( /^[0-9a-f]{24}$/ ) ;
		expect( users.createId() ).to.match( /^[0-9a-f]{24}$/ ) ;
		expect( users.createId() ).to.match( /^[0-9a-f]{24}$/ ) ;
	} ) ;
} ) ;



describe( "Get documents" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "should get a document (create, save and retrieve)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'John' ,
			lastName: 'McGregor'
		} ) ;
		
		var id = user.$._id ;
		
		async.series( [
			function( callback ) {
				user.save( callback ) ;
			} ,
			function( callback ) {
				users.get( id , { raw: true } , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ;
					expect( error ).not.to.be.ok() ;
					expect( user ).not.to.be.an( odm.Document ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
					expect( user ).to.eql( tree.extend( null , { _id: user._id , firstName: 'John' , lastName: 'McGregor' , memberSid: 'John McGregor' } ) ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ; 
					expect( error ).not.to.be.ok() ;
					expect( user ).to.be.an( odm.Document ) ;
					expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user.$._id ).to.eql( id ) ;
					expect( tree.extend( null , {} , user.$ ) ).to.eql( tree.extend( null , { _id: user.$._id } , expectedDefaultUser , { firstName: 'John' , lastName: 'McGregor' , memberSid: 'John McGregor' } ) ) ;
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



describe( "Get documents by unique fingerprint" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "should get a document (create, save and retrieve)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Bill' ,
			lastName: "Cut'throat"
		} , { useMemProxy: false } ) ;
		
		var id = user.$._id ;
		var memberSid = user.$.memberSid ;
		
		var job = jobs.createDocument() ;
		var jobId = job.id ;
		user.$.job = job ;
		
		async.series( [
			function( callback ) {
				user.save( callback ) ;
			} ,
			function( callback ) {
				job.save( callback ) ;
			} ,
			function( callback ) {
				users.getUnique( { memberSid: memberSid , jobId: jobId } , { raw: true } , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ;
					expect( error ).not.to.be.ok() ;
					expect( user ).not.to.be.an( odm.Document ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
					expect( user ).to.eql( tree.extend( null , { _id: user._id , jobId: jobId , firstName: 'Bill' , lastName: "Cut'throat" , memberSid: "Bill Cut'throat" } ) ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				users.getUnique( { memberSid: memberSid , jobId: jobId } , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ; 
					expect( error ).not.to.be.ok() ;
					expect( user ).to.be.an( odm.Document ) ;
					expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user.$._id ).to.eql( id ) ;
					expect( tree.extend( null , {} , user.$ ) ).to.eql( tree.extend( null , { _id: user.$._id } , expectedDefaultUser , { jobId: jobId , firstName: 'Bill' , lastName: "Cut'throat" , memberSid: "Bill Cut'throat" } ) ) ;
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
		} , { useMemProxy: false } ) ;
		
		async.series( [
			function( callback ) {
				user.save( callback ) ;
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



describe( "Save/update documents" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "should save correctly and only non-default value are registered into the upstream (create, save and retrieve)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Jack'
		} ) ;
		
		var id = user.$._id ;
		
		async.series( [
			function( callback ) {
				user.save( callback ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ; 
					expect( error ).not.to.be.ok() ;
					expect( user ).to.be.an( odm.Document ) ;
					expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user.$._id ).to.eql( id ) ;
					expect( tree.extend( null , {} , user.$ ) ).to.eql( tree.extend( null , { _id: user.$._id } , expectedDefaultUser , { firstName: 'Jack' , lastName: 'Doe' , memberSid: 'Jack Doe' } ) ) ;
					
					// upstream should not contains lastName
					// since doormen integration: it SHOULD!
					//expect( tree.extend( { own: true } , {} , user.upstream ) ).to.eql( { _id: user.$._id , firstName: 'Jack' , memberSid: 'Jack Doe' } ) ;
					
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				users.get( id , { raw: true } , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ; 
					expect( error ).not.to.be.ok() ;
					expect( user ).not.to.be.an( odm.Document ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
					
					// upstream should not contains lastName
					// since doormen integration: it SHOULD!
					//expect( user ).to.eql( { _id: user._id , firstName: 'Jack' , memberSid: 'Jack Doe' } ) ;
					
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "should save creating a minimalistic patch so parallel save do not overwrite each others (create, save, retrieve, patch² and retrieve)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Johnny' ,
			lastName: 'Starks'
		} ) ;
		
		var id = user.$._id ;
		var user2 ;
		//id = users.createDocument()._id ;
		
		
		async.series( [
			function( callback ) {
				user.save( callback ) ;
			} ,
			function( callback ) {
				users.get( id , { useMemProxy: false } , function( error , u ) {
					user2 = u ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user2 ) ; 
					expect( error ).not.to.be.ok() ;
					expect( user2 ).to.be.an( odm.Document ) ;
					expect( user2.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user2.$._id ).to.eql( id ) ;
					expect( tree.extend( null , {} , user2.$ ) ).to.eql( tree.extend( null , { _id: user2.$._id } , expectedDefaultUser , { firstName: 'Johnny' , lastName: 'Starks' , memberSid: 'Johnny Starks' } ) ) ;
					
					// upstream should not contains lastName
					expect( tree.extend( { own: true } , {} , user.upstream ) ).to.eql( { _id: user.$._id , firstName: 'Johnny' , lastName: 'Starks' , memberSid: 'Johnny Starks' } ) ;
					
					callback() ;
				} ) ;
			} ,
			async.parallel( [
				function( callback ) {
					user.$.lastName = 'Smith' ;
					user.save( callback ) ;
				} ,
				function( callback ) {
					user2.$.firstName = 'Joey' ;
					user2.save( callback ) ;
				}
			] ) ,
			function( callback ) {
				users.get( id , { useMemProxy: false } , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ; 
					expect( error ).not.to.be.ok() ;
					expect( user ).to.be.an( odm.Document ) ;
					expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user.$._id ).to.eql( id ) ;
					expect( tree.extend( null , {} , user.$ ) ).to.eql( tree.extend( null , { _id: user.$._id } , expectedDefaultUser , { firstName: 'Joey' , lastName: 'Smith' , memberSid: 'Johnny Starks' } ) ) ;
					
					// upstream should not contains lastName
					expect( tree.extend( { own: true } , {} , user.upstream ) ).to.eql( { _id: user.$._id , firstName: 'Joey' , lastName: 'Smith' , memberSid: 'Johnny Starks' } ) ;
					
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				users.get( id , { raw: true , useMemProxy: false } , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ; 
					expect( error ).not.to.be.ok() ;
					expect( user ).not.to.be.an( odm.Document ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
					
					// upstream should not contains lastName
					expect( user ).to.eql( { _id: user._id , firstName: 'Joey' , lastName: 'Smith' , memberSid: 'Johnny Starks' } ) ;
					
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
		
		var id = user.$._id ;
		var user2 ;
		
		
		async.series( [
			function( callback ) {
				user.save( callback ) ;
			} ,
			function( callback ) {
				users.get( id , { useMemProxy: false } , function( error , u ) {
					user2 = u ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user2 ) ; 
					expect( error ).not.to.be.ok() ;
					expect( user2 ).to.be.an( odm.Document ) ;
					expect( user2.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user2.$._id ).to.eql( id ) ;
					expect( tree.extend( null , {} , user2.$ ) ).to.eql( tree.extend( null , { _id: user2.$._id } , expectedDefaultUser , { firstName: 'Johnny B.' , lastName: 'Starks' , memberSid: 'Johnny B. Starks' } ) ) ;
					
					// upstream should not contains lastName
					expect( tree.extend( { own: true } , {} , user.upstream ) ).to.eql( { _id: user.$._id , firstName: 'Johnny B.' , lastName: 'Starks' , memberSid: 'Johnny B. Starks' } ) ;
					
					callback() ;
				} ) ;
			} ,
			async.parallel( [
				function( callback ) {
					user.$.lastName = 'Smith' ;
					user.save( { fullSave: true } , callback ) ;
				} ,
				function( callback ) {
					user2.$.firstName = 'Joey' ;
					user2.save( { fullSave: true } , callback ) ;
				}
			] ) ,
			function( callback ) {
				users.get( id , { useMemProxy: false } , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ; 
					expect( error ).not.to.be.ok() ;
					expect( user ).to.be.an( odm.Document ) ;
					expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user.$._id ).to.eql( id ) ;
					expect( tree.extend( null , {} , user.$ ) ).to.eql( tree.extend( null , { _id: user.$._id } , expectedDefaultUser , { firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny B. Starks' } ) ) ;
					
					// upstream should not contains lastName
					expect( tree.extend( { own: true } , {} , user.upstream ) ).to.eql( { _id: user.$._id , firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny B. Starks' } ) ;
					
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				users.get( id , { raw: true , useMemProxy: false } , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ; 
					expect( error ).not.to.be.ok() ;
					expect( user ).not.to.be.an( odm.Document ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
					
					// upstream should not contains lastName
					expect( user ).to.eql( { _id: user._id , firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny B. Starks' } ) ;
					
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
} ) ;



describe( "Delete documents" , function() {
	
	it( "should delete a document (create, save, retrieve, then delete it so it cannot be retrieved again)" , function( done ) {
		
		var user = users.createDocument() ;
		expect( user.useMemProxy ).to.be.ok() ;
		
		//console.log( user ) ;
		var id = user.$._id ;
		//id = users.createDocument()._id ;
		user.$.firstName = 'John' ;
		user.$.lastName = 'McGregor' ;
		
		async.series( [
			function( callback ) {
				user.save( callback ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ; 
					expect( error ).not.to.be.ok() ;
					expect( user.useMemProxy ).to.be.ok() ;
					expect( user ).to.be.an( odm.Document ) ;
					expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user.$._id ).to.eql( id ) ;
					expect( tree.extend( null , {} , user.$ ) ).to.eql( tree.extend( null , { _id: user.$._id } , expectedDefaultUser , { firstName: 'John' , lastName: 'McGregor' } ) ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				user.delete( function( error ) {
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



describe( "Suspects and revealing" , function() {
	
	it( "Synchronous 'get()' should provide an 'identified' suspect, and reveal it later" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Dilbert' ,
			lastName: 'Dugommier'
		} , { useMemProxy: false } ) ;
		
		var id = user.$._id ;
		
		expect( user.state() ).to.equal( 'app-side' ) ;
		
		async.series( [
			function( callback ) {
				user.save( callback ) ;
			} ,
			function( callback ) {
				var user = users.get( id ) ;
				expect( user ).to.be.an( odm.Document ) ;
				expect( user.suspected ).to.be.ok() ;
				expect( user.loaded ).not.to.be.ok() ;
				expect( user.upstreamExists ).not.to.be.ok() ;
				expect( user.state() ).to.equal( 'suspected' ) ;
				expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
				expect( user.$._id ).to.eql( id ) ;
				expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: id, firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
				
				user.reveal( function( error ) {
					expect( user ).to.be.an( odm.Document ) ;
					expect( user.suspected ).not.to.be.ok() ;
					expect( user.loaded ).to.be.ok() ;
					expect( user.upstreamExists ).to.be.ok() ;
					//delete user.$._id ;
					//console.log( '----------------------' , Object.keys( user.$ ) ) ;
					expect( Object.keys( user.$ ).length ).to.equal( 0 ) ;
					expect( user.state() ).to.equal( 'synced' ) ;
					expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user.$._id ).to.eql( id ) ;
					expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: id, firstName: 'Dilbert', lastName: 'Dugommier', jobId: undefined, godfatherId: undefined , memberSid: 'Dilbert Dugommier' } ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "Synchronous 'get()' should provide a suspect with a bad identity, then reveal it as nothing" , function( done ) {
		
		var id = new mongodb.ObjectID() ;
		var user = users.get( id ) ;
		
		expect( user ).to.be.an( odm.Document ) ;
		expect( user.suspected ).to.be.ok() ;
		expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
		expect( user.$._id ).to.eql( id ) ;
		expect( user.loaded ).not.to.be.ok() ;
		expect( user.upstreamExists ).not.to.be.ok() ;
		expect( user.deleted ).not.to.be.ok() ;
		expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: id, firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
		
		user.reveal( function( error ) {
			expect( user ).to.be.an( odm.Document ) ;
			expect( user.suspected ).not.to.be.ok() ;
			expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user.$._id ).to.eql( id ) ;
			expect( user.loaded ).not.to.be.ok() ;
			expect( user.upstreamExists ).not.to.be.ok() ;
			expect( user.deleted ).to.be.ok() ;
			expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: id, firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
			done() ;
		} ) ;
	} ) ;
	
	it( "Synchronous 'getUnique()' should provide an 'identified' suspect, and reveal it later" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Joe' ,
			lastName: 'Pink'
		} , { useMemProxy: false } ) ;
		
		var id = user.$._id ;
		var memberSid = user.$.memberSid ;
		
		var job = jobs.createDocument() ;
		var jobId = job.id ;
		user.$.job = job ;
		
		expect( user.state() ).to.equal( 'app-side' ) ;
		
		async.series( [
			function( callback ) {
				user.save( callback ) ;
			} ,
			function( callback ) {
				job.save( callback ) ;
			} ,
			function( callback ) {
				var user = users.getUnique( { memberSid: memberSid , jobId: jobId } ) ;
				expect( user ).to.be.an( odm.Document ) ;
				expect( user.suspected ).to.be.ok() ;
				expect( user.loaded ).not.to.be.ok() ;
				expect( user.upstreamExists ).not.to.be.ok() ;
				expect( user.state() ).to.equal( 'suspected' ) ;
				expect( user.$._id ).to.be( undefined ) ;
				expect( tree.extend( null , {} , user.$ ) ).to.eql( { firstName: undefined, lastName: undefined, jobId: jobId, godfatherId: undefined, memberSid: "Joe Pink" } ) ;
				
				user.reveal( function( error ) {
					expect( user ).to.be.an( odm.Document ) ;
					expect( user.suspected ).not.to.be.ok() ;
					expect( user.loaded ).to.be.ok() ;
					expect( user.upstreamExists ).to.be.ok() ;
					//delete user.$._id ;
					//console.log( '----------------------' , Object.keys( user.$ ) ) ;
					expect( Object.keys( user.$ ).length ).to.equal( 0 ) ;
					expect( user.state() ).to.equal( 'synced' ) ;
					expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user.$._id ).to.eql( id ) ;
					expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: id, firstName: 'Joe', lastName: 'Pink', jobId: jobId, godfatherId: undefined , memberSid: 'Joe Pink' } ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "Save a suspect" ) ;
	it( "Update a suspect" ) ;
	it( "Delete a suspect" ) ;
} ) ;



describe( "Collect batchs" , function() {
	
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
				odm.bulk( 'save' , marleys , callback ) ;
			} ,
			function( callback ) {
				users.collect( { lastName: 'Marley' } , { raw: true, useMemProxy: false } , function( error , batch ) {
					var i , map = {} ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'RawBatch:' , batch ) ; 
					expect( error ).not.to.be.ok() ;
					expect( batch ).to.have.length( 5 ) ;
					
					for ( i = 0 ; i < batch.length ; i ++ )
					{
						expect( batch[ i ].firstName ).to.be.ok() ;
						expect( batch[ i ].lastName ).to.equal( 'Marley' ) ;
						map[ batch[ i ].firstName ] = true ;
					}
					
					expect( map ).to.only.have.keys( 'Bob' , 'Julian' , 'Stephen' , 'Ziggy' , 'Rita' ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				users.collect( { lastName: 'Marley' } , { useMemProxy: false } , function( error , batch ) {
					var i , map = {} ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'Batch:' , batch ) ; 
					expect( error ).not.to.be.ok() ;
					expect( batch ).to.be.an( odm.Batch ) ;
					expect( batch.documents ).to.have.length( 5 ) ;
					
					for ( i = 0 ; i < batch.documents.length ; i ++ )
					{
						expect( batch.documents[ i ] ).to.be.an( odm.Document ) ;
						expect( batch.documents[ i ].$.firstName ).to.ok() ;
						expect( batch.documents[ i ].$.lastName ).to.equal( 'Marley' ) ;
						map[ batch.documents[ i ].$.firstName ] = true ;
					}
					
					expect( map ).to.only.have.keys( 'Bob' , 'Julian' , 'Stephen' , 'Ziggy' , 'Rita' ) ;
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
} ) ;



describe( "Links" , function() {
	
	it( "should retrieve a 'suspected' document from a document's link (create both, link, save both, memProxyReset both, retrieve parent, navigate to child, reveal child)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;
		
		var id = user.$._id ;
		
		var job = jobs.createDocument() ;
		//console.log( job ) ;
		var jobId = job.id ;
		
		// Link the documents!
		user.$.job = job ;
		//user.$.jobId = jobId ;
		expect( user.$.jobId ).to.eql( jobId ) ;
		
		// Problème... stocker les liens dans les meta...
		expect( tree.extend( null , {} , user.$.job.$ ) ).to.eql( tree.extend( null , {} , job.$ ) ) ;
		expect( user.$.job.suspected ).not.to.be.ok() ;
		
		//console.log( '>>>' , jobId ) ;
		
		async.series( [
			function( callback ) {
				job.save( callback ) ;
			} ,
			function( callback ) {
				user.save( callback ) ;
			} ,
			function( callback ) {
				jobs.get( jobId , function( error , job ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'Job:' , job ) ;
					expect( error ).not.to.be.ok() ;
					expect( job ).to.be.an( odm.Document ) ;
					expect( job.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( job.$._id ).to.eql( jobId ) ;
					expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job.$._id , title: 'unemployed' , salary: 0 } ) ;
					
					// memProxyReset them! So we can test suspected document!
					users.memProxyReset() ;
					jobs.memProxyReset() ;
					
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ;
					expect( error ).not.to.be.ok() ;
					expect( user ).to.be.an( odm.Document ) ;
					expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user.$._id ).to.eql( id ) ;
					expect( user.$.jobId ).to.be.an( mongodb.ObjectID ) ;
					expect( user.$.jobId ).to.eql( jobId ) ;
					expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: user.$._id, jobId: user.$.jobId, godfatherId: undefined, firstName: 'Jilbert', lastName: 'Polson' , memberSid: 'Jilbert Polson' } ) ;
					
					//user.$.toto = 'toto' ;
					
					var job = user.$.job ;
					expect( job ).to.be.an( odm.Document ) ;
					expect( job.suspected ).to.be.ok() ;
					expect( job.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( job.$._id ).to.eql( jobId ) ;
					expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job.$._id , title: undefined , salary: undefined } ) ;
					
					job.reveal( function( error ) {
						// Not suspected anymore
						expect( job.suspected ).not.to.be.ok() ;
						expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job.$._id , title: 'unemployed' , salary: 0 } ) ;
						callback() ;
					} ) ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "should retrieve a 'suspected' document from a 'suspected' document's link (suspected²: create both, link, save both, memProxyReset both, retrieve parent as suspect, navigate to child, reveal child)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Wilson' ,
			lastName: 'Andrews'
		} ) ;
		
		var id = user.$._id ;
		
		var job = jobs.createDocument() ;
		var jobId = job.id ;
		job.$.title = 'mechanic' ;
		job.$.salary = 2100 ;
		//console.log( job ) ;
		
		// Link the documents!
		user.$.job = job ;
		//user.$.jobId = jobId ;
		expect( user.$.jobId ).to.eql( jobId ) ;
		expect( user.$.job ).to.equal( job ) ;
		expect( user.$.job.suspected ).not.to.be.ok() ;
		
		//console.log( '>>>' , jobId ) ;
		
		async.series( [
			function( callback ) {
				job.save( callback ) ;
			} ,
			function( callback ) {
				user.save( callback ) ;
			} ,
			function( callback ) {
				jobs.get( jobId , function( error , job ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'Job:' , job ) ;
					expect( error ).not.to.be.ok() ;
					expect( job ).to.be.an( odm.Document ) ;
					expect( job.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( job.$._id ).to.eql( jobId ) ;
					expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job.$._id , title: 'mechanic' , salary: 2100 } ) ;
					
					// memProxyReset them! So we can test suspected document!
					users.memProxyReset() ;
					jobs.memProxyReset() ;
					
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				
				// The real test begins NOW!
				
				var user = users.get( id ) ;
				expect( user ).to.be.an( odm.Document ) ;
				expect( user.suspected ).to.be.ok() ;
				expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
				expect( user.$._id ).to.eql( id ) ;
				expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: id, firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
				
				var job = user.$.job ;
				expect( job ).to.be.an( odm.Document ) ;
				expect( job.suspected ).to.be.ok() ;
				//expect( job.$._id ).to.be.an( mongodb.ObjectID ) ;
				//expect( job.$._id ).to.eql( jobId ) ;
				expect( job.witness.document ).to.equal( user ) ;
				expect( job.witness.property ).to.equal( 'jobId' ) ;
				expect( job.witness.type ).to.equal( 'link' ) ;
				expect( tree.extend( null , {} , job.$ ) ).to.eql( { title: undefined , salary: undefined } ) ;
				
				job.reveal( function( error ) {
					// Not a suspected anymore
					expect( job.suspected ).not.to.be.ok() ;
					expect( job.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( job.$._id ).to.eql( jobId ) ;
					expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job.$._id , title: 'mechanic' , salary: 2100 } ) ;
					
					// user should be revealed
					expect( user.suspected ).not.to.be.ok() ;
					expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user.$._id ).to.eql( id ) ;
					expect( user.$.jobId ).to.be.an( mongodb.ObjectID ) ;
					expect( user.$.jobId ).to.eql( jobId ) ;
					expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: user.$._id, jobId: user.$.jobId, godfatherId: undefined, firstName: 'Wilson', lastName: 'Andrews' , memberSid: 'Wilson Andrews' } ) ;
					
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "should retrieve a 'suspected' document from a 'suspected' document's link² (suspected³: create x3, link x2, save x3, memProxyReset x3, retrieve grand-parent as suspect, navigate to parent, navigate to child, reveal child)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Paul' ,
			lastName: 'Williams'
		} ) ;
		
		var id = user.$._id ;
		
		var godfather = users.createDocument( {
			firstName: 'Maxwell' ,
			lastName: 'Jersey'
		} ) ;
		
		var godfatherId = godfather.$._id ;
		
		var job = jobs.createDocument() ;
		var jobId = job.id ;
		job.$.title = 'plumber' ;
		job.$.salary = 1900 ;
		
		// Link the documents!
		user.$.godfather = godfather ;
		godfather.$.job = job ;
		
		expect( user.$.godfatherId ).to.eql( godfatherId ) ;
		expect( user.$.godfather ).to.equal( godfather ) ;
		expect( user.$.godfather.suspected ).not.to.be.ok() ;
		
		expect( godfather.$.jobId ).to.eql( jobId ) ;
		expect( godfather.$.job ).to.equal( job ) ;
		expect( godfather.$.job.suspected ).not.to.be.ok() ;
		
		//console.log( '>>>' , jobId ) ;
		
		async.series( [
			function( callback ) {
				job.save( callback ) ;
			} ,
			function( callback ) {
				godfather.save( callback ) ;
			} ,
			function( callback ) {
				user.save( callback ) ;
			} ,
			function( callback ) {
				// memProxyReset them! So we can test suspected document!
				users.memProxyReset() ;
				jobs.memProxyReset() ;
				callback() ;
			} ,
			function( callback ) {
				
				// The real test begins NOW!
				
				var user = users.get( id ) ;
				expect( user ).to.be.an( odm.Document ) ;
				expect( user.suspected ).to.be.ok() ;
				expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
				expect( user.$._id ).to.eql( id ) ;
				expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: id, firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
				
				var godfather = user.$.godfather ;
				expect( godfather ).to.be.an( odm.Document ) ;
				expect( godfather.suspected ).to.be.ok() ;
				//expect( godfather.$._id ).to.be.an( mongodb.ObjectID ) ;
				//expect( godfather.$._id ).to.eql( id ) ;
				expect( godfather.witness.document ).to.equal( user ) ;
				expect( godfather.witness.property ).to.equal( 'godfatherId' ) ;
				expect( godfather.witness.type ).to.equal( 'link' ) ;
				expect( tree.extend( null , {} , godfather.$ ) ).to.eql( { firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
				
				var job = godfather.$.job ;
				expect( job ).to.be.an( odm.Document ) ;
				expect( job.suspected ).to.be.ok() ;
				//expect( job.$._id ).to.be.an( mongodb.ObjectID ) ;
				//expect( job.$._id ).to.eql( jobId ) ;
				expect( job.witness.document ).to.equal( godfather ) ;
				expect( job.witness.property ).to.equal( 'jobId' ) ;
				expect( job.witness.type ).to.equal( 'link' ) ;
				expect( tree.extend( null , {} , job.$ ) ).to.eql( { title: undefined , salary: undefined } ) ;
				
				job.reveal( function( error ) {
					// Not a suspected anymore
					expect( job.suspected ).not.to.be.ok() ;
					expect( job.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( job.$._id ).to.eql( jobId ) ;
					expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job.$._id , title: 'plumber' , salary: 1900 } ) ;
					
					// godfather should be revealed
					expect( godfather.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( godfather.$._id ).to.eql( godfatherId ) ;
					expect( godfather.$.jobId ).to.be.an( mongodb.ObjectID ) ;
					expect( godfather.$.jobId ).to.eql( jobId ) ;
					expect( tree.extend( null , {} , godfather.$ ) ).to.eql( { _id: godfather.$._id, jobId: godfather.$.jobId, godfatherId: undefined, firstName: 'Maxwell', lastName: 'Jersey' , memberSid: 'Maxwell Jersey' } ) ;
					
					// user should be revealed
					expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user.$._id ).to.eql( id ) ;
					expect( user.$.godfatherId ).to.be.an( mongodb.ObjectID ) ;
					expect( user.$.godfatherId ).to.eql( godfatherId ) ;
					expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: user.$._id, jobId: user.$.jobId, godfatherId: godfatherId, firstName: 'Paul', lastName: 'Williams' , memberSid: 'Paul Williams' } ) ;
					
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
} ) ;



describe( "Backlinks" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "should retrieve a batch of 'suspected' document from a document's backlink (create, assign backlink ID, save, get parent, get backlink suspect batch containing childs, reveal batch)" , function( done ) {
		
		var job = jobs.createDocument( { title: 'bowler' } ) ;
		var jobId = job.id ;
		
		var friends = [
			users.createDocument( { firstName: 'Jeffrey' , lastName: 'Lebowski' , jobId: jobId } ) ,
			users.createDocument( { firstName: 'Walter' , lastName: 'Sobchak' , jobId: jobId } ) ,
			users.createDocument( { firstName: 'Donny' , lastName: 'Kerabatsos' , jobId: jobId } )
		] ;
		
		async.series( [
			function( callback ) {
				odm.bulk( 'save' , friends.concat( job ) , callback ) ;
			} ,
			function( callback ) {
				jobs.get( jobId , function( error , job ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'Job:' , job ) ;
					expect( error ).not.to.be.ok() ;
					expect( job ).to.be.an( odm.Document ) ;
					expect( job.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( job.$._id ).to.eql( jobId ) ;
					expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job.$._id , title: 'bowler' , salary: 0 } ) ;
					
					// memProxyReset them! So we can test suspected document!
					users.memProxyReset() ;
					jobs.memProxyReset() ;
					
					var userBatch = job.$.members ;
					expect( userBatch ).to.be.an( odm.Batch ) ;
					expect( userBatch.suspected ).to.be.ok() ;
					
					userBatch.reveal( function( error , batch ) {
						expect( error ).not.to.be.ok() ;
						expect( batch ).to.be( userBatch ) ;
						expect( userBatch.suspected ).not.to.be.ok() ;
						expect( userBatch.documents ).to.be.an( Array ) ;
						expect( userBatch.documents.length ).to.be( 3 ) ;
						
						var i , mapFirstName = {} , mapLastName = {} ;
						
						for ( i = 0 ; i < userBatch.documents.length ; i ++ )
						{
							expect( userBatch.documents[ i ].$.firstName ).to.be.ok() ;
							expect( userBatch.documents[ i ].$.lastName ).to.be.ok() ;
							mapFirstName[ userBatch.documents[ i ].$.firstName ] = true ;
							mapLastName[ userBatch.documents[ i ].$.lastName ] = true ;
						}
						
						expect( mapFirstName ).to.only.have.keys( 'Jeffrey' , 'Walter' , 'Donny' ) ;
						expect( mapLastName ).to.only.have.keys( 'Lebowski' , 'Sobchak' , 'Kerabatsos' ) ;
						
						callback() ;
					} ) ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "should retrieve a batch of 'suspected' document from a 'suspected' document's backlink (suspect²: create, assign backlink ID, save, get parent, get backlink suspect batch containing childs, reveal batch)" , function( done ) {
		
		var job = jobs.createDocument( { title: 'bowler' } ) ;
		var jobId = job.id ;
		
		var friends = [
			users.createDocument( { firstName: 'Jeffrey' , lastName: 'Lebowski' , jobId: jobId } ) ,
			users.createDocument( { firstName: 'Walter' , lastName: 'Sobchak' , jobId: jobId } ) ,
			users.createDocument( { firstName: 'Donny' , lastName: 'Kerabatsos' , jobId: jobId } )
		] ;
		
		async.series( [
			function( callback ) {
				odm.bulk( 'save' , friends.concat( job ) , callback ) ;
			} ,
			function( callback ) {
				jobs.memProxyReset() ;
				users.memProxyReset() ;
				
				job = jobs.get( jobId ) ;
				expect( job ).to.be.an( odm.Document ) ;
				expect( job.id ).to.be.an( mongodb.ObjectID ) ;
				expect( job.id ).to.eql( jobId ) ;
				expect( job.$._id ).to.be.an( mongodb.ObjectID ) ;
				expect( job.$._id ).to.eql( jobId ) ;
				expect( job.suspected ).to.be.ok() ;
				
				var userBatch = job.$.members ;
				expect( userBatch ).to.be.an( odm.Batch ) ;
				expect( userBatch.suspected ).to.be.ok() ;
				expect( userBatch.witness ).not.to.be.ok( job ) ;
				
				userBatch.reveal( function( error , batch ) {
					expect( error ).not.to.be.ok() ;
					
					expect( job.suspected ).to.be.ok() ;	// Should not be loaded
					expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job.$._id , title: undefined , salary: undefined } ) ;
					
					expect( batch ).to.be( userBatch ) ;
					expect( userBatch.suspected ).not.to.be.ok() ;
					expect( userBatch.documents ).to.be.an( Array ) ;
					expect( userBatch.documents.length ).to.be( 3 ) ;
					
					var i , mapFirstName = {} , mapLastName = {} ;
					
					for ( i = 0 ; i < userBatch.documents.length ; i ++ )
					{
						expect( userBatch.documents[ i ].$.firstName ).to.be.ok() ;
						expect( userBatch.documents[ i ].$.lastName ).to.be.ok() ;
						mapFirstName[ userBatch.documents[ i ].$.firstName ] = true ;
						mapLastName[ userBatch.documents[ i ].$.lastName ] = true ;
					}
					
					expect( mapFirstName ).to.only.have.keys( 'Jeffrey' , 'Walter' , 'Donny' ) ;
					expect( mapLastName ).to.only.have.keys( 'Lebowski' , 'Sobchak' , 'Kerabatsos' ) ;
					
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "should retrieve a batch of 'suspected' document from a 'suspected' document's backlink (suspect³: create, assign backlink ID, save, get parent, get backlink suspect batch containing childs, reveal batch)" , function( done ) {
		
		var job = jobs.createDocument( { title: 'bowler' } ) ;
		var jobId = job.id ;
		
		var friends = [
			users.createDocument( { firstName: 'Jeffrey' , lastName: 'Lebowski' , jobId: jobId } ) ,
			users.createDocument( { firstName: 'Walter' , lastName: 'Sobchak' , jobId: jobId } ) ,
			users.createDocument( { firstName: 'Donny' , lastName: 'Kerabatsos' , jobId: jobId } )
		] ;
		
		var dudeId = friends[ 0 ].id ;
		
		async.series( [
			function( callback ) {
				odm.bulk( 'save' , friends.concat( job ) , callback ) ;
			} ,
			function( callback ) {
				jobs.memProxyReset() ;
				users.memProxyReset() ;
				
				var dude = users.get( dudeId ) ;
				
				expect( dude ).to.be.an( odm.Document ) ;
				expect( dude.suspected ).to.be.ok() ;
				expect( dude.$._id ).to.be.an( mongodb.ObjectID ) ;
				expect( dude.$._id ).to.eql( dudeId ) ;
				expect( tree.extend( null , {} , dude.$ ) ).to.eql( { _id: dudeId, firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
				
				var job = dude.$.job ;
				expect( job ).to.be.an( odm.Document ) ;
				expect( job.suspected ).to.be.ok() ;
				expect( job.witness.document ).to.equal( dude ) ;
				expect( job.witness.property ).to.equal( 'jobId' ) ;
				expect( job.witness.type ).to.equal( 'link' ) ;
				expect( tree.extend( null , {} , job.$ ) ).to.eql( { title: undefined , salary: undefined } ) ;
				
				var userBatch = job.$.members ;
				expect( userBatch ).to.be.an( odm.Batch ) ;
				expect( userBatch.suspected ).to.be.ok() ;
				expect( userBatch.witness.document ).to.equal( job ) ;
				expect( userBatch.witness.property ).to.equal( 'jobId' ) ;
				expect( userBatch.witness.type ).to.equal( 'backlink' ) ;
				
				userBatch.reveal( function( error , batch ) {
					expect( error ).not.to.be.ok() ;
					
					expect( job.suspected ).to.be.ok() ;	// Should not be loaded
					expect( tree.extend( null , {} , job.$ ) ).to.eql( { title: undefined , salary: undefined } ) ;
					
					expect( batch ).to.be( userBatch ) ;
					expect( userBatch.suspected ).not.to.be.ok() ;
					expect( userBatch.documents ).to.be.an( Array ) ;
					expect( userBatch.documents.length ).to.be( 3 ) ;
					
					var i , mapFirstName = {} , mapLastName = {} ;
					
					for ( i = 0 ; i < userBatch.documents.length ; i ++ )
					{
						expect( userBatch.documents[ i ].$.firstName ).to.be.ok() ;
						expect( userBatch.documents[ i ].$.lastName ).to.be.ok() ;
						mapFirstName[ userBatch.documents[ i ].$.firstName ] = true ;
						mapLastName[ userBatch.documents[ i ].$.lastName ] = true ;
					}
					
					expect( mapFirstName ).to.only.have.keys( 'Jeffrey' , 'Walter' , 'Donny' ) ;
					expect( mapLastName ).to.only.have.keys( 'Lebowski' , 'Sobchak' , 'Kerabatsos' ) ;
					
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
} ) ;



describe( "Embedded documents" , function() {
	
	beforeEach( clearDB ) ;
	
	it( "should be able to modify '$'s embedded data without updating 'upstream's embedded data (internally, we are using the 'deep inherit' feature of tree-kit)" , function( done ) {
		
		var town = towns.createDocument( {
			name: 'Paris' ,
			meta: {
				population: '2200K'
			}
		} ) ;
		
		var id = town.$._id ;
		
		async.series( [
			function( callback ) {
				town.save( callback ) ;
			} ,
			function( callback ) {
				towns.get( id , { useMemProxy: false } , function( error , town ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'Town:' , string.inspect( { style: 'color' , proto: true } , town.$.meta ) ) ;
					expect( error ).not.to.be.ok() ;
					expect( town ).to.be.an( odm.Document ) ;
					expect( town.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( town.$._id ).to.eql( id ) ;
					expect( tree.extend( null , {} , town.upstream ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2200K' } } ) ;
					expect( protoflatten( town.$ ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2200K' } } ) ;
					
					town.$.meta.population = '2300K' ;
					expect( tree.extend( null , {} , town.upstream ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2200K' } } ) ;
					expect( protoflatten( town.$ ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2300K' } } ) ;
					
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
		
		var id = town.$._id ;
		
		async.series( [
			function( callback ) {
				town.save( callback ) ;
			} ,
			function( callback ) {
				towns.get( id , { useMemProxy: false } , function( error , town ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'Town:' , town ) ; 
					expect( error ).not.to.be.ok() ;
					expect( town ).to.be.an( odm.Document ) ;
					expect( town.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( town.$._id ).to.eql( id ) ;
					expect( tree.extend( null , {} , town.upstream ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2200K' , country: 'France' } } ) ;
					expect( protoflatten( town.$ ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2200K' , country: 'France' } } ) ;
					
					town.$.meta.population = '2300K' ;
					expect( tree.extend( null , {} , town.upstream ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2200K' , country: 'France' } } ) ;
					expect( protoflatten( town.$ ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2300K' , country: 'France' } } ) ;
					
					town.save( callback ) ;
				} ) ;
			} ,
			function( callback ) {
				towns.get( id , { useMemProxy: false } , function( error , town ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'Town:' , town ) ; 
					expect( error ).not.to.be.ok() ;
					expect( town ).to.be.an( odm.Document ) ;
					expect( town.$._id ).to.be.an( mongodb.ObjectID ) ;
					expect( town.$._id ).to.eql( id ) ;
					expect( tree.extend( null , {} , town.upstream ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2300K' , country: 'France' } } ) ;
					expect( protoflatten( town.$ ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2300K' , country: 'France' } } ) ;
					
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
				odm.bulk( 'save' , townList , callback ) ;
			} ,
			function( callback ) {
				towns.collect( { "meta.country": 'USA' } , { raw: true, useMemProxy: false } , function( error , batch ) {
					var i , map = {} ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'RawBatch:' , batch ) ; 
					expect( error ).not.to.be.ok() ;
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
				towns.collect( { "meta.country": 'USA' } , { useMemProxy: false } , function( error , batch ) {
					var i , map = {} ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'Batch:' , batch ) ; 
					expect( error ).not.to.be.ok() ;
					expect( batch ).to.be.an( odm.Batch ) ;
					expect( batch.documents ).to.have.length( 3 ) ;
					
					for ( i = 0 ; i < batch.documents.length ; i ++ )
					{
						expect( batch.documents[ i ] ).to.be.an( odm.Document ) ;
						expect( batch.documents[ i ].$.name ).to.ok() ;
						expect( batch.documents[ i ].$.meta.country ).to.equal( 'USA' ) ;
						map[ batch.documents[ i ].$.name ] = true ;
					}
					
					expect( map ).to.only.have.keys( 'New York' , 'Washington' , 'San Francisco' ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				towns.collect( { "meta.country": 'USA' , "meta.capital": false } , { useMemProxy: false } , function( error , batch ) {
					var i , map = {} ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'Batch:' , batch ) ; 
					expect( error ).not.to.be.ok() ;
					expect( batch ).to.be.an( odm.Batch ) ;
					expect( batch.documents ).to.have.length( 2 ) ;
					
					for ( i = 0 ; i < batch.documents.length ; i ++ )
					{
						expect( batch.documents[ i ] ).to.be.an( odm.Document ) ;
						expect( batch.documents[ i ].$.name ).to.ok() ;
						expect( batch.documents[ i ].$.meta.country ).to.equal( 'USA' ) ;
						map[ batch.documents[ i ].$.name ] = true ;
					}
					
					expect( map ).to.only.have.keys( 'New York' , 'San Francisco' ) ;
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				towns.getUnique( { name: 'Tokyo', "meta.country": 'Japan' } , { useMemProxy: false } , function( error , town ) {
					var i , map = {} ;
					//console.log( 'Error:' , error ) ;
					//console.log( 'Town:' , town ) ; 
					expect( error ).not.to.be.ok() ;
					expect( town ).to.be.an( odm.Document ) ;
					expect( protoflatten( town.$ ) ).to.eql( {
						_id: town.$._id ,
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
	

