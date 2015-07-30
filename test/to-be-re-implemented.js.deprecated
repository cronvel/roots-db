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



// it flatten prototype chain, so a single object owns every property of its parents
var protoflatten = tree.extend.bind( undefined , { deep: true , deepFilter: { blacklist: [ mongodb.ObjectID.prototype ] } } , null ) ;



describe( "Suspects and revealing" , function() {
	
	it( "Synchronous 'get()' should provide an 'identified' suspect, and reveal it later" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Dilbert' ,
			lastName: 'Dugommier'
		} ) ;
		
		var id = user._id ;
		
		expect( user.state() ).to.equal( 'app-side' ) ;
		
		async.series( [
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				var user = users.get( id ) ;
				expect( user ).to.be.an( rootsDb.DocumentWrapper ) ;
				expect( user.suspected ).to.be.ok() ;
				expect( user.loaded ).not.to.be.ok() ;
				expect( user.upstreamExists ).not.to.be.ok() ;
				expect( user.state() ).to.equal( 'suspected' ) ;
				expect( user._id ).to.be.an( mongodb.ObjectID ) ;
				expect( user._id ).to.eql( id ) ;
				expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: id, firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
				
				user.reveal( function( error ) {
					expect( user ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( user.suspected ).not.to.be.ok() ;
					expect( user.loaded ).to.be.ok() ;
					expect( user.upstreamExists ).to.be.ok() ;
					//delete user._id ;
					//console.log( '----------------------' , Object.keys( user.$ ) ) ;
					expect( Object.keys( user.$ ).length ).to.equal( 0 ) ;
					expect( user.state() ).to.equal( 'synced' ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
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
		
		expect( user ).to.be.an( rootsDb.DocumentWrapper ) ;
		expect( user.suspected ).to.be.ok() ;
		expect( user._id ).to.be.an( mongodb.ObjectID ) ;
		expect( user._id ).to.eql( id ) ;
		expect( user.loaded ).not.to.be.ok() ;
		expect( user.upstreamExists ).not.to.be.ok() ;
		expect( user.deleted ).not.to.be.ok() ;
		expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: id, firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
		
		user.reveal( function( error ) {
			expect( user ).to.be.an( rootsDb.DocumentWrapper ) ;
			expect( user.suspected ).not.to.be.ok() ;
			expect( user._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user._id ).to.eql( id ) ;
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
		} ) ;
		
		var id = user._id ;
		var memberSid = user.$.memberSid ;
		
		var job = jobs.createDocument() ;
		var jobId = job.id ;
		user.$.job = job ;
		
		expect( user.state() ).to.equal( 'app-side' ) ;
		
		async.series( [
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				job.$.save( callback ) ;
			} ,
			function( callback ) {
				var user = users.getUnique( { memberSid: memberSid , jobId: jobId } ) ;
				expect( user ).to.be.an( rootsDb.DocumentWrapper ) ;
				expect( user.suspected ).to.be.ok() ;
				expect( user.loaded ).not.to.be.ok() ;
				expect( user.upstreamExists ).not.to.be.ok() ;
				expect( user.state() ).to.equal( 'suspected' ) ;
				expect( user._id ).to.be( undefined ) ;
				expect( tree.extend( null , {} , user.$ ) ).to.eql( { firstName: undefined, lastName: undefined, jobId: jobId, godfatherId: undefined, memberSid: "Joe Pink" } ) ;
				
				user.reveal( function( error ) {
					expect( user ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( user.suspected ).not.to.be.ok() ;
					expect( user.loaded ).to.be.ok() ;
					expect( user.upstreamExists ).to.be.ok() ;
					//delete user._id ;
					//console.log( '----------------------' , Object.keys( user.$ ) ) ;
					expect( Object.keys( user.$ ).length ).to.equal( 0 ) ;
					expect( user.state() ).to.equal( 'synced' ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
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



describe( "Links" , function() {
	
	it( "should retrieve a 'suspected' document from a document's link (create both, link, save both, retrieve parent, navigate to child, reveal child)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;
		
		var id = user._id ;
		
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
					expect( job ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( job._id ).to.be.an( mongodb.ObjectID ) ;
					expect( job._id ).to.eql( jobId ) ;
					expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job._id , title: 'unemployed' , salary: 0 } ) ;
					
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				users.get( id , function( error , user ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'User:' , user ) ;
					expect( error ).not.to.be.ok() ;
					expect( user ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
					expect( user.$.jobId ).to.be.an( mongodb.ObjectID ) ;
					expect( user.$.jobId ).to.eql( jobId ) ;
					expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: user._id, jobId: user.$.jobId, godfatherId: undefined, firstName: 'Jilbert', lastName: 'Polson' , memberSid: 'Jilbert Polson' } ) ;
					
					//user.$.toto = 'toto' ;
					
					var job = user.$.job ;
					expect( job ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( job.suspected ).to.be.ok() ;
					expect( job._id ).to.be.an( mongodb.ObjectID ) ;
					expect( job._id ).to.eql( jobId ) ;
					expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job._id , title: undefined , salary: undefined } ) ;
					
					job.reveal( function( error ) {
						// Not suspected anymore
						expect( job.suspected ).not.to.be.ok() ;
						expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job._id , title: 'unemployed' , salary: 0 } ) ;
						callback() ;
					} ) ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "should retrieve a 'suspected' document from a 'suspected' document's link (suspected²: create both, link, save both, retrieve parent as suspect, navigate to child, reveal child)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Wilson' ,
			lastName: 'Andrews'
		} ) ;
		
		var id = user._id ;
		
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
					expect( job ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( job._id ).to.be.an( mongodb.ObjectID ) ;
					expect( job._id ).to.eql( jobId ) ;
					expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job._id , title: 'mechanic' , salary: 2100 } ) ;
					
					callback() ;
				} ) ;
			} ,
			function( callback ) {
				
				// The real test begins NOW!
				
				var user = users.get( id ) ;
				expect( user ).to.be.an( rootsDb.DocumentWrapper ) ;
				expect( user.suspected ).to.be.ok() ;
				expect( user._id ).to.be.an( mongodb.ObjectID ) ;
				expect( user._id ).to.eql( id ) ;
				expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: id, firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
				
				var job = user.$.job ;
				expect( job ).to.be.an( rootsDb.DocumentWrapper ) ;
				expect( job.suspected ).to.be.ok() ;
				//expect( job._id ).to.be.an( mongodb.ObjectID ) ;
				//expect( job._id ).to.eql( jobId ) ;
				expect( job.witness.document ).to.equal( user ) ;
				expect( job.witness.property ).to.equal( 'jobId' ) ;
				expect( job.witness.type ).to.equal( 'link' ) ;
				expect( tree.extend( null , {} , job.$ ) ).to.eql( { title: undefined , salary: undefined } ) ;
				
				job.reveal( function( error ) {
					// Not a suspected anymore
					expect( job.suspected ).not.to.be.ok() ;
					expect( job._id ).to.be.an( mongodb.ObjectID ) ;
					expect( job._id ).to.eql( jobId ) ;
					expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job._id , title: 'mechanic' , salary: 2100 } ) ;
					
					// user should be revealed
					expect( user.suspected ).not.to.be.ok() ;
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
					expect( user.$.jobId ).to.be.an( mongodb.ObjectID ) ;
					expect( user.$.jobId ).to.eql( jobId ) ;
					expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: user._id, jobId: user.$.jobId, godfatherId: undefined, firstName: 'Wilson', lastName: 'Andrews' , memberSid: 'Wilson Andrews' } ) ;
					
					callback() ;
				} ) ;
			}
		] )
		.exec( done ) ;
	} ) ;
	
	it( "should retrieve a 'suspected' document from a 'suspected' document's link² (suspected³: create x3, link x2, save x3, retrieve grand-parent as suspect, navigate to parent, navigate to child, reveal child)" , function( done ) {
		
		var user = users.createDocument( {
			firstName: 'Paul' ,
			lastName: 'Williams'
		} ) ;
		
		var id = user._id ;
		
		var godfather = users.createDocument( {
			firstName: 'Maxwell' ,
			lastName: 'Jersey'
		} ) ;
		
		var godfatherId = godfather._id ;
		
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
				job.$.save( callback ) ;
			} ,
			function( callback ) {
				godfather.$.save( callback ) ;
			} ,
			function( callback ) {
				user.$.save( callback ) ;
			} ,
			function( callback ) {
				
				// The real test begins NOW!
				
				var user = users.get( id ) ;
				expect( user ).to.be.an( rootsDb.DocumentWrapper ) ;
				expect( user.suspected ).to.be.ok() ;
				expect( user._id ).to.be.an( mongodb.ObjectID ) ;
				expect( user._id ).to.eql( id ) ;
				expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: id, firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
				
				var godfather = user.$.godfather ;
				expect( godfather ).to.be.an( rootsDb.DocumentWrapper ) ;
				expect( godfather.suspected ).to.be.ok() ;
				//expect( godfather._id ).to.be.an( mongodb.ObjectID ) ;
				//expect( godfather._id ).to.eql( id ) ;
				expect( godfather.witness.document ).to.equal( user ) ;
				expect( godfather.witness.property ).to.equal( 'godfatherId' ) ;
				expect( godfather.witness.type ).to.equal( 'link' ) ;
				expect( tree.extend( null , {} , godfather.$ ) ).to.eql( { firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
				
				var job = godfather.$.job ;
				expect( job ).to.be.an( rootsDb.DocumentWrapper ) ;
				expect( job.suspected ).to.be.ok() ;
				//expect( job._id ).to.be.an( mongodb.ObjectID ) ;
				//expect( job._id ).to.eql( jobId ) ;
				expect( job.witness.document ).to.equal( godfather ) ;
				expect( job.witness.property ).to.equal( 'jobId' ) ;
				expect( job.witness.type ).to.equal( 'link' ) ;
				expect( tree.extend( null , {} , job.$ ) ).to.eql( { title: undefined , salary: undefined } ) ;
				
				job.reveal( function( error ) {
					// Not a suspected anymore
					expect( job.suspected ).not.to.be.ok() ;
					expect( job._id ).to.be.an( mongodb.ObjectID ) ;
					expect( job._id ).to.eql( jobId ) ;
					expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job._id , title: 'plumber' , salary: 1900 } ) ;
					
					// godfather should be revealed
					expect( godfather._id ).to.be.an( mongodb.ObjectID ) ;
					expect( godfather._id ).to.eql( godfatherId ) ;
					expect( godfather.$.jobId ).to.be.an( mongodb.ObjectID ) ;
					expect( godfather.$.jobId ).to.eql( jobId ) ;
					expect( tree.extend( null , {} , godfather.$ ) ).to.eql( { _id: godfather._id, jobId: godfather.$.jobId, godfatherId: undefined, firstName: 'Maxwell', lastName: 'Jersey' , memberSid: 'Maxwell Jersey' } ) ;
					
					// user should be revealed
					expect( user._id ).to.be.an( mongodb.ObjectID ) ;
					expect( user._id ).to.eql( id ) ;
					expect( user.$.godfatherId ).to.be.an( mongodb.ObjectID ) ;
					expect( user.$.godfatherId ).to.eql( godfatherId ) ;
					expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: user._id, jobId: user.$.jobId, godfatherId: godfatherId, firstName: 'Paul', lastName: 'Williams' , memberSid: 'Paul Williams' } ) ;
					
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
				rootsDb.bulk( 'save' , friends.concat( job ) , callback ) ;
			} ,
			function( callback ) {
				jobs.get( jobId , function( error , job ) {
					//console.log( 'Error:' , error ) ;
					//console.log( 'Job:' , job ) ;
					expect( error ).not.to.be.ok() ;
					expect( job ).to.be.an( rootsDb.DocumentWrapper ) ;
					expect( job._id ).to.be.an( mongodb.ObjectID ) ;
					expect( job._id ).to.eql( jobId ) ;
					expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job._id , title: 'bowler' , salary: 0 } ) ;
					
					var userBatch = job.$.members ;
					expect( userBatch ).to.be.an( rootsDb.BatchWrapper ) ;
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
							expect( userBatch.documents[ i ].firstName ).to.be.ok() ;
							expect( userBatch.documents[ i ].lastName ).to.be.ok() ;
							mapFirstName[ userBatch.documents[ i ].firstName ] = true ;
							mapLastName[ userBatch.documents[ i ].lastName ] = true ;
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
				rootsDb.bulk( 'save' , friends.concat( job ) , callback ) ;
			} ,
			function( callback ) {
				
				job = jobs.get( jobId ) ;
				expect( job ).to.be.an( rootsDb.DocumentWrapper ) ;
				expect( job.id ).to.be.an( mongodb.ObjectID ) ;
				expect( job.id ).to.eql( jobId ) ;
				expect( job._id ).to.be.an( mongodb.ObjectID ) ;
				expect( job._id ).to.eql( jobId ) ;
				expect( job.suspected ).to.be.ok() ;
				
				var userBatch = job.$.members ;
				expect( userBatch ).to.be.an( rootsDb.BatchWrapper ) ;
				expect( userBatch.suspected ).to.be.ok() ;
				expect( userBatch.witness ).not.to.be.ok( job ) ;
				
				userBatch.reveal( function( error , batch ) {
					expect( error ).not.to.be.ok() ;
					
					expect( job.suspected ).to.be.ok() ;	// Should not be loaded
					expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job._id , title: undefined , salary: undefined } ) ;
					
					expect( batch ).to.be( userBatch ) ;
					expect( userBatch.suspected ).not.to.be.ok() ;
					expect( userBatch.documents ).to.be.an( Array ) ;
					expect( userBatch.documents.length ).to.be( 3 ) ;
					
					var i , mapFirstName = {} , mapLastName = {} ;
					
					for ( i = 0 ; i < userBatch.documents.length ; i ++ )
					{
						expect( userBatch.documents[ i ].firstName ).to.be.ok() ;
						expect( userBatch.documents[ i ].lastName ).to.be.ok() ;
						mapFirstName[ userBatch.documents[ i ].firstName ] = true ;
						mapLastName[ userBatch.documents[ i ].lastName ] = true ;
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
				rootsDb.bulk( 'save' , friends.concat( job ) , callback ) ;
			} ,
			function( callback ) {
				
				var dude = users.get( dudeId ) ;
				
				expect( dude ).to.be.an( rootsDb.DocumentWrapper ) ;
				expect( dude.suspected ).to.be.ok() ;
				expect( dude._id ).to.be.an( mongodb.ObjectID ) ;
				expect( dude._id ).to.eql( dudeId ) ;
				expect( tree.extend( null , {} , dude.$ ) ).to.eql( { _id: dudeId, firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
				
				var job = dude.$.job ;
				expect( job ).to.be.an( rootsDb.DocumentWrapper ) ;
				expect( job.suspected ).to.be.ok() ;
				expect( job.witness.document ).to.equal( dude ) ;
				expect( job.witness.property ).to.equal( 'jobId' ) ;
				expect( job.witness.type ).to.equal( 'link' ) ;
				expect( tree.extend( null , {} , job.$ ) ).to.eql( { title: undefined , salary: undefined } ) ;
				
				var userBatch = job.$.members ;
				expect( userBatch ).to.be.an( rootsDb.BatchWrapper ) ;
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
						expect( userBatch.documents[ i ].firstName ).to.be.ok() ;
						expect( userBatch.documents[ i ].lastName ).to.be.ok() ;
						mapFirstName[ userBatch.documents[ i ].firstName ] = true ;
						mapLastName[ userBatch.documents[ i ].lastName ] = true ;
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



