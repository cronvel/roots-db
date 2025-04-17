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



/*
	FUTURE FEATURE!

	Load a part of the DB in memory, and produce an efficient and useful data model.
	It's built on top of Batch: the MemoryModel contains one Batch per Collection, and acts exactly like a local in-memory database.
	All linked document will be loaded in the batch of the appropriate collection, those links will be replaced by reference
	of the real document.
	Later, the whole data of the MemoryModel can be saved.

	The key point of this feature is that the data model should be exactly what one would expect from a program that works
	without database.

	Also it has index-like feature.
	A basic batch is only a big array: not that useful in many case.
	So to improve things, one may access a document using e.g. the ObjectId, using `batch.$.id[ id ]`, that's a map with
	the ObjectId as a key and a reference to the document as the value.
	For games, a 2D index can be produced, e.g. using `batch.$.cell[ x ][ y ]`.

	Use case: game.
	One node instance load a whole Level, and load any characters, NPCs, weapon drop, area effect (and so on) that are currently
	tied to that Level.
	A character can only be on one Level at a time, so no data are replicated in another instance.
	Once a character leave the Level, it is unloaded and saved back to the database.
	Once in a time, all document of the Level are saved (to prevent a crash of the node instance).
*/


/*
	Used as a one-time cache for instance.
*/

"use strict" ;



const rootsDb = require( './rootsDb' ) ;



// This MUST be constructed FASTLY!
function MemoryModel( world , options = {} ) {
	this.world = world ;
	this.collections = {} ;

	if ( ! options.lazy ) {
		this.createAllWorldCollections() ;
	}
}

module.exports = MemoryModel ;

MemoryModel.Collection = require( './MemoryModelCollection.js' ) ;



MemoryModel.prototype.createAllWorldCollections = function() {
	for ( let collectionName of Object.keys( this.world.collections ) ) {
		let collection = this.world.collections[ collectionName ] ;
		if ( ! this.collections[ collectionName ] ) { this.collections[ collectionName ] = new MemoryModel.Collection( collection ) ; }
	}
} ;



MemoryModel.prototype.createCollection = function( collectionName ) {
	if ( this.collections[ collectionName ] ) { return this.collections[ collectionName ] ; }

	var collection = this.world.collections[ collectionName ] ;

	if ( ! collection ) {
		throw new Error( "Unknown collection '" + collectionName + "'." ) ;
	}

	this.collections[ collectionName ] = new MemoryModel.Collection( collection ) ;

	return this.collections[ collectionName ] ;
} ;



// Only add if the document does not exist, it returns that rawDocument in that case, else it returns the input rawDocument
MemoryModel.prototype.addRaw = function( collectionName , rawDocument , clone ) {
	var collection = this.collections[ collectionName ] ;
	if ( ! collection ) { collection = this.createCollection( collectionName ) ; }
	return collection.addRaw( rawDocument , clone ) ;
} ;

MemoryModel.prototype.addProxy = function( collectionName , documentProxy , clone ) {
	var collection = this.collections[ collectionName ] ;
	if ( ! collection ) { collection = this.createCollection( collectionName ) ; }
	return collection.addProxy( documentProxy , clone ) ;
} ;



MemoryModel.prototype.getRaw = function( collectionName , id , clone ) {
	var collection = this.collections[ collectionName ] ;
	if ( ! collection ) { return null ; }
	return collection.getRaw( id , clone ) ;
} ;

MemoryModel.prototype.getProxy = function( collectionName , id , clone ) {
	var collection = this.collections[ collectionName ] ;
	if ( ! collection ) { return null ; }
	return collection.getProxy( id , clone ) ;
} ;

MemoryModel.prototype.getProxyFromRaw = function( collectionName , rawDocument ) {
	var collection = this.collections[ collectionName ] ;
	if ( ! collection ) { return null ; }
	return collection.getProxyFromRaw( rawDocument ) ;
} ;



MemoryModel.prototype.multiGetRaw = function( collectionName , ids , notFoundArray , clone ) {
	var collection = this.collections[ collectionName ] ;
	if ( ! collection ) { return null ; }
	return collection.multiGetRaw( ids , notFoundArray , clone ) ;
} ;

MemoryModel.prototype.multiGetProxy = function( collectionName , ids , notFoundArray , clone ) {
	var collection = this.collections[ collectionName ] ;
	if ( ! collection ) { return null ; }
	return collection.multiGetProxy( ids , notFoundArray , clone ) ;
} ;

