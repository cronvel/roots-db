/*
	The Cedric's Swiss Knife (CSK) - CSK RootsDB

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



/*
	FUTUR FEATURE!
	
	Load a part of the DB in memory, and produce an efficient and useful data model.
	It's built on top of Batch: the MemoryModel contains one Batch per Collection, and acts exactly like a local in-memory database.
	All linked document will be loaded in the batch of the appropriate collection, those links will be replaced by reference
	of the real document.
	Later, the whole data of the MemoryModel can be saved.
	
	The key point of this feature is that the data model should be exactly what one would expect from a program that works
	without database.
	
	Also it has index-like feature.
	A basic batch is only a big array: not that useful in many case.
	So to improve things, one may access a document using e.g. the ObjectID, using `batch.$.id[ id ]`, that's a map with
	the ObjectID as a key and a reference to the document as the value.
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



var rootsDb = require( './rootsDb' ) ;



function MemoryModel() { throw new Error( "Use MemoryModel.create() instead" ) ; }
module.exports = MemoryModel ;

MemoryModel.Collection = require( './MemoryModelCollection.js' ) ;



MemoryModel.create = function create( world )
{
	// This MUST be constructed FASTLY!
	
	var mem = Object.create( MemoryModel.prototype , {
		world: { value: world , enumerable: true } ,
		collections: { value: {} , enumerable: true }
	} ) ;
	
	return mem ;
} ;



MemoryModel.prototype.createCollection = function createCollection( collectionName )
{
	if ( this.collections[ collectionName ] ) { return this.collections[ collectionName ] ; }
	
	this.collections[ collectionName ] = MemoryModel.Collection.create() ;
	
	return this.collections[ collectionName ] ;
} ;



MemoryModel.prototype.add = function add( collectionName , document , clone )
{
	var collection = this.collections[ collectionName ] ;
	if ( ! collection ) { collection = this.createCollection( collectionName ) ; }
	collection.add( document , clone ) ;
} ;



MemoryModel.prototype.get = function get( collectionName , id , clone )
{
	var collection = this.collections[ collectionName ] ;
	if ( ! collection ) { return null ; }
	return collection.get( id , clone ) ;
} ;



MemoryModel.prototype.multiGet = function multiGet( collectionName , ids , notFoundArray , clone )
{
	var collection = this.collections[ collectionName ] ;
	if ( ! collection ) { return null ; }
	return collection.multiGet( ids , notFoundArray , clone ) ;
} ;


