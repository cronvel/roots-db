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



var rootsDb = require( './rootsDb' ) ;



function MemoryModelCollection() { throw new Error( "Use MemoryModelCollection.create() instead" ) ; }
module.exports = MemoryModelCollection ;



MemoryModelCollection.create = function create()
{
	var collection = Object.create( MemoryModelCollection.prototype , {
		documents: { value: {} , enumerable: true }
	} ) ;
	
	return collection ;
} ;



// Only add if the document does not exist, it returns that document in that case, else it returns the input document
MemoryModelCollection.prototype.add = function add( document , clone )
{
	var id = document._id.toString() ;
	
	// A document already exists, return it
	if ( this.documents[ id ] )
	{
		return clone ?
			rootsDb.misc.clone( this.documents[ id ] ) :
			this.documents[ id ] ;
	}
	
	this.documents[ id ] = clone ?
		rootsDb.misc.clone( document ) :
		document ;
	
	// In either case (clone or not), return 'document'
	return document ;
} ;



MemoryModelCollection.prototype.remove = function remove( id )
{
	delete this.documents[ id.toString() ] ;
} ;



MemoryModelCollection.prototype.get = function get( id , clone )
{
	var document ;
	
	document = this.documents[ id.toString() ] ;
	
	if ( ! document ) { return null ; }
	if ( clone ) { document = rootsDb.misc.clone( document ) ; }
	return document ;
} ;



MemoryModelCollection.prototype.multiGet = function multiGet( ids , notFoundArray , clone )
{
	var i ,
		length = ids.length ,
		rawBatch = [] ,
		document ;
	
	for ( i = 0 ; i < length ; i ++ )
	{
		document = this.documents[ ids[ i ].toString() ] ;
		
		if ( document )
		{
			if ( clone ) { document = rootsDb.misc.clone( document ) ; }
			rawBatch.push( document ) ;
		}
		else if ( notFoundArray )
		{
			notFoundArray.push( ids[ i ] ) ;
		}
	}
	
	return rawBatch ;
} ;

