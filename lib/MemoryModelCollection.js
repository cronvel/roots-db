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

"use strict" ;



var rootsDb = require( './rootsDb' ) ;



function MemoryModelCollection() {
	this.documentProxies = {} ;
	this.rawDocuments = {} ;
} ;

module.exports = MemoryModelCollection ;



// Only add if the document does not exist, it returns that document in that case, else it returns the input document
MemoryModelCollection.prototype.add = function( rawDocument , clone ) {
	var key = rawDocument._id ;

	// A document already exists, return it
	if ( this.rawDocuments[ key ] ) {
		return clone ?
			rootsDb.misc.clone( this.rawDocuments[ key ] ) :
			this.rawDocuments[ key ] ;
	}

	this.rawDocuments[ key ] = clone ?
		rootsDb.misc.clone( rawDocument ) :
		rawDocument ;

	// In either case (clone or not), return 'rawDocument'
	return rawDocument ;
} ;



MemoryModelCollection.prototype.remove = function( id ) {
	var key = '' + id ;
	delete this.rawDocuments[ key ] ;
} ;



MemoryModelCollection.prototype.get = function( id , clone ) {
	var key = '' + id ,
		rawDocument = this.rawDocuments[ key ] ;

	if ( ! rawDocument ) { return null ; }
	if ( clone ) { rawDocument = rootsDb.misc.clone( rawDocument ) ; }
	return rawDocument ;
} ;



MemoryModelCollection.prototype.multiGet = function( ids , notFoundArray , clone ) {
	var key , i ,
		length = ids.length ,
		rawBatch = [] ,
		rawDocument ;

	for ( i = 0 ; i < length ; i ++ ) {
		key = '' + ids[ i ] ;
		rawDocument = this.rawDocuments[ key ] ;

		if ( rawDocument ) {
			if ( clone ) { rawDocument = rootsDb.misc.clone( rawDocument ) ; }
			rawBatch.push( rawDocument ) ;
		}
		else if ( notFoundArray ) {
			notFoundArray.push( ids[ i ] ) ;
		}
	}

	return rawBatch ;
} ;





// using document proxies (deprecated?)



// Only add if the document does not exist, it returns that document in that case, else it returns the input document
MemoryModelCollection.prototype.add_ = function( document , clone ) {
	document = document._ ;
	
	var documentProxy = document.proxy ,
		key = document.getKey() ;

	// A document already exists, return it
	if ( this.documentProxies[ key ] ) {
		return clone ?
			rootsDb.misc.clone( this.documentProxies[ key ] ) :
			this.documentProxies[ key ] ;
	}

	this.documentProxies[ key ] = clone ?
		rootsDb.misc.clone( documentProxy ) :
		documentProxy ;

	// In either case (clone or not), return 'documentProxy'
	return documentProxy ;
} ;



MemoryModelCollection.prototype.remove_ = function( id ) {
	var key = '' + id ;
	delete this.documentProxies[ key ] ;
} ;



MemoryModelCollection.prototype.get_ = function( id , clone ) {
	var key = '' + id ,
		documentProxy = this.documentProxies[ key ] ;

	if ( ! documentProxy ) { return null ; }
	if ( clone ) { documentProxy = rootsDb.misc.clone( documentProxy ) ; }
	return documentProxy ;
} ;



MemoryModelCollection.prototype.multiGet_ = function( ids , notFoundArray , clone ) {
	var key , i ,
		length = ids.length ,
		rawBatch = [] ,
		documentProxy ;

	for ( i = 0 ; i < length ; i ++ ) {
		key = '' + ids[ i ] ;
		documentProxy = this.documentProxies[ key ] ;

		if ( documentProxy ) {
			if ( clone ) { documentProxy = rootsDb.misc.clone( documentProxy ) ; }
			rawBatch.push( documentProxy ) ;
		}
		else if ( notFoundArray ) {
			notFoundArray.push( ids[ i ] ) ;
		}
	}

	return rawBatch ;
} ;

