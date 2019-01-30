/*
	Roots DB

	Copyright (c) 2014 - 2019 Cédric Ronvel

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

const log = require( 'logfella' ).global.use( 'roots-db' ) ;



function MemoryModelCollection( collection ) {
	this.collection = collection ;
	this.rawDocuments = {} ;
	this.documentProxies = new WeakMap() ;
}

module.exports = MemoryModelCollection ;



// Only add if the document does not exist, it returns that document in that case, else it returns the input document
MemoryModelCollection.prototype.addRaw = function( rawDocument , clone ) {
	var key = '' + rawDocument._id ;

	log.trace( "MemoryModelCollection#addRaw() %I" , rawDocument ) ;

	if ( this.rawDocuments[ key ] ) {
		// A document already exists, return it
		rawDocument = this.rawDocuments[ key ] ;

		if ( clone ) {
			rawDocument = rootsDb.misc.clone( rawDocument ) ;
		}

		return rawDocument ;
	}

	this.rawDocuments[ key ] = clone ?
		rootsDb.misc.clone( rawDocument ) :
		rawDocument ;

	// In either case (clone or not), return 'rawDocument'
	return rawDocument ;
} ;



MemoryModelCollection.prototype.addProxy = function( documentProxy , clone ) {
	var rawDocument = documentProxy._.raw ,
		key = '' + rawDocument._id ;

	//log.trace( "MemoryModelCollection#addProxy() %I" , rawDocument ) ;

	if ( this.rawDocuments[ key ] ) {
		//log.fatal( "rawDocument already exists" ) ;
		// A document already exists, return it
		rawDocument = this.rawDocuments[ key ] ;

		if ( clone ) {
			rawDocument = rootsDb.misc.clone( rawDocument ) ;
		}
		else {
			// Try to get the Proxy out of the cache
			documentProxy = this.documentProxies.get( rawDocument ) ;
			if ( documentProxy ) { return documentProxy ; }
		}

		// /!\ maintain 'fromUpstream': true ?
		documentProxy = ( new this.collection.Document( this.collection , rawDocument , { fromUpstream: true , skipValidation: true } ) ).proxy ;

		if ( ! clone ) {
			// Cache the Proxy
			this.documentProxies.set( rawDocument , documentProxy ) ;
		}

		return documentProxy ;
	}

	//log.fatal( "rawDocument does not exist" ) ;
	if ( clone ) {
		rawDocument = rootsDb.misc.clone( rawDocument ) ;

		// /!\ maintain 'fromUpstream': true ?
		documentProxy = ( new this.collection.Document( this.collection , rawDocument , { fromUpstream: true , skipValidation: true } ) ).proxy ;
	}
	else {
		// Cache the Proxy
		this.documentProxies.set( rawDocument , documentProxy ) ;
	}

	this.rawDocuments[ key ] = rawDocument ;

	return documentProxy ;
} ;



MemoryModelCollection.prototype.remove = function( id ) {
	var key = '' + id ;

	if ( this.rawDocuments[ key ] ) {
		this.documentProxies.delete( this.rawDocuments[ key ] ) ;
		delete this.rawDocuments[ key ] ;
	}
} ;



MemoryModelCollection.prototype.getRaw = function( id , clone ) {
	var key = '' + id ,
		rawDocument = this.rawDocuments[ key ] ;

	if ( ! rawDocument ) { return null ; }
	if ( clone ) { rawDocument = rootsDb.misc.clone( rawDocument ) ; }

	return rawDocument ;
} ;



MemoryModelCollection.prototype.getProxy = function( id , clone ) {
	var key = '' + id ,
		documentProxy ,
		rawDocument = this.rawDocuments[ key ] ;

	if ( ! rawDocument ) { return null ; }

	if ( clone ) {
		rawDocument = rootsDb.misc.clone( rawDocument ) ;
	}
	else {
		// Try to get the Proxy out of the cache
		documentProxy = this.documentProxies.get( rawDocument ) ;
		if ( documentProxy ) { return documentProxy ; }
	}

	// /!\ maintain 'fromUpstream': true ?
	documentProxy = ( new this.collection.Document( this.collection , rawDocument , { fromUpstream: true , skipValidation: true } ) ).proxy ;

	if ( ! clone ) {
		// Cache the Proxy
		this.documentProxies.set( rawDocument , documentProxy ) ;
	}

	return documentProxy ;
} ;



// Get the proxy for a raw document, if the
MemoryModelCollection.prototype.getProxyFromRaw = function( rawDocument ) {
	// Try to get the Proxy out of the cache
	var documentProxy = this.documentProxies.get( rawDocument ) ;
	if ( documentProxy ) { return documentProxy ; }

	var key = '' + rawDocument._id ;

	if ( this.rawDocuments[ key ] ) {
		rawDocument = this.rawDocuments[ key ] ;
	}
	else {
		return null ;
	}

	// /!\ maintain 'fromUpstream': true ?
	documentProxy = ( new this.collection.Document( this.collection , rawDocument , { fromUpstream: true , skipValidation: true } ) ).proxy ;
	this.documentProxies.set( rawDocument , documentProxy ) ;

	return documentProxy ;
} ;



MemoryModelCollection.prototype.multiGetRaw = function( ids , notFoundArray , clone ) {
	var rawBatch = [] ;

	ids.forEach( id => {
		var rawDocument = this.getRaw( id , clone ) ;

		if ( rawDocument ) {
			rawBatch.push( rawDocument ) ;
		}
		else if ( notFoundArray ) {
			notFoundArray.push( id ) ;
		}
	} ) ;

	return rawBatch ;
} ;



MemoryModelCollection.prototype.multiGetProxy = function( ids , notFoundArray , clone ) {
	var rawBatch = [] ;

	ids.forEach( id => {
		var documentProxy = this.getProxy( id , clone ) ;

		if ( documentProxy ) {
			rawBatch.push( documentProxy ) ;
		}
		else if ( notFoundArray ) {
			notFoundArray.push( id ) ;
		}
	} ) ;

	return rawBatch ;
} ;

