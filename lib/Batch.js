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



const Promise = require( 'seventh' ) ;
const tree = require( 'tree-kit' ) ;



/*
	FEATURE TODO:
		* set common properties with batch.$
*/



class Batch extends Array {
	constructor( collection , rawBatch = [] , options = {} ) {
		// Already wrapped?
		if ( rawBatch._ instanceof Batch ) { return rawBatch._ ; }

		// This can be costly on large batch
		Batch.fromRaw( rawBatch , collection , options ) ;

		super( ... rawBatch ) ;

		this.collection = collection ;

		// Useful here?
		/*
		this.loaded = !! options.fromUpstream ;
		this.saved = false ;
		this.deleted = false ;
		this.upstreamExists = !! options.fromUpstream ;

		if ( options.fromUpstream ) {
			this.loaded = true ;
			this.upstreamExists = true ;
		}
		*/
	}

	// This allow batch.map()/.slice()/etc to return an Array, not a Batch
	static get [Symbol.species]() { return Array ; }

	static fromRaw( rawBatch , collection , options ) {
		rawBatch.forEach( ( rawDocument , index ) => {
			if ( ! ( rawDocument._ instanceof collection.Document ) ) {
				rawBatch[ index ] = ( new collection.Document( collection , rawDocument , options ) ).proxy ;
			}
		} ) ;
	}

	fromRaw( rawBatch , options ) {
		return Batch.fromRaw( rawBatch , this.collection , options ) ;
	}

	push( ... args ) {
		this.fromRaw( args ) ;
		super.push( ... args ) ;
	}

	index() {
		return Batch.raw.index( this.batch ) ;
	}

	indexPathOfId() {
		return Batch.raw.indexPathOfId( this.batch ) ;
	}

	indexUniquePathOfId() {
		return Batch.raw.indexUniquePathOfId( this.batch ) ;
	}

	save() {
		// .every() ? .forEach() ? .concurrent() ?
		// syntax: Promise.concurrent( limit , iterable , iterator )
		return Promise.every( this , document => document.save() ) ;
	}
}

module.exports = Batch ;



/* Operation on raw batch */



Batch.raw = {} ;



Batch.raw.index = function index( rawBatch ) {
	var i , iMax , batchIndex = {} ;

	for ( i = 0 , iMax = rawBatch.length ; i < iMax ; i ++ ) {
		batchIndex[ rawBatch[ i ]._id.toString() ] = rawBatch[ i ] ;
	}

	return batchIndex ;
} ;



// Create index of a path containing an ID, the target of each index is not a document but a batch
// Compatible with array of IDs: in that case, one item may appear multiple time in the index
Batch.raw.indexPathOfId = function indexPathOfId( rawBatch , path ) {
	var i , iMax , j , jMax , batchIndex = {} , indexName , element ;

	for ( i = 0 , iMax = rawBatch.length ; i < iMax ; i ++ ) {
		element = tree.path.get( rawBatch[ i ] , path ) ;

		if ( Array.isArray( element ) ) {
			for ( j = 0 , jMax = element.length ; j < jMax ; j ++ ) {
				indexName = element[ j ].toString() ;
				if ( ! batchIndex[ indexName ] ) { batchIndex[ indexName ] = [] ; }
				batchIndex[ indexName ].push( rawBatch[ i ] ) ;
			}
		}
		else {
			indexName = element.toString() ;
			if ( ! batchIndex[ indexName ] ) { batchIndex[ indexName ] = [] ; }
			batchIndex[ indexName ].push( rawBatch[ i ] ) ;
		}
	}

	return batchIndex ;
} ;



// Create index of a path containing an ID
// /!\ should be compatible with array of IDs??? /!\
Batch.raw.indexUniquePathOfId = function indexUniquePathOfId( rawBatch , path ) {
	var i , iMax , batchIndex = {} ;

	for ( i = 0 , iMax = rawBatch.length ; i < iMax ; i ++ ) {
		batchIndex[ tree.path.get( rawBatch[ i ] , path ).toString() ] = rawBatch[ i ] ;
	}

	return batchIndex ;
} ;
