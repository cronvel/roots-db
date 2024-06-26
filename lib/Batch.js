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

"use strict" ;



const Promise = require( 'seventh' ) ;
const tree = require( 'tree-kit' ) ;

const Population = require( './Population.js' ) ;

const log = require( 'logfella' ).global.use( 'roots-db' ) ;



/*
	FEATURE TODO:
		* set common properties with batch.$
*/



class Batch extends Array {
	constructor( collection , rawBatch = [] , options = {} ) {
		// Already wrapped?
		if ( rawBatch._ instanceof Batch ) { return rawBatch._ ; }

		// This can be costly on large batch
		// Note: this is the correct spelling for the verb instantiate (not "instanciate")
		if ( ! options.instantiated ) {
			Batch.fromRaw( rawBatch , collection , options ) ;
		}

		super( ... rawBatch ) ;

		this.collection = collection ;
		this.meta = {
			lockId: options.lockId ?? null
		} ;
	}

	fromExtraction( extractedArray ) {
		return new Batch( this.collection , extractedArray , {
			instantiated: true ,
			lockId: this.meta.lockId
		} ) ;
	}

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

	/*
		Array prototype's methods
	*/

	// This allow batch.map(), to return an Array, not a Batch (it wouldn't make any sense for .map())
	static get [Symbol.species]() { return Array ; }

	concat( ... args ) { return this.fromExtraction( Array.prototype.concat.call( this , ... args ) ) ; }
	filter( fn ) { return this.fromExtraction( Array.prototype.filter.call( this , fn ) ) ; }
	slice( start , end ) { return this.fromExtraction( Array.prototype.slice.call( this , start , end ) ) ; }
	toReversed() { return this.fromExtraction( Array.prototype.toReversed.call( this ) ) ; }
	toSorted( fn ) { return this.fromExtraction( Array.prototype.toSorted.call( this , fn ) ) ; }
	toSpliced( ... args ) { return this.fromExtraction( Array.prototype.toSpliced.call( this , ... args ) ) ; }
	with( index , value ) { return this.fromExtraction( Array.prototype.with.call( this , index , value ) ) ; }

	push( ... args ) {
		this.fromRaw( args ) ;
		super.push( ... args ) ;
	}

	/*
		Async version of some array-like prototype methods
	*/

	async filterAsync( asyncFn ) {
		return this.fromExtraction( await Promise.filter( this , asyncFn ) ) ;
	}

	/*
		API
	*/

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

	commit() {
		// .every() ? .forEach() ? .concurrent() ?
		// syntax: Promise.concurrent( limit , iterable , iterator )
		return Promise.every( this , document => document.commit() ) ;
	}

	async populate( paths , options = {} , population = null ) {
		if ( ! paths ) { paths = [] ; }
		if ( ! population ) { population = new Population( this.collection.world , options ) ; }

		// Here we do not call .populate() for each document, instead we only prepare population,
		// so all population of the same kind are done at once by a single call to .world.populate().
		this.forEach( documentProxy => documentProxy._.preparePopulate( paths , population , options ) ) ;
		await this.collection.world.populate( population , options ) ;
	}

	releaseLocks() {
		if ( this.meta.lockId === null ) { return Promise.resolved ; }
		return this.collection.driver.releaseLocks( this.meta.lockId ).then( count => {
			if ( ! count ) { return count ; }
			for ( let document of this ) {
				if ( document._.meta.lockId === this.meta.lockId ) { document._.meta.lockId = null ; }
			}
			return count ;
		} ) ;
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
// Compatible with array of IDs: in that case, one item may appear multiple times in the index
Batch.raw.indexPathOfId = function indexPathOfId( rawBatch , path , extraKey = null ) {
	//log.error( "indexPathOfId() should get %s inside of --> %I" , path , rawBatch ) ;

	var i , iMax , j , jMax , batchIndex = {} , indexName , element ;

	for ( i = 0 , iMax = rawBatch.length ; i < iMax ; i ++ ) {
		element = tree.dotPath.get( rawBatch[ i ] , path ) ;

		//log.error( "item: should get %s inside of --> %I" , path , rawBatch[ i ] ) ;
		//log.error( "element: %I" , element ) ;

		if ( Array.isArray( element ) ) {
			for ( j = 0 , jMax = element.length ; j < jMax ; j ++ ) {
				if ( extraKey !== null ) {
					indexName = '' + element[ j ][ extraKey ] ;
				}
				else {
					indexName = '' + element[ j ] ;
				}

				if ( ! batchIndex[ indexName ] ) { batchIndex[ indexName ] = [] ; }
				batchIndex[ indexName ].push( rawBatch[ i ] ) ;
			}
		}
		else {
			if ( extraKey !== null ) {
				indexName = '' + element[ extraKey ] ;
			}
			else {
				indexName = '' + element ;
			}

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
		batchIndex[ tree.dotPath.get( rawBatch[ i ] , path ).toString() ] = rawBatch[ i ] ;
	}

	return batchIndex ;
} ;

