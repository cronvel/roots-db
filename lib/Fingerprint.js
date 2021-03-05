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



const rootsDb = require( './rootsDb.js' ) ;
const tree = require( 'tree-kit' ) ;



/*
	The goal of this class is to figure out weither a fingerprint is unique or not, thus detecting if a query should return a Batch or a Document.
	Some other data may be calculated at that time.
	For instance, it does not much.

	options:
		* isPartial: a sparse document is given
*/
function Fingerprint( collection , rawFingerprint = {} , isPartial = false ) {
	this.collection = collection ;
	collection.driver.checkId( rawFingerprint ) ;

	this.fromPartial = !! isPartial ;
	this.def = null ;
	this.partial = null ;

	if ( this.fromPartial ) {
		this.partial = rawFingerprint ;
		Object.defineProperty( this , 'def' , {
			configurable: true ,
			enumerable: true ,
			get: Fingerprint.prototype.flatten.bind( this )
		} ) ;
	}
	else {
		this.def = rawFingerprint ;
		Object.defineProperty( this , 'partial' , {
			configurable: true ,
			enumerable: true ,
			get: Fingerprint.prototype.unflatten.bind( this )
		} ) ;
	}

	//Object.defineProperty( rawFingerprint , '$' , { value: this } ) ;
}

module.exports = Fingerprint ;



// Lazyloading
Object.defineProperties( Fingerprint.prototype , {
	unique: {
		configurable: true ,
		enumerable: true ,
		get: function() { return this.uniquenessCheck() ; }
	}
} ) ;



Fingerprint.prototype.uniquenessCheck = function uniquenessCheck() {
	var i , j , index , match , uniques = this.collection.uniques ;

	for ( i = 0 ; i < uniques.length ; i ++ ) {
		index = uniques[ i ] ;
		match = 0 ;
		for ( j = 0 ; j < index.length ; j ++ ) {
			if ( index[ j ] in this.def ) { match ++ ; }
		}

		if ( match === index.length ) {
			Object.defineProperty( this , 'unique' , { value: true , enumerable: true } ) ;
			return true ;
		}
	}

	Object.defineProperty( this , 'unique' , { value: false , enumerable: true } ) ;
	return false ;
} ;



Fingerprint.prototype.flatten = function flatten() {
	var def = tree.extend( { flat: true , immutables: this.collection.immutables } , null , this.partial ) ;

	//Object.defineProperty( def , '$' , { value: this } ) ;
	Object.defineProperty( this , 'def' , { value: def , enumerable: true } ) ;

	return def ;
} ;



Fingerprint.prototype.unflatten = function unflatten() {
	var partial = tree.extend( { unflat: true , immutables: this.collection.immutables } , null , this.def ) ;

	//Object.defineProperty( partial , '$' , { value: this } ) ;
	Object.defineProperty( this , 'partial' , { value: partial , enumerable: true } ) ;

	return partial ;
} ;

