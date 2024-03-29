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
const url = require( 'url' ) ;



const misc = {} ;
module.exports = misc ;



// Clone a raw document safely, nested array and plain-object are cloned recursively, non-plain object are referenced
misc.clone = function( rawDocument ) {
	var k ,
		exported = Array.isArray( rawDocument ) ? [] : {} ;

	for ( k in rawDocument ) {
		if ( rawDocument[ k ] && typeof rawDocument[ k ] === 'object' ) {
			switch ( rawDocument[ k ].constructor ) {
				case Array :
				case Object :
					exported[ k ] = misc.clone( rawDocument[ k ] ) ;
					break ;
				case Date :
					exported[ k ] = new Date( rawDocument[ k ] ) ;
					break ;
				default :
					exported[ k ] = rawDocument[ k ] ;
			}
		}
		else {
			exported[ k ] = rawDocument[ k ] ;
		}
	}

	return exported ;
} ;



misc.connectionlessUrl = function( urlString ) {
	var urlObject = new url.URL( urlString ) ;
	urlObject.username = '' ;
	urlObject.password = '' ;
	return urlObject.toString() ;
} ;



// DEPRECATED
misc.mapIds = function( element ) { return element._id ; } ;



// Use it with .bind()
// DEPRECATED
misc.mapIdsAndCheckCollection = function( collectionName , element ) {
	if ( ! element || ! element.$ || ! ( element.$ instanceof rootsDb.Document ) || element.$.collection.name !== collectionName ) {
		//console.error( arguments ) ;
		throw new TypeError( ".mapIdsAndCheckCollection(): not a Document." ) ;
	}

	return element._id ;
} ;



// Use it with .bind()
// DEPRECATED
misc.filterOutId = function( id , element ) {
	//console.error( '\nfilterOutId:' , arguments ) ;
	return id.toString() !== element.toString() ;
} ;

