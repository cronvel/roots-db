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



// Load modules
const Promise = require( 'seventh' ) ;



// bulk( method , arrayOfObjects , [arg1] , [arg2] , [...] , callback )
// An utility for bulk action
module.exports = function bulk( method , arrayOfObjects , ... args ) {
	var callback = args.pop() ;

	if ( typeof method !== 'string' || ! Array.isArray( arrayOfObjects ) || typeof callback !== 'function' ) {
		throw new Error( "[roots-db] bulk() usage is: bulk( method , arrayOfObjects , [arg1] , [arg2] , [...] , callback )" ) ;
	}

	Promise.map( arrayOfObjects , object => {
		if ( object.$ ) { object = object.$ ; }
		return Promise.promisifyAll( object[ method ] , object )( ... args ) ;
	} )
		.callbackAll( callback ) ;
} ;

