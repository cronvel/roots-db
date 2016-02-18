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

"use strict" ;



// Load modules
var async = require( 'async-kit' ) ;



// bulk( method , arrayOfObjects , [arg1] , [arg2] , [...] , callback )
// An utility for bulk action
module.exports = function bulk()
{
	if (
		arguments.length < 3 ||
		typeof arguments[ 0 ] !== 'string' ||
		! arguments[ 1 ] || typeof arguments[ 1 ] !== 'object' ||
		typeof arguments[ arguments.length - 1 ] !== 'function'
	)
	{
		throw new Error( "[roots-db] bulk() usage is: bulk( method , arrayOfObjects , [arg1] , [arg2] , [...] , callback )" ) ;
	}
	
	var bulkMethod = arguments[ 0 ] ;
	var objectArray = arguments[ 1 ] ;
	var bulkCallback = arguments[ arguments.length - 1 ] ;
	var bulkArgs = Array.prototype.slice.call( arguments , 2 , -1 ) ;
	
	async.foreach( objectArray , function( object , foreachCallback ) {
		if ( object.$ ) { object = object.$ ; }
		object[ bulkMethod ].apply( object , bulkArgs.concat( foreachCallback ) ) ;
	} )
	.parallel()
	.exec( bulkCallback ) ;
} ;
