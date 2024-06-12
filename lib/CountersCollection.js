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
const Collection = require( './Collection.js' ) ;
//const Document = require( './Document.js' ) ;
//const Batch = require( './Batch.js' ) ;
//const Population = require( './Population.js' ) ;
const tree = require( 'tree-kit' ) ;

const Promise = require( 'seventh' ) ;

//const ErrorStatus = require( 'error-status' ) ;

const log = require( 'logfella' ).global.use( 'roots-db' ) ;



function CountersCollection( world , name , schema = {} ) {
	Object.assign( schema , CountersCollection.schema ) ;

	if ( ! schema.indexes ) { schema.indexes = [] ; }

	schema.indexes.push( { properties: { name: 1 } , unique: true } ) ;

	Collection.call( this , world , name , schema ) ;
}

CountersCollection.prototype = Object.create( Collection.prototype ) ;
CountersCollection.prototype.constructor = CountersCollection ;

module.exports = CountersCollection ;



CountersCollection.prototype.isCounters = true ;



CountersCollection.schema = {
	properties: {
		name: {
			type: 'string' ,
			sanitize: [ 'toString' ] ,
			default: 'auto' ,
			system: true ,
			tags: [ 'system-content' ]
		} ,
		counter: {
			type: 'integer' ,
			sanitize: [ 'toInteger' ] ,
			default: 0 ,
			system: true ,
			tags: [ 'system-content' ]
		}
	}
} ;



CountersCollection.prototype.getNextCounterFor = async function( name ) {
	
} ;

