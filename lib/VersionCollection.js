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



const rootsDb = require( './rootsDb.js' ) ;
const Collection = require( './Collection.js' ) ;
//const Document = require( './Document.js' ) ;
//const Batch = require( './Batch.js' ) ;
//const Population = require( './Population.js' ) ;

const Promise = require( 'seventh' ) ;

//const ErrorStatus = require( 'error-status' ) ;

const log = require( 'logfella' ).global.use( 'roots-db' ) ;



function VersionCollection( world , name , schema ) {
	Object.assign( schema , VersionCollection.schema ) ;
	Collection.call( this , world , name , schema ) ;

	//Collection.ensureIndex( schema.indexes , { properties: { '_active._id': 1 , '_active._collection': 1 } } ) ;
}

VersionCollection.prototype = Object.create( Collection.prototype ) ;
VersionCollection.prototype.constructor = VersionCollection ;

module.exports = VersionCollection ;



VersionCollection.schema = {
	extraProperties: true ,
	properties: {
		_version: {
			type: 'integer' ,
			sanitize: [ 'toInteger' ] ,
			system: true ,
			tags: [ 'system' ]
		} ,
		_lastModified: {
			type: 'date' ,
			sanitize: [ 'toDate' ] ,
			system: true ,
			tags: [ 'system' ]
		} ,
		_activeVersion: {
			type: 'link' ,
			anyCollection: true
		}
	} ,
	indexes: [
		{ properties: { '_active._id': 1 , '_active._collection': 1 } } ,
		{ properties: { '_active._id': 1 , '_active._collection': 1 , _version: 1 } , unique: true }
	]
} ;



VersionCollection.versioningSchemaPropertiesOveride = {
	_version: {
		type: 'integer' ,
		sanitize: [ 'toInteger' ] ,
		system: true ,
		tags: [ 'system' ]
	} ,
	_lastModified: {
		type: 'date' ,
		sanitize: [ 'toDate' ] ,
		system: true ,
		tags: [ 'system' ]
	}
} ;

