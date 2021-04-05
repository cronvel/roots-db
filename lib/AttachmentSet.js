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



const Attachment = require( './Attachment.js' ) ;

const Promise = require( 'seventh' ) ;

const fs = require( 'fs' ) ;
const fsKit = require( 'fs-kit' ) ;

const tree = require( 'tree-kit' ) ;
const dotPath = tree.dotPath ;

const path = require( 'path' ) ;

const stream = require( 'stream' ) ;
const crypto = require( 'crypto' ) ;

const ErrorStatus = require( 'error-status' ) ;

const log = require( 'logfella' ).global.use( 'roots-db' ) ;



function AttachmentSet( params , incoming ) {
	//this.id = '' + params.id ;
	this.set = {} ;

	Object.defineProperties( this , {
		// Not enumerable: not JSON-stringified
		collectionName: { writable: true , value: params.collectionName } ,
		documentId: { writable: true , value: '' + params.documentId } ,
		path: { writable: true , value: null } ,

		// Support string, Buffer and stream
		incoming: { writable: true , value: null } ,

		// Document stuffs
		document: { configurable: true , value: null } ,	// a ref to the host document
		documentPath: { configurable: true , value: null } ,	// the path of the attachment inside of the document
		documentRaw: { configurable: true , value: null }	// raw value, that will be passed to the DB, in Document#raw
	} ) ;

	if ( incoming ) { this.setIncoming( incoming ) ; }
}

module.exports = AttachmentSet ;



AttachmentSet.prototype.setIncoming = function( incoming ) {
	//if ( ! this.set.source ) {}
} ;



// Internal
AttachmentSet.prototype.createAttachment = function( key , params , incoming ) {
	params = Object.assign( {} , params , {
		id: params.id ,
		//id: this.id + '-' + key ,
		collectionName: this.collectionName ,
		documentId: '' + this.documentId
	} ) ;

	this.set[ key ] = new Attachment( params , incoming ) ;
} ;

