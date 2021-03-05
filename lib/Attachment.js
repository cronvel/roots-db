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

const fs = require( 'fs' ) ;
const fsKit = require( 'fs-kit' ) ;

const path = require( 'path' ) ;
const stream = require( 'stream' ) ;

const ErrorStatus = require( 'error-status' ) ;

const log = require( 'logfella' ).global.use( 'roots-db' ) ;



function Attachment( params , incoming ) {
	this.collectionName = params.collectionName ;
	this.documentId = params.documentId ;
	this.id = params.id ;
	this.driver = params.driver ;
	this.path = null ;

	// Metadata
	this.filename = params.filename ;		// the filename when the client download it
	this.contentType = params.contentType ;

	// Support string, Buffer and stream
	this.incoming = incoming ;

	this.driver.initAttachment( this ) ;
}

module.exports = Attachment ;



Attachment.prototype.export = function() {
	return {
		id: this.id ,
		filename: this.filename ,
		contentType: this.contentType
	} ;
} ;



// Update an attachment, replace all metadata, but preserve few metadata like id & path (overwrite the current file)
Attachment.prototype.update = function( metadata , incoming ) {
	// Only change read-only data
	if ( metadata.filename ) { this.filename = metadata.filename ; }
	if ( metadata.contentType ) { this.contentType = metadata.contentType ; }

	this.incoming = incoming ;
} ;



Attachment.prototype.save = function() {
	return this.driver.save( this ) ;
} ;



Attachment.prototype.load = function() {
	return this.driver.load( this ) ;
} ;



Attachment.prototype.getReadStream = function() {
	return this.driver.getReadStream( this ) ;
} ;



Attachment.prototype.delete = function() {
	return this.driver.delete( this ) ;
} ;

