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



const Promise = require( 'seventh' ) ;

const fs = require( './promise-fs.js' ) ;
const fsKit = require( './promise-fs-kit.js' ) ;

const path = require( 'path' ) ;
const stream = require( 'stream' ) ;

const ErrorStatus = require( 'error-status' ) ;

const log = require( 'logfella' ).global.use( 'roots-db' ) ;



function Attachment( metaData , incoming ) {
	this.collectionName = metaData.collectionName ;
	this.documentId = metaData.documentId ;
	this.id = metaData.id ;
	this.baseUrl = metaData.baseUrl ;
	this.filename = metaData.filename ;
	this.contentType = metaData.contentType ;
	this.fullUrl = metaData.baseUrl + metaData.documentId + '/' + metaData.id ;

	// Temp... Should support string, buffer and stream
	this.incoming = incoming ;
}

module.exports = Attachment ;



Attachment.prototype.export = function() {
	return {
		id: this.id ,
		filename: this.filename ,
		contentType: this.contentType
	} ;
} ;



// Update an attachment, replace all metaData, but preserve few metaData like id & path (overwrite the current file)
Attachment.prototype.update = function( metaData , incoming ) {
	// Only change read-only data
	if ( metaData.filename ) { this.filename = metaData.filename ; }
	if ( metaData.contentType ) { this.contentType = metaData.contentType ; }

	this.incoming = incoming ;
} ;



Attachment.prototype.save = async function() {
	// First try to save without any checking access, if it fails, try to fix that and retry.
	try {
		await this.saveNoCheck() ;
	}
	catch ( error ) {
		try {
			await fsKit.ensurePathAsync( path.dirname( this.fullUrl ) ) ;
			await this.saveNoCheck() ;
		}
		catch ( error_ ) {
			throw ErrorStatus.internalError( "RootsDb attachment: cannot write to path '" + this.fullUrl + "', " + error_.toString() ) ;
		}
	}
} ;



// Try saving without checking FS accesses
Attachment.prototype.saveNoCheck = async function() {
	var fileStream , promise , fd ;

	if ( typeof this.incoming === 'string' || Buffer.isBuffer( this.incoming ) ) {
		log.debug( "Attachment debug: Write" ) ;
		return fs.writeFileAsync( this.fullUrl , this.incoming ) ;
	}
	else if ( this.incoming instanceof stream.Readable ) {
		promise = new Promise() ;
		log.debug( "Attachment debug: Stream/Pipe" ) ;

		fd = await fs.openAsync( this.fullUrl , 'w' ) ;
		fileStream = fs.createWriteStream( this.fullUrl , { fd: fd , defaultEncoding: 'binary' } ) ;
		this.incoming.pipe( fileStream ) ;

		fileStream.once( 'error' , error => promise.reject( error ) ) ;

		// Should listen the readable or the writable stream for that?
		this.incoming.once( 'end' , () => promise.resolve() ) ;
		fileStream.once( 'end' , () => promise.resolve() ) ;

		return promise ;
	}

	log.error( "Attachment, type of data is not supported, should be string, Buffer or ReadableStream" ) ;
	throw new Error( "[roots-db] Attachment, type of data is not supported, should be string, Buffer or ReadableStream" ) ;

} ;



Attachment.prototype.load = function() {
	return fs.readFileAsync( this.fullUrl ) ;
} ;



Attachment.prototype.getReadStream = function() {
	var fileStream ,
		promise = new Promise() ;

	fileStream = fs.createReadStream( this.fullUrl , { defaultEncoding: 'binary' } ) ;

	fileStream.once( 'error' , ( error ) => {
		log.error( 'Attachment .getReadStream() error event: %E' , error ) ;

		if ( error.code === 'ENOENT' ) {
			promise.reject( ErrorStatus.notFound( {
				message: 'File not found: ' + error.toString() ,
				safeMessage: 'File not found'
			} ) ) ;
		}
		else {
			promise.reject( error ) ;
		}
	} ) ;

	fileStream.once( 'open' , () => {
		promise.resolve( fileStream ) ;
	} ) ;

	return promise ;
} ;

