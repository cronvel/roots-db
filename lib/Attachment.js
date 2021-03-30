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

const tree = require( 'tree-kit' ) ;
const dotPath = tree.dotPath ;

const path = require( 'path' ) ;

const stream = require( 'stream' ) ;
const crypto = require( 'crypto' ) ;

const ErrorStatus = require( 'error-status' ) ;

const log = require( 'logfella' ).global.use( 'roots-db' ) ;



function Attachment( params , incoming ) {
	this.collectionName = params.collectionName ;
	this.documentId = params.documentId ;
	this.id = params.id ;
	this.driver = params.driver ;
	this.path = null ;
	this.publicUrl = null ;

	// System metadata
	this.filename = params.filename ;		// the filename when the client download it
	this.contentType = params.contentType ;
	this.fileSize = params.fileSize || null ;
	this.hash = params.hash || null ;
	this.hashType =
		params.hashType && typeof params.hashType === 'string' ? params.hashType :
		params.hashType ? 'sha256' :
		null ;

	// Content metadata
	this.metadata = params.metadata && typeof params.metadata === 'object' ? params.metadata : {} ;

	// Support string, Buffer and stream
	this.incoming = null ;
	if ( incoming ) { this.setIncoming( incoming ) ; }

	// Document stuffs
	this.document = null ;	// a ref to the host document
	this.documentPath = null ;	// the path of the attachment inside of the document
	this.documentRaw = null ;	// raw value, that will be passed to the DB, in Document#raw

	this.driver.initAttachment( this ) ;
}

module.exports = Attachment ;



Attachment.prototype.attachToDocument = function( document , documentPath ) {
	document = document._ ;	// Force the Document instance

	if ( this.document ) {
		if ( this.document === document ) { return ; }
		throw new Error( 'Attachment is already attached to a Document' ) ;
	}

	this.document = document ;
	this.documentPath = documentPath ;

	// This is the raw data (saved to the DB)
	this.documentRaw = {
		id: this.id ,
	} ;

	this.updateRaw() ;
	
	// Place the raw data in the correct document's path
	dotPath.set( this.document.raw , this.documentPath , this.documentRaw ) ;

	// Immediately populate it
	this.document.populatedDocumentProxies.set( this.documentRaw , this ) ;

	// Stage the change now!
	this.document.stage( this.documentPath ) ;
} ;



// Update only things that could be updated
Attachment.prototype.updateRaw = function() {
	Object.assign( this.documentRaw , {
		filename: this.filename ,
		contentType: this.contentType ,
		fileSize: this.fileSize ,
		hash: this.hash ,
		hashType: this.hashType ,
		metadata: this.metadata
	} ;
} ;



// Update an attachment, replace all metadata, but preserve few metadata like id & path (overwrite the current file)
Attachment.prototype.updateMeta = function( params ) {
	// Don't change read-only data
	if ( params.filename ) { this.filename = params.filename ; }
	if ( params.contentType ) { this.contentType = params.contentType ; }

	if ( params.hash ) {
		this.hash = params.hash ;
		if ( params.hashType ) { this.hashType = params.hashType ; }
	}
	else if ( params.hashType ) {
		this.hash = null ;
		this.hashType = params.hashType ;
	}

	if ( params.metadata && typeof params.metadata === 'object' ) {
		Object.assign( this.metadata , params.metadata ) ;
	}

	if ( this.document ) {
		// Update documentRaw if needed, and don't forget to stage the change
		this.updateRaw() ;
		this.document.stage( this.documentPath ) ;
	}
} ;



Attachment.prototype.setIncoming = function( incoming ) {
	var hash , cryptoHash , fileSize = 0 ;

	this.incoming = incoming ;

	if ( this.hashType ) { cryptoHash = crypto.createHash( this.hashType ) ; }


	// Stream case

	if ( this.incoming instanceof stream.Readable ) {
		// Pause the stream now, because listening for 'data' will start consuming the stream immediately,
		// it may even kick in before piping it (rare bug, that has already happened).
		this.incoming.pause() ;

		this.incoming.on( 'data' , data => {
			fileSize += typeof data === 'string' ? Buffer.byteLength( data ) : data.length ;
			if ( cryptoHash ) { cryptoHash.update( data ) ; }
		} ) ;

		this.incoming.once( 'end' , () => {
			if ( this.fileSize !== null && this.fileSize !== fileSize ) {
				let error = new Error( "Attachment (stream): expecting file size '" + this.fileSize + "' but got '" + fileSize + "'." ) ;
				error.expected = this.fileSize ;
				error.actual = fileSize ;
				error.code = 'badFileSize' ;
				this.incoming.emit( 'error' , error ) ;
				return ;
			}

			this.fileSize = fileSize ;

			if ( cryptoHash ) {
				hash = cryptoHash.digest( 'base64' ) ;

				if ( this.hash && this.hash !== hash ) {
					let error = new Error( "Attachment (stream): expecting hash '" + this.hash + "' but got '" + hash + "'." ) ;
					error.expected = this.hash ;
					error.actual = hash ;
					error.code = 'badHash' ;
					this.incoming.emit( 'error' , error ) ;
					return ;
				}

				this.hash = hash ;
			}
		} ) ;

		return ;
	}


	// Buffer/string case

	fileSize = typeof incoming === 'string' ? Buffer.byteLength( incoming ) : incoming.length ;

	if ( this.fileSize !== null && this.fileSize !== fileSize ) {
		let error = new Error( "Attachment: expecting file size '" + this.fileSize + "' but got '" + fileSize + "'." ) ;
		error.expected = this.fileSize ;
		error.actual = fileSize ;
		error.code = 'badFileSize' ;
		throw error ;
	}

	this.fileSize = fileSize ;

	if ( cryptoHash ) {
		hash = cryptoHash.update( incoming ).digest( 'base64' ) ;

		if ( this.hash && this.hash !== hash ) {
			let error = new Error( "Attachment: expecting hash '" + this.hash + "' but got '" + hash + "'." ) ;
			error.expected = this.hash ;
			error.actual = hash ;
			error.code = 'badHash' ;
			throw error ;
		}

		this.hash = hash ;
	}
} ;



Attachment.prototype.save = async function() {
	var hasError , error ;

	if ( this.incoming instanceof stream.Readable ) {
		// Force an error handler, since we don't know if the driver will handle it
		this.incoming.once( 'error' , error_ => hasError = error_ ) ;
	}

	await this.driver.save( this ) ;

	if ( hasError ) {
		error = new Error( 'Save attachment error: ' + hasError ) ;
		error.from = hasError ;
		throw error ;
	}

	// Modify hash and fileSize value on the Document's raw object
	if ( this.document && ( this.incoming instanceof stream.Readable ) ) {
		this.documentRaw.fileSize = this.fileSize ;
		this.documentRaw.hash = this.hash ;

		// Don't forget to stage the change
		this.document.stage( this.documentPath ) ;
	}
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

