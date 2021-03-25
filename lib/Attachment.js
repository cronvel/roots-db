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
const streamKit = require( 'stream-kit' ) ;
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
	this.metadata = params.metadata && typeof params.metadata === 'object' ? params.metadata : null ;

	// Support string, Buffer and stream
	this._incoming = this.incoming = null ;
	if ( incoming ) { this.setIncoming( incoming ) ; }

	// Kind of a hacky, this is modified afterward when the hash is computed
	this.lastExported = null ;

	this.driver.initAttachment( this ) ;
}

module.exports = Attachment ;



Attachment.prototype.export = function() {
	return this.lastExported = {
		id: this.id ,
		filename: this.filename ,
		contentType: this.contentType ,
		fileSize: this.fileSize ,
		hash: this.hash || null ,
		hashType: this.hashType || null ,
		metadata: this.metadata || null
	} ;
} ;



Attachment.prototype.setIncoming = function( incoming ) {
	this.incoming = this._incoming = incoming ;

	if ( this._incoming instanceof stream.Readable ) {
		if ( this.hashType ) {
			this.incoming = new streamKit.HashStream( this.hashType , this.hash ) ;
			streamKit.pipe( this._incoming , this.incoming ) ;
			this.incoming.once( 'end' , () => this.hash = this.incoming.hash ) ;
		}

		console.error( "bob?" ) ;
		let fileSize = 0 ;
		//this.incoming.on( 'data' , data => fileSize += typeof data === 'string' ? Buffer.byteLength( data ) : data.length ) ;
		//this.incoming.once( 'end' , () => this.fileSize = fileSize ) ;

		this.incoming.on( 'data' , data => {
			fileSize += typeof data === 'string' ? Buffer.byteLength( data ) : data.length ;
			console.error( "yep!" , fileSize ) ;
		} ) ;
		this.incoming.once( 'end' , () => {
			this.fileSize = fileSize ;
			console.error( "finished!" , this.fileSize ) ;
		} ) ;

		return ;
	}

	if ( this.hashType ) {
		let hash = crypto.createHash( this.hashType ).update( incoming ).digest( 'base64' ) ;

		if ( this.hash && this.hash !== hash ) {
			let error = new Error( "Attachment: expecting hash '" + this.hash + "' but got '" + hash + "'." ) ;
			error.expected = this.hash ;
			error.actual = hash ;
			error.code = 'badHash' ;
			throw error ;
		}

		this.hash = hash ;
	}

	this.fileSize = typeof incoming === 'string' ? Buffer.byteLength( incoming ) : incoming.length ;
} ;

Attachment.prototype.setIncoming__ = function( incoming ) {
	this._incoming = incoming ;

	if ( ! this.hashType ) {
		this.incoming = incoming ;
		return ;
	}

	if ( this._incoming instanceof stream.Readable ) {
		this.incoming = new streamKit.HashStream( this.hashType , this.hash ) ;
		streamKit.pipe( this._incoming , this.incoming ) ;
		this.incoming.once( 'end' , () => this.hash = this.incoming.hash ) ;
		return ;
	}

	var hash = crypto.createHash( this.hashType ).update( incoming )
		.digest( 'base64' ) ;

	if ( this.hash && this.hash !== hash ) {
		let error = new Error( "Attachment: expecting hash '" + this.hash + "' but got '" + hash + "'." ) ;
		error.expected = this.hash ;
		error.actual = hash ;
		error.code = 'badHash' ;
		throw error ;
	}

	this.hash = hash ;

	this.incoming = this._incoming ;
} ;



// Update an attachment, replace all metadata, but preserve few metadata like id & path (overwrite the current file)
Attachment.prototype.update = function( params , incoming ) {
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
		if ( this.metadata ) { Object.assign( this.metadata , params.metadata ) ; }
		else { this.metadata = params.metadata ; }
	}

	if ( incoming ) { this.setIncoming( incoming ) ; }
} ;



Attachment.prototype.save = async function() {
	var hasError , error ;

	if ( this.incoming instanceof stream.Readable ) {
		this.incoming.once( 'error' , error_ => hasError = error_ ) ;
	}

	await this.driver.save( this ) ;

	if ( hasError ) {
		error = new Error( 'Save attachment error: ' + hasError ) ;
		error.from = hasError ;
		throw error ;
	}

	if ( this.incoming instanceof streamKit.HashStream ) {
		this.hash = this.incoming.hash ;

		// Hacky: modify the hash value from the last export
		if ( this.lastExported ) { this.lastExported.hash = this.hash ; }
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

