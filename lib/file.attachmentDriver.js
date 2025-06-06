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
const stream = require( 'stream' ) ;
const path = require( 'path' ) ;
//const url = require( 'url' ) ;

const hash = require( 'hash-kit' ) ;
const events = require( 'events' ) ;

const ErrorStatus = require( 'error-status' ) ;

const log = require( 'logfella' ).global.use( 'roots-db:file' ) ;



const storageEndpointFoolproof = new Map() ;



function FileAttachmentDriver( collection ) {
	this.collection = collection ;
	this.storageDirPath = collection.attachmentConfig.pathname ;
	this.appendExtension = collection.attachmentAppendExtension ;

	if ( storageEndpointFoolproof.has( this.storageDirPath ) ) {
		let firstCollectionUrl = storageEndpointFoolproof.get( this.storageDirPath ) ;

		if ( firstCollectionUrl !== collection.url ) {
			log.error(
				"Multiple collections should not share the same file storage endpoint: %s\nFirst collection using it: %s\nNew collection trying to use it: %s" ,
				this.storageDirPath ,
				firstCollectionUrl ,
				collection.url
			) ;
			let error = new Error( "Multiple collections should not share the same file storage endpoint: " + this.storageDirPath ) ;
			error.code = 'storageEndpointSharing' ;
			throw error ;
		}
	}
	else {
		storageEndpointFoolproof.set( this.storageDirPath , collection.url ) ;
	}
}

module.exports = FileAttachmentDriver ;
FileAttachmentDriver.prototype = Object.create( events.prototype ) ;
FileAttachmentDriver.prototype.constructor = FileAttachmentDriver ;

FileAttachmentDriver.prototype.type = 'file' ;



FileAttachmentDriver.prototype.initAttachment = function( attachment ) {
	// First, check if the ID is set
	if ( ! attachment.id ) {
		attachment.id = hash.randomBase36String( 24 ) ;
		if ( this.appendExtension && attachment.extension ) { attachment.id += '.' + attachment.extension ; }
	}

	var pathEnd = path.join( attachment.documentId , attachment.id ) ;
	attachment.path = path.join( this.storageDirPath , pathEnd ) ;

	// It always ends with a slash, so we can concat it immediately
	attachment.publicUrl = this.collection.attachmentPublicBaseUrl ?
		this.collection.attachmentPublicBaseUrl + pathEnd :
		null ;

	//console.log( ".initAttachment():" , attachment.path ) ;
} ;



FileAttachmentDriver.prototype.load = function( attachment ) {
	return fs.promises.readFile( attachment.path ).catch( error => {
		switch ( error.code ) {
			case 'ENOENT' :
				throw ErrorStatus.notFound( "File not found" , error ) ;
			default :
				throw error ;
		}
	} ) ;
} ;



FileAttachmentDriver.prototype.getReadStream = function( attachment ) {
	var fileStream ,
		promise = new Promise() ;

	fileStream = fs.createReadStream( attachment.path , { defaultEncoding: 'binary' } ) ;

	fileStream.once( 'error' , ( error ) => {
		log.error( 'FileAttachmentDriver .getReadStream() error event: %E' , error ) ;

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



FileAttachmentDriver.prototype.save = async function( attachment ) {
	await fsKit.ensurePath( path.dirname( attachment.path ) ) ;

	if ( typeof attachment.incoming === 'string' || Buffer.isBuffer( attachment.incoming ) ) {
		return this.saveContent( attachment ) ;
	}
	else if ( attachment.incoming instanceof stream.Readable ) {
		return this.saveStream( attachment ) ;
	}

	log.error( "FileAttachmentDriver, type of data is not supported, should be string, Buffer or ReadableStream" ) ;
	throw new Error( "[roots-db] FileAttachmentDriver, type of data is not supported, should be string, Buffer or ReadableStream" ) ;
} ;



FileAttachmentDriver.prototype.saveContent = function( attachment ) {
	return fs.promises.writeFile( attachment.path , attachment.incoming ) ;
} ;



FileAttachmentDriver.prototype.saveStream = function( attachment ) {
	if ( attachment.incoming.errored ) {
		return Promise.reject( new Error( "File Attachment Driver (stream): stream already have an error: " + attachment.incoming.errored ) ) ;
	}

	if ( attachment.incoming.readableEnded || attachment.incoming.closed || attachment.incoming.destroyed ) {
		return Promise.reject( new Error( "File Attachment Driver (stream): stream is already consumed" ) ) ;
	}

	var fileStream = fs.createWriteStream( attachment.path ) ;
	attachment.incoming.pipe( fileStream ) ;

	return Promise.all( [
		Promise.onceEventOrError( attachment.incoming , 'end' ) ,
		Promise.onceEventOrError( fileStream , 'finish' )
	] ) ;
} ;



FileAttachmentDriver.prototype.delete = function( attachment ) {
	return fs.promises.unlink( attachment.path ).catch( error => {
		switch ( error.code ) {
			case 'ENOENT' :
				throw ErrorStatus.notFound( "File not found" , error ) ;
			default :
				throw error ;
		}
	} ) ;
} ;



// Delete all attachment for the current document
FileAttachmentDriver.prototype.deleteAllInDocument = function( documentId ) {
	var dirPath = path.join( this.storageDirPath , documentId ) ;
	log.debug( "FileAttachmentDriver#deleteAllInDocument() deleting '%s'" , dirPath ) ;
	if ( ! dirPath ) { return Promise.resolved ; }
	return fsKit.deltree( dirPath ) ;
} ;



// Delete all attachment for the current collection
FileAttachmentDriver.prototype.clear = function() {
	var dirPath = path.join( this.storageDirPath ) ;
	log.debug( "FileAttachmentDriver#clear() deleting '%s'" , dirPath ) ;
	if ( ! dirPath ) { return Promise.resolved ; }
	return fsKit.deltree( dirPath ) ;
} ;

