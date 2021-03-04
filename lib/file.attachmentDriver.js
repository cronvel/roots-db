/*
	Roots DB

	Copyright (c) 2014 - 2020 Cédric Ronvel

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

const events = require( 'events' ) ;
//const url = require( 'url' ) ;

const ErrorStatus = require( 'error-status' ) ;

const log = require( 'logfella' ).global.use( 'roots-db:file' ) ;



function FileAttachmentDriver( collection ) {
	this.collection = collection ;
	this.client = null ;

	this.storageDirPath = collection.attachmentConfig.pathname ;
	console.log( "!!!" , this.storageDirPath , collection.attachmentConfig ) ;
	
	//this.isInit = false ;
}



module.exports = FileAttachmentDriver ;
FileAttachmentDriver.prototype = Object.create( events.prototype ) ;
FileAttachmentDriver.prototype.constructor = FileAttachmentDriver ;



FileAttachmentDriver.prototype.type = 'file' ;



/*
// Since .ensurePath() is already used in .save()
FileAttachmentDriver.prototype.init = async function() {
	try {
		await fsKit.ensurePath( this.storageDirPath ) ;
	}
	catch ( error_ ) {
		throw ErrorStatus.internalError( "FileAttachmentDriver: cannot write to directory '" + this.storageDirPath + "', " + error_.toString() ) ;
	}

	this.isInit = true ;
} ;
*/



FileAttachmentDriver.prototype.initAttachment = function( attachment ) {
	attachment.path = path.join( this.storageDirPath , attachment.documentId , attachment.id ) ;
} ;



FileAttachmentDriver.prototype.save = async function( attachment ) {
	console.log( "filePath:" , attachment.path ) ;
	await fsKit.ensurePath( path.dirname( attachment.path ) ) ;

	//if ( ! this.isInit ) { await this.init() ; }

	if ( typeof attachment.incoming === 'string' || Buffer.isBuffer( attachment.incoming ) ) {
		return this.saveContent( attachment ) ;
	}
	else if ( attachment.incoming instanceof stream.Readable ) {
		return this.saveStream( attachment ) ;
	}
	else {
		log.error( "FileAttachmentDriver, type of data is not supported, should be string, Buffer or ReadableStream" ) ;
		throw new Error( "[roots-db] FileAttachmentDriver, type of data is not supported, should be string, Buffer or ReadableStream" ) ;
	}
} ;



FileAttachmentDriver.prototype.saveContent = async function( attachment ) {
	log.debug( "FileAttachmentDriver debug: Write" ) ;
	try {
		return fs.promises.writeFile( attachment.path , attachment.incoming ) ;
	}
	catch ( error ) {
		console.error( "FileAttachmentDriver.prototype.saveContent():" , error ) ;
		throw error ;
	}
} ;



FileAttachmentDriver.prototype.saveStream = async function( attachment ) {
	var promise = new Promise() ;
	log.debug( "FileAttachmentDriver debug: Stream/Pipe" ) ;

	//fileHandle = await fs.promises.open( attachment.path , 'w' ) ;
	var fileStream = fs.createWriteStream( attachment.path , { fd: fileHandle.fd , defaultEncoding: 'binary' } ) ;
	attachment.incoming.pipe( fileStream ) ;

	fileStream.once( 'error' , error => promise.reject( error ) ) ;

	// Should listen the readable or the writable stream for that?
	attachment.incoming.once( 'end' , () => promise.resolve() ) ;
	fileStream.once( 'end' , () => promise.resolve() ) ;

	return promise ;
} ;



FileAttachmentDriver.prototype.load = function( attachment ) {
	var filePath = path.join( this.storageDirPath , attachment.url ) ;
	return fs.promises.readFile( filePath ) ;
} ;



FileAttachmentDriver.prototype.getReadStream = function( attachment ) {
	var fileStream ,
		promise = new Promise() ,
		filePath = path.join( this.storageDirPath , attachment.url ) ;

	fileStream = fs.createReadStream( filePath , { defaultEncoding: 'binary' } ) ;

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

