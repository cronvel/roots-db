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
	//this.client = null ;
	this.storageDirPath = collection.attachmentConfig.pathname ;
	//console.log( "this.storageDirPath:" , this.storageDirPath ) ;
}

module.exports = FileAttachmentDriver ;
FileAttachmentDriver.prototype = Object.create( events.prototype ) ;
FileAttachmentDriver.prototype.constructor = FileAttachmentDriver ;

FileAttachmentDriver.prototype.type = 'file' ;



FileAttachmentDriver.prototype.initAttachment = function( attachment ) {
	attachment.path = path.join( this.storageDirPath , attachment.documentId , attachment.id ) ;
	//console.log( ".initAttachment():" , attachment.path ) ;
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



FileAttachmentDriver.prototype.saveStream = async function( attachment ) {
	var promise = new Promise() ;

	var fileStream = fs.createWriteStream( attachment.path ) ;
	attachment.incoming.pipe( fileStream ) ;

	fileStream.once( 'error' , error => promise.reject( error ) ) ;

	// Should listen the readable or the writable stream for that?
	attachment.incoming.once( 'end' , () => promise.resolve() ) ;
	fileStream.once( 'end' , () => promise.resolve() ) ;

	return promise ;
} ;



FileAttachmentDriver.prototype.load = function( attachment ) {
	return fs.promises.readFile( attachment.path ).catch( error => {
		switch ( error.code ) {
			case 'ENOENT' :
				throw Error.notFound( "File not found" , error ) ;
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



FileAttachmentDriver.prototype.delete = function( attachment ) {
	return fs.promises.unlink( attachment.path ).catch( error => {
		switch ( error.code ) {
			case 'ENOENT' :
				throw Error.notFound( "File not found" , error ) ;
			default :
				throw error ;
		}
	} ) ;
} ;

