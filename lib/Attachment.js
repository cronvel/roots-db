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

const tree = require( 'tree-kit' ) ;
const dotPath = tree.dotPath ;

const path = require( 'path' ) ;
const stream = require( 'stream' ) ;
const crypto = require( 'crypto' ) ;

const fileType = require( 'file-type' ) ;

function noop() {}

const ErrorStatus = require( 'error-status' ) ;

const log = require( 'logfella' ).global.use( 'roots-db' ) ;



function Attachment( params , incoming ) {
	this.id = params.id && typeof params.id === 'string' ? params.id : null ;
	this.publicUrl = null ;

	// System metadata
	this.filename = params.filename || 'unnamed' ;		// the filename when the client download it
	this.extension = params.extension || this.getExtension( this.filename ) ;			// the file extension
	this.contentType = params.contentType || 'application/octet-stream' ;	// This is either pass by the client, or in rare case guessed by the file extension
	this.binaryContentType = params.binaryContentType || null ;	// This is directly guessed by the file content, mostly using magic numbers, but it's not always activated
	this.fileSize = params.fileSize || null ;
	this.hash = params.hash || null ;
	this.hashType =
		params.hashType && typeof params.hashType === 'string' ? params.hashType :
		params.hashType ? 'sha256' :
		null ;

	// Content metadata
	this.metadata = params.metadata && typeof params.metadata === 'object' ? params.metadata : {} ;


	// Internal not enumerable stuffs, and maybe not writable as well

	Object.defineProperties( this , {
		// Not enumerable: not JSON-stringified
		collectionName: { writable: true , value: params.collectionName } ,
		documentId: { writable: true , value: '' + params.documentId } ,
		path: { writable: true , value: null } ,

		// Immutable
		driver: { value: params.driver } ,
		constraints: { value: params.constraints ?? null } ,

		// Support string, Buffer and stream
		incoming: { writable: true , value: null } ,

		// Controle data
		upstreamExists: { writable: true , value: !! params.upstreamExists } ,
		savePromise: { writable: true , value: null } ,
		toDelete: { writable: true , value: false } ,		// mark attachments that needs to be deleted
		deleted: { writable: true , value: false } ,
		deletePromise: { writable: true , value: null } ,
		isPartOfSet: { writable: true , value: !! params.isPartOfSet } ,	// if true, it is part of an AttachmentSet

		// Async flow controle
		fileCheckerPromise: { writable: true , value: null } ,

		// Document stuffs
		document: { configurable: true , value: null } ,	// a ref to the host document
		documentPath: { configurable: true , value: null } ,	// the path of the attachment inside of the document
		documentRaw: { configurable: true , value: null }	// raw value, that will be passed to the DB, in Document#raw
	} ) ;

	this.driver.initAttachment( this ) ;

	if ( incoming ) { this.setIncoming( incoming ) ; }
}

module.exports = Attachment ;



Attachment.prototype.attachToDocument = function( document , documentPath , restoredFromRaw = null ) {
	document = document._ ;	// Force the Document instance

	if ( this.document ) {
		if ( this.document === document ) { return ; }
		throw new Error( 'Attachment is already attached to a Document' ) ;
	}

	// Make those properties not writable anymore
	Object.defineProperties( this , {
		document: { value: document } ,
		documentPath: { value: documentPath } ,

		// This is the raw data (saved to/restored from the DB)
		documentRaw: { value: restoredFromRaw || { id: this.id } }
	} ) ;

	if ( ! this.documentRaw.id ) { this.documentRaw.id = this.id ; }

	if ( ! restoredFromRaw ) {
		// Update raw data now. Should not update on restore? (except maybe if the lib has changed).
		this.updateRaw() ;

		// Place the raw data in the correct document's path
		dotPath.set( this.document.raw , this.documentPath , this.documentRaw ) ;

		// Stage the change now!
		this.document.stage( this.documentPath ) ;
	}

	// Immediately populate it
	this.document.populatedDocumentProxies.set( this.documentRaw , this ) ;
} ;



// Internal
// Update only things that could be updated
Attachment.prototype.updateRaw = function() {
	Object.assign( this.documentRaw , {
		filename: this.filename ,
		extension: this.extension ,
		contentType: this.contentType ,
		binaryContentType: this.binaryContentType ,
		fileSize: this.fileSize ,
		hash: this.hash ,
		hashType: this.hashType ,
		metadata: this.metadata
	} ) ;
} ;



// Update an attachment, replace all metadata, but preserve few metadata like id & path (overwrite the current file)
Attachment.prototype.set = function( params ) {
	var key , toStage = [] ;

	// Don't change read-only data
	if ( params.filename && this.filename !== params.filename ) {
		this.filename = params.filename ;
		this.extension = this.getExtension( this.filename ) ;
		toStage.push( this.documentPath + '.filename' ) ;
		toStage.push( this.documentPath + '.extension' ) ;
	}

	if ( params.extension && this.extension !== params.extension ) {
		this.extension = params.extension ;
		toStage.push( this.documentPath + '.extension' ) ;
	}

	if ( params.contentType && this.contentType !== params.contentType ) {
		this.contentType = params.contentType ;
		toStage.push( this.documentPath + '.contentType' ) ;
	}

	if ( params.binaryContentType && this.binaryContentType !== params.binaryContentType ) {
		this.binaryContentType = params.binaryContentType ;
		toStage.push( this.documentPath + '.binaryContentType' ) ;
	}

	if ( params.hash && this.hash !== params.hash ) {
		this.hash = params.hash ;
		toStage.push( this.documentPath + '.hash' ) ;

		if ( params.hashType && this.hashType !== params.hashType ) {
			this.hashType = params.hashType ;
			toStage.push( this.documentPath + '.hashType' ) ;
		}
	}
	else if ( params.hashType && this.hashType !== params.hashType ) {
		this.hash = null ;
		this.hashType = params.hashType ;
	}

	if ( params.metadata && typeof params.metadata === 'object' ) {
		for ( key in params.metadata ) {
			if ( this.metadata[ key ] !== params.metadata[ key ] ) {
				this.metadata[ key ] = params.metadata[ key ] ;
				toStage.push( this.documentPath + '.metadata.' + key ) ;
			}
		}
	}

	if ( this.document && toStage.length ) {
		// Update documentRaw if needed, and don't forget to stage the change
		this.updateRaw() ;
		this.document.stage( toStage ) ;
	}
} ;



const CONTENT_TYPE_SAMPLE_SIZE = 1024 ;		// Better value is 4100

Attachment.prototype.setIncoming = function( incoming ) {
	var hash , cryptoHash ,
		fileSize = 0 ,
		contentTypeConstraint = this.constraints?.contentType || this.constraints?.binaryContentType ;

	if ( this.upstreamExists ) {
		throw new Error( "Attachment: already saved/exists upstream" ) ;
	}

	if ( contentTypeConstraint ) {
		if ( contentTypeConstraint.includes( '/' ) ? this.contentType !== contentTypeConstraint : this.contentType.split( '/' )[ 0 ] !== contentTypeConstraint ) {
			throw ErrorStatus.badRequest( { message: "Content-type mismatch (expecting '" + contentTypeConstraint + "' but got '" + this.contentType + "')" } ) ;
		}
	}

	this.incoming = incoming ;

	if ( this.hashType ) { cryptoHash = crypto.createHash( this.hashType ) ; }


	// Stream case

	if ( this.incoming instanceof stream.Readable ) {
		let detectContentTypeBuffers = null ,
			ensureBinaryContentTypeCalled = false ;

		if ( this.incoming.errored ) {
			throw new Error( "Attachment (stream): stream already have an error: " + this.incoming.errored ) ;
		}

		if ( this.incoming.readableEnded || this.incoming.closed || this.incoming.destroyed ) {
			throw new Error( "Attachment (stream): stream is already consumed" ) ;
		}

		if ( this.constraints?.binaryContentType ) {
			detectContentTypeBuffers = [] ;
			this.fileCheckerPromise = new Promise() ;
			// Avoid pesky and useless "Unhandled promise rejection", it is handled, but race condition may handle them too late for Nodejs taste
			this.fileCheckerPromise.catch( noop ) ;
		}

		// Pause the stream now, because listening for 'data' will start consuming the stream immediately,
		// it may even kick in before piping it (rare bug, that has already happened).
		this.incoming.pause() ;

		this.incoming.on( 'data' , data => {
			fileSize += typeof data === 'string' ? Buffer.byteLength( data ) : data.length ;

			if ( cryptoHash ) { cryptoHash.update( data ) ; }

			if ( detectContentTypeBuffers && ! ensureBinaryContentTypeCalled ) {
				detectContentTypeBuffers.push( data ) ;
				if ( fileSize >= CONTENT_TYPE_SAMPLE_SIZE ) {
					ensureBinaryContentTypeCalled = true ;
					// This is async:
					this._checkBinaryContentType( Buffer.concat( detectContentTypeBuffers ) , this.constraints.binaryContentType , this.fileCheckerPromise ) ;
				}
			}
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

			if ( detectContentTypeBuffers && ! ensureBinaryContentTypeCalled ) {
				ensureBinaryContentTypeCalled = true ;
				// This is async:
				this._checkBinaryContentType( Buffer.concat( detectContentTypeBuffers ) , this.constraints.binaryContentType , this.fileCheckerPromise ) ;
			}
		} ) ;

		return ;
	}


	// Buffer/string case

	fileSize = typeof this.incoming === 'string' ? Buffer.byteLength( this.incoming ) : this.incoming.length ;

	if ( this.fileSize !== null && this.fileSize !== fileSize ) {
		let error = new Error( "Attachment: expecting file size '" + this.fileSize + "' but got '" + fileSize + "'." ) ;
		error.expected = this.fileSize ;
		error.actual = fileSize ;
		error.code = 'badFileSize' ;
		throw error ;
	}

	this.fileSize = fileSize ;

	if ( cryptoHash ) {
		hash = cryptoHash.update( this.incoming ).digest( 'base64' ) ;

		if ( this.hash && this.hash !== hash ) {
			let error = new Error( "Attachment: expecting hash '" + this.hash + "' but got '" + hash + "'." ) ;
			error.expected = this.hash ;
			error.actual = hash ;
			error.code = 'badHash' ;
			throw error ;
		}

		this.hash = hash ;
	}

	if ( this.constraints?.binaryContentType ) {
		this.fileCheckerPromise = new Promise() ;
		// Avoid pesky and useless "Unhandled promise rejection", it is handled, but race condition may handle them too late for Nodejs taste
		this.fileCheckerPromise.catch( noop ) ;
		// This is async:
		this._checkBinaryContentType( this.incoming , this.constraints.binaryContentType , this.fileCheckerPromise ) ;
	}
} ;



Attachment.prototype.save = function() {
	// Avoid concurrent call to the real ._save(), which could mess things up big time, especially with streams

	if ( ! this.incoming ) { throw new Error( "Attachment: can't save, no incoming" ) ; }

	// It makes no sense to save something that is about to be deleted
	if ( this.toDelete || this.deleted || this.deletePromise ) { return Promise.resolved ; }

	// Remove it immediately from unsaved attachements (because of concurrent issues)
	if ( this.document ) { this.document.outOfSyncAttachments.delete( this ) ; }

	// Call the real _save() only if necessary
	if ( this.savePromise ) { return this.savePromise ; }
	this.savePromise = this._save() ;

	// But in case of error, add it again to unsaved attachments
	if ( this.document ) {
		this.savePromise.catch( () => this.document.outOfSyncAttachments.add( this ) ) ;
	}

	return this.savePromise ;
} ;



Attachment.prototype._save = async function() {
	var hasError ;

	if ( this.upstreamExists ) {
		throw new Error( "Attachment: already saved/exists upstream" ) ;
	}

	if ( this.incoming instanceof stream.Readable ) {
		if ( this.incoming.errored ) {
			throw new Error( "Attachment (stream): stream already have an error: " + this.incoming.errored ) ;
		}

		if ( this.incoming.readableEnded || this.incoming.closed || this.incoming.destroyed ) {
			throw new Error( "Attachment (stream): stream is already consumed" ) ;
		}

		// Force an error handler, since we don't know if the driver will handle it
		this.incoming.once( 'error' , error => hasError = error ) ;
	}

	await this.driver.save( this ) ;
	this.upstreamExists = true ;

	if ( hasError ) {
		let error = new Error( 'Save attachment error: ' + hasError ) ;
		error.from = hasError ;
		throw error ;
	}

	try {
		await this.fileCheckerPromise ;
	}
	catch ( error ) {
		this.delete() ;
		throw error ;
	}

	// Modify hash and fileSize value on the Document's raw object
	if ( this.document && ( this.incoming instanceof stream.Readable ) ) {
		this.documentRaw.fileSize = this.fileSize ;
		this.documentRaw.hash = this.hash ;
		this.documentRaw.binaryContentType = this.binaryContentType ;

		// Don't forget to stage the change
		this.document.stage( this.documentPath ) ;
	}
} ;



Attachment.prototype.load = async function() {
	return this.driver.load( this ) ;
} ;



Attachment.prototype.getReadStream = function() {
	return this.driver.getReadStream( this ) ;
} ;



Attachment.prototype.delete = function() {
	// Avoid concurrent call to the real ._delete(), which could mess things up big time

	this.toDelete = true ;	// ensure this, in case of error, it could be retried on Document's .save() and .commit()
	if ( this.deleted ) { return Promise.resolved ; }

	// Remove it immediately from out of sync attachements (because of concurrent issues)
	if ( this.document ) { this.document.outOfSyncAttachments.delete( this ) ; }

	// Call the real _delete() only if necessary
	if ( this.deletePromise ) { return this.deletePromise ; }
	this.deletePromise = this._delete() ;

	// But in case of error, add it again to unsaved attachments
	if ( this.document ) {
		this.deletePromise.catch( () => this.document.outOfSyncAttachments.add( this ) ) ;
	}

	return this.deletePromise ;
} ;



Attachment.prototype._delete = async function() {
	// It makes no sense to delete something that does not yet exist upstream...
	if ( ! this.upstreamExists ) { return ; }

	await this.driver.delete( this ) ;

	this.upstreamExists = false ;
	this.deleted = true ;

	// Don't forget to stage the change
	if ( this.document ) { this.document.stage( this.documentPath ) ; }
} ;



Attachment.prototype._checkBinaryContentType = async function( buffer , binaryContentType , promise ) {
	try {
		let result = await fileType.fileTypeFromBuffer( buffer ) ;
		this.binaryContentType = result?.mime ;
	}
	catch ( error_ ) {
		let error = ErrorStatus.badRequest( { message: "Binary content-type detection error: " + error_.message } ) ;
		promise.reject( error ) ;
		return ;
	}

	if ( binaryContentType.includes( '/' ) ) {
		// We have to ensure a complete content-type

		if ( fileType.supportedMimeTypes.has( binaryContentType ) ) {
			if ( this.binaryContentType !== binaryContentType ) {
				log.debug( "._ensureStreamBinaryContentType(): binary content-type mismatch (expecting '%s' but got '%s')" , binaryContentType , this.binaryContentType ) ;
				let error = ErrorStatus.badRequest( { message: "Binary content-type mismatch (expecting '" + binaryContentType + "' but got '" + this.binaryContentType + "')" } ) ;
				promise.reject( error ) ;
				return ;
			}
		}
		else {
			log.debug( "._ensureStreamBinaryContentType(): unsupported mime-type: '%s', file type detection is off" , binaryContentType ) ;
		}
	}
	else {
		// We have to ensure a content-type category only (the part left to the '/')

		if ( ! this.binaryContentType || this.binaryContentType.split( '/' )[ 0 ] !== binaryContentType ) {
			log.debug( "._ensureStreamBinaryContentType(): binary content-type mismatch (expecting '%s' but got '%s')" , binaryContentType , this.binaryContentType ) ;
			let error = ErrorStatus.badRequest( { message: "Binary content-type mismatch (expecting '" + binaryContentType + "' but got '" + this.binaryContentType + "')" } ) ;
			promise.reject( error ) ;
			return ;
		}
	}

	promise.resolve() ;
} ;



Attachment.prototype.getExtension = function( str ) {
	// Only 10 characters max, and check for bad chars
	var extension = path.extname( str ).slice( 1 , 11 ).toLowerCase() ;
	return extension.match( /[a-z0-9]+/ ) ? extension : '' ;
} ;

