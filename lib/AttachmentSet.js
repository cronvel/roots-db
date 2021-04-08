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



function AttachmentSet( params ) {
	//this.id = '' + params.id ;
	this.attachments = {} ;

	// Common metadata
	this.metadata = params.metadata && typeof params.metadata === 'object' ? params.metadata : {} ;

	Object.defineProperties( this , {
		// Not enumerable: not JSON-stringified
		collectionName: { writable: true , value: params.collectionName } ,
		documentId: { writable: true , value: '' + params.documentId } ,
		path: { writable: true , value: null } ,

		// Immutable
		driver: { value: params.driver } ,
		defaultHashType: { value: params.defaultHashType || null } ,

		// Document stuffs
		document: { configurable: true , value: null } ,	// a ref to the host document
		documentPath: { configurable: true , value: null } ,	// the path of the attachment inside of the document
		documentRaw: { configurable: true , value: null }	// raw value, that will be passed to the DB, in Document#raw
	} ) ;

	if ( params.attachments && typeof params.attachments === 'object' ) {
		for ( let name in params.attachments ) { this.set( name , params.attachments[ name ] , null ) ; }
	}
}

module.exports = AttachmentSet ;



AttachmentSet.prototype.attachToDocument = function( document , documentPath , restoredFromRaw = null ) {
	var name , attachment ;

	document = document._ ;	// Force the Document instance

	if ( this.document ) {
		if ( this.document === document ) { return ; }
		throw new Error( 'AttachmentSet is already attached to a Document' ) ;
	}

	// Make those properties not writable anymore
	Object.defineProperties( this , {
		document: { value: document } ,
		documentPath: { value: documentPath } ,

		// This is the raw data (saved to/restored from the DB)
		documentRaw: { value: restoredFromRaw || {} }
	} ) ;

	if ( ! this.documentRaw.attachments ) { this.documentRaw.attachments = {} ; }
	if ( ! this.documentRaw.metadata ) { this.documentRaw.metadata = this.metadata ; }

	if ( ! restoredFromRaw ) {
		// Update raw data now. Should not update on restore? (except maybe if the lib has changed).
		this.updateRaw() ;

		// Place the raw data in the correct document's path
		dotPath.set( this.document.raw , this.documentPath , this.documentRaw ) ;

		// Stage the change now!
		this.document.stage( this.documentPath ) ;
	}

	for ( name in this.attachments ) {
		attachment = this.attachments[ name ] ;

		// Prepare raw for attachment
		this.documentRaw.attachments[ name ] = { id: attachment.id } ;

		// Share
		Object.defineProperties( attachment , {
			document: { value: this.document } ,
			documentPath: { value: this.documentPath + '.attachments.' + name } ,
			documentRaw: { value: this.documentRaw.attachments[ name ] }
		} ) ;

		// It's a restore, so it exists upstream
		if ( restoredFromRaw ) { attachment.upstreamExists = true ; }

		// Use attachment's own .updateRaw()
		attachment.updateRaw() ;
	}

	// Immediately populate it
	this.document.populatedDocumentProxies.set( this.documentRaw , this ) ;
} ;



// Internal
// Update only things that could be updated
AttachmentSet.prototype.updateRaw = function() {
	this.documentRaw.metadata = this.metadata ;
} ;



// Set common metadata
AttachmentSet.prototype.setMetadata = function( metadata ) {
	if ( ! metadata || typeof metadata !== 'object' ) { return ; }

	var key , toStage = [] ;

	for ( key in metadata ) {
		if ( this.metadata[ key ] !== metadata[ key ] ) {
			this.metadata[ key ] = metadata[ key ] ;
			toStage.push( this.documentPath + '.metadata.' + key ) ;
		}
	}

	if ( this.document && toStage.length ) {
		// Update documentRaw if needed, and don't forget to stage the change
		this.updateRaw() ;
		this.document.stage( toStage ) ;
	}
} ;



// Get an attchment alternative
AttachmentSet.prototype.get = function( name ) { return this.attachments[ name ] ; } ;



// Set an attachment alternative
AttachmentSet.prototype.set = function( name , attachmentData = null , incoming = null ) {
	var attachment , existingAttachment = this.attachments[ name ] ,
		documentPath = this.documentPath + '.attachments.' + name ;

	if ( ! attachmentData ) {
		// We want to delete this alternative
		if ( ! existingAttachment ) { return null ; }
		existingAttachment.toDelete = true ;

		if ( this.document ) {
			this.document.outOfSyncAttachments.add( existingAttachment ) ;
			delete this.documentRaw.attachments[ name ] ;
			this.document.stage( documentPath ) ;
		}

		delete this.attachments[ name ] ;
		return null ;
	}

	if ( existingAttachment ) {
		// Don't forget to mark orphaned attachments
		existingAttachment.toDelete = true ;
		if ( this.document ) { this.document.outOfSyncAttachments.add( existingAttachment ) ; }
	}

	if ( attachmentData instanceof Attachment ) {
		attachment = attachmentData ;

		// Set this alternative
		if ( attachment.document ) {
			throw new Error( 'AttachmentSet: Attachment already attached' ) ;
		}

		if ( attachment.collectionName !== this.collectionName || attachment.documentId !== this.documentId || attachment.documentPath !== documentPath ) {
			throw new Error( 'AttachmentSet: Attachment collection/document mismatch' ) ;
		}

		attachment.isPartOfSet = true ;	// ensure this is correct
		this.attachments[ name ] = attachment ;
		if ( incoming ) { attachment.setIncoming( incoming ) ; }
	}
	else {
		// Set this alternative
		attachmentData = Object.assign( {} , attachmentData , {
			collectionName: this.collectionName ,
			documentId: '' + this.documentId ,
			driver: this.driver ,
			isPartOfSet: true
		} ) ;

		if ( ! attachmentData.hashType ) { attachmentData.hashType = this.defaultHashType ; }

		attachment = new Attachment( attachmentData , incoming ) ;
		this.attachments[ name ] = attachment ;
	}

	if ( this.document ) {
		if ( attachment ) {
			// Prepare raw for attachment
			this.documentRaw.attachments[ name ] = { id: attachment.id } ;

			// Share
			Object.defineProperties( attachment , {
				document: { value: this.document } ,
				documentPath: { value: documentPath } ,
				documentRaw: { value: this.documentRaw.attachments[ name ] }
			} ) ;

			// Use attachment's own .updateRaw()
			attachment.updateRaw() ;
			this.document.outOfSyncAttachments.add( attachment ) ;
		}

		this.document.stage( documentPath ) ;
	}

	return attachment ;
} ;



// Delete an attachment alternative
AttachmentSet.prototype.delete = function( name ) {
	return this.set( name , null ) ;
} ;

