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

		// Support string, Buffer and stream
		incoming: { writable: true , value: null } ,

		// Document stuffs
		document: { configurable: true , value: null } ,	// a ref to the host document
		documentPath: { configurable: true , value: null } ,	// the path of the attachment inside of the document
		documentRaw: { configurable: true , value: null }	// raw value, that will be passed to the DB, in Document#raw
	} ) ;

	if ( params.attachments && typeof params.attachments === 'object' ) {
		for ( let name in params.attachments ) { this.set( name , params.attachments[ name ] ) ; }
	}
}

module.exports = AttachmentSet ;



AttachmentSet.prototype.attachToDocument = function( document , documentPath , restoredFromRaw = null ) {
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
AttachmentSet.prototype.updateRaw = function() {
	Object.assign( this.documentRaw , {
		attachments: this.attachments ,
		metadata: this.metadata
	} ) ;
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
AttachmentSet.prototype.set = function( name , attachment = null , incoming = null ) {
	if ( ! attachment ) {
		// Delete this alternative
		delete this.set[ name ] ;
	}
	else if ( attachment instanceof Attachment ) {
		if ( attachment.document ) {
			throw new Error( 'AttachmentSet: Attachment already attached' ) ;
		}

		if ( attachment.collectionName !== this.collectionName || attachment.documentId !== this.documentId ) {
			throw new Error( 'AttachmentSet: Attachment collection/document mismatch' ) ;
		}

		// Set this alternative
		this.set[ name ] = new Attachment( attachment , incoming ) ;
	}
	else {
		// Set this alternative
		attachment = Object.assign( {} , attachment , {
			id: attachment.id ,
			collectionName: this.collectionName ,
			documentId: '' + this.documentId
		} ) ;

		this.set[ name ] = new Attachment( attachment , incoming ) ;
	}

	if ( this.document ) {
		// Update documentRaw if needed, and don't forget to stage the change
		this.updateRaw() ;
		this.document.stage( this.documentPath + '.attachments.' + name ) ;
	}
} ;

