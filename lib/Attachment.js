/*
	The Cedric's Swiss Knife (CSK) - CSK RootsDB

	Copyright (c) 2015 Cédric Ronvel 
	
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



// Load modules
var fs = require( 'fs' ) ;
var path = require( 'path' ) ;
var stream = require( 'stream' ) ;

var fsKit = require( 'fs-kit' ) ;
var tree = require( 'tree-kit' ) ;
var ErrorStatus = require( 'error-status' ) ;

var log = require( 'logfella' ).global.use( 'roots-db' ) ;

var rootsDb = require( './rootsDb.js' ) ;

//var tree = require( 'tree-kit' ) ;
//var doormen = require( 'doormen' ) ;



function Attachment() { throw new Error( '[roots-db] Use Attachment.create() instead' ) ; }
module.exports = Attachment ;



// Internal usage only, use document.$.createAttachment()
Attachment.create = function create( metaData , incoming )
{
	var attachment = Object.create( Attachment.prototype ) ;
	attachment.create( metaData , incoming ) ;
	return attachment ;
} ;



Attachment.prototype.create = function create( metaData , incoming )
{
	Object.defineProperties( this , {
		collectionName: { value: metaData.collectionName , enumerable: true } ,
		documentId: { value: metaData.documentId , enumerable: true } ,
		id: { value: metaData.id , enumerable: true } ,
		baseUrl: { value: metaData.baseUrl , enumerable: true } ,
		filename: { value: metaData.filename , writable: true , enumerable: true } ,
		contentType: { value: metaData.contentType , writable: true , enumerable: true } ,
		fullUrl: { value: metaData.baseUrl + metaData.documentId + '/' + metaData.id , enumerable: true }
	} ) ;
	
	// Temp... Should support string, buffer and stream
	this.incoming = incoming ;
} ;



Attachment.prototype.export = function export_()
{
	return {
		id: this.id ,
		filename: this.filename ,
		contentType: this.contentType
	} ;
} ;



// Update an attachment, replace all metaData, but preserve few metaData like id & path (overwrite the current file)
Attachment.prototype.update = function update( metaData , incoming )
{
	// Just extend(), data that should not be changed are already read-only
	tree.extend( { own: true } , this , metaData ) ;
	this.incoming = incoming ;
} ;



Attachment.prototype.save = function save( callback )
{
	var self = this , onceCallback , calledBack , fileStream ;
	
	fsKit.ensurePath( path.dirname( this.fullUrl ) , function( error ) {
		
		if ( error ) { callback( error ) ; return ; }
		
		if ( typeof self.incoming === 'string' || Buffer.isBuffer( self.incoming ) )
		{
			log.debug( ">>>>>>>>>>>>>>>>>>>>> Write" ) ;
			fs.writeFile( self.fullUrl , self.incoming , callback ) ;
		}
		else if ( self.incoming instanceof stream.Readable )
		{
			log.debug( ">>>>>>>>>>>>>>>>>>>>> Pipe" ) ;
			
			fileStream = fs.createWriteStream( self.fullUrl , { defaultEncoding: 'binary' } ) ;
			
			var onceCallback = function() {
				if ( calledBack ) { return ; }
				calledBack = true ;
				log.debug( ">>>>>>>>>>>>>>>>>>>>> D2: onceCallback()" ) ;
				callback.apply( self , arguments ) ;
			} ;
			
			self.incoming.pipe( fileStream ) ;
			
			fileStream.once( 'error' , onceCallback ) ;
			
			// Should listen the readable or the writable stream for that?
			self.incoming.once( 'end' , onceCallback ) ;
			fileStream.once( 'end' , onceCallback ) ;
		}
		else
		{
			log.error( "Attachment, type of data is not supported, should be string, Buffer or ReadableStream" ) ;
			callback( new Error( "[roots-db] Attachment, type of data is not supported, should be string, Buffer or ReadableStream" ) ) ;
		}
	} ) ;
} ;



Attachment.prototype.load = function load( callback )
{
	fs.readFile( this.fullUrl , callback ) ;
} ;



Attachment.prototype.getReadStream = function getReadStream( callback )
{
	var calledBack , fileStream ;
	
	fileStream = fs.createReadStream( this.fullUrl , { defaultEncoding: 'binary' } ) ;
	
	fileStream.once( 'error' , function( error ) {
		
		log.error( 'Attachment .getReadStream() error event: %E' , error ) ;
		
		if ( ! calledBack )
		{
			if ( error.code === 'ENOENT' )
			{
				callback( ErrorStatus.notFound( { message: 'File not found: ' + error.toString() } ) ) ;
			}
			else
			{
				callback( error ) ;
			}
		}
	} ) ;
	
	fileStream.once( 'open' , function() {
		if ( ! calledBack ) { callback( undefined , fileStream ) ; }
	} ) ;
} ;



