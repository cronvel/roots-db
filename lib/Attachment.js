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

var rootsDb = require( './rootsDb.js' ) ;

//var tree = require( 'tree-kit' ) ;
//var doormen = require( 'doormen' ) ;
//var ErrorStatus = require( 'error-status' ) ;



// Internal usage only, use document.$.createAttachment()
function Attachment( metaData , incoming )
{
	var attachment = Object.create( Attachment.prototype , {
		collectionName: { value: metaData.collectionName , enumerable: true } ,
		documentId: { value: metaData.documentId , enumerable: true } ,
		id: { value: metaData.id , enumerable: true } ,
		baseUrl: { value: metaData.baseUrl , enumerable: true } ,
		filename: { value: metaData.filename , writable: true , enumerable: true } ,
		fullUrl: { value: metaData.baseUrl /*+ metaData.documentId + '/'*/ + metaData.id , enumerable: true }
	} ) ;
	
	// Temp... Should support string, buffer and stream
	attachment.incoming = incoming ;
	
	return attachment ;
} ;

module.exports = Attachment ;



Attachment.prototype.export = function export_()
{
	return {
		id: this.id ,
		filename: this.filename
	} ;
} ;



Attachment.prototype.save = function save( callback )
{
	fs.writeFile( this.fullUrl , this.incoming , callback ) ;
} ;



Attachment.prototype.load = function load( callback )
{
	fs.readFile( this.fullUrl , callback ) ;
} ;






