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
var events = require( 'events' ) ;



function AttachmentStreams() { throw new Error( '[roots-db] Use AttachmentStream.create() instead' ) ; }

module.exports = AttachmentStreams ;

AttachmentStreams.prototype = Object.create( events.prototype ) ;
AttachmentStreams.prototype.constructor = AttachmentStreams ;



AttachmentStreams.create = function create()
{
	var attachmentStreams = Object.create( AttachmentStreams.prototype , {
		list: { value: [] , enumerable: true , writable: true } ,
		ended: { value: false , enumerable: true , writable: true } ,
	} ) ;
	
	return attachmentStreams ;
} ;



AttachmentStreams.prototype.addStream = function addStream( stream , documentPath , metaData )
{
	if ( this.ended ) { return ; }
	
	var descriptor = {
		stream: stream ,
		documentPath: documentPath ,
		index: this.list.length ,
		metaData: metaData
	} ;
	
	this.list.push( descriptor ) ;
	this.emit( 'attachment' , descriptor ) ;
} ;



// No new streams will be added
AttachmentStreams.prototype.end = function end()
{
	this.ended = true ;
	this.emit( 'end' ) ;
} ;





