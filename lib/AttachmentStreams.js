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



const events = require( 'events' ) ;
const log = require( 'logfella' ).global.use( 'roots-db' ) ;



function AttachmentStreams() {
	this.list = [] ;
	this.ended = false ;
}

module.exports = AttachmentStreams ;

AttachmentStreams.prototype = Object.create( events.prototype ) ;
AttachmentStreams.prototype.constructor = AttachmentStreams ;



// .addStream( stream , documentPath , [setKey] , metadata , [resumeOffset] ) {
AttachmentStreams.prototype.addStream = function( stream , documentPath , setKey , metadata , resumeOffset ) {
	if ( this.ended ) {
		stream.destroy() ;
		return ;
	}

	// Manage arguments
	if ( typeof setKey !== 'string' ) {
		resumeOffset = metadata ;
		metadata = setKey ;
		setKey = null ;
	}

	var descriptor = {
		stream ,
		documentPath ,
		setKey ,
		index: this.list.length ,
		metadata ,
		resumeOffset: resumeOffset ?? null		// If we resume an aborted upload, not supported directly by roots-DB ATM
	} ;

	this.list.push( descriptor ) ;
	this.emit( 'attachment' , descriptor ) ;
} ;



// No new stream will be added anymore
AttachmentStreams.prototype.end = function() {
	this.ended = true ;
	this.emit( 'end' ) ;
} ;

