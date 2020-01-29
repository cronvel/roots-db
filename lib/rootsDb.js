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

/*
	In progress:
	- Embedded

	TODO:
	- Fingerprint hash
	- === HOOKS ===
	- set on backlink
*/

"use strict" ;



const rootsDb = {} ;
module.exports = rootsDb ;



require( './doormen-extensions.js' ) ;

rootsDb.driver = {} ;
rootsDb.bulk = require( './bulk.js' ) ;
rootsDb.misc = require( './misc.js' ) ;
rootsDb.World = require( './World.js' ) ;
rootsDb.Collection = require( './Collection.js' ) ;
rootsDb.Document = require( './Document.js' ) ;
rootsDb.Batch = require( './Batch.js' ) ;
rootsDb.Attachment = require( './Attachment.js' ) ;
rootsDb.AttachmentStreams = require( './AttachmentStreams.js' ) ;
rootsDb.Population = require( './Population.js' ) ;
rootsDb.Fingerprint = require( './Fingerprint.js' ) ;
rootsDb.MemoryModel = require( './MemoryModel.js' ) ;

// Special collections
rootsDb.VersionCollection = require( './VersionCollection.js' ) ;



// Those things will be removed later

Object.defineProperties( rootsDb , {
	NONE: { value: 0 } ,
	UPSTREAM: { value: 1 } ,
	MEMPROXY: { value: 2 } ,
	INTERNAL: { value: 3 }
} ) ;


