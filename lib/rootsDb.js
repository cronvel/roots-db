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

/*
	In progress:
	- Embedded

	TODO:
	- Fingerprint hash
	- === HOOKS ===
	- set on backlink
*/

"use strict" ;



const path = require( 'path' ) ;



const rootsDb = {} ;
module.exports = rootsDb ;



rootsDb.driver = { mongodb: path.join( __dirname , 'mongodb.driver.js' ) } ;
rootsDb.attachmentDriver = { file: path.join( __dirname , 'file.attachmentDriver.js' ) } ;
rootsDb.importer = {} ;
rootsDb.fakeDataGenerator = {} ;



// Exm API, should come before loading any modules
rootsDb.registerDriver = ( name , modulePath ) => {
	if ( rootsDb.driver[ name ] ) {
		if ( rootsDb.driver[ name ] === modulePath ) { return ; }
		throw new Error( "Attempt to redefine driver '" + name + "'." ) ;
	}

	rootsDb.driver[ name ] = modulePath ;
} ;

rootsDb.hasDriver = name => !! rootsDb.driver[ name ] ;



rootsDb.registerAttachmentDriver = ( name , modulePath ) => {
	if ( rootsDb.attachmentDriver[ name ] ) {
		if ( rootsDb.attachmentDriver[ name ] === modulePath ) { return ; }
		throw new Error( "Attempt to redefine attachment driver '" + name + "'." ) ;
	}

	rootsDb.attachmentDriver[ name ] = modulePath ;
} ;

rootsDb.hasAttachmentDriver = name => !! rootsDb.attachmentDriver[ name ] ;



rootsDb.registerImporter = ( name , modulePath ) => {
	if ( rootsDb.importer[ name ] ) {
		if ( rootsDb.importer[ name ] === modulePath ) { return ; }
		throw new Error( "Attempt to redefine importer '" + name + "'." ) ;
	}

	rootsDb.importer[ name ] = modulePath ;
} ;

rootsDb.hasImporter = name => !! rootsDb.importer[ name ] ;



rootsDb.registerFakeDataGenerator = ( name , modulePath ) => {
	if ( rootsDb.fakeDataGenerator[ name ] ) {
		if ( rootsDb.fakeDataGenerator[ name ] === modulePath ) { return ; }
		throw new Error( "Attempt to redefine fake data generator '" + name + "'." ) ;
	}

	rootsDb.fakeDataGenerator[ name ] = modulePath ;
} ;

rootsDb.hasFakeDataGenerator = name => !! rootsDb.fakeDataGenerator[ name ] ;



rootsDb.exm = require( './exm.js' ) ;

// Init and load active extensions
rootsDb.initExtensions = () => rootsDb.exm.init() ;



require( './doormen-extensions.js' ) ;

rootsDb.bulk = require( './bulk.js' ) ;
rootsDb.misc = require( './misc.js' ) ;
rootsDb.World = require( './World.js' ) ;
rootsDb.Collection = require( './Collection.js' ) ;
rootsDb.Document = require( './Document.js' ) ;
rootsDb.Batch = require( './Batch.js' ) ;
rootsDb.Attachment = require( './Attachment.js' ) ;
rootsDb.AttachmentSet = require( './AttachmentSet.js' ) ;
rootsDb.AttachmentStreams = require( './AttachmentStreams.js' ) ;
rootsDb.Population = require( './Population.js' ) ;
rootsDb.Fingerprint = require( './Fingerprint.js' ) ;
rootsDb.MemoryModel = require( './MemoryModel.js' ) ;

// Special collections
rootsDb.VersionsCollection = require( './VersionsCollection.js' ) ;
rootsDb.CountersCollection = require( './CountersCollection.js' ) ;

rootsDb.Import = require( './Import.js' ) ;
rootsDb.Export = require( './Export.js' ) ;



// Those things will be removed later
Object.defineProperties( rootsDb , {
	NONE: { value: 0 } ,
	UPSTREAM: { value: 1 } ,
	MEMPROXY: { value: 2 } ,
	INTERNAL: { value: 3 }
} ) ;

