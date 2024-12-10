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



const rootsDb = require( './rootsDb.js' ) ;

const fs = require( 'fs' ) ;
const path = require( 'path' ) ;

const Promise = require( 'seventh' ) ;

const log = require( 'logfella' ).global.use( 'roots-db' ) ;



function Export( world , directory , options = {} , stats = {} ) {
	this.world = world ;
	this.baseDir = directory ;

	//this.concurrency = options.concurrency || 50 ;

	this.stats = stats ;
	Export.createExportStats( this.stats ) ;
}

module.exports = Export ;



// Export data, in-memory process, not appropriate for big DB migration.
// If memory limit is hit, run node with the option: --max-old-space-size=8192 (or whatever size you want)
Export.prototype.export = async function() {
	this.stats.startTime = Date.now() ;
	( { heapUsed: this.stats.startingHeapMemory , external: this.stats.startingExternalMemory } = process.memoryUsage() ) ;

	await this.saveToDisk() ;

	// Total duration
	this.stats.duration = Date.now() - this.stats.startTime ;
	//log.hdebug( "Export stats: %[10l50000]Y" , this.stats ) ;
} ;



Export.prototype.saveToDisk = async function() {
	// First step, collect raw batches
	this.stats.step = 1 ;
	this.stats.stepStr = '1/1 Save documents to disk' ;
	this.stats.exportToDiskStartTime = Date.now() ;

	for ( let collectionName in this.world.collections ) {
		let collection = this.world.collections[ collectionName ] ;
		let filePath = path.join( this.baseDir , collectionName + '.jsonstream' ) ;

		let file = await fs.promises.open( filePath , 'w' ) ;

// Fix that, replace with .findGenerator()

		collection.findEach( {} , { raw: true } , async ( rawDocument ) => {
			let lineStr = JSON.stringify( rawDocument ) + "\n" ;
			await file.write( Buffer.from( lineStr ) ) ;
		} ) ;
	}

	this.stats.exportToDiskDuration = Date.now() - this.stats.exportToDiskStartTime ;
	( { heapUsed: this.stats.exportToDiskHeapMemory , external: this.stats.exportToDiskExternalMemory } = process.memoryUsage() ) ;
} ;




Export.createExportStats = function( stats = {} ) {
	stats.step = 0 ;
	stats.stepStr = '' ;

	stats.documents = 0 ;
	stats.savedDocuments = 0 ;

	// Timers:
	stats.startTime = null ;
	stats.duration = null ;
	stats.exportToDiskStartTime = null ;
	stats.exportToDiskDuration = null ;

	// Memory usage
	stats.startingHeapMemory = null ;
	stats.startingExternalMemory = null ;
	stats.saveToDisk = null ;
	stats.saveToDisk = null ;

	// Errors:

	return stats ;
} ;

