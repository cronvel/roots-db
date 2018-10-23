/*
	Roots DB

	Copyright (c) 2014 - 2018 Cédric Ronvel

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



function Population( world , options = {} ) {
	this.world = world ;

	if ( options.cache ) {
		this.cache = options.cache ;
	}
	else {
		this.cache = this.world.createMemoryModel( { lazy: true } ) ;
	}

	// For debug purpose
	if ( options.stats && ! options.stats.population ) { options.stats.population = this ; }

	// Things to be populated, see .prepare() for the structure details
	this.populate = null ;

	// Things currently populating, same structure than 'populate'
	this.populating = null ;

	// Mostly for stats/debug
	this.depth = 0 ;	// The depth of the population
	this.dbQueries = 0 ;	// The number of DB queries for population

	this.prepare() ;
}

module.exports = Population ;



Population.prototype.prepare = function() {
	if ( ! this.populate ) {
		this.populate = {
			/*
				This is the list of document x path to populate with the foreign document reference.
				Array of object:
					* hostDocument: a document Proxy
					* hostPath: the path in the host document that should be populated
					* foreignCollection: the collection name of the foreign document
					* foreignId: the foreign document id
			*/
			targets: [] ,

			/*
				This is the list of documents to retrieve.
				Object of Set, the key is the collection name, the Set contains all IDs to retrieve in this collection in one request.
			*/
			refs: {} ,

			/*
				It works mostly the same way than 'targets', it used for backLink.
				This is the list of document x path to populate with the foreign document reference.
				Array of object:
					* hostDocument: a document Proxy
					* hostPath: the path in the host document that should be populated
					* foreignCollection: the collection name of the foreign document
					* foreignValue: the value to found in the foreign document
					* foreignPath: path of that value in the foreign document
			*/
			complexTargets: [] ,

			/*
				This is the list of documents to retrieve, an Object of Object of Object.
				The key of the first object is the collection name.
				The key of the second object is a path where to find a matching value.
				The key of the third object is the stringified (or hashed) value, while the value is the searching value.

				The singified/hash thing is required because MongoID are object, so a Set would permit the actual ID to be found
				to be added as many time as there are different objects mapping the same ID.
			*/
			complexRefs: {}
		} ;
	}
} ;

