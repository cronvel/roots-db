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



const Promise = require( 'seventh' ) ;
const DeepProxy = require( 'nested-proxies' ) ;

/*
const rootsDb = require( './rootsDb.js' ) ;


const tree = require( 'tree-kit' ) ;
const doormen = require( 'doormen' ) ;
const fs = require( 'fs' ) ;
const fsKit = require( 'fs-kit' ) ;

const ErrorStatus = require( 'error-status' ) ;

const log = require( 'logfella' ).global.use( 'roots-db' ) ;

function noop() {}
*/



const METHODS = {} ;
const PROXY_OPTIONS = { pathArray: true } ;



const PROXY_HANDLER = {
	get: function( target , property , receiver , path ) {
		var proto ;
		
		console.log( "path" , path ) ;
		if ( path.length > 1 ) {
			// We are already in an embedded object
			if ( target[ property ] && typeof target[ property ] === 'object' ) {
				proto = Object.getPrototypeOf( target[ property ] ) ;
				console.log( "Proto:" , proto ) ;
				
				if ( this.root.target.collection.driver.immutables.has( proto ) ) {
					console.log( ">>> Immutable" ) ;
					return target[ property ] ;
				}
				else {
					return this.nested( property ) ;
				}
			}
			
			return target[ property ] ;
		}

		// This is a document method
		if ( METHODS[ property ] ) {
			return target[ property ] ;
		}

		if ( target.raw[ property ] && typeof target.raw[ property ] === 'object' ) {
			proto = Object.getPrototypeOf( target.raw[ property ] ) ;
			console.log( "Proto:" , proto ) ;
			
			if ( target.collection.driver.immutables.has( proto ) ) {
				console.log( ">>> Immutable" ) ;
				return target.raw[ property ] ;
			}
			else {
				return this.nested( property , target.raw ) ;
			}
		}

		return target.raw[ property ] ;
	} ,
	set: ( target , property , value , receiver , path ) => {
		if ( path.length > 1 ) {
			target[ property ] = value ;
		}
		else {
			target.raw[ property ] = value ;
		}
		
		return true ;
	}
} ;



function Document( collection , rawDocument , options ) {
	this.collection = collection ;
	this.raw = rawDocument ;
	this.localChanges = null ;

	this.meta = {
		id: collection.driver.checkId( rawDocument , true ) ,
		upstreamExists: false ,
		saved: false ,
		loaded: false ,
		deleted: false
	} ;

	this.proxy = new DeepProxy( this , PROXY_HANDLER , PROXY_OPTIONS ) ;
}

module.exports = Document ;



METHODS.save = true ;
Document.prototype.save = async function( options = {} ) {
	if ( this.upstreamExists ) {
		// Full save (update)
		return this.collection.driver.update( this.id , this.raw ).then( () => {
			this.meta.saved = true ;
			//this.localPatch = false ;
			//this.staged = {} ;
		} ) ;
	}
	else if ( options.overwrite ) {
		// overwrite wanted
		return this.collection.driver.overwrite( this.document ).then( () => {
			this.meta.saved = true ;
			this.meta.upstreamExists = true ;
			//this.localPatch = false ;
			//this.staged = {} ;
		} ) ;
	}

	// create (insert) needed
	return this.collection.driver.create( this.document ).then( () => {
		this.meta.saved = true ;
		this.meta.upstreamExists = true ;
		//this.localPatch = false ;
		//this.staged = {} ;
	} ) ;

} ;


