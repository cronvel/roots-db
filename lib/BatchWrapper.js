/*
	The Cedric's Swiss Knife (CSK) - CSK Object-Document Mapping

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
var rootsDb = require( './rootsDb.js' ) ;

//var tree = require( 'tree-kit' ) ;
var ErrorStatus = require( 'error-status' ) ;



/*
	FEATURE TODO:
		* set common properties with batch.$
*/



/*
	BatchWrapper( collection , rawBatch , options )
	
	ALL ARGUMENTS are MANDATORY!
	Internal usage only.
*/
function BatchWrapper( collection , rawBatch , options )
{
	var i , length ;
	
	// Already wrapped?
	if ( rawBatch.$ instanceof BatchWrapper ) { return rawBatch.$ ; }
	
	// This can be costly on large batch
	length = rawBatch.length ;
	for ( i = 0 ; i < length ; i ++ )
	{
		rootsDb.DocumentWrapper( collection , rawBatch[ i ] , options ) ;
	}
	
	// Validation?
	
	var wrapper = Object.create( BatchWrapper.prototype , {
		batch: { value: rawBatch } ,
		world: { value: collection.world } ,
		collection: { value: collection } ,
		//suspected: { writable: true, value: false } ,
		
		// Useful here?
		loaded: { value: options.fromUpstream ? true : false , writable: true , enumerable: true } ,
		saved: { value: false , writable: true , enumerable: true } ,
		deleted: { value: false , writable: true , enumerable: true } ,
		upstreamExists: { value: options.fromUpstream ? true : false , writable: true , enumerable: true }
	} ) ;
	
	// Useful?
	if ( options.fromUpstream )
	{
		wrapper.loaded = true ;
		wrapper.upstreamExists = true ;
	}
	
	Object.defineProperty( rawBatch , '$' , { value: wrapper } ) ;
	
	return wrapper ;
}



module.exports = BatchWrapper ;



function noop() {}











// Old batch methods...



function oldBatchWrapper( collection , fingerprint , options )
{
	if ( ! ( collection instanceof rootsDb.Collection ) ) { throw new TypeError( '[roots-db] Argument #0 of rootsDb.BatchWrapper() should be an instance of rootsDb.Collection' ) ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	
	var batch = Object.create( BatchWrapper.prototype , {
		world: { value: collection.world } ,
		collection: { value: collection } ,
		documents: { value: [] } ,
		suspected: { writable: true, value: false }
	} ) ;
	
	batch.fingerprint = null ;
	
	if ( fingerprint && typeof fingerprint === 'object' )
	{
		if ( fingerprint instanceof rootsDb.FingerprintWrapper ) { batch.fingerprint = fingerprint ; }
		else { batch.fingerprint = collection.createFingerprint( fingerprint ) ; }
	}
	
	// Suspect is set when the object is in a state where it may exist upstream but should be loaded first
	if ( options.suspected )
	{
		batch.suspected = true ;
		
		if ( options.witness && typeof options.witness === 'object' && Object.keys( options.witness ).length )
		{
			batch.witness = options.witness ;
		}
		else
		{
			batch.witness = null ;
		}
		
		if ( ! batch.fingerprint && ! batch.witness )
		{
			throw new Error( '[roots-db] cannot instanciate a suspect without fingerprint or witness' ) ;
		}
		
		return batch ;
	}
	
	return batch ;
}



/*
	Should handle hooks, e.g. if the batch is related to a multilink, it should update the parent multilink property.
*/
BatchWrapper.prototype.add = function batchAdd( document )
{
	if ( ! ( document instanceof rootsDb.DocumentWrapper ) ) { throw new TypeError( '[roots-db] Argument #0 of rootsDb.BatchWrapper.prototype.add() should be an instance of rootsDb.DocumentWrapper' ) ; }
	this.documents.push( document ) ;
} ;



// Reveal a suspected Batch
BatchWrapper.prototype.reveal = function batchReveal( options , callback )
{
	if ( ! options || typeof options !== 'object' )
	{
		if ( typeof options === 'function' ) { callback = options ; }
		options = {} ;
	}
	
	if ( typeof callback !== 'function' ) { callback = noop ; }
	
	if ( ! this.suspected )
	{
		callback( undefined , this ) ;
		return ;
	}
	
	if ( this.fingerprint ) { this.revealByFingerprint( options , callback ) ; return ; }
	
	if ( this.witness ) { this.revealByWitness( options , callback ) ; return ; }
} ;



BatchWrapper.prototype.revealByFingerprint = function batchRevealByFingerprint( options , callback )
{
	var self = this ;
	
	this.collection.driver.collect( this.fingerprint.$ , function( error , rawBatch ) {
		
		if ( error ) { callback( error ) ; return ; }
		if ( ! rawBatch ) { callback( ErrorStatus.notFound( { message: 'Batch not found' } ) ) ; return ; }	// should never happen?
		
		var i , length = rawBatch.length ;
		
		for ( i = 0 ; i < length ; i ++ )
		{
			self.add( rootsDb.DocumentWrapper( self.collection , rawBatch[ i ] , {} ) ) ;
		}
		
		self.suspected = false ;
		
		callback( undefined , self ) ;
	} ) ;
	
} ;



BatchWrapper.prototype.revealByWitness = function batchRevealByWitness( options , callback )
{
	var self = this ;
	
	if ( ! this.witness || typeof this.witness !== 'object' ) { throw new Error( '[roots-db] no witness for this suspect' ) ; }
	
	switch ( this.witness.type )
	{
		case 'backlink' :
			if ( this.witness.document.suspected && ! this.witness.document.id )
			{
				// Do not transmit random options...
				this.witness.document.reveal( { idOnly: true } , function( error ) {
					if ( error ) { callback( error ) ; return ; }
					self.revealByWitness( options , callback ) ;
				} ) ;
				
				return ;
			}
			
			this.fingerprint = {} ;
			this.fingerprint[ this.witness.property ] = this.witness.document.id ;
			this.fingerprint = this.collection.createFingerprint( this.fingerprint ) ;
			this.revealByFingerprint( options , callback ) ;
			break ;
		
		// those type cannot exist for a batch:
		//case 'link' :
		default :
			throw new Error( '[roots-db] Cannot reveal batch with this type of witness: ' + this.witness.type ) ;
	}
} ;



