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

var tree = require( 'tree-kit' ) ;



/*
	The goal of this class is to figure out weither a fingerprint is unique or not, thus detecting if a query should return a Batch or a DocumentWrapper.
	Some other data may be calculated at that time.
	For instance, it does not much.
	
	options:
		* convert: a sparse document is given, it should be converted to the 'flat' format
*/
function FingerprintWrapper( collection , rawFingerprint , options )
{
	if ( ! rawFingerprint || typeof rawFingerprint !== 'object' ) { rawFingerprint = {} ; }
	if ( ! options || typeof options !== 'object' ) { options = {} ; }
	
	// Already wrapped?
	if ( rawFingerprint.$ instanceof FingerprintWrapper ) { return rawFingerprint.$ ; }
	
	var wrapper = Object.create( FingerprintWrapper.prototype , {
		//world: { value: collection.world } ,
		collection: { value: collection }
	} ) ;
	
	collection.driver.checkId( rawFingerprint ) ;
	
	if ( options.fromPartialDocument )
	{
		Object.defineProperties( wrapper , {
			fromPartialDocument: { value: true , enumerable: true } ,
			fingerprint: {
				configurable: true ,
				enumerable: true ,
				get: FingerprintWrapper.prototype.flatten.bind( wrapper )
			} ,
			partialDocument: { value: rawFingerprint , enumerable: true } ,
		} ) ;
	}
	else
	{
		Object.defineProperties( wrapper , {
			fromPartialDocument: { value: false , enumerable: true } ,
			fingerprint: { value: rawFingerprint , enumerable: true } ,
			partialDocument: {
				configurable: true ,
				enumerable: true ,
				get: FingerprintWrapper.prototype.unflatten.bind( wrapper )
			}
		} ) ;
	}
	
	// Lazyloading
	Object.defineProperties( wrapper , {
		unique: {
			configurable: true ,
			enumerable: true ,
			get: FingerprintWrapper.prototype.uniquenessCheck.bind( wrapper )
		}
	} ) ;
	
	Object.defineProperty( rawFingerprint , '$' , { value: wrapper } ) ;
	
	return wrapper ;
}

FingerprintWrapper.prototype.constructor = FingerprintWrapper ;
module.exports = FingerprintWrapper ;



FingerprintWrapper.prototype.uniquenessCheck = function uniquenessCheck()
{
	var i , j , index , match , uniques = this.collection.uniques ;
	
	for ( i = 0 ; i < uniques.length ; i ++ )
	{
		index = uniques[ i ] ;
		match = 0 ;
		for ( j = 0 ; j < index.length ; j ++ )
		{
			if ( index[ j ] in this.fingerprint ) { match ++ ; }
		}
		
		if ( match === index.length )
		{
			Object.defineProperty( this , 'unique' , { value: true , enumerable: true } ) ;
			return true ;
		}
	}
	
	Object.defineProperty( this , 'unique' , { value: false , enumerable: true } ) ;
	return false ;
} ;



FingerprintWrapper.prototype.flatten = function flatten()
{
	var fingerprint = tree.extend(
		{
			flat: true ,
			deepFilter: this.collection.driver.objectFilter
		} ,
		null ,
		this.partialDocument
	) ;
	
	Object.defineProperty( fingerprint , '$' , { value: this } ) ;
	Object.defineProperty( this , 'fingerprint' , { value: fingerprint , enumerable: true } ) ;
	
	return fingerprint ;
} ;



FingerprintWrapper.prototype.unflatten = function unflatten()
{
	var partialDocument = tree.extend(
		{
			unflat: true ,
			deepFilter: this.collection.driver.objectFilter
		} ,
		null ,
		this.fingerprint
	) ;
	
	Object.defineProperty( partialDocument , '$' , { value: this } ) ;
	Object.defineProperty( this , 'partialDocument' , { value: partialDocument , enumerable: true } ) ;
	
	return partialDocument ;
} ;

