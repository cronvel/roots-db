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



var doormen = require( 'doormen' ) ;
var Attachment = require( './Attachment.js' ) ;


// /!\ Forced to do that ATM... because of multiLink, needing a true MongoDB object ID
var mongodb = require( 'mongodb' ) ;



function checkObjectId( data ) {
	if ( data && typeof data === 'object' && ( data.constructor.name === 'ObjectID' || data.constructor.name === 'ObjectId' ) ) {
		return true ;
	}

	return typeof data === 'string' && ( data === '/' || ( data.length === 24 && /^[0-9a-f]{24}$/.test( data ) ) ) ;
}



function checkLink( data ) {
	if ( data === undefined || data === null ) { return true ; }
	if ( typeof data !== 'object' ) { return false ; }
	return checkObjectId( data._id ) ;
}



function checkMultiLink( data ) {
	var i , iMax ;

	if ( ! Array.isArray( data ) ) { return false ; }

	for ( i = 0 , iMax = data.length ; i < iMax ; i ++ ) {
		if ( ! checkLink( data[ i ] ) ) { return false ; }
	}

	return true ;
}



var backLinkSchema = {
	type: 'object' ,
	optional: true ,
	properties: {
		batch: {
			optional: true ,
			type: 'array' ,
			of: { type: 'object' }
		}
	}
} ;



// /!\ This is sub-optimal! Filename should filter bad name, contentType should check for valid content-type
var attachmentSchema = {
	type: 'object' ,
	properties: {
		id: { type: 'objectId' } ,
		filename: { type: 'string' } ,
		contentType: { type: 'string' }
	}
} ;



doormen.extendTypeChecker( {
	"objectId": checkObjectId ,
	"link": checkLink ,
	"multiLink": checkMultiLink ,
	//"backLink": data => data === undefined || data === null || Array.isArray( data ) ,
	"backLink": data => {
		try {
			doormen( backLinkSchema , data ) ;
			return true ;
		}
		catch ( error ) {
			return false ;
		}
	} ,
	"attachment": data => {
		if ( data instanceof Attachment ) { return true ; }

		try {
			doormen( attachmentSchema , data ) ;
			return true ;
		}
		catch ( error ) {
			return false ;
		}
	}
} ) ;



// Allow one to pass the whole linked target object
function toObjectId( data ) {
	if ( typeof data === 'string' ) {
		try {
			return mongodb.ObjectID( data ) ;
		}
		catch ( error ) {}
	}

	return data ;
}



// Allow one to pass the whole linked target object
function toLink( data ) {
	if ( data && typeof data === 'object' ) {
		if ( data.constructor.name === 'ObjectID' || data.constructor.name === 'ObjectId' ) {
			return { _id: data } ;
		}

		data._id = toObjectId( data._id ) ;
	}
	else if ( typeof data === 'string' ) {
		try {
			data = { _id: mongodb.ObjectID( data ) } ;
		}
		catch ( error ) {}
	}

	return data ;
}



// Allow one to pass the whole array (batch) of linked target object
function toMultiLink( data ) {
	if ( ! Array.isArray( data ) ) { return [] ; }
	data.forEach( ( link , index ) => data[ index ] = toLink( link ) ) ;
	return data ;
}



function toBackLink( data ) {
	if ( ! data || typeof data !== 'object' ) {
		return {} ;
	}

	if ( Array.isArray( data ) ) { data = { batch: data } ; }

	return data ;
}



doormen.extendSanitizer( {
	"toObjectId": toObjectId ,
	"toLink": toLink ,
	"toMultiLink": toMultiLink ,
	"toBackLink": toBackLink ,
	"toBackMultiLink": toBackLink
} ) ;



/*
doormen.extendSanitizer( {

	// Create a random slug for restQuery
	"restQuery.randomSlug": function restQueryRandomSlug( data ) {
		if ( data !== undefined && data !== null ) { return data ; }
		return Date.now().toString( 36 ) + '-' + crypto.pseudoRandomBytes( 4 ).readUInt32LE( 0 , true ).toString( 36 ) ;
	}
} ) ;
*/

