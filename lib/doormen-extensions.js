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



const doormen = require( 'doormen' ) ;
const Attachment = require( './Attachment.js' ) ;
const AttachmentSet = require( './AttachmentSet.js' ) ;
const Document = require( './Document.js' ) ;


// /!\ Forced to do that ATM... because of multiLink, needing a true MongoDB object ID
const mongodb = require( 'mongodb' ) ;



function checkObjectId( data ) {
	if ( data && typeof data === 'object' && ( data.constructor.name === 'ObjectID' || data.constructor.name === 'ObjectId' ) ) {
		return true ;
	}

	return typeof data === 'string' && ( data === '/' || ( data.length === 24 && /^[0-9a-f]{24}$/.test( data ) ) ) ;
}



function checkLink( data , schema ) {
	//console.log( "checkLink:" , data ) ;
	if ( data === undefined || data === null ) { return true ; }
	if ( typeof data !== 'object' ) { return false ; }
	if ( ! schema.anyCollection ) { return checkObjectId( data._id ) ; }
	return data._collection && typeof data._collection === 'string' && checkObjectId( data._id ) ;
}



function checkRequiredLink( data ) {
	//console.log( "checkRequiredLink:" , data ) ;
	if ( ! data ) { return false ; }
	if ( typeof data !== 'object' ) { return false ; }
	return checkObjectId( data._id ) ;
}



function checkMultiLink( data ) {
	// Now it's done using:   of: { type: 'link' } and constraints, as override, see "typeOverrides" in Collection.js
	return Array.isArray( data ) ;
}



const backLinkSchema = {
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
const attachmentSchema = {
	type: 'object' ,
	properties: {
		id: { type: 'objectId' } ,
		filename: { type: 'string' } ,
		contentType: { type: 'string' } ,
		fileSize: { optional: true , type: 'integer' } ,
		hash: { optional: true , type: 'string' } ,
		hashType: { optional: true , type: 'string' } ,
		metadata: { type: 'object' , default: {} }
	}
} ;



const attachmentSetSchema = {
	type: 'object' ,
	properties: {
		set: { type: 'object' , of: attachmentSchema }
	}
} ;



doormen.extendTypeCheckers( {
	"objectId": checkObjectId ,
	"link": checkLink ,
	"requiredLink": checkRequiredLink ,
	"multiLink": checkMultiLink ,
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
	} ,
	"attachmentSet": data => {
		if ( data instanceof AttachmentSet ) { return true ; }

		try {
			doormen( attachmentSetSchema , data ) ;
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



const LINK_INNER_PROPERTIES = new Set( [ '_id' , '_collection' ] ) ;

// Allow one to pass the whole linked target object
function toLink( data , schema ) {
	if ( data && typeof data === 'object' ) {
		if ( data.constructor.name === 'ObjectID' || data.constructor.name === 'ObjectId' ) {
			return { _id: data } ;
		}


		if ( data._ && ( data._ instanceof Document ) ) {
			data = schema.anyCollection ?
				{ _id: toObjectId( data._id ) , _collection: data._.collection && data._.collection.name } :
				{ _id: toObjectId( data._id ) } ;
		}
		else if ( data instanceof Document ) {
			data = schema.anyCollection ?
				{ _id: toObjectId( data.raw._id ) , _collection: data.collection && data.collection.name } :
				{ _id: toObjectId( data.raw._id ) } ;
		}
		else {
			// Remove extra properties? Or create a new object?
			data._id = toObjectId( data._id ) ;
			for ( let key in data ) {
				if ( ! LINK_INNER_PROPERTIES.has( key ) ) { delete data[ key ] ; }
			}

			/*
			data = schema.anyCollection ?
				{ _id: toObjectId( data._id ) , _collection: data._collection } :
				{ _id: toObjectId( data._id ) } ;
			*/
		}
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
	// Now it's done using:   of: { type: 'link' , sanitize: 'toLink' } and constraints
	if ( ! data ) { return [] ; }
	if ( ! Array.isArray( data ) ) { return [ data ] ; }
	return data ;
}



function toBackLink( data ) {
	if ( ! data || typeof data !== 'object' ) {
		return {} ;
	}

	if ( Array.isArray( data ) ) { data = { batch: data } ; }

	return data ;
}



doormen.extendSanitizers( {
	"toObjectId": toObjectId ,
	"toLink": toLink ,
	"toMultiLink": toMultiLink ,
	"toBackLink": toBackLink ,
	"toBackMultiLink": toBackLink
} ) ;

