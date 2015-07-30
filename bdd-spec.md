# TOC
   - [Create new documents](#create-new-documents)
   - [Fingerprint](#fingerprint)
   - [Build collections' indexes](#build-collections-indexes)
   - [ID creation](#id-creation)
   - [Get documents](#get-documents)
   - [Get documents by unique fingerprint](#get-documents-by-unique-fingerprint)
   - [Save/update documents](#saveupdate-documents)
   - [Delete documents](#delete-documents)
   - [Suspects and revealing](#suspects-and-revealing)
   - [Collect batchs](#collect-batchs)
   - [Links](#links)
   - [Backlinks](#backlinks)
   - [Embedded documents](#embedded-documents)
<a name=""></a>
 
<a name="create-new-documents"></a>
# Create new documents
should create an document.

```js
var user = users.createDocument() ;

expect( user ).to.be.an( odm.Document ) ;
expect( users.useMemProxy ).to.be.ok() ;
expect( user.useMemProxy ).to.be.ok() ;
expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
expect( tree.extend( null , {} , user.$ ) ).to.eql( tree.extend( null , { _id: user.$._id } , expectedDefaultUser ) ) ;
```

<a name="fingerprint"></a>
# Fingerprint
should create a fingerprint.

```js
var f = users.createFingerprint( { firstName: 'Terry' } ) ;

expect( f ).to.be.an( odm.Fingerprint ) ;
expect( tree.extend( null , {} , f.$ ) ).to.eql( { firstName: 'Terry' } ) ;
```

should detect uniqueness correctly.

```js
expect( users.createFingerprint( { _id: 'somehash' } ).unique ).to.be( true ) ;
expect( users.createFingerprint( { firstName: 'Terry' } ).unique ).to.be( false ) ;
expect( users.createFingerprint( { firstName: 'Terry', lastName: 'Bogard' } ).unique ).to.be( false ) ;
expect( users.createFingerprint( { _id: 'somehash', firstName: 'Terry', lastName: 'Bogard' } ).unique ).to.be( true ) ;
expect( users.createFingerprint( { jobId: 'somehash' } ).unique ).to.be( false ) ;
expect( users.createFingerprint( { memberSid: 'terry-bogard' } ).unique ).to.be( false ) ;
expect( users.createFingerprint( { jobId: 'somehash', memberSid: 'terry-bogard' } ).unique ).to.be( true ) ;
```

<a name="build-collections-indexes"></a>
# Build collections' indexes
should build indexes.

```js
expect( users.uniques ).to.be.eql( [ [ '_id' ], [ 'jobId', 'memberSid' ] ] ) ;
expect( jobs.uniques ).to.be.eql( [ [ '_id' ] ] ) ;

async.foreach( world.collections , function( collection , name , foreachCallback )
{
	collection.buildIndexes( function( error ) {
		
		expect( error ).not.to.be.ok() ;
		
		collection.driver.getIndexes( function( error , indexes ) {
			expect( indexes ).to.be.eql( collection.indexes ) ;
			foreachCallback() ;
		} ) ;
	} ) ;
} )
.exec( done ) ;
```

<a name="id-creation"></a>
# ID creation
should create ID (like Mongo ID).

```js
expect( users.createId() ).to.match( /^[0-9a-f]{24}$/ ) ;
expect( users.createId() ).to.match( /^[0-9a-f]{24}$/ ) ;
expect( users.createId() ).to.match( /^[0-9a-f]{24}$/ ) ;
expect( users.createId() ).to.match( /^[0-9a-f]{24}$/ ) ;
expect( users.createId() ).to.match( /^[0-9a-f]{24}$/ ) ;
```

<a name="get-documents"></a>
# Get documents
should get a document (create, save and retrieve).

```js
var user = users.createDocument( {
	firstName: 'John' ,
	lastName: 'McGregor'
} ) ;

var id = user.$._id ;

async.series( [
	function( callback ) {
		user.save( callback ) ;
	} ,
	function( callback ) {
		users.get( id , { raw: true } , function( error , user ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ;
			expect( error ).not.to.be.ok() ;
			expect( user ).not.to.be.an( odm.Document ) ;
			expect( user._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user._id ).to.eql( id ) ;
			expect( user ).to.eql( tree.extend( null , { _id: user._id , firstName: 'John' , lastName: 'McGregor' , memberSid: 'John McGregor' } ) ) ;
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		users.get( id , function( error , user ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ; 
			expect( error ).not.to.be.ok() ;
			expect( user ).to.be.an( odm.Document ) ;
			expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user.$._id ).to.eql( id ) ;
			expect( tree.extend( null , {} , user.$ ) ).to.eql( tree.extend( null , { _id: user.$._id } , expectedDefaultUser , { firstName: 'John' , lastName: 'McGregor' , memberSid: 'John McGregor' } ) ) ;
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

when trying to get an unexistant document, an ErrorStatus (type: notFound) should be issued.

```js
// Unexistant ID
var id = new mongodb.ObjectID() ;

async.parallel( [
	function( callback ) {
		users.get( id , function( error , user ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ; 
			expect( error ).to.be.an( ErrorStatus ) ;
			expect( error.type ).to.equal( 'notFound' ) ;
			expect( user ).to.be( undefined ) ;
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		users.get( id , { raw: true } , function( error , user ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ; 
			expect( error ).to.be.an( ErrorStatus ) ;
			expect( error.type ).to.equal( 'notFound' ) ;
			expect( user ).to.be( undefined ) ;
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

<a name="get-documents-by-unique-fingerprint"></a>
# Get documents by unique fingerprint
should get a document (create, save and retrieve).

```js
var user = users.createDocument( {
	firstName: 'Bill' ,
	lastName: "Cut'throat"
} , { useMemProxy: false } ) ;

var id = user.$._id ;
var memberSid = user.$.memberSid ;

var job = jobs.createDocument() ;
var jobId = job.id ;
user.$.job = job ;

async.series( [
	function( callback ) {
		user.save( callback ) ;
	} ,
	function( callback ) {
		job.save( callback ) ;
	} ,
	function( callback ) {
		users.getUnique( { memberSid: memberSid , jobId: jobId } , { raw: true } , function( error , user ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ;
			expect( error ).not.to.be.ok() ;
			expect( user ).not.to.be.an( odm.Document ) ;
			expect( user._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user._id ).to.eql( id ) ;
			expect( user ).to.eql( tree.extend( null , { _id: user._id , jobId: jobId , firstName: 'Bill' , lastName: "Cut'throat" , memberSid: "Bill Cut'throat" } ) ) ;
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		users.getUnique( { memberSid: memberSid , jobId: jobId } , function( error , user ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ; 
			expect( error ).not.to.be.ok() ;
			expect( user ).to.be.an( odm.Document ) ;
			expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user.$._id ).to.eql( id ) ;
			expect( tree.extend( null , {} , user.$ ) ).to.eql( tree.extend( null , { _id: user.$._id } , expectedDefaultUser , { jobId: jobId , firstName: 'Bill' , lastName: "Cut'throat" , memberSid: "Bill Cut'throat" } ) ) ;
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

when trying to get a document with a non-unique fingerprint, an ErrorStatus (type: badRequest) should be issued.

```js
var user = users.createDocument( {
	firstName: 'Bill' ,
	lastName: "Tannen"
} , { useMemProxy: false } ) ;

async.series( [
	function( callback ) {
		user.save( callback ) ;
	} ,
	function( callback ) {
		users.getUnique( { firstName: 'Bill' , lastName: "Tannen" } , { raw: true } , function( error ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ;
			expect( error ).to.be.an( Error ) ;
			expect( error.type ).to.be( 'badRequest' ) ;
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		users.getUnique( { firstName: 'Bill' , lastName: "Tannen" } , function( error ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ; 
			expect( error ).to.be.an( Error ) ;
			expect( error.type ).to.be( 'badRequest' ) ;
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

<a name="saveupdate-documents"></a>
# Save/update documents
should save correctly and only non-default value are registered into the upstream (create, save and retrieve).

```js
var user = users.createDocument( {
	firstName: 'Jack'
} ) ;

var id = user.$._id ;

async.series( [
	function( callback ) {
		user.save( callback ) ;
	} ,
	function( callback ) {
		users.get( id , function( error , user ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ; 
			expect( error ).not.to.be.ok() ;
			expect( user ).to.be.an( odm.Document ) ;
			expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user.$._id ).to.eql( id ) ;
			expect( tree.extend( null , {} , user.$ ) ).to.eql( tree.extend( null , { _id: user.$._id } , expectedDefaultUser , { firstName: 'Jack' , lastName: 'Doe' , memberSid: 'Jack Doe' } ) ) ;
			
			// upstream should not contains lastName
			expect( tree.extend( { own: true } , {} , user.upstream ) ).to.eql( { _id: user.$._id , firstName: 'Jack' , memberSid: 'Jack Doe' } ) ;
			
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		users.get( id , { raw: true } , function( error , user ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ; 
			expect( error ).not.to.be.ok() ;
			expect( user ).not.to.be.an( odm.Document ) ;
			expect( user._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user._id ).to.eql( id ) ;
			
			// upstream should not contains lastName
			expect( user ).to.eql( { _id: user._id , firstName: 'Jack' , memberSid: 'Jack Doe' } ) ;
			
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

should save creating a minimalistic patch so parallel save do not overwrite each others (create, save, retrieve, patch² and retrieve).

```js
var user = users.createDocument( {
	firstName: 'Johnny' ,
	lastName: 'Starks'
} ) ;

var id = user.$._id ;
var user2 ;
//id = users.createDocument()._id ;


async.series( [
	function( callback ) {
		user.save( callback ) ;
	} ,
	function( callback ) {
		users.get( id , { useMemProxy: false } , function( error , u ) {
			user2 = u ;
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user2 ) ; 
			expect( error ).not.to.be.ok() ;
			expect( user2 ).to.be.an( odm.Document ) ;
			expect( user2.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user2.$._id ).to.eql( id ) ;
			expect( tree.extend( null , {} , user2.$ ) ).to.eql( tree.extend( null , { _id: user2.$._id } , expectedDefaultUser , { firstName: 'Johnny' , lastName: 'Starks' , memberSid: 'Johnny Starks' } ) ) ;
			
			// upstream should not contains lastName
			expect( tree.extend( { own: true } , {} , user.upstream ) ).to.eql( { _id: user.$._id , firstName: 'Johnny' , lastName: 'Starks' , memberSid: 'Johnny Starks' } ) ;
			
			callback() ;
		} ) ;
	} ,
	async.parallel( [
		function( callback ) {
			user.$.lastName = 'Smith' ;
			user.save( callback ) ;
		} ,
		function( callback ) {
			user2.$.firstName = 'Joey' ;
			user2.save( callback ) ;
		}
	] ) ,
	function( callback ) {
		users.get( id , { useMemProxy: false } , function( error , user ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ; 
			expect( error ).not.to.be.ok() ;
			expect( user ).to.be.an( odm.Document ) ;
			expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user.$._id ).to.eql( id ) ;
			expect( tree.extend( null , {} , user.$ ) ).to.eql( tree.extend( null , { _id: user.$._id } , expectedDefaultUser , { firstName: 'Joey' , lastName: 'Smith' , memberSid: 'Johnny Starks' } ) ) ;
			
			// upstream should not contains lastName
			expect( tree.extend( { own: true } , {} , user.upstream ) ).to.eql( { _id: user.$._id , firstName: 'Joey' , lastName: 'Smith' , memberSid: 'Johnny Starks' } ) ;
			
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		users.get( id , { raw: true , useMemProxy: false } , function( error , user ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ; 
			expect( error ).not.to.be.ok() ;
			expect( user ).not.to.be.an( odm.Document ) ;
			expect( user._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user._id ).to.eql( id ) ;
			
			// upstream should not contains lastName
			expect( user ).to.eql( { _id: user._id , firstName: 'Joey' , lastName: 'Smith' , memberSid: 'Johnny Starks' } ) ;
			
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

should save a full document so parallel save *DO* overwrite each others (create, save, retrieve, full update² and retrieve).

```js
var user = users.createDocument( {
	firstName: 'Johnny B.' ,
	lastName: 'Starks'
} ) ;

var id = user.$._id ;
var user2 ;


async.series( [
	function( callback ) {
		user.save( callback ) ;
	} ,
	function( callback ) {
		users.get( id , { useMemProxy: false } , function( error , u ) {
			user2 = u ;
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user2 ) ; 
			expect( error ).not.to.be.ok() ;
			expect( user2 ).to.be.an( odm.Document ) ;
			expect( user2.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user2.$._id ).to.eql( id ) ;
			expect( tree.extend( null , {} , user2.$ ) ).to.eql( tree.extend( null , { _id: user2.$._id } , expectedDefaultUser , { firstName: 'Johnny B.' , lastName: 'Starks' , memberSid: 'Johnny B. Starks' } ) ) ;
			
			// upstream should not contains lastName
			expect( tree.extend( { own: true } , {} , user.upstream ) ).to.eql( { _id: user.$._id , firstName: 'Johnny B.' , lastName: 'Starks' , memberSid: 'Johnny B. Starks' } ) ;
			
			callback() ;
		} ) ;
	} ,
	async.parallel( [
		function( callback ) {
			user.$.lastName = 'Smith' ;
			user.save( { fullSave: true } , callback ) ;
		} ,
		function( callback ) {
			user2.$.firstName = 'Joey' ;
			user2.save( { fullSave: true } , callback ) ;
		}
	] ) ,
	function( callback ) {
		users.get( id , { useMemProxy: false } , function( error , user ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ; 
			expect( error ).not.to.be.ok() ;
			expect( user ).to.be.an( odm.Document ) ;
			expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user.$._id ).to.eql( id ) ;
			expect( tree.extend( null , {} , user.$ ) ).to.eql( tree.extend( null , { _id: user.$._id } , expectedDefaultUser , { firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny B. Starks' } ) ) ;
			
			// upstream should not contains lastName
			expect( tree.extend( { own: true } , {} , user.upstream ) ).to.eql( { _id: user.$._id , firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny B. Starks' } ) ;
			
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		users.get( id , { raw: true , useMemProxy: false } , function( error , user ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ; 
			expect( error ).not.to.be.ok() ;
			expect( user ).not.to.be.an( odm.Document ) ;
			expect( user._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user._id ).to.eql( id ) ;
			
			// upstream should not contains lastName
			expect( user ).to.eql( { _id: user._id , firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny B. Starks' } ) ;
			
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

<a name="delete-documents"></a>
# Delete documents
should delete a document (create, save, retrieve, then delete it so it cannot be retrieved again).

```js
var user = users.createDocument() ;
expect( user.useMemProxy ).to.be.ok() ;

//console.log( user ) ;
var id = user.$._id ;
//id = users.createDocument()._id ;
user.$.firstName = 'John' ;
user.$.lastName = 'McGregor' ;

async.series( [
	function( callback ) {
		user.save( callback ) ;
	} ,
	function( callback ) {
		users.get( id , function( error , user ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ; 
			expect( error ).not.to.be.ok() ;
			expect( user.useMemProxy ).to.be.ok() ;
			expect( user ).to.be.an( odm.Document ) ;
			expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user.$._id ).to.eql( id ) ;
			expect( tree.extend( null , {} , user.$ ) ).to.eql( tree.extend( null , { _id: user.$._id } , expectedDefaultUser , { firstName: 'John' , lastName: 'McGregor' } ) ) ;
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		user.delete( function( error ) {
			expect( error ).not.to.be.ok() ;
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		users.get( id , function( error , user ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ; 
			expect( error ).to.be.an( ErrorStatus ) ;
			expect( error.type ).to.equal( 'notFound' ) ;
			expect( user ).to.be( undefined ) ;
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

<a name="suspects-and-revealing"></a>
# Suspects and revealing
Synchronous 'get()' should provide an 'identified' suspect, and reveal it later.

```js
var user = users.createDocument( {
	firstName: 'Dilbert' ,
	lastName: 'Dugommier'
} , { useMemProxy: false } ) ;

var id = user.$._id ;

expect( user.state() ).to.equal( 'app-side' ) ;

async.series( [
	function( callback ) {
		user.save( callback ) ;
	} ,
	function( callback ) {
		var user = users.get( id ) ;
		expect( user ).to.be.an( odm.Document ) ;
		expect( user.suspected ).to.be.ok() ;
		expect( user.loaded ).not.to.be.ok() ;
		expect( user.upstreamExists ).not.to.be.ok() ;
		expect( user.state() ).to.equal( 'suspected' ) ;
		expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
		expect( user.$._id ).to.eql( id ) ;
		expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: id, firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
		
		user.reveal( function( error ) {
			expect( user ).to.be.an( odm.Document ) ;
			expect( user.suspected ).not.to.be.ok() ;
			expect( user.loaded ).to.be.ok() ;
			expect( user.upstreamExists ).to.be.ok() ;
			//delete user.$._id ;
			//console.log( '----------------------' , Object.keys( user.$ ) ) ;
			expect( Object.keys( user.$ ).length ).to.equal( 0 ) ;
			expect( user.state() ).to.equal( 'synced' ) ;
			expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user.$._id ).to.eql( id ) ;
			expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: id, firstName: 'Dilbert', lastName: 'Dugommier', jobId: undefined, godfatherId: undefined , memberSid: 'Dilbert Dugommier' } ) ;
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

Synchronous 'get()' should provide a suspect with a bad identity, then reveal it as nothing.

```js
var id = new mongodb.ObjectID() ;
var user = users.get( id ) ;

expect( user ).to.be.an( odm.Document ) ;
expect( user.suspected ).to.be.ok() ;
expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
expect( user.$._id ).to.eql( id ) ;
expect( user.loaded ).not.to.be.ok() ;
expect( user.upstreamExists ).not.to.be.ok() ;
expect( user.deleted ).not.to.be.ok() ;
expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: id, firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;

user.reveal( function( error ) {
	expect( user ).to.be.an( odm.Document ) ;
	expect( user.suspected ).not.to.be.ok() ;
	expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
	expect( user.$._id ).to.eql( id ) ;
	expect( user.loaded ).not.to.be.ok() ;
	expect( user.upstreamExists ).not.to.be.ok() ;
	expect( user.deleted ).to.be.ok() ;
	expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: id, firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
	done() ;
} ) ;
```

Synchronous 'getUnique()' should provide an 'identified' suspect, and reveal it later.

```js
var user = users.createDocument( {
	firstName: 'Joe' ,
	lastName: 'Pink'
} , { useMemProxy: false } ) ;

var id = user.$._id ;
var memberSid = user.$.memberSid ;

var job = jobs.createDocument() ;
var jobId = job.id ;
user.$.job = job ;

expect( user.state() ).to.equal( 'app-side' ) ;

async.series( [
	function( callback ) {
		user.save( callback ) ;
	} ,
	function( callback ) {
		job.save( callback ) ;
	} ,
	function( callback ) {
		var user = users.getUnique( { memberSid: memberSid , jobId: jobId } ) ;
		expect( user ).to.be.an( odm.Document ) ;
		expect( user.suspected ).to.be.ok() ;
		expect( user.loaded ).not.to.be.ok() ;
		expect( user.upstreamExists ).not.to.be.ok() ;
		expect( user.state() ).to.equal( 'suspected' ) ;
		expect( user.$._id ).to.be( undefined ) ;
		expect( tree.extend( null , {} , user.$ ) ).to.eql( { firstName: undefined, lastName: undefined, jobId: jobId, godfatherId: undefined, memberSid: "Joe Pink" } ) ;
		
		user.reveal( function( error ) {
			expect( user ).to.be.an( odm.Document ) ;
			expect( user.suspected ).not.to.be.ok() ;
			expect( user.loaded ).to.be.ok() ;
			expect( user.upstreamExists ).to.be.ok() ;
			//delete user.$._id ;
			//console.log( '----------------------' , Object.keys( user.$ ) ) ;
			expect( Object.keys( user.$ ).length ).to.equal( 0 ) ;
			expect( user.state() ).to.equal( 'synced' ) ;
			expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user.$._id ).to.eql( id ) ;
			expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: id, firstName: 'Joe', lastName: 'Pink', jobId: jobId, godfatherId: undefined , memberSid: 'Joe Pink' } ) ;
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

<a name="collect-batchs"></a>
# Collect batchs
should collect a batch using a (non-unique) fingerprint (create, save and collect batch).

```js
var marleys = [
	users.createDocument( {
		firstName: 'Bob' ,
		lastName: 'Marley'
	} ) ,
	users.createDocument( {
		firstName: 'Julian' ,
		lastName: 'Marley'
	} ) ,
	users.createDocument( {
		firstName: 'Thomas' ,
		lastName: 'Jefferson'
	} ) ,
	users.createDocument( {
		firstName: 'Stephen' ,
		lastName: 'Marley'
	} ) ,
	users.createDocument( {
		firstName: 'Mr' ,
		lastName: 'X'
	} ) ,
	users.createDocument( {
		firstName: 'Ziggy' ,
		lastName: 'Marley'
	} ) ,
	users.createDocument( {
		firstName: 'Rita' ,
		lastName: 'Marley'
	} )
] ;

async.series( [
	function( callback ) {
		odm.bulk( 'save' , marleys , callback ) ;
	} ,
	function( callback ) {
		users.collect( { lastName: 'Marley' } , { raw: true, useMemProxy: false } , function( error , batch ) {
			var i , map = {} ;
			//console.log( 'Error:' , error ) ;
			//console.log( 'RawBatch:' , batch ) ; 
			expect( error ).not.to.be.ok() ;
			expect( batch ).to.have.length( 5 ) ;
			
			for ( i = 0 ; i < batch.length ; i ++ )
			{
				expect( batch[ i ].firstName ).to.be.ok() ;
				expect( batch[ i ].lastName ).to.equal( 'Marley' ) ;
				map[ batch[ i ].firstName ] = true ;
			}
			
			expect( map ).to.only.have.keys( 'Bob' , 'Julian' , 'Stephen' , 'Ziggy' , 'Rita' ) ;
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		users.collect( { lastName: 'Marley' } , { useMemProxy: false } , function( error , batch ) {
			var i , map = {} ;
			//console.log( 'Error:' , error ) ;
			//console.log( 'Batch:' , batch ) ; 
			expect( error ).not.to.be.ok() ;
			expect( batch ).to.be.an( odm.Batch ) ;
			expect( batch.documents ).to.have.length( 5 ) ;
			
			for ( i = 0 ; i < batch.documents.length ; i ++ )
			{
				expect( batch.documents[ i ] ).to.be.an( odm.Document ) ;
				expect( batch.documents[ i ].$.firstName ).to.ok() ;
				expect( batch.documents[ i ].$.lastName ).to.equal( 'Marley' ) ;
				map[ batch.documents[ i ].$.firstName ] = true ;
			}
			
			expect( map ).to.only.have.keys( 'Bob' , 'Julian' , 'Stephen' , 'Ziggy' , 'Rita' ) ;
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

<a name="links"></a>
# Links
should retrieve a 'suspected' document from a document's link (create both, link, save both, memProxyReset both, retrieve parent, navigate to child, reveal child).

```js
var user = users.createDocument( {
	firstName: 'Jilbert' ,
	lastName: 'Polson'
} ) ;

var id = user.$._id ;

var job = jobs.createDocument() ;
//console.log( job ) ;
var jobId = job.id ;

// Link the documents!
user.$.job = job ;
//user.$.jobId = jobId ;
expect( user.$.jobId ).to.eql( jobId ) ;

// Problème... stocker les liens dans les meta...
expect( tree.extend( null , {} , user.$.job.$ ) ).to.eql( tree.extend( null , {} , job.$ ) ) ;
expect( user.$.job.suspected ).not.to.be.ok() ;

//console.log( '>>>' , jobId ) ;

async.series( [
	function( callback ) {
		job.save( callback ) ;
	} ,
	function( callback ) {
		user.save( callback ) ;
	} ,
	function( callback ) {
		jobs.get( jobId , function( error , job ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'Job:' , job ) ;
			expect( error ).not.to.be.ok() ;
			expect( job ).to.be.an( odm.Document ) ;
			expect( job.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( job.$._id ).to.eql( jobId ) ;
			expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job.$._id , title: 'unemployed' , salary: 0 } ) ;
			
			// memProxyReset them! So we can test suspected document!
			users.memProxyReset() ;
			jobs.memProxyReset() ;
			
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		users.get( id , function( error , user ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ;
			expect( error ).not.to.be.ok() ;
			expect( user ).to.be.an( odm.Document ) ;
			expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user.$._id ).to.eql( id ) ;
			expect( user.$.jobId ).to.be.an( mongodb.ObjectID ) ;
			expect( user.$.jobId ).to.eql( jobId ) ;
			expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: user.$._id, jobId: user.$.jobId, godfatherId: undefined, firstName: 'Jilbert', lastName: 'Polson' , memberSid: 'Jilbert Polson' } ) ;
			
			//user.$.toto = 'toto' ;
			
			var job = user.$.job ;
			expect( job ).to.be.an( odm.Document ) ;
			expect( job.suspected ).to.be.ok() ;
			expect( job.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( job.$._id ).to.eql( jobId ) ;
			expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job.$._id , title: undefined , salary: undefined } ) ;
			
			job.reveal( function( error ) {
				// Not suspected anymore
				expect( job.suspected ).not.to.be.ok() ;
				expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job.$._id , title: 'unemployed' , salary: 0 } ) ;
				callback() ;
			} ) ;
		} ) ;
	}
] )
.exec( done ) ;
```

should retrieve a 'suspected' document from a 'suspected' document's link (suspected²: create both, link, save both, memProxyReset both, retrieve parent as suspect, navigate to child, reveal child).

```js
var user = users.createDocument( {
	firstName: 'Wilson' ,
	lastName: 'Andrews'
} ) ;

var id = user.$._id ;

var job = jobs.createDocument() ;
var jobId = job.id ;
job.$.title = 'mechanic' ;
job.$.salary = 2100 ;
//console.log( job ) ;

// Link the documents!
user.$.job = job ;
//user.$.jobId = jobId ;
expect( user.$.jobId ).to.eql( jobId ) ;
expect( user.$.job ).to.equal( job ) ;
expect( user.$.job.suspected ).not.to.be.ok() ;

//console.log( '>>>' , jobId ) ;

async.series( [
	function( callback ) {
		job.save( callback ) ;
	} ,
	function( callback ) {
		user.save( callback ) ;
	} ,
	function( callback ) {
		jobs.get( jobId , function( error , job ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'Job:' , job ) ;
			expect( error ).not.to.be.ok() ;
			expect( job ).to.be.an( odm.Document ) ;
			expect( job.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( job.$._id ).to.eql( jobId ) ;
			expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job.$._id , title: 'mechanic' , salary: 2100 } ) ;
			
			// memProxyReset them! So we can test suspected document!
			users.memProxyReset() ;
			jobs.memProxyReset() ;
			
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		
		// The real test begins NOW!
		
		var user = users.get( id ) ;
		expect( user ).to.be.an( odm.Document ) ;
		expect( user.suspected ).to.be.ok() ;
		expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
		expect( user.$._id ).to.eql( id ) ;
		expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: id, firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
		
		var job = user.$.job ;
		expect( job ).to.be.an( odm.Document ) ;
		expect( job.suspected ).to.be.ok() ;
		//expect( job.$._id ).to.be.an( mongodb.ObjectID ) ;
		//expect( job.$._id ).to.eql( jobId ) ;
		expect( job.witness.document ).to.equal( user ) ;
		expect( job.witness.property ).to.equal( 'jobId' ) ;
		expect( job.witness.type ).to.equal( 'link' ) ;
		expect( tree.extend( null , {} , job.$ ) ).to.eql( { title: undefined , salary: undefined } ) ;
		
		job.reveal( function( error ) {
			// Not a suspected anymore
			expect( job.suspected ).not.to.be.ok() ;
			expect( job.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( job.$._id ).to.eql( jobId ) ;
			expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job.$._id , title: 'mechanic' , salary: 2100 } ) ;
			
			// user should be revealed
			expect( user.suspected ).not.to.be.ok() ;
			expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user.$._id ).to.eql( id ) ;
			expect( user.$.jobId ).to.be.an( mongodb.ObjectID ) ;
			expect( user.$.jobId ).to.eql( jobId ) ;
			expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: user.$._id, jobId: user.$.jobId, godfatherId: undefined, firstName: 'Wilson', lastName: 'Andrews' , memberSid: 'Wilson Andrews' } ) ;
			
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

should retrieve a 'suspected' document from a 'suspected' document's link² (suspected³: create x3, link x2, save x3, memProxyReset x3, retrieve grand-parent as suspect, navigate to parent, navigate to child, reveal child).

```js
var user = users.createDocument( {
	firstName: 'Paul' ,
	lastName: 'Williams'
} ) ;

var id = user.$._id ;

var godfather = users.createDocument( {
	firstName: 'Maxwell' ,
	lastName: 'Jersey'
} ) ;

var godfatherId = godfather.$._id ;

var job = jobs.createDocument() ;
var jobId = job.id ;
job.$.title = 'plumber' ;
job.$.salary = 1900 ;

// Link the documents!
user.$.godfather = godfather ;
godfather.$.job = job ;

expect( user.$.godfatherId ).to.eql( godfatherId ) ;
expect( user.$.godfather ).to.equal( godfather ) ;
expect( user.$.godfather.suspected ).not.to.be.ok() ;

expect( godfather.$.jobId ).to.eql( jobId ) ;
expect( godfather.$.job ).to.equal( job ) ;
expect( godfather.$.job.suspected ).not.to.be.ok() ;

//console.log( '>>>' , jobId ) ;

async.series( [
	function( callback ) {
		job.save( callback ) ;
	} ,
	function( callback ) {
		godfather.save( callback ) ;
	} ,
	function( callback ) {
		user.save( callback ) ;
	} ,
	function( callback ) {
		// memProxyReset them! So we can test suspected document!
		users.memProxyReset() ;
		jobs.memProxyReset() ;
		callback() ;
	} ,
	function( callback ) {
		
		// The real test begins NOW!
		
		var user = users.get( id ) ;
		expect( user ).to.be.an( odm.Document ) ;
		expect( user.suspected ).to.be.ok() ;
		expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
		expect( user.$._id ).to.eql( id ) ;
		expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: id, firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
		
		var godfather = user.$.godfather ;
		expect( godfather ).to.be.an( odm.Document ) ;
		expect( godfather.suspected ).to.be.ok() ;
		//expect( godfather.$._id ).to.be.an( mongodb.ObjectID ) ;
		//expect( godfather.$._id ).to.eql( id ) ;
		expect( godfather.witness.document ).to.equal( user ) ;
		expect( godfather.witness.property ).to.equal( 'godfatherId' ) ;
		expect( godfather.witness.type ).to.equal( 'link' ) ;
		expect( tree.extend( null , {} , godfather.$ ) ).to.eql( { firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
		
		var job = godfather.$.job ;
		expect( job ).to.be.an( odm.Document ) ;
		expect( job.suspected ).to.be.ok() ;
		//expect( job.$._id ).to.be.an( mongodb.ObjectID ) ;
		//expect( job.$._id ).to.eql( jobId ) ;
		expect( job.witness.document ).to.equal( godfather ) ;
		expect( job.witness.property ).to.equal( 'jobId' ) ;
		expect( job.witness.type ).to.equal( 'link' ) ;
		expect( tree.extend( null , {} , job.$ ) ).to.eql( { title: undefined , salary: undefined } ) ;
		
		job.reveal( function( error ) {
			// Not a suspected anymore
			expect( job.suspected ).not.to.be.ok() ;
			expect( job.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( job.$._id ).to.eql( jobId ) ;
			expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job.$._id , title: 'plumber' , salary: 1900 } ) ;
			
			// godfather should be revealed
			expect( godfather.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( godfather.$._id ).to.eql( godfatherId ) ;
			expect( godfather.$.jobId ).to.be.an( mongodb.ObjectID ) ;
			expect( godfather.$.jobId ).to.eql( jobId ) ;
			expect( tree.extend( null , {} , godfather.$ ) ).to.eql( { _id: godfather.$._id, jobId: godfather.$.jobId, godfatherId: undefined, firstName: 'Maxwell', lastName: 'Jersey' , memberSid: 'Maxwell Jersey' } ) ;
			
			// user should be revealed
			expect( user.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user.$._id ).to.eql( id ) ;
			expect( user.$.godfatherId ).to.be.an( mongodb.ObjectID ) ;
			expect( user.$.godfatherId ).to.eql( godfatherId ) ;
			expect( tree.extend( null , {} , user.$ ) ).to.eql( { _id: user.$._id, jobId: user.$.jobId, godfatherId: godfatherId, firstName: 'Paul', lastName: 'Williams' , memberSid: 'Paul Williams' } ) ;
			
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

<a name="backlinks"></a>
# Backlinks
should retrieve a batch of 'suspected' document from a document's backlink (create, assign backlink ID, save, get parent, get backlink suspect batch containing childs, reveal batch).

```js
var job = jobs.createDocument( { title: 'bowler' } ) ;
var jobId = job.id ;

var friends = [
	users.createDocument( { firstName: 'Jeffrey' , lastName: 'Lebowski' , jobId: jobId } ) ,
	users.createDocument( { firstName: 'Walter' , lastName: 'Sobchak' , jobId: jobId } ) ,
	users.createDocument( { firstName: 'Donny' , lastName: 'Kerabatsos' , jobId: jobId } )
] ;

async.series( [
	function( callback ) {
		odm.bulk( 'save' , friends.concat( job ) , callback ) ;
	} ,
	function( callback ) {
		jobs.get( jobId , function( error , job ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'Job:' , job ) ;
			expect( error ).not.to.be.ok() ;
			expect( job ).to.be.an( odm.Document ) ;
			expect( job.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( job.$._id ).to.eql( jobId ) ;
			expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job.$._id , title: 'bowler' , salary: 0 } ) ;
			
			// memProxyReset them! So we can test suspected document!
			users.memProxyReset() ;
			jobs.memProxyReset() ;
			
			var userBatch = job.$.members ;
			expect( userBatch ).to.be.an( odm.Batch ) ;
			expect( userBatch.suspected ).to.be.ok() ;
			
			userBatch.reveal( function( error , batch ) {
				expect( error ).not.to.be.ok() ;
				expect( batch ).to.be( userBatch ) ;
				expect( userBatch.suspected ).not.to.be.ok() ;
				expect( userBatch.documents ).to.be.an( Array ) ;
				expect( userBatch.documents.length ).to.be( 3 ) ;
				
				var i , mapFirstName = {} , mapLastName = {} ;
				
				for ( i = 0 ; i < userBatch.documents.length ; i ++ )
				{
					expect( userBatch.documents[ i ].$.firstName ).to.be.ok() ;
					expect( userBatch.documents[ i ].$.lastName ).to.be.ok() ;
					mapFirstName[ userBatch.documents[ i ].$.firstName ] = true ;
					mapLastName[ userBatch.documents[ i ].$.lastName ] = true ;
				}
				
				expect( mapFirstName ).to.only.have.keys( 'Jeffrey' , 'Walter' , 'Donny' ) ;
				expect( mapLastName ).to.only.have.keys( 'Lebowski' , 'Sobchak' , 'Kerabatsos' ) ;
				
				callback() ;
			} ) ;
		} ) ;
	}
] )
.exec( done ) ;
```

should retrieve a batch of 'suspected' document from a 'suspected' document's backlink (suspect²: create, assign backlink ID, save, get parent, get backlink suspect batch containing childs, reveal batch).

```js
var job = jobs.createDocument( { title: 'bowler' } ) ;
var jobId = job.id ;

var friends = [
	users.createDocument( { firstName: 'Jeffrey' , lastName: 'Lebowski' , jobId: jobId } ) ,
	users.createDocument( { firstName: 'Walter' , lastName: 'Sobchak' , jobId: jobId } ) ,
	users.createDocument( { firstName: 'Donny' , lastName: 'Kerabatsos' , jobId: jobId } )
] ;

async.series( [
	function( callback ) {
		odm.bulk( 'save' , friends.concat( job ) , callback ) ;
	} ,
	function( callback ) {
		jobs.memProxyReset() ;
		users.memProxyReset() ;
		
		job = jobs.get( jobId ) ;
		expect( job ).to.be.an( odm.Document ) ;
		expect( job.id ).to.be.an( mongodb.ObjectID ) ;
		expect( job.id ).to.eql( jobId ) ;
		expect( job.$._id ).to.be.an( mongodb.ObjectID ) ;
		expect( job.$._id ).to.eql( jobId ) ;
		expect( job.suspected ).to.be.ok() ;
		
		var userBatch = job.$.members ;
		expect( userBatch ).to.be.an( odm.Batch ) ;
		expect( userBatch.suspected ).to.be.ok() ;
		expect( userBatch.witness ).not.to.be.ok( job ) ;
		
		userBatch.reveal( function( error , batch ) {
			expect( error ).not.to.be.ok() ;
			
			expect( job.suspected ).to.be.ok() ;	// Should not be loaded
			expect( tree.extend( null , {} , job.$ ) ).to.eql( { _id: job.$._id , title: undefined , salary: undefined } ) ;
			
			expect( batch ).to.be( userBatch ) ;
			expect( userBatch.suspected ).not.to.be.ok() ;
			expect( userBatch.documents ).to.be.an( Array ) ;
			expect( userBatch.documents.length ).to.be( 3 ) ;
			
			var i , mapFirstName = {} , mapLastName = {} ;
			
			for ( i = 0 ; i < userBatch.documents.length ; i ++ )
			{
				expect( userBatch.documents[ i ].$.firstName ).to.be.ok() ;
				expect( userBatch.documents[ i ].$.lastName ).to.be.ok() ;
				mapFirstName[ userBatch.documents[ i ].$.firstName ] = true ;
				mapLastName[ userBatch.documents[ i ].$.lastName ] = true ;
			}
			
			expect( mapFirstName ).to.only.have.keys( 'Jeffrey' , 'Walter' , 'Donny' ) ;
			expect( mapLastName ).to.only.have.keys( 'Lebowski' , 'Sobchak' , 'Kerabatsos' ) ;
			
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

should retrieve a batch of 'suspected' document from a 'suspected' document's backlink (suspect³: create, assign backlink ID, save, get parent, get backlink suspect batch containing childs, reveal batch).

```js
var job = jobs.createDocument( { title: 'bowler' } ) ;
var jobId = job.id ;

var friends = [
	users.createDocument( { firstName: 'Jeffrey' , lastName: 'Lebowski' , jobId: jobId } ) ,
	users.createDocument( { firstName: 'Walter' , lastName: 'Sobchak' , jobId: jobId } ) ,
	users.createDocument( { firstName: 'Donny' , lastName: 'Kerabatsos' , jobId: jobId } )
] ;

var dudeId = friends[ 0 ].id ;

async.series( [
	function( callback ) {
		odm.bulk( 'save' , friends.concat( job ) , callback ) ;
	} ,
	function( callback ) {
		jobs.memProxyReset() ;
		users.memProxyReset() ;
		
		var dude = users.get( dudeId ) ;
		
		expect( dude ).to.be.an( odm.Document ) ;
		expect( dude.suspected ).to.be.ok() ;
		expect( dude.$._id ).to.be.an( mongodb.ObjectID ) ;
		expect( dude.$._id ).to.eql( dudeId ) ;
		expect( tree.extend( null , {} , dude.$ ) ).to.eql( { _id: dudeId, firstName: undefined, lastName: undefined, jobId: undefined, godfatherId: undefined , memberSid: undefined } ) ;
		
		var job = dude.$.job ;
		expect( job ).to.be.an( odm.Document ) ;
		expect( job.suspected ).to.be.ok() ;
		expect( job.witness.document ).to.equal( dude ) ;
		expect( job.witness.property ).to.equal( 'jobId' ) ;
		expect( job.witness.type ).to.equal( 'link' ) ;
		expect( tree.extend( null , {} , job.$ ) ).to.eql( { title: undefined , salary: undefined } ) ;
		
		var userBatch = job.$.members ;
		expect( userBatch ).to.be.an( odm.Batch ) ;
		expect( userBatch.suspected ).to.be.ok() ;
		expect( userBatch.witness.document ).to.equal( job ) ;
		expect( userBatch.witness.property ).to.equal( 'jobId' ) ;
		expect( userBatch.witness.type ).to.equal( 'backlink' ) ;
		
		userBatch.reveal( function( error , batch ) {
			expect( error ).not.to.be.ok() ;
			
			expect( job.suspected ).to.be.ok() ;	// Should not be loaded
			expect( tree.extend( null , {} , job.$ ) ).to.eql( { title: undefined , salary: undefined } ) ;
			
			expect( batch ).to.be( userBatch ) ;
			expect( userBatch.suspected ).not.to.be.ok() ;
			expect( userBatch.documents ).to.be.an( Array ) ;
			expect( userBatch.documents.length ).to.be( 3 ) ;
			
			var i , mapFirstName = {} , mapLastName = {} ;
			
			for ( i = 0 ; i < userBatch.documents.length ; i ++ )
			{
				expect( userBatch.documents[ i ].$.firstName ).to.be.ok() ;
				expect( userBatch.documents[ i ].$.lastName ).to.be.ok() ;
				mapFirstName[ userBatch.documents[ i ].$.firstName ] = true ;
				mapLastName[ userBatch.documents[ i ].$.lastName ] = true ;
			}
			
			expect( mapFirstName ).to.only.have.keys( 'Jeffrey' , 'Walter' , 'Donny' ) ;
			expect( mapLastName ).to.only.have.keys( 'Lebowski' , 'Sobchak' , 'Kerabatsos' ) ;
			
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

<a name="embedded-documents"></a>
# Embedded documents
should be able to modify '$'s embedded data without updating 'upstream's embedded data (internally, we are using the 'deep inherit' feature of tree-kit).

```js
var town = towns.createDocument( {
	name: 'Paris' ,
	meta: {
		population: '2200K'
	}
} ) ;

var id = town.$._id ;

async.series( [
	function( callback ) {
		town.save( callback ) ;
	} ,
	function( callback ) {
		towns.get( id , { useMemProxy: false } , function( error , town ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'Town:' , string.inspect( { style: 'color' , proto: true } , town.$.meta ) ) ;
			expect( error ).not.to.be.ok() ;
			expect( town ).to.be.an( odm.Document ) ;
			expect( town.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( town.$._id ).to.eql( id ) ;
			expect( tree.extend( null , {} , town.upstream ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2200K' } } ) ;
			expect( protoflatten( town.$ ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2200K' } } ) ;
			
			town.$.meta.population = '2300K' ;
			expect( tree.extend( null , {} , town.upstream ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2200K' } } ) ;
			expect( protoflatten( town.$ ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2300K' } } ) ;
			
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

should be able to update embedded data (patch).

```js
var town = towns.createDocument( {
	name: 'Paris' ,
	meta: {
		population: '2200K',
		country: 'France'
	}
} ) ;

var id = town.$._id ;

async.series( [
	function( callback ) {
		town.save( callback ) ;
	} ,
	function( callback ) {
		towns.get( id , { useMemProxy: false } , function( error , town ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'Town:' , town ) ; 
			expect( error ).not.to.be.ok() ;
			expect( town ).to.be.an( odm.Document ) ;
			expect( town.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( town.$._id ).to.eql( id ) ;
			expect( tree.extend( null , {} , town.upstream ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2200K' , country: 'France' } } ) ;
			expect( protoflatten( town.$ ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2200K' , country: 'France' } } ) ;
			
			town.$.meta.population = '2300K' ;
			expect( tree.extend( null , {} , town.upstream ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2200K' , country: 'France' } } ) ;
			expect( protoflatten( town.$ ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2300K' , country: 'France' } } ) ;
			
			town.save( callback ) ;
		} ) ;
	} ,
	function( callback ) {
		towns.get( id , { useMemProxy: false } , function( error , town ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'Town:' , town ) ; 
			expect( error ).not.to.be.ok() ;
			expect( town ).to.be.an( odm.Document ) ;
			expect( town.$._id ).to.be.an( mongodb.ObjectID ) ;
			expect( town.$._id ).to.eql( id ) ;
			expect( tree.extend( null , {} , town.upstream ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2300K' , country: 'France' } } ) ;
			expect( protoflatten( town.$ ) ).to.eql( { _id: town.$._id , name: 'Paris' , meta: { population: '2300K' , country: 'France' } } ) ;
			
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

should collect a batch & get unique using embedded data as fingerprint (create, save and collect batch).

```js
var townList = [
	towns.createDocument( {
		name: 'Paris' ,
		meta: {
			country: 'France' ,
			capital: true
		}
	} ) ,
	towns.createDocument( {
		name: 'Tokyo' ,
		meta: {
			country: 'Japan' ,
			capital: true
		}
	} ) ,
	towns.createDocument( {
		name: 'New York' ,
		meta: {
			country: 'USA' ,
			capital: false
		}
	} ) ,
	towns.createDocument( {
		name: 'Washington' ,
		meta: {
			country: 'USA' ,
			capital: true
		}
	} ) ,
	towns.createDocument( {
		name: 'San Francisco' ,
		meta: {
			country: 'USA' ,
			capital: false
		}
	} )
] ;

async.series( [
	function( callback ) {
		odm.bulk( 'save' , townList , callback ) ;
	} ,
	function( callback ) {
		towns.collect( { "meta.country": 'USA' } , { raw: true, useMemProxy: false } , function( error , batch ) {
			var i , map = {} ;
			//console.log( 'Error:' , error ) ;
			//console.log( 'RawBatch:' , batch ) ; 
			expect( error ).not.to.be.ok() ;
			expect( batch ).to.have.length( 3 ) ;
			
			for ( i = 0 ; i < batch.length ; i ++ )
			{
				expect( batch[ i ].name ).to.be.ok() ;
				expect( batch[ i ].meta.country ).to.equal( 'USA' ) ;
				map[ batch[ i ].name ] = true ;
			}
			
			expect( map ).to.only.have.keys( 'New York' , 'Washington' , 'San Francisco' ) ;
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		towns.collect( { "meta.country": 'USA' } , { useMemProxy: false } , function( error , batch ) {
			var i , map = {} ;
			//console.log( 'Error:' , error ) ;
			//console.log( 'Batch:' , batch ) ; 
			expect( error ).not.to.be.ok() ;
			expect( batch ).to.be.an( odm.Batch ) ;
			expect( batch.documents ).to.have.length( 3 ) ;
			
			for ( i = 0 ; i < batch.documents.length ; i ++ )
			{
				expect( batch.documents[ i ] ).to.be.an( odm.Document ) ;
				expect( batch.documents[ i ].$.name ).to.ok() ;
				expect( batch.documents[ i ].$.meta.country ).to.equal( 'USA' ) ;
				map[ batch.documents[ i ].$.name ] = true ;
			}
			
			expect( map ).to.only.have.keys( 'New York' , 'Washington' , 'San Francisco' ) ;
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		towns.collect( { "meta.country": 'USA' , "meta.capital": false } , { useMemProxy: false } , function( error , batch ) {
			var i , map = {} ;
			//console.log( 'Error:' , error ) ;
			//console.log( 'Batch:' , batch ) ; 
			expect( error ).not.to.be.ok() ;
			expect( batch ).to.be.an( odm.Batch ) ;
			expect( batch.documents ).to.have.length( 2 ) ;
			
			for ( i = 0 ; i < batch.documents.length ; i ++ )
			{
				expect( batch.documents[ i ] ).to.be.an( odm.Document ) ;
				expect( batch.documents[ i ].$.name ).to.ok() ;
				expect( batch.documents[ i ].$.meta.country ).to.equal( 'USA' ) ;
				map[ batch.documents[ i ].$.name ] = true ;
			}
			
			expect( map ).to.only.have.keys( 'New York' , 'San Francisco' ) ;
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		towns.getUnique( { name: 'Tokyo', "meta.country": 'Japan' } , { useMemProxy: false } , function( error , town ) {
			var i , map = {} ;
			//console.log( 'Error:' , error ) ;
			//console.log( 'Town:' , town ) ; 
			expect( error ).not.to.be.ok() ;
			expect( town ).to.be.an( odm.Document ) ;
			expect( protoflatten( town.$ ) ).to.eql( {
				_id: town.$._id ,
				name: 'Tokyo' ,
				meta: {
					country: 'Japan' ,
					capital: true
				}
			} ) ;
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

