# TOC
   - [Build collections' indexes](#build-collections-indexes)
   - [ID creation](#id-creation)
   - [Document creation](#document-creation)
   - [Get documents](#get-documents)
   - [Save documents](#save-documents)
   - [Patch, stage and commit documents](#patch-stage-and-commit-documents)
   - [Delete documents](#delete-documents)
   - [Fingerprint](#fingerprint)
   - [Get documents by unique fingerprint](#get-documents-by-unique-fingerprint)
   - [Collect & find batchs](#collect--find-batchs)
   - [Embedded documents](#embedded-documents)
   - [Hooks](#hooks)
<a name=""></a>
 
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

<a name="document-creation"></a>
# Document creation
should create a document with default values.

```js
var user = users.createDocument() ;

expect( user.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
expect( user._id ).to.be.an( mongodb.ObjectID ) ;
expect( user ).to.eql( tree.extend( null , { _id: user._id } , expectedDefaultUser ) ) ;
```

should create a document using the given correct values.

```js
var user = users.createDocument( {
	firstName: 'Bobby',
	lastName: 'Fischer'
} ) ;

expect( user.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
expect( user._id ).to.be.an( mongodb.ObjectID ) ;
expect( user ).to.eql( {
	_id: user._id ,
	firstName: 'Bobby' ,
	lastName: 'Fischer' ,
	memberSid: 'Bobby Fischer'
} ) ;
```

should throw when trying to create a document that does not validate the schema.

```js
var user ;

doormen.shouldThrow( function() {
	user = users.createDocument( {
		firstName: true,
		lastName: 3
	} ) ;
} ) ;

doormen.shouldThrow( function() {
	user = users.createDocument( {
		firstName: 'Bobby',
		lastName: 'Fischer',
		extra: 'property'
	} ) ;
} ) ;
```

<a name="get-documents"></a>
# Get documents
should get a document (create, save and retrieve).

```js
var user = users.createDocument( {
	firstName: 'John' ,
	lastName: 'McGregor'
} ) ;

var id = user._id ;

async.series( [
	function( callback ) {
		user.$.save( callback ) ;
	} ,
	function( callback ) {
		users.get( id , function( error , user ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ; 
			expect( error ).not.to.be.ok() ;
			expect( user.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
			expect( user._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user._id ).to.eql( id ) ;
			expect( user ).to.eql( { _id: user._id , firstName: 'John' , lastName: 'McGregor' , memberSid: 'John McGregor' } ) ;
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		users.get( id , { raw: true } , function( error , user ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ;
			expect( error ).not.to.be.ok() ;
			expect( user.$ ).not.to.be.an( rootsDb.DocumentWrapper ) ;
			expect( user._id ).to.be.an( mongodb.ObjectID ) ;
			expect( user._id ).to.eql( id ) ;
			expect( user ).to.eql( { _id: user._id , firstName: 'John' , lastName: 'McGregor' , memberSid: 'John McGregor' } ) ;
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

<a name="save-documents"></a>
# Save documents
should save correctly and only non-default value are registered into the upstream (create, save and retrieve).

```js
var user = users.createDocument( {
	firstName: 'Jack'
} ) ;

var id = user._id ;

async.series( [
	function( callback ) {
		user.$.save( callback ) ;
	} ,
	function( callback ) {
		users.get( id , function( error , user ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ; 
			expect( error ).not.to.be.ok() ;
			expect( user ).to.eql( { _id: user._id , firstName: 'Jack' , lastName: 'Doe' , memberSid: 'Jack Doe' } ) ;
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

var id = user._id ;
var user2 ;


async.series( [
	function( callback ) {
		user.$.save( callback ) ;
	} ,
	function( callback ) {
		users.get( id , function( error , u ) {
			user2 = u ;
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user2 ) ; 
			expect( error ).not.to.be.ok() ;
			expect( user2._id ).to.eql( id ) ;
			expect( user2 ).to.eql( { _id: user2._id , firstName: 'Johnny B.' , lastName: 'Starks' , memberSid: 'Johnny B. Starks' } ) ;
			callback() ;
		} ) ;
	} ,
	async.parallel( [
		function( callback ) {
			user.lastName = 'Smith' ;
			user.$.save( callback ) ;
		} ,
		function( callback ) {
			user2.firstName = 'Joey' ;
			user2.$.save( callback ) ;
		}
	] ) ,
	function( callback ) {
		users.get( id , function( error , u ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ; 
			expect( error ).not.to.be.ok() ;
			expect( u._id ).to.eql( id ) ;
			expect( u ).to.eql( { _id: u._id , firstName: 'Joey' , lastName: 'Starks' , memberSid: 'Johnny B. Starks' } ) ;
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

<a name="patch-stage-and-commit-documents"></a>
# Patch, stage and commit documents
'commit' should save staged data and do nothing on data not staged.

```js
var user = users.createDocument( {
	firstName: 'Johnny' ,
	lastName: 'Starks'
} ) ;

var id = user._id ;
var user2 ;
//id = users.createDocument()._id ;


async.series( [
	function( callback ) {
		user.$.save( callback ) ;
	} ,
	function( callback ) {
		users.get( id , function( error , u ) {
			user2 = u ;
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user2 ) ; 
			expect( error ).not.to.be.ok() ;
			expect( user2._id ).to.eql( id ) ;
			expect( user2 ).to.eql( { _id: user._id , firstName: 'Johnny' , lastName: 'Starks' , memberSid: 'Johnny Starks' } ) ;
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		user2.firstName = 'Joey' ;
		user2.lastName = 'Smith' ;
		user2.$.stage( 'lastName' ) ;
		expect( user2 ).to.eql( { _id: user2._id , firstName: 'Joey' , lastName: 'Smith' , memberSid: 'Johnny Starks' } ) ;
		user2.$.commit( callback ) ;
	} ,
	function( callback ) {
		users.get( id , function( error , u ) {
			expect( error ).not.to.be.ok() ;
			expect( u._id ).to.eql( id ) ;
			expect( u ).to.eql( { _id: u._id , firstName: 'Johnny' , lastName: 'Smith' , memberSid: 'Johnny Starks' } ) ;
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

'commit' should save data staged using .patch() and do nothing on data modified by .patch().

```js
var user = users.createDocument( {
	firstName: 'Johnny' ,
	lastName: 'Starks'
} ) ;

var id = user._id ;
var user2 ;
//id = users.createDocument()._id ;


async.series( [
	function( callback ) {
		user.$.save( callback ) ;
	} ,
	function( callback ) {
		users.get( id , function( error , u ) {
			user2 = u ;
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user2 ) ; 
			expect( error ).not.to.be.ok() ;
			expect( user2._id ).to.eql( id ) ;
			expect( user2 ).to.eql( { _id: user._id , firstName: 'Johnny' , lastName: 'Starks' , memberSid: 'Johnny Starks' } ) ;
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		user2.firstName = 'Joey' ;
		user2.$.patch( { lastName: 'Smith' } ) ;
		expect( user2 ).to.eql( { _id: user2._id , firstName: 'Joey' , lastName: 'Smith' , memberSid: 'Johnny Starks' } ) ;
		user2.$.commit( callback ) ;
	} ,
	function( callback ) {
		users.get( id , function( error , u ) {
			expect( error ).not.to.be.ok() ;
			expect( u._id ).to.eql( id ) ;
			expect( u ).to.eql( { _id: u._id , firstName: 'Johnny' , lastName: 'Smith' , memberSid: 'Johnny Starks' } ) ;
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

should save creating a minimalistic patch so parallel save do not overwrite each others (create, save, retrieve, patch², commit² and retrieve).

```js
var user = users.createDocument( {
	firstName: 'Johnny' ,
	lastName: 'Starks'
} ) ;

var id = user._id ;
var user2 ;
//id = users.createDocument()._id ;


async.series( [
	function( callback ) {
		user.$.save( callback ) ;
	} ,
	function( callback ) {
		users.get( id , function( error , u ) {
			user2 = u ;
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user2 ) ; 
			expect( error ).not.to.be.ok() ;
			expect( user2._id ).to.eql( id ) ;
			expect( user2 ).to.eql( { _id: user._id , firstName: 'Johnny' , lastName: 'Starks' , memberSid: 'Johnny Starks' } ) ;
			callback() ;
		} ) ;
	} ,
	async.parallel( [
		function( callback ) {
			user.$.patch( { lastName: 'Smith' } ) ;
			expect( user.lastName ).to.be( 'Smith' ) ;
			user.$.commit( callback ) ;
		} ,
		function( callback ) {
			user2.$.patch( { firstName: 'Joey' } ) ;
			expect( user2.firstName ).to.be( 'Joey' ) ;
			user2.$.commit( callback ) ;
		}
	] ) ,
	function( callback ) {
		users.get( id , function( error , u ) {
			expect( error ).not.to.be.ok() ;
			expect( u._id ).to.eql( id ) ;
			expect( u ).to.eql( { _id: u._id , firstName: 'Joey' , lastName: 'Smith' , memberSid: 'Johnny Starks' } ) ;
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
var user = users.createDocument( {
	firstName: 'John' ,
	lastName: 'McGregor'
} ) ;

//console.log( user ) ;
var id = user._id ;

async.series( [
	function( callback ) {
		user.$.save( callback ) ;
	} ,
	function( callback ) {
		users.get( id , function( error , u ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ; 
			expect( error ).not.to.be.ok() ;
			expect( u._id ).to.eql( id ) ;
			expect( u ).to.eql( { _id: user._id , firstName: 'John' , lastName: 'McGregor' , memberSid: "John McGregor" } ) ;
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		user.$.delete( function( error ) {
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

<a name="fingerprint"></a>
# Fingerprint
should create a fingerprint.

```js
var f = users.createFingerprint( { firstName: 'Terry' } ) ;

expect( f.$ ).to.be.an( rootsDb.FingerprintWrapper ) ;
expect( f ).to.eql( { firstName: 'Terry' } ) ;
```

should detect uniqueness correctly.

```js
expect( users.createFingerprint( { _id: 'somehash' } ).$.unique ).to.be( true ) ;
expect( users.createFingerprint( { firstName: 'Terry' } ).$.unique ).to.be( false ) ;
expect( users.createFingerprint( { firstName: 'Terry', lastName: 'Bogard' } ).$.unique ).to.be( false ) ;
expect( users.createFingerprint( { _id: 'somehash', firstName: 'Terry', lastName: 'Bogard' } ).$.unique ).to.be( true ) ;
expect( users.createFingerprint( { jobId: 'somehash' } ).$.unique ).to.be( false ) ;
expect( users.createFingerprint( { memberSid: 'terry-bogard' } ).$.unique ).to.be( false ) ;
expect( users.createFingerprint( { jobId: 'somehash', memberSid: 'terry-bogard' } ).$.unique ).to.be( true ) ;
```

<a name="get-documents-by-unique-fingerprint"></a>
# Get documents by unique fingerprint
should get a document (create, save and retrieve).

```js
var user = users.createDocument( {
	firstName: 'Bill' ,
	lastName: "Cut'throat"
} ) ;

var id = user._id ;
var memberSid = user.memberSid ;

var job = jobs.createDocument() ;
user.jobId = job._id ;

async.series( [
	function( callback ) {
		user.$.save( callback ) ;
	} ,
	function( callback ) {
		job.$.save( callback ) ;
	} ,
	function( callback ) {
		users.getUnique( { memberSid: memberSid , jobId: job._id } , function( error , u ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'User:' , user ) ;
			expect( error ).not.to.be.ok() ;
			expect( u.$ ).to.be.a( rootsDb.DocumentWrapper ) ;
			expect( u._id ).to.be.an( mongodb.ObjectID ) ;
			expect( u._id ).to.eql( id ) ;
			expect( u ).to.eql( tree.extend( null , { _id: user._id , jobId: job._id , firstName: 'Bill' , lastName: "Cut'throat" , memberSid: "Bill Cut'throat" } ) ) ;
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
} ) ;

async.series( [
	function( callback ) {
		user.$.save( callback ) ;
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

<a name="collect--find-batchs"></a>
# Collect & find batchs
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
		rootsDb.bulk( 'save' , marleys , callback ) ;
	} ,
	function( callback ) {
		users.collect( { lastName: 'Marley' } , function( error , batch ) {
			var i , map = {} ;
			//console.log( 'Error:' , error ) ;
			//console.log( 'Batch:' , batch ) ; 
			expect( error ).not.to.be.ok() ;
			expect( batch.$ ).to.be.a( rootsDb.BatchWrapper ) ;
			expect( batch ).to.have.length( 5 ) ;
			
			for ( i = 0 ; i < batch.length ; i ++ )
			{
				//expect( batch[ i ] ).to.be.an( rootsDb.DocumentWrapper ) ;
				expect( batch[ i ].firstName ).to.be.ok() ;
				expect( batch[ i ].lastName ).to.equal( 'Marley' ) ;
				map[ batch[ i ].firstName ] = true ;
			}
			
			expect( map ).to.only.have.keys( 'Bob' , 'Julian' , 'Stephen' , 'Ziggy' , 'Rita' ) ;
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

should find documents (in a batch) using a queryObject (create, save and find).

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
		rootsDb.bulk( 'save' , marleys , callback ) ;
	} ,
	function( callback ) {
		users.find( { firstName: { $regex: /^[thomasstepn]+$/ , $options: 'i' } } , function( error , batch ) {
			var i , map = {} ;
			//console.log( 'Error:' , error ) ;
			//console.log( 'Batch:' , batch ) ; 
			expect( error ).not.to.be.ok() ;
			expect( batch.$ ).to.be.a( rootsDb.BatchWrapper ) ;
			expect( batch ).to.have.length( 2 ) ;
			
			for ( i = 0 ; i < batch.length ; i ++ )
			{
				//expect( batch[ i ] ).to.be.an( rootsDb.DocumentWrapper ) ;
				expect( batch[ i ].firstName ).to.be.ok() ;
				map[ batch[ i ].firstName ] = true ;
			}
			
			expect( map ).to.only.have.keys( 'Thomas' , 'Stephen' ) ;
			callback() ;
		} ) ;
	}
] )
.exec( done ) ;
```

<a name="embedded-documents"></a>
# Embedded documents
should save and retrieve embedded data.

```js
var town = towns.createDocument( {
	name: 'Paris' ,
	meta: {
		population: '2200K' ,
		country: 'France'
	}
} ) ;

async.series( [
	function( callback ) {
		town.$.save( callback ) ;
	} ,
	function( callback ) {
		towns.get( town._id , function( error , t ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'Town:' , string.inspect( { style: 'color' , proto: true } , town.$.meta ) ) ;
			expect( error ).not.to.be.ok() ;
			expect( t.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
			expect( t._id ).to.be.an( mongodb.ObjectID ) ;
			expect( t ).to.eql( { _id: town._id , name: 'Paris' , meta: { population: '2200K' , country: 'France' } } ) ;
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

async.series( [
	function( callback ) {
		town.$.save( callback ) ;
	} ,
	function( callback ) {
		towns.get( town._id , function( error , t ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'Town:' , town ) ; 
			expect( error ).not.to.be.ok() ;
			expect( t ).to.eql( { _id: town._id , name: 'Paris' , meta: { population: '2200K' , country: 'France' } } ) ;
			expect( t.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
			
			t.$.patch( { "meta.population": "2300K" } ) ;
			t.$.commit( callback ) ;
		} ) ;
	} ,
	function( callback ) {
		towns.get( town._id , function( error , t ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'Town:' , town ) ; 
			expect( error ).not.to.be.ok() ;
			expect( t.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
			expect( t._id ).to.be.an( mongodb.ObjectID ) ;
			expect( t ).to.eql( { _id: town._id , name: 'Paris' , meta: { population: '2300K' , country: 'France' } } ) ;
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
		rootsDb.bulk( 'save' , townList , callback ) ;
	} ,
	function( callback ) {
		towns.collect( { "meta.country": 'USA' } , function( error , batch ) {
			var i , map = {} ;
			//console.log( 'Error:' , error ) ;
			//console.log( 'RawBatch:' , batch ) ; 
			expect( error ).not.to.be.ok() ;
			expect( batch.$ ).to.be.an( rootsDb.BatchWrapper ) ;
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
		towns.collect( { "meta.country": 'USA' , "meta.capital": false } , function( error , batch ) {
			var i , map = {} ;
			//console.log( 'Error:' , error ) ;
			//console.log( 'Batch:' , batch ) ; 
			expect( error ).not.to.be.ok() ;
			expect( batch.$ ).to.be.an( rootsDb.BatchWrapper ) ;
			expect( batch ).to.have.length( 2 ) ;
			
			for ( i = 0 ; i < batch.length ; i ++ )
			{
				expect( batch[ i ].name ).to.ok() ;
				expect( batch[ i ].meta.country ).to.equal( 'USA' ) ;
				map[ batch[ i ].name ] = true ;
			}
			
			expect( map ).to.only.have.keys( 'New York' , 'San Francisco' ) ;
			callback() ;
		} ) ;
	} ,
	function( callback ) {
		towns.getUnique( { name: 'Tokyo', "meta.country": 'Japan' } , function( error , town ) {
			//console.log( 'Error:' , error ) ;
			//console.log( 'Town:' , town ) ; 
			expect( error ).not.to.be.ok() ;
			expect( town.$ ).to.be.an( rootsDb.DocumentWrapper ) ;
			expect( town ).to.eql( {
				_id: town._id ,
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

<a name="hooks"></a>
# Hooks
