#!/opt/iojs/bin/iojs --harmony_proxies

//var Reflect = require( 'harmony-reflect' ) ;
require( 'harmony-reflect' ) ;


var o = function() {} ;
o.save = "me!" ;

var p = new Proxy( o , {
	get: function( target , property , proxy ) {
		console.log( 'access: ' + property ) ;
		return property.toUpperCase() ;
	} ,
	apply: function( target , thisArg , args ) {
		console.log( 'applied!' ) ;
	} ,
} ) ;

console.log( o ) ;
console.log( p.bob + p.jack ) ;
p() ;



