/*
	Roots DB

	Copyright (c) 2014 - 2017 Cédric Ronvel

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



/*
	FUTUR FEATURE!

	If a property has the 'cachedLink' option, then it contains the whole target document.
	It is first populated the first time a .getLink() is called upon it, after that, .getLink() will always return the local copy,
	except if a 'force' option is passed, in this case a new request is performed and populate the cache with fresh data.

	The target document may have a 'populateBackLink' option.
	In that case, any modifications to it will remove or update any cachedLink.

	The target document has a 'lastModified' timestamp, thus the cachedLink has it too.

	It is possible to use this timestamp as the update mechanism, in that case any .getLink() attempt will check that timestamp,
	and if it's too old, will update the data.
*/
