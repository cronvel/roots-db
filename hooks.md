


## Hooks

Hooks are powerful tools, it allows you to define user function to automatically execute when some conditions are met.

Hooks always have a prefix:

* before: this hook is executed before the action, most of time it is useful for filtering
* after: this hook is executed after the completion of an action



### beforeCreateDocument( rawDocument ) return rawDocument

* Not triggered when creating default documents (i.e. calling .createDocument() without argument).



### afterCreateDocument( document )


