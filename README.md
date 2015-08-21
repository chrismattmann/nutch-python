nutch-python
===========
A Python client library for the [Apache Nutch](http://nutch.apache.org/)
that makes Nutch 1.x capabilities available using the
[Nutch REST Server](https://wiki.apache.org/nutch/Nutch_1.X_RESTAPI).

See (https://wiki.apache.org/nutch/NutchTutorial) for installing
Nutch 1.x and alternatively operating it via the command line.

This Python client library for Nutch is installable via Setuptools,
Pip and Easy Install.

Installation (with pip)
-----------------------
1. `pip install nutch`

Installation (without pip)
--------------------------
1. `python setup.py build`  
2. `python setup.py install`  


New Command Line Tool
============================
When you install Nutch-Python you also get a new command
line client tool, `nutch-python` installed in your /path/to/python/bin
directory.

The options and help for the command line tool can be seen by typing
`nutch-python` without any arguments.

Questions, comments?
===================
Send them to [Brian D. Wilson](mailto:brian.d.wilson@jpl.nasa.gov).

Contributors
============
* Brian D. Wilson, JPL
* Chris A. Mattmann, JPL

License
=======
[Apache License, version 2](http://www.apache.org/licenses/LICENSE-2.0)
