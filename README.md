# screwdb

An absolutely miniscule key-value store for Go based on an append-only B+tree design (ala. ldapd/CouchDB). The name
screwdb is a play on the name [boltdb](https://github.com/boltdb/bolt) as we share some common ancestry.

Every additional line of code and feature will be extensively scrutinized.

## TODOs

* Delete everything not absolutely necessary.
* Port Martin Hedenfalk's btree implementation to native Go.

## Credits

* [ldapd](https://www.bzero.se/ldapd/) by Martin Hedenfalk.