This project contains an example TCP based cache eviction broker server.
========================================================================

The examples are _not_ production ready yet - feel free to create pull requests!

This example provides you with a TCP server that broadcasts all messages to all clients (except the sender).
Because it uses plain TCP, you should be able to deploy it in nearly every environment. 

The command line takes an optional parameter which is the port number, per default 9999. If you pass another
value there, you must update the MulticastAddress property of the Synchronization settings instance in the
DataAccess context accordingly.
