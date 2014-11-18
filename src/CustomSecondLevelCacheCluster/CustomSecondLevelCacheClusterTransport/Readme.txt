This project contains 3 example implementations for the Telerik.OpenAccess.Cluster.OpenAccessClusterTransport interface.
========================================================================================================================

The examples are _not_ production ready yet - feel free to create pull requests!

Examples:
(1) Simple TCP based implementation. Main disadvantage: You need to run the accompanying TcpCacheClusterServer, which creates a 
    single point of failure.
(2) UDP multicast based implementation. Main disadvantage: Messages can be lost, and this will not be detected.
(3) RDM socket with PGM implementation. Main disadvantage: You will need to install MSMQ before this works.


All three share a common base class, SecondLevelCacheClusterTransportBase. This abstract base class is providing
basic services, and also has some properties, which can be set from the 
Telerik.OpenAccess.BackendConfiguration.SecondLevelCache.Synchronization instance. The need to match the names and types
as given there.


Points worth pointing out
=========================

Init() will be called exactly one time when the database is opened. Throw exceptions there to prevent the opening.

Close() will be called exactly one time when the database is closing. Always use context.DisposeDatabase() when you are
done with the database instance, otherwise the database will not be closed for a long time ... and your communication endpoints
will be held open.

Send() can be called in parallel. Lock against parallel usage if needed.

The MessageSize property must return the maximum number of bytes that the OpenAccess runtime should use for a message. 
The exact value will depend the transport mechanism choosen. If the actual number of bytes needed by the runtime is bigger
than the one given by the MessageSize, the runtime will instead send a EvictAll message.

In the example implementation, there is a protocol overhead of 20 bytes built in. The first 16 bytes are for a simple detection
of the sender, and created from a random Guid value. The next 4 bytes house a 3 byte length value, so never go with a MessageSize
value greater than 2^24 = 16MB. In addition, a OpCode is passed additionally. The usage of the OpCode is not defined yet, but you
might want to take it for monitoring purposes in your implementation.

When using multicasting/broadcasting, the sender might actually get its own message back. You can filter this message out - 
there is no need for a local eviction (which was already done). The detection whether the message was sent by the local or another
participant in the cache cluster is implemented with the Guid value that is send always as the first 16 bytes.

The OpenAccessClusterMsgHandler instance that is passed into the Init() call needs to be used to delegate the received 
eviction messages back to the local cache instance. The implementation instance is multi-thread safe.


Extension options
=================

Either you like one of the provided example implementations and adapt it for your needs, or you use other mechanisms like
Redis, Emcaster, JGroups or your own farm synchronization mechanisms. We would be happy to accept pull requests too!
