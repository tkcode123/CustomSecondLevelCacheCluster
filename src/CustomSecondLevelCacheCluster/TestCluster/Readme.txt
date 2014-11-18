This project contains an example DataAccess application that uses a custom second level cache transport.
========================================================================================================

The examples are _not_ production ready yet - feel free to create pull requests!

The main point of this example is to show that the eviction is working. You need to start a minimum of two instances,
at least one one of those must be writer (that then creates the eviction messages). Just use an additional command line
parameter (any value) to start a writer.

The effects of the cache cluster can be seen when you look at the generated SQL. In the reader instances, normally you 
do not get to the database to fetch the instance values (as they are coming from the second level cache). Only when the
writer stored an update, there will be SQL requests from the readers. The update process also evicted the data from the
second level caches of the readers, therefore more SQL will be required to fetch the current content.

There are 4 cache transport mechanisms that are available here:
(a) The built-in via MSMQ. To use this mechanism, just leave the Name property of the Sychronization setting empty.
(b) TCP based. You need to start an additional instance from the TcpCacheClusterCache project.
(c) UDP based. Your network will need to provide multicast capabilities. Attention! UDP messages can be lost and there
    is no code to even detect yet handle this situation!
(d) RPM (Reliable Pragmatic Multicast) based. Not all networks will allow to use it. On Windows, the RPM protocol comes
    in the same package as the MSMQ implementation.



