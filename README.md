CustomSecondLevelCacheCluster
=============================

A playground for Telerik DataAccess L2 cache cluster implementations

The playground contains a small console application that shows the effects of second level cache clustering. Three example implementations for cache cluster transport are provided. The protocols used are based on TCP, UDP and RPM.

The playground code is __not__ officially supported by Telerik, but can serve your own experiments.

The main entry point is in the TestCluster application, that just modifies some data to trigger the sending of eviction messages into the DataAccess L2 cache cluster.

The main point of interest is in the classes implementing the OpenAccessClusterTransport interface.

Please see the sub folder Readme.txt files for more information.
