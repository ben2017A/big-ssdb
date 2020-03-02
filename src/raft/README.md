# Raft

## Features

* Leader election
* Membership changes
* Log replication
* Built-in log management
	* Log persistency
* Built-in RPC support
* Pluggable Log management interface for log managments
* Pluggable RPC interface for RPC implements
* Log snapshot

TODO: leader lease

### NOs

Does not implement something that is strongly considered as not part of the Raft protocol.

* Log compaction
