# TODO

* Service snapshot
	* node.make_snapshot
	* node.install_snapshot

* group.add_node(type=MEMBER, id, addr)
	* Install raft snapshot
	* Install service snapshot

* group.del_node(id)
	* Delete raft state
	* Do not delete service data

* container.split_group
