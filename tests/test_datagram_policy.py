from agentfork.criu_backend import external_datagrams


def socket(remote, port, inode="owned"):
    return {"remote": remote, "remote_port": port, "inode": inode}


def test_connected_external_udp_is_ineligible():
    value = socket("192.0.2.1", 53)
    assert external_datagrams([value], {"owned"}, ["10.0.0.2"]) == [value]


def test_local_listening_and_unowned_udp_are_not_external_peers():
    sockets = [
        socket("127.0.0.1", 5000),
        socket("::1", 5000),
        socket("0.0.0.0", 0),
        socket("10.0.0.2", 5000),
        socket("192.0.2.1", 53, "unowned"),
    ]
    assert not external_datagrams(sockets, {"owned"}, ["10.0.0.2"])
