Programming task, SKJ – winter semester 2022/23
Subject: A distributed database
Author: Serhii Ivanov (s26757@pjwstk.edu.pl)
Note: The programming task description (requirements) is described in the `Programming project SKJ 2022.pdf` file.

Description:

  The protocol message format for communication between the database client and database server is described in the original task description, providing a short description here for reference purposes.

  Text-based (single line) message in format `<operation> [<parameter>]` terminated by newline character, where <operation> is one of the known operations stated below, a mandatory value, and <parameter> is an optional value that depends on <operation> value.

  List of mandatory <operation>:
    - set-value <key>:<value> # sets [IntegerAsString]<value> for an existing [IntegerAsString]<key> recursively (any valid)
      returns:
        - OK
        - ERROR
    - get-value <key> # gets [IntegerAsString]<value> for an existing [IntegerAsString]<key> recursively (any valid)
      returns:
        - <key>:<value> # [IntegerAsString]:[IntegerAsString]
        - ERROR
    - find-key <key> # finds node that has [IntegerAsString]<key> (single node, any valid)
      returns:
        - <address>:<port>
        - ERROR
    - get-max # finds maximum [IntegerAsString]<value> in database (single value, any valid)
      returns:
        - <key>:<value> # [IntegerAsString]:[IntegerAsString]
        - ERROR
    - get-min # finds minimum [IntegerAsString]<value> in database (single value, any valid)
      returns:
        - <key>:<value> # [IntegerAsString]:[IntegerAsString]
        - ERROR
    - new-record <key>:<value> # creates new [IntegerAsString]<value> for [IntegerAsString]<key> inside the node to which the client is connected (not recursive)
      returns:
        - OK
        - ERROR # original specification does not explicitly state that ERROR must be returned here, still, it's best to keep signatures similar, like in `set-value`
    - terminate # signals neighbors that this node is going to be shut down (not recursive)

    Note: internally <key> and <value> that are represented as strings in message protocol are stored as Integers in thread-safe Key-Value datatype.

  Due to the restrictions imposed by task requirements, unknown network topology, possible binding port conflicts, and restrictions on
  command-line parameters modification, node-to-node communication reuses the same TCP address:port as client-to-node communication.
  That way it is ensured that each node, when started, would be operational and could connect to its neighbors.

  Original text-based (single line) message in format is augmented with a single optional parameter that is required for node-to-node
  and is optional for client-to-node communication, effectively original format is ensured to work while the node-to-node message format is
  kept as close as possible and intercompatible with the original format.

  Client-To-Node message format:
    `<operation> [<parameter>]`
    Example:
      set-value 3:1

  Node-To-Node message format:
    `[[UUIDv4]] <operation> [<parameter>]`
    Where
      `[[UUIDv4]]` is an optional parameter,
      value is UUIDv4 (Random UUID) represented as String
      and wrapped in square brackets `[` `]`.
    Example:
      [b076e7fd-d112-4ea2-a424-311ccd7e2c72] set-value 3:1

  For proper Node-To-Node communication `[UUIDv4]` parameter is mandatory, it is added internally to the original message from the client, and by adding this parameter message is transformed into a Node-To-Node message format.

  Internally `[UUIDv4]` value is used as a tracking id for all requests/operations that require recursive queries (find-key, get-max, get-min, get-value, set-value). By using this kind of recursive requests tracking database gets protected against recursive requests storm, when the same request gets resend over and over again by node-to-node communication effectively making this communication a DoS attack on the distributed database itself.

  The value of `[UUIDv4]` is stored in the internal state of nodes that process incoming Node-to-Node requests and also in the internal state of the node that produced this request in the first place. This value is stored in thread-safe datatype and it has a TTL value. When TTL expires, the value is opportunistically removed from the state (uses synchronized LinkedHashMap with custom `removeEldestEntry` method for TTL-based invalidation). This approach, RandomUUID + opportunistic TTL invalidation allows avoiding implementing additional network-based invalidation operations, as all UUIDs will eventually expire themselves on every new insert and every new insert of new UUID can be considered as unique, as UUIDv4 collision is highly improbable in our use-case.

  Default `[UUIDv4]` TTL is 300 seconds.

  Additional operations where implemented:
    - ping # is used to check node liveness
      returns:
        - OK
    - register <port-number> # is used to register the node as a neighbor, <port-number> is the TCP port of the server that requests to be registered
      returns:
        - OK
        - ERROR
    - unregister <port-number> # is used to unregister the node as a neighbor, <port-number> is the TCP port of the server that requests to be unregistered
      returns:
        - OK
        - ERROR
    - list-remotes # convinience operation for listing active node neighbors
      returns:
        - localhost/127.0.0.1:9000 localhost/127.0.0.1:9001 # space separated list of active node neighbors
        - EMPTY # no active node neighbors

  Every recursive query operation response (find-key, get-max, get-min, get-value, set-value) got extended with an additional response type of `SEEN` that compliments the original response types of each operation. This `SEEN` response only makes sense in Node-To-Node communication, the node that sees this response stops operations processing, which means this node will not send any more requests to neighbors, this behavior as previously mentioned stops recursive request storms.

  Both Node-To-Node and Client-To-Node messages are parsed by the same code in/by Operation class.

  Node liveliness checks:

    Before the node gets added to the cluster it gets checked for liveliness by `ping` operation, only when all remote nodes are operational, during the startup of the new node, the new node binds to the TCP socket and starts to handle requests. This behavior is needed to protect against possible network timing issues during node initialization.

    Each node is also constantly checking the health status of its neighbors, by pinging them, this is a very basic Heartbeat. When the remote node stops answering pings it is (temporarily) marked as none-operational until the next heartbeat (ping) marks this node as operational. Any non-operational remote node does not get requests from the current node until the node is operational again, by leveraging this basic async Heartbeat system somewhat avoids Node-To-Node timeouts for recursive requests.

  Note: there is a possibility to supply multiple `-record` values to seed new node with data.

Changes to originally supplied code and tests:

  There was a need to change the DatabaseClient code a bit to help with the readability of tests that are located in `e2e` folder. Changes are mostly to the output format, they are optional and disabled by default, also some IDE linting where applied. `DatabaseClient.java.org` file contains unmodified code for comparison.

  There are also some tests in `e2e` folder that needed more love, the original code is in `*.org` files. The most interesting changes are located in `script-7-2.sh` and `script-7-p.sh`.

How to build and run:
  To install: just CWD into this folder, no external Java dependencies are used.
  To build: run `build.sh` in CWD of the project.
    Compiled Java classes will be built in CWD near their respective Java files.
  To run: run `run.sh` wrapper in CWD or preferably call `java DatabaseNode ...` / `java DatabaseClient ...` directly.

Additional notes:
  - Code was developed on the UNIX system, not tested in other systems but should work with some caveats probably due to some differences in line endings.
  - Although the code is trying to be IP stack independent (IPv4 or IPv6), due to posed format restrictions in the requirements project it would need some minor changes in handling IPv6 <address>:<port> notation to function properly.
    Not tested in IPv6 environment, also `System.setProperty("java.net.preferIPv4Stack", "true")` is set in code so that all Node-To-Node and Client-To-Node communication would prefer the IPv4 network stack instead of the IPv6 network stack.
  - IntelliJ IDEA project files are also supplied for convinience.
