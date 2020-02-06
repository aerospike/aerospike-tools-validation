# Aerospike Validation Tool

This tool scans all records in a namespace and validates bins with Complex Data
Type (CDT) values, optionally attempting to repair any damage detected.
Records with unrecoverable CDT errors are backed up in **asbackup** format if an output file is
specified. Records without CDTs or detected errors are ignored.

By default, no fixes are applied and the fix counts should report zero in
the summary printout.  Aerospike recommends first running the validation tool without the fix
option to assess the state of the namespace.

## Usage Criteria

The validation tool should be run on all namespaces that created or
modified record bins with **Ordered List** CDTs under a pre-4.6 server
release.  There exist edge cases in this scenario that could result in
some CDTs becoming corrupted: the validation tool is designed to
detect &mdash;and if possible correct&mdash; those bins.

## Options

Following is the minimal set of options to that must be specified to run this tool:

|Option|Default|Description|
|------|-------|-----------|
|`--cdt-fix-ordered-list-unique`| disabled | Fix ordered lists that were not stored in order and also remove duplicate elements.|
| `-n <namespace>` | - | Namespace to validate **Mandatory.** |
| `-o <path>` | - | The single file to write corrupted records to. `-` means `stdout`. **Mandatory, unless `'--directory'` is given.** |
| `-d <path>` | - | Directory to store the output files. If the directory does not exist, it will be created before use. **Mandatory, unless `'-o'` is given.** |
| `--help` | Get a comprehensive list of options for the tool. |

The following additional options are available.  Options starting with '--' follow Linux **getopt_long(3)** syntax: a parameter may be specified either as '`--opt=param`' or '`--opt param`'.

| Option | Default | Description|
|--------|---------|------------|
| `-h <host1>[:<tlsname1>][:<port1>][,...]` | 127.0.0.1 | The host that acts as the entry point to the cluster. Any of the cluster nodes can be specified. The remaining cluster nodes will be automatically discovered.|
| `-p <port>` or `--port <port>` | 3000 | Port to connect to. |
| `-U <user>` or `--user <user>` | - | User name with read permission. **Mandatory if the server has security enabled.** |
| `-P<password>` or `--password`| - | Password to authenticate the given user. The first form passes the password on the command line. The second form prompts for the password. |
| `-l <host1>[:<tlsname1>]:<port1>[,...]` or `--node-list <addr1>[:<tlsname1>]:<port1>[,...]` | All nodes | While `--host`/`--port` will make `asvalidation` automatically discover and validate all cluster nodes, `--node-list` can be used to validate only a subset of the cluster nodes. |
| `-w <nodes>` or `--parallel <nodes>` | 10 | Maximal number of nodes to validate in parallel. asvalidate will concurrently scan up to the requested number of nodes. **Setting this number too high may result in client overload.** |
| `--tls-enable` | disabled | Indicates a TLS connection should be used. |
| `--services-alternate` | false | Use to connect to [`alternate-access-address`](/docs/reference/configuration/#alternate-access-address) when the cluster nodes publish IP addresses through [`access-address`](/docs/reference/configuration/#access-address) which are not accessible over WAN and alternate IP addresses accessible over WAN through [`alternate-access-address`](/docs/reference/configuration/#alternate-access-address). |

The following TLS-specific options are available if the `--tls-enable` option is specified.

| Option | Description |
|--------|---------|
|<img width=250/>|<img width=100/>|
| `--tls-cafile <path>` | Path to a trusted CA certificate file. |
| `--tls-capath <path>` | Path to a directory of trusted CA certificates. |
| `--tls-protocols <protocols>` | Set the TLS protocol selection criteria. This format is the same as Apache's `SSLprotocol` documented at [https://httpd.apache.org/docs/current/mod/mod_ssl.html#sslprotocol](https://httpd.apache.org/docs/current/mod/mod_ssl.html#sslprotocol) . If not specified `asvalidation` will use '`-all +TLSv1.2`' if it supports TLSv1.2, otherwise it will use '`-all +TLSv1`'. |
| `--tls-cipher-suite <suite>` | Set the TLS cipher selection criteria. The format is the same as OpenSSL's Cipher List Format documented at [https://www.openssl.org/docs/manmaster/man1/ciphers.html](https://www.openssl.org/docs/manmaster/man1/ciphers.html). |
| `--tls-keyfile <path>` | Path to the key for mutual authentication (if Aerospike Cluster is supporting it). |
| `--tls-keyfile-password <password>` | Password to load protected tls-keyfile. It can be one of the following:<br/>1) Environment varaible: 'env:&lt;VAR&gt;'<br/>2) File: 'file:&lt;PATH&gt;'<br/>3) String: 'PASSWORD'<br/>User will be prompted on command line if '`--tls-keyfile-password`' is specified and no password is given. |
| `--tls-certfile  <path>` | Path to the chain file for mutual authentication (if Aerospike Cluster is supporting it). |
| `--tls-cert-blacklist <path>` | Path to a certificate blacklist file. The file should contain one line for each blacklisted certificate. Each line starts with the certificate serial number expressed in hexadecimal. Each entry may optionally specify the issuer name of the certificate (serial numbers are only required to be unique per issuer). Example: `'867EC87482B2 /C=US/ST=CA/O=Acme/OU=Engineering/CN=TestChainCA'` |
| `--tls-crl-check` | Enable CRL checking for leaf certificate. An error occurs if a valid CRL file cannot be found in the directory specified with '`--tls_capath`'. |
| `--tls-crl-checkall` | Enable CRL checking for entire certificate chain. An error occurs if a valid CRL file cannot be found in the directory specified with '`--tls_capath`'. |


## Output

Following is an example of typical output expected when `asvalidate` is run on a namespace.

```
$ asvalidation -n test -o temp.bin

...
2020-01-06 22:12:28 GMT [INF] [24662] Found 10 invalid record(s) from 1 node(s), 2620 byte(s) in total (~262 B/rec)
2020-01-06 22:12:28 GMT [INF] [24662] CDT Mode: validate
2020-01-06 22:12:28 GMT [INF] [24662]        100 Lists
2020-01-06 22:12:28 GMT [INF] [24662]          0   Unfixable
2020-01-06 22:12:28 GMT [INF] [24662]          0     Has non-storage
2020-01-06 22:12:28 GMT [INF] [24662]          0     Corrupted
2020-01-06 22:12:28 GMT [INF] [24662]         10   Need Fix
2020-01-06 22:12:28 GMT [INF] [24662]          0     Fixed
2020-01-06 22:12:28 GMT [INF] [24662]          0     Fix failed
2020-01-06 22:12:28 GMT [INF] [24662]         10     Order
2020-01-06 22:12:28 GMT [INF] [24662]          0     Padding
2020-01-06 22:12:28 GMT [INF] [24662]          0 Maps
2020-01-06 22:12:28 GMT [INF] [24662]          0   Unfixable
2020-01-06 22:12:28 GMT [INF] [24662]          0     Has duplicate keys
2020-01-06 22:12:28 GMT [INF] [24662]          0     Has non-storage
2020-01-06 22:12:28 GMT [INF] [24662]          0     Corrupted
2020-01-06 22:12:28 GMT [INF] [24662]          0   Need Fix
2020-01-06 22:12:28 GMT [INF] [24662]          0     Fixed
2020-01-06 22:12:28 GMT [INF] [24662]          0     Fix failed
2020-01-06 22:12:28 GMT [INF] [24662]          0     Order
2020-01-06 22:12:28 GMT [INF] [24662]          0     Padding
```

* In this case, the tool was run in validation mode.
* It found 100 lists (100 Lists) and no maps (0 Maps).
* 10 of the lists were corrupted (10 Need Fix under Lists).
* The reason for corruption is listed as out of order (10 Order).
* Because it was in validation mode, no fixes were applied (0 Fixed).
* NOTE: Numbers under a heading do not necessarily add up to the count of the line. For example, there could be (1 Need Fix) but it could have both an Order and Padding error.

Other corruption reasons:

* Has non-storage -- The bin contains an infinite or wildcard element which are not allowed as storage (unfixable).
* Has duplicate keys -- The map bin has duplicate key entries (unfixable).
* Corrupted -- Unfixable corruption not covered by the above.
* Order -- The bin has elements out of order. Can be fixed by reordering list or map.
* Padding -- The bin has garbage bytes after the valid list or map. Can be fixed by truncating the extra bytes.

Fixes are applied to the server and fix failed can be due to (but not limited to) the server version not supporting the operations used in the fix algorithm or network error.

## Building

Building the validation tools requires the source code of the Aerospike C client. Please clone it from GitHub.

    git clone https://github.com/aerospike/aerospike-client-c.

Then build the client.

    cd aerospike-client-c
    make
    cd ..

Then set the `CLIENTREPO` environment variable to point to the `aerospike-client-c` directory. The validation tools build process uses that variable to find the client code.

    export CLIENTREPO=$(pwd)/aerospike-client-c

Now clone the source code of the Aerospike validation tools from GitHub.

    git clone https://github.com/aerospike/aerospike-tools-validation

Then build the validation tools and generate the Doxygen documentation.

    cd aerospike-tools-validation
    make
    make docs

This provides `asvalidation` binary in the `bin` subdirectory -- as well as the Doxygen HTML documentation in `docs`. Open `docs/index.html` to access the generated documentation.

## Validation Source Code

Let's take a quick look at the overall structure of the `asvalidation` source code, at `src/backup.c`. The code does the following, starting at `main()`.

  * Parse command line options into local variables or, if they need to be passed to a worker thread later, into a `backup_config` structure.
  * Initialize an Aerospike client and connect it to the cluster to be validated.
  * Create the counter thread, which starts at `counter_thread_func()`. That's the thread that outputs the status and counter updates during the validation, among other things.
  * When backing up to a single file (`--output-file` option, as opposed to backing up to a directory using `--directory`), create and open that validation file.
  * Populate a `backup_thread_args` structure for each node to be validated and submit it to the `job_queue` queue. Note two things:
    - Only one of the `backup_thread_args` structures gets its `first` member set to `true`.
    - When backing up to a single file, the `shared_fd` member gets the file handle of the created validation file (and `NULL` otherwise).
  * Spawn validation worker threads, which start at `backup_thread_func()`. There's one of those for each cluster node to be validated.
  * Wait for all validation worker threads to finish.
  * When backing up to a single file, close that file.
  * Shut down the Aerospike client.

Let's now look at what the worker threads do, starting at `backup_thread_func()`.

  * Pop a `backup_thread_args` structure off the job queue. The job queue contains exactly one of those for each thread.
  * Initialize a `per_node_context` structure. That's where all the data local to a worker thread is kept. Some of the data is initialized from the `backup_thread_args` structure. In particular, when backing up to a single file, the `fd` member of the `per_node_context` structure is initialized from the `shared_fd` member of the `backup_thread_args` structure. In that way, all validation threads share the same validation file handle.
  * When backing up to a directory, open an exclusive validation file for the worker thread by invoking `open_dir_file()`.
  * If the validation thread is the single thread that has `first` set to `true` in its `backup_thread_args` structure, store secondary index definitions by invoking `process_secondary_indexes()`, and store UDF files by invoking `process_udfs()`. So, this work is done by a single thread, and that thread is chosen by setting its `first` member to `true`.
  * All other threads wait for the chosen thread to finish its secondary index and UDF file work by invoking `wait_one_shot()`. The chosen thread signals completion by invoking `signal_one_shot()`.
  * Initiate validation of records by invoking `aerospike_scan_node()` with `scan_callback()` as the callback function that gets invoked for each record in the namespace to be validated. From here on, all worker threads work in parallel.

Let's now look at what the callback function, `scan_callback()`, does.

  * When backing up to a directory and the current validation file of a worker thread has grown beyond its maximal size, switch to a new validation file by invoking `close_dir_file()` for the old and `open_dir_file()` for the new validation file.
  * When backing up to a single file, acquire the file lock by invoking `safe_lock()`. As all worker threads share the same validation file, we can only allow one thread to write at a time.
  * Invoke the `put_record()` function of the validation encoder for the current record. The encoder implements the validation file format by taking record information and serializing it to the validation file. Its code is in `src/enc_text.c`, its interface in `include/enc_text.h`. Besides `put_record()`, the interface contains `put_secondary_index()` and `put_udf_file()`, which are used to store secondary index definitions and UDF files in a validation file.
  * When backing up to a single file, release the file lock.

### Record Specifications

A record specification defines the bins of a generated record, i.e., how many there are and what type of data they contain: a 50-character string, a 100-element list of integers, or something more deeply nested, such as a 100-element list of 50-element maps that map integer keys to 500-character string values.

The available record specifications are read from a file, `spec.txt` by default. The format of the file is slightly Lisp-like. Each record specification has the following general structure.

    (record "{spec-id}"
        {bin-count-1} {bin-type-1}
        {bin-count-2} {bin-type-2}
        ...)

This declares a record specification that can be accessed under the unique identifier `{spec-id}`. It defines a record that has `{bin-count-1}`-many bins with data of type `{bin-type-1}`, `{bin-count-2}`-many bins with data of type `{bin-type-2}`, etc.

Accordingly, the `{bin-count-x}` placeholders are just integer values. The `{bin-type-x}` placeholders, on the other hand, are a little more complex. They have to be able to describe nested data types. They have one of six forms.

| `{bin-type-x}` | Semantics |
|----------------|-----------|
| `(integer)`    | A 64-bit integer value. |
| `(double)`     | A 64-bit floating-point value |
| `(string {length})` | A string value of the given length. |
| `(list {length} {element-type})` | A list of the given length, whose elements are of the given type. This type can then again have one of these six forms. |
| `(map {size} {key-type} {value-type})` | A map of the given size, whose keys and values are of the given types. These types can then again have one of these six forms. |

Let's reconsider the above examples: a 50-character string, a 100-element list of integers, and a 100-element list of 50-element maps that map integer keys to 500-character string values. Let's specify a record that has 1, 3, and 5 bins of those types, respectively.

    (record "example"
        1 (string 50)
        3 (list 100 (integer))
        5 (list 100 (map 50 (integer) (string 500))))

### Fill Source Code

The specification file is parsed by a Ragel (http://www.colm.net/open-source/ragel/) parser. The state machine for the parser is in `src/spec.rl`. Ragel automatically generates the C parser code from this file. Not everybody has Ragel installed, so the auto-generated C file, `src/spec.c`, is included in the Git repository. If you want to re-generate `spec.c` from `spec.rl`, do the following.

    make ragel

The parser interfaces with the rest of the code via a single function, parse(). This parses the specification file into a linked list of record specifications (@ref rec_node). Each record specification points to a linked list of bin specifications (@ref bin_node), each of which, in turn, says how many bins to add to the record and with which data type. The data type is given by a tree of @ref type_node. See the documentation of spec.h and the `struct` types declared there for more information.

In its most basic form, the `fill` command could be invoked as follows, for example.

    fill test-ns test-set 1000 test-spec-1 2000 test-spec-2 3000 test-spec-3

This would add a total of 6,000 records to set `test-set` in namespace `test-ns`: 1,000 records based on `test-spec-1`, 2,000 records based on `test-spec-2`, and 3,000 records based on `test-spec-3`.

The three (count, record specification) pairs -- (1000, "test-spec-1"), (2000, "test-spec-2"), (3000, "test-spec-3") -- are parsed into a linked list of _fill jobs_ (@ref job_node). The code then iterates through this list and invokes fill() for each job.

The fill() function fires up the worker threads and then just sits there and prints progress information until the worker threads are all done.

The worker threads start at the fill_worker() function. This function generates records according to the given record specification (create_record()), generates a key (init_key()), and puts the generated record in the given set in the given namespace using the generated key. The individual bin values are created by generate(), which recurses in the case of nested data types. Please consult the documentation for fill.c for more details on the code.

The following options to `fill` are probably non-obvious.

| Option             | Effect |
|--------------------|--------|
| `-k {key-type}`    | By default, we randomly pick an integer key, a string key, or a bytes key for each record. Specifying `integer`, `string`, or `bytes` as the `{key-type}` forces a random key of the given type to be created instead. |
| `-c {tps-ceiling}` | Limits the total number of records put per second by the `fill` tool (TPS) to the given ceiling. Handy to prevent server overload. |
| `-b`               | Enables benchmark mode, which speeds up the `fill` tool. In benchmark mode we generate just one single record for a fill job and repeatedly put this same record with different keys; all records of a job thus contain identical data. Without benchmark mode, each record to be put is re-generated from scratch, which results in unique data in each record. |
| `-z`               | Enables fuzzing. Fuzzing uses random junk data for bin names, string and BLOB bin values, etc. in order to try to trip the validation file format parser. |

## Validation File Format

Currently, there is only a single, text-based validation file format, which provides compatibility with previous versions of Aerospike. However, validation file formats are pluggable and a binary format could be supported in the future.

Regardless of the format, any validation file starts with a header line that identifies it as an Aerospike validation file and specifies the version of the validation file format.

    ["Version"] [SP] ["1.1"] [LF]

Let's use the above to agree on a few things regarding notation.

  * `["Version"]` is a 7-character string literal that consists of the letters `V`, `e`, `r`, `s`, `i`, `o`, and `n`. Likewise, `["1.1"]` is a 3-character string literal.

  * `[SP]` is a single space character (ASCII code 32).

  * `[LF]` is a single line feed character (ASCII code 10).

Note that the validation file format is pretty strict. When the specification says `[SP]`, it really means a single space character: not more than one, no tabs, etc. Also, `[LF]` really is a single line feed character: no carriage returns, not more than one (i.e., no empty lines), etc. Maybe it's helpful to look at the text-based format as a binary format that just happens to be readable by humans.

### Meta Data Section

The header line is always followed by zero or more lines that contain meta information about the validation file ("meta data section"). These lines always start with a `["#"] [SP]` prefix. Currently, there are two different meta information lines.

    ["#"] [SP] ["namespace"] [SP] [escape({namespace})] [LF]
    ["#"] [SP] ["first-file"] [LF]

  * The first line specifies the namespace from which this validation file was created.

  * The second line marks this validation file as the first in a set of validation files. We discussed above what exactly this means and why it is important.

We also introduced a new notation, `escape(...)`. Technically, a namespace identifier can contain space characters or line feeds. As the validation file format uses spaces and line feeds as token separators, they need to be escaped when they appear inside a token. We escape a token by adding a backslash ("\\") character before any spaces, line feeds, and backslashes in the token. And that's what `escape(...)` means.

Escaping naively works on bytes. It thus works without having to know about character encodings. If we have a UTF-8 string that contains a byte with value 32, this byte will be escaped regardless of whether it is an actual space character or part of the encoding of a Unicode code point.

We also introduced placeholders into the notation, which represent dynamic data. `{namespace}` is a placeholder for, and thus replaced by, the namespace that the validation file belongs to.

For a namespace `Name Space`, the meta data line would look like this.

    ["#"] [SP] ["namespace"] [SP] ["Name\ Space"] [LF]

In general, any byte value is allowed in the dynamic data represented by a placeholder. The exception to that are placeholders that are passed to `escape(...)`.  As a general rule of thumb, escaped data may not contain NUL bytes. This is due to the design of the Aerospike C client API, which, for developer convenience, uses NUL-terminated C strings for things like namespace, set, or index names. (Not for bin values, though. Those may very well contain NUL bytes and, in line with the rule of thumb, aren't escaped. See below.)

### Global Section

The meta data section is always followed by zero or more lines that contain global cluster data, i.e., data that pertains to all nodes in the cluster ("global section"). This data currently encompasses secondary indexes and UDF files.

Lines in the global section always start with a `["*"] [SP]` prefix. Let's first look at lines that describe secondary indexes.

    ["*"] [SP] [escape({namespace})] [SP] [escape({set})] [SP] [escape({name})] [SP]
               [{index-type}] [SP] ["1"] [SP] [escape({path})] [SP] [{data-type}] [LF]

Let's look at the placeholders, there are quite a few.

| Placeholder    | Content |
|----------------|---------|
| `{namespace}`  | The namespace that the index applies to. |
| `{set}`        | The set that the index applies to. Note that this can be empty, i.e., a zero-length string, as indexes do not necessarily have to be associated with a set. |
| `{name}`       | The name of the index. |
| `{index-type}` | The type of index: `N` = index on bins, `L` = index on list elements, `K` = index on map keys, `V` = index on map values |
| `{path}`       | The bin name |
| `{data-type}`  | The data type of the indexed value: `N` = numeric, `S` = string |

The `["1"]` token is actually the number of values covered by the index. This is for future extensibility, i.e., for composite indexes that span more than one value. Right now, this token is always `["1"]`, though.

Let's now look at how UDF files are represented in the global section.

    ["*"] [SP] [{type}] [SP] [escape({name})] [SP] [{length}] [SP] [{content}] [LF]

Here's what the placeholders stand for.

| Placeholder | Content |
|-------------|---------|
| `{type}`    | The type of the UDF file. Currently always `L` for Lua. |
| `{name}`    | The file name of the UDF file. |
| `{length}`  | The length of the UDF file, which is a decimal unsigned 32-bit value. |
| `{content}` | The content of the UDF file: `{length}` raw bytes of data. The UDF file is simply copied to the validation file. As we know the length of the UDF file, no escaping is required. Also, the UDF file will most likely contain line feeds, so this "line" will actually span multiple lines in the validation file. |

### Records Section

The global section is followed by zero or more records. Each record starts with a multi-line header. Record header lines always start with a `["+"] [SP]` prefix. They have to appear in the given order. Two of these lines are optional, though.

The first line is optional. It is only present, if the actual value of the key -- as opposed to just the key digest -- was stored with the record. If it is present, it has one of the following four forms, depending on whether the key value is an integer, a double, a string, or a bytes value.

    ["+"] [SP] ["k"] [SP] ["I"] [SP] [{int-value}] [LF]
    ["+"] [SP] ["k"] [SP] ["D"] [SP] [{float-value}] [LF]
    ["+"] [SP] ["k"] [SP] ["S"] [SP] [{string-length}] [SP] [{string-data}] [LF]
    ["+"] [SP] ["k"] [SP] ["B"] ["!"]? [SP] [{bytes-length}] [SP] [{bytes-data}] [LF]

Note that we introduced one last notation, `[...]?`, which indicates that a token is optional. For bytes-valued keys, `["B"]` can thus optionally be followed by `["!"]`.

Here's what the placeholders in the above four forms mean.

| Placeholder       | Content |
|-------------------|---------|
| `{int-value}`     | The signed decimal 64-bit integer value of the key. |
| `{float-value}`   | The decimal 64-bit floating point value of the key, including `nan`, `+inf`, and `-inf`. |
| `{string-length}` | The length of the string value of the key, measured in raw bytes; an unsigned decimal 32-bit value. |
| `{string-data}`   | The content of the string value of the key: `{string-length}` raw bytes of data; not escaped, may contain NUL, etc. |
| `{bytes-length}`  | If `["!"]` present: The length of the bytes value of the key.<br>Else: The length of the base-64 encoded bytes value of the key.<br>In any case, an unsigned decimal 32-bit value. |
| `{bytes-data}`    | If `["!"]` present: The content of the bytes value of the key: `{bytes-length}` raw bytes of data; not escaped, may contain NUL, etc.<br>Else: The base-64 encoded content of the bytes value of the key: `{bytes-length}` base-64 characters. |

The next two lines of the record header specify the namespace of the record and its key digest and look like this.

    ["+"] [SP] ["n"] [SP] [escape({namespace})] [LF]
    ["+"] [SP] ["d"] [SP] [{digest}] [LF]

`{namespace}` is the namespace of the record, `{digest}` is its base-64 encoded key digest.

The next line is optional again. It specifies the set of the record.

    ["+"] [SP] ["s"] [SP] [escape({set})] [LF]

`{set}` is the set of the record.

The remainder of the record header specifies the generation count, the expiration time, and the bin count of the record. It looks as follows.

    ["+"] [SP] ["g"] [SP] [{gen-count}] [LF]
    ["+"] [SP] ["t"] [SP] [{expiration}] [LF]
    ["+"] [SP] ["b"] [SP] [{bin-count}] [LF]

Here's what the above placeholders stand for.

| Placeholder    | Content |
|----------------|---------|
| `{gen-count}`  | The record generation count. An unsigned 16-bit decimal integer value. |
| `{expiration}` | The record expiration time in seconds since the Aerospike epoch (2010-01-01 00:00:00 UTC). An unsigned decimal 32-bit integer value. |
| `{bin-count}`  | The number of bins in the record. An unsigned decimal 16-bit integer value. |

The record header lines are followed by `{bin-count}`-many lines of bin data. Each bin data line starts with a `["-"] [SP]` prefix. Depending on the bin data type, a bin data line can generally have one of the following five forms.

    ["-"] [SP] ["N"] [SP] [escape({bin-name})]
    ["-"] [SP] ["I"] [SP] [escape({bin-name})] [SP] [{int-value}] [LF]
    ["-"] [SP] ["D"] [SP] [escape({bin-name})] [SP] [{float-value}] [LF]
    ["-"] [SP] ["S"] [SP] [escape({bin-name})] [SP] [{string-length}] [SP] [{string-data}] [LF]
    ["-"] [SP] ["B"] ["!"]? [SP] [escape({bin-name})] [SP] [{bytes-length}] [SP] [{bytes-data}] [LF]

The first form represents a `NIL`-valued bin. The remaining four forms represent an integer-valued, a double-valued, a string-valued, and a bytes-valued bin. They are completely analogous to the above four forms for an integer, a double, a string, and a bytes record key value. Accordingly, the placeholders `{int-value}`, `{float-value}`, `{string-length}`, `{string-data}`, `{bytes-length}`, and `{bytes-data}` work in exactly the same way -- just for bin values instead of key values.

| Placeholder       | Content |
|-------------------|---------|
| `{bin-name}`      | The name of the bin. |
| `{int-value}`     | The signed decimal 64-bit integer value of the bin. |
| `{float-value}`   | The decimal 64-bit floating point value of the bin, including `nan`, `+inf`, and `-inf`. |
| `{string-length}` | The length of the string value of the bin, measured in raw bytes; an unsigned decimal 32-bit value. |
| `{string-data}`   | The content of the string value of the bin: `{string-length}` raw bytes of data; not escaped, may contain NUL, etc. |
| `{bytes-length}`  | If `["!"]` present: The length of the bytes value of the bin.<br>Else: The length of the base-64 encoded bytes value of the bin.<br>In any case, an unsigned decimal 32-bit value. |
| `{bytes-data}`    | If `["!"]` present: The content of the bytes value of the bin: `{bytes-length}` raw bytes of data; not escaped, may contain NUL, etc.<br>Else: The base-64 encoded content of the bytes value of the bin: `{bytes-length}` base-64 characters. |

Actually, the above `["B"]` form is not the only way to represent bytes-valued bins. It gets a little more specific than that. There are other tokens that refer to more specific bytes values. In particular, list-valued and map-valued bins are represented as a bytes value.

| Token   | Type |
|---------|------|
| `["B"]` | Generic bytes value. |
| `["J"]` | Java bytes value. |
| `["C"]` | C# bytes value. |
| `["P"]` | Python bytes value. |
| `["R"]` | Ruby bytes value. |
| `["H"]` | PHP bytes value. |
| `["E"]` | Erlang bytes value. |
| `["M"]` | Map value, opaquely represented as a bytes value. |
| `["L"]` | List value, opaquely represented as a bytes value. |
| `["U"]` | LDT value, opaquely represented as a bytes value. Deprecated. |

### Sample Validation File

The following backup file contains two secondary indexes, a UDF file, and a record. The two empty lines stem from the UDF file, which contains two line feeds.

    Version 1.1
    # namespace test
    # first-file
    * i test test-set int-index N 1 int-bin N
    * i test test-set string-index N 1 string-bin S
    * u L test.lua 27 -- just an empty Lua file


    + n test
    + d q+LsiGs1gD9duJDbzQSXytajtCY=
    + s test-set
    + g 1
    + t 0
    + b 2
    - I int-bin 12345
    - S string-bin 5 abcde

In greater detail:

  * The validation was taken from namespace `test` and set `test-set`.
  * The record never expires and it has two bins: an integer bin, `int-bin`, and a string bin, `string-bin`, with values `12345` and `"abcde"`, respectively.
