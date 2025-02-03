# Aerospike Validation Tool

This tool scans all records in a namespace and validates bins with Collection Data
Type (CDT) values (List and Map bins), optionally attempting to repair any damage detected.
Records with unrecoverable CDT errors are written to output if an output file is
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

A minimal set of options to run this tool.

|config|definition|
|------|---|
|--cdt-fix-ordered-list-unique|Fix ordered lists that were not stored in order and also remove duplicate elements.|
|--no-cdt-check-map-keys | Do not check cdt map keys.|
| -n | Namespace |
| -o | Output File Name |
| -d | Output Directory |
| --help | Get a comprehensive list of options for tool |

## Descriptions of possible corruption reasons
|Reason|Description|Disposition|
|------|---|---|
|Has non-storage|The bin contains an infinite or wildcard element which is not allowed as storage.|This type of error is unfixable without your manual intervention.|
|Has duplicate keys|A map bin has duplicate key entries.|This type of error is unfixable without your manual intervention.|
|Corrupted|A problem not attributable to any of the other categories of errors.|This type of error is unfixable without your manual intervention.|
|Invalid Keys|The bin has a map with at least one invalid key.|This type of error is unfixable without your manual intervention.|
|Order|The bin has elements out of order.|Can be fixed by reordering the list with the --cdt-fix-ordered-list-unique option. See server version requirements in Recommendations and Server Versions for When to Run asvalidation.|
|Padding|The bin has garbage bytes after the valid list or map.|Can be fixed by truncating the extra bytes. See server version requirements in Recommendations and Server Versions for When to Run asvalidation.|

## asvalidation Modes
asvalidation can be run in the following modes. Records without CDTs or detected errors are ignored. Records with detected errors are saved unless otherwise specified. By default, no fixes are applied.

* "Validation" mode discovers problems and produces a report.
* "Fix" mode, triggered by the --cdt-fix-ordered-list-unique option, attempts to correct discovered problems where possible.
You should probably run asvalidation first in validation mode to see the kinds of errors it discovers before running it in fix mode to fix them.

## Options

### Namespace data selection options

| Option | Default | Description|
|--------|---------|------------|
| `-n NAMESPACE` or `--namespace NAMESPACE` | - | Namespace to validate. **Mandatory.** |
| `-s SETS` or `--set SETS` | All sets | The set(s) to validate. May pass in a comma-separated list of sets to validate.|
| `-B  BIN1,BIN2,...` or `--bin-list BIN1,BIN2,...` | All bins | The bins to validate. |
| `-M` or `--max-records N` | 0 = all records. | An approximate limit for the number of records to process. Available in server 4.9 and above. Note: this option is mutually exclusive to `--partition-list` and `--after-digest`. |

### Connection options

| Option                                                             | Default          | Description                                                                                                                                                                                                                                                                                                                                                                                                                          |
|--------------------------------------------------------------------|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `-h HOST1:TLSNAME1:PORT1,...` or `--host HOST1:TLSNAME1:PORT1,...` | 127.0.0.1        | The host that acts as the entry point to the cluster. Any nodes in the cluster can be specified. The remaining nodes are discovered automatically.                                                                                                                                                                                                                                                                       |
| `-p PORT` or `--port PORT`                                         | 3000             | Port to connect to.                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `-U USER` or `--user USER`                                         | -                | User name with read permission. **Mandatory if the server has security enabled.**                                                                                                                                                                                                                                                                                                                                                    |
| `-P PASSWORD` or `--password`                                      | -                | Password to authenticate the given user. The first form passes the password on the command line. The second form prompts for the password.                                                                                                                                                                                                                                                                                           |
| `-A` or `--auth`                                                   | INTERNAL         | Set authentication mode when user and password are defined. Modes are (INTERNAL, EXTERNAL, EXTERNAL_INSECURE, PKI) and the default is INTERNAL. This mode must be set EXTERNAL when using LDAP.                                                                                                                                                                                                                                      |
| `-l` or `--node-list ADDR1:TLSNAME1:PORT1,...`                     | `localhost:3000` | While `--host` and `--port` automatically discover all cluster nodes, `--node-list` scans a subset of cluster nodes by first calculating the subset of partitions owned by the listed nodes, and then validating that list of partitions. This option is mutually exclusive with [`--partition-list`](#partition-list) and [`--after-digest`](#after-specific-digest). |
| `--parallel N`                                                     | 1                | Maximum number of scans to run in parallel. If only one partition range is given, or the entire namespace is being validated, the range of partitions is evenly divided by this number to be processed in parallel. Otherwise, each filter cannot be parallelized individually, so you may only achieve as much parallelism as there are partition filters.                                                                |
| `--tls-enable`                                                     | disabled         | Indicates a TLS connection should be used.                                                                                                                                                                                                                                                                                                                                                                                           |
| `-S` or `--services-alternate`                                     | false            | Set this to `true` to connect to Aerospike node's [`alternate-access-address`](https://aerospike.com/docs/server/reference/configuration?context=all&version=7#network__alternate-access-address).                     |
| `--prefer-racks RACKID1,...`                                       | disabled         | A comma separated list of rack IDs to prefer when reading records. This is useful for limiting cross datacenter network traffic.                                                                                                                                                                                                                                                                       |

### Output options

| Option | Default | Description|
|--------|---------|------------|
| `-d PATH` or `--directory PATH` | - | Directory to store the `.asb` validation files in. If the directory does not exist, it will be created before use. **Mandatory, unless `--output-file` or `--estimate` is given.** |
| `-o PATH` or `--output-file PATH` | - | The single file to write to. `-` means `stdout`. **Mandatory, unless `--directory` or `--estimate` is given.** |
| -q `DESIRED-PREFIX`<br /><br />or<br /><br />`--output-file-prefix DESIRED-PREFIX` |  | Must be used with the `--directory` option. A desired prefix for all output files. |
| `-F LIMIT` or `--file-limit LIMIT` | 250 MiB | File size limit (in MiB) for `--directory`. If a `.asb` validation file crosses this size threshold, `asvalidation` will switch to a new file. |
| `-r` or `--remove-files` | - | Clear directory or remove output file. By default, `asvalidation` refuses to write to a non-empty directory or to overwrite an existing validation file. This option clears the given `--directory` or removes an existing `--output-file`. Mutually exclusive to `--continue`. |
| `--remove-artifacts` | - | Clear directory or remove output file, like `--remove-files`, without running a validation. This option is mutually exclusive to `--continue` and `--estimate`. |
| `-N BANDWIDTH` or `--nice BANDWIDTH` | - | Throttles `asvalidation`'s write operations to the validation file(s) to not exceed the given bandwidth in MiB/s. Effectively also throttles the scan on the server side as `asvalidation` refuses to accept more data than it can write. |

### Timeout options

| Option | Default | Description|
|--------|---------|------------|
| `--socket-timeout MS` | 10000 | Socket timeout in milliseconds. If this value is 0, it is set to total-timeout. If both are 0, there is no socket idle time limit. |
| `--total-timeout MS` | 0 | Total socket timeout in milliseconds. Default is 0, that is, no timeout. |
| `--max-retries N` | 5 | Maximum number of retries before aborting the current transaction. |
| `--sleep-between-retries MS` | 0 | The amount of time to sleep between retries. |

### TLS options

| Option | Default | Description|
|--------|---------|------------|
| `--tls-cafile=TLS_CAFILE` | | Path to a trusted CA certificate file. |
| `--tls-capath=TLS_CAPATH` | | Path to a directory of trusted CA certificates. |
| `--tls-name=TLS_NAME` | | The default TLS name used to authenticate each TLS socket connection. *Note: this must also match the cluster name.*|
| `--tls-protocols=TLS_PROTOCOLS` | | Set the TLS protocol selection criteria. This format is the same as Apache's [SSL Protocol](https://httpd.apache.org/docs/2.4/mod/mod_ssl.html). If not specified, `asvalidation` uses `TLSv1.2` if supported. Otherwise it uses `-all +TLSv1`. |
| `--tls-cipher-suite=TLS_CIPHER_SUITE` | | Set the TLS cipher selection criteria. The format is the same as OpenSSL's [Cipher List Format](https://www.openssl.org/docs/man1.1.1/man1/ciphers.html). |
| `--tls-keyfile=TLS_KEYFILE` | | Path to the key for mutual authentication (if Aerospike cluster supports it). |
| `--tls-keyfile-password=TLS_KEYFILE_PASSWORD` | | Password to load protected TLS-keyfile. Can be one of the following:<br/>1) Environment variable: `env:VAR`<br/>2) File: `file:PATH`<br/>3) String: `PASSWORD`<br/>User will be prompted on command line if `--tls-keyfile-password` specified and no password is given. |
| `--tls-certfile=TLS_CERTFILE  <path>` | | Path to the chain file for mutual authentication (if Aerospike Cluster supports it). |
| `--tls-cert-blacklist <path>` | | Path to a certificate blocklist file. The file should contain one line for each blocklisted certificate. Each line starts with the certificate serial number expressed in hex. Each entry may optionally specify the issuer name of the certificate (serial numbers are only required to be unique per issuer). Example: `867EC87482B2 /C=US/ST=CA/O=Acme/OU=Engineering/CN=TestChainCA` |
| `--tls-crl-check` | | Enable CRL checking for leaf certificate. An error occurs if a valid CRL files cannot be found in `TLS_CAPATH`. |
| `--tls-crl-checkall` | | Enable CRL checking for entire certificate chain. An error occurs if a valid CRL files cannot be found in `TLS_CAPATH`. |
| `--tls-log-session-info` | | Enable logging session information for each TLS connection. |

`TLS_NAME` is only used when connecting with a secure TLS enabled server.

### Validation resumption

| Option | Default | Description|
|--------|---------|------------|
| `--continue STATE-FILE` | disabled | Enables the resumption of an interrupted validation from provided state file. All other command line arguments should match those used in the initial run (except `--remove-files`, which is mutually exclusive with `--continue`). |
| `--state-file-dst` | see below | Specifies where to save the validation state file to. If this points to a directory, the state file is saved within the directory using the same naming convention as save-to-directory state files. If this does not point to a directory, the path is treated as a path to the state file. |

### Other options

| Option | Default | Description|
|--------|---------|------------|
| `-v` or `--verbose` | disabled | Output considerably more information about the running validation. |
| `-m` or `--machine PATH` | - | Output machine-readable status updates to the given path, typically a FIFO. |
| `-L` or `--records-per-second RPS` | 0 | Available only for Aerospike Database 4.7 and later.<br /><br />Limit total returned records per second (RPS). If `RPS` is zero (the default), a records-per-second limit is not applied. |

## Output

```
> asvalidation -n test -o temp.bin

...
2020-01-06 22:12:28 GMT [INF] [24662] Found 10 invalid record(s) from 1 node(s), 2620 byte(s) in total (~262 B/rec)
2020-01-06 22:12:28 GMT [INF] [24662] CDT Mode: validate
2020-01-06 22:12:28 GMT [INF] [24662]        100 Lists
2020-01-06 22:12:28 GMT [INF] [24662]          0   Unfixable
2020-01-06 22:12:28 GMT [INF] [24662]          0     Has non-storage
2020-01-06 22:12:28 GMT [INF] [24662]          0     Corrupted
2020-01-06 22:12:28 GMT [INF] [24662]          0     Invalid Keys
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
2020-01-06 22:12:28 GMT [INF] [24662]          0     Invalid Keys
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
    git checkout 5.2.8
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

The following validation file contains two secondary indexes, a UDF file, and a record. The two empty lines stem from the UDF file, which contains two line feeds.

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
