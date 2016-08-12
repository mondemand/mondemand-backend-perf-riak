# Overview

This mondemand backend is meant to collate and index performance traces.

# Setup

## Riak Setup
This backend requires the use of two buckets in Riak with particular types,
which require some shell code to setup.

```
% riak-admin bucket-type create perfid_to_traces '{"props":{"datatype":"map"}}'
% riak-admin bucket-type create indexes '{"props":{"datatype":"set"}}'
% riak-admin bucket-type activate perfid_to_traces
% riak-admin bucket-type activate indexes
```

The `perfid_to_traces` bucket is used for storing the collated traces keyed
by the performance id.  It contains maps with 2 keys.  The first key is
`traces` and is of type `set`.  This set will contain all the traces for
a given performance id.  The second key is `references` which is a counter
which will be incremented for each index which references this peformance id.
The `indexes` bucket is used for storing indexes.  The key .....
The value is a `set` which contains a list of performance ids.

## Indexing

Indexes are meant to be created, and live for some amount of time, so when
configuring an index you specify 2 things, the matching parameters and the
time to live in seconds.

The configuration is done via a file which is re-read periodically and any
changes will tend to take effect within a minute or two.
The file format is
```erlang
[
  { index, [ {datetime, minute} ], 60}
].
```
This would create a datetime based index with a minute interval.  The index
key would be of the form '2016-08-11T23:45' and a new index would be created
each minute, and that index would have a lifetime of 60 minutes (after 60
minutes the index will be removed and any traces which are no longer indexed
will also be removed).  This is the default indexing scheme.

The datetime index is a special index in that you can specify different
duration.  Each performance trace has a receipt time.  If the receipt time
falls within that window it will be added to the given index.  You can specify
different window sizes, although the current allowed set, and an example
of the index keys is

window size | example
----------- | -------
minute | '2016-08-11T23:45'
hour | '2016-08-11T23'
day | '2016-08-11'

Any thing coarser than a day is un-implemented as it can lead to large objects
if there are too many traces (actually, any of these window sizes could lead
to large objects, so watch the rate at which you send performance traces based
on the performance of your Riak cluster backing this).

In addition you can specify context keys as indexes and either use a wildcard
or specify a particular context value.  For instance, lets say you add a 
context called 'customer' and have two possible values 'foo' and 'bar' you
can specify an index like

```erlang
[
  { index, [ {datetime, minute}, {customer,'*'} ], 60}
].
```

Which will result in indexes like

```
2016-08-11T23:45/foo
2016-08-11T23:45/bar
2016-08-11T23:46/foo
2016-08-11T23:46/bar
```

Alternatively if you only cared about customer 'foo' you could do

```erlang
[
  { index, [ {datetime, minute}, {customer,foo} ], 60}
].
```

Resulting in indexes like

```
2016-08-11T23:45/foo
2016-08-11T23:46/foo
```

Multiple entries are allowed in the index file and all indexes will have
matching traces added to them.  An example might be

```erlang
[
  { index, [ {datetime, minute} ], 1440 },
  { index, [ {datetime, minute}, {customer,'*'} ], 60}
].
```
