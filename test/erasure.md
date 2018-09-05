# Erasure backend

Erasure backend is a disk containing multiple local and/or remote
disks.  The erasure disk does Put, Get, Delete and List of data with
cluster level lock (involving all disks) with minimum critical region.

```
               ErasureDisk
                    |
  +---------+-------+---------------+
  |         |                       |
Disk-1    Disk-2    ...    ...    Disk-N
```

Any local disk or remote disk layout is as follows

```
<DISK>/
|-- buckets/
|   `-- <BUCKET>/
|       |-- meta.json
|       `-- objects/
|           `-- <OBJECT>/
|               |-- meta.json
|               `-- meta.json.<VERSION_ID>
|-- data/
|   `-- <INDEX>/
|       `-- <UUID>/
|           |-- <DATA>
|           `-- <DATA>.checksum
|-- tmp/
`-- trans/
```

## Put with minimum lock
1. Upload input stream into datastore by UUID to all disks.
2. Lock cluster level.
3. Create object meta.json with reference to datastore.
4. Unlock cluster level.


## Get with minimum lock
1. Read-Lock cluster level.
2. Read object's meta.json
3. Get data stream from datastore of all disks.
4. Read-unlock cluster level.
5. Erasure decode the data stream and write to the client.
