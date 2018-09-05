# Namespace
Namespace comes two kind of locking methods for mutable and immutable operations.

## Mutable operations
Operations like `PUT`, `POST`, `DELETE` on object needs to get write lock on root level element first. With write lock on the root level, its required to get write lock on the object e.g. PUT operation on key `mybucket/path/to/myobject` should get write lock on `mybucket/path` first then `mybucket/path/to/myobject`. Using this double lock system prevents hierarchical mutuable operations where two sub tree elements cannot be removed or created. For example; `DELETE` on `B/A` does not race with `PUT` on `B/A/O` vice versa, but `B/Q` path is continued to obtain write lock.  Finally unlock on the object, then the root level element are done.

## Immutable operations
Operations like `GET`, `HEAD` on object needs to take read lock on key. Finally unlock on the key is done. Example obtain read lock on key `mybucket/path/to/myobject` prevents any racy mutable operation on `mybucket/path/to/myobject`.
