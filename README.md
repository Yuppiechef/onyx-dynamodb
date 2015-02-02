## onyx-dynamodb

Onyx plugin providing read and write facilities for batch processing a DynamoDB database. This library uses the faraday dynamodb library under the hood : https://github.com/ptaoussanis/faraday/

#### Installation

In your project file:

```clojure
[yuppiechef/onyx-dynamodb "0.5.0"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.dynamodb])
```

#### Catalog entries

##### scan
```clojure
{:onyx/name :scan
 :onyx/ident :dynamodb/scan
 :onyx/type :input
 :onyx/medium :dynamodb-scan
 :onyx/consumption :concurrent
 :dynamodb/config client-opts
 :dynamodb/scan-options {:return :all-attributes}
 :dynamodb/table :people
 :onyx/batch-size batch-size
 :onyx/max-peers 1
 :onyx/doc "Creates ranges over an :eavt index to parellelize loading datoms downstream"}
```

##### query

Still to be implemented

##### commit-tx

Be sure to send full entities in since it uses (batch-write-item {tablename {:put [batch]}})

```clojure
{:onyx/name :out
 :onyx/ident :dynamodb/commit-tx
 :onyx/type :output
 :onyx/medium :dynamodb
 :onyx/consumption :concurrent
 :dynamodb/table :people
 :dynamodb/config client-opts
 :onyx/batch-size 2
 :onyx/doc "Transacts segments to dynamodb"}
```

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2014 Yuppiechef Online (Pty) Ltd.

Distributed under the Eclipse Public License, the same as Clojure.
