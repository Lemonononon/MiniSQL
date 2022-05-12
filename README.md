# MiniSQL
A DB project for DB course in ZJU.

[toc]

## Disk and Buffer Pool Manager

### Bitmap



### Disk Manager



### Replacer

#### LRU



#### Clock



### Buffer Pool Manager

#### pin_count_

当且仅当pin\_count\_为0时，一个page会被纳入lru的管理，等待合适的时机将它回收。在每一次FetchPage和NewPage时，我们都使该page的pin\_count\_++，而在UnpinPage时令pin\_count\_--。为了维护好pin\_count\_的值，如果一个方法并不生产出Page*供其他方法使用，那么它应该在内部使用完fetch或new得到的page后，及时地unpin它。如果一个方法用fetch和new获得满足特定条件的page提供给调用者，那么应该由调用者来unpin它。

## Record Manager



## Index Manager

![graphviz](https://beetpic.oss-cn-hangzhou.aliyuncs.com/img/graphviz.svg)

### b_plus_internal_page

中间节点类，需要注意的是这里采用的internal node策略是第一个key为invalid。

### b_plus_leaf_page

叶子节点类，key和value完全成对匹配。

## Catalog Manager



## SQL Executor

