# MiniSQL
A DB project for DB course in ZJU.

[toc]

## Disk and Buffer Pool Manager

### Bitmap



### Disk Manager



### Replacer

在buffer pool中，replacer接收不再被引用的数据页，并在合适的时机lazy地回收它们。

#### LRU

Least Recently Used算法，将最近最少使用的数据页回收。具体的实现是一个队列（实际上是双向链表），队列容量为内存能容纳的最大数据页数。将内存中已经不再引用的数据页号丢进队列，当内存满了，需要回收一个页来开辟出新空间时，把队列最里面（最早丢进去）的页号对应的页给回收了。当复用队列中的页时，将其从队列中取出，用完后重新丢进队列。

看起来是一个deque能够解决的问题，然而实际上复用队列中的页时，我们需要去里面遍历找它，然后取出来，连接其前后节点。为了降低查找的复杂度，加上哈希表实现。map<key, LinkedNode>将key直接映射到deque中的链表节点，这样查找就变成了$$O(1)$$。

[LRU Cache Implementation - GeeksforGeeks](https://www.geeksforgeeks.org/lru-cache-implementation/?ref=leftbar-rightbar)

#### Clock



### Buffer Pool Manager

#### FetchPage

1. 如果内存里还留有这个page，直接取，并把它从lru淘汰队列中拿出来（如果在里面的话）。
2. 如果内存里没有，那么看看free_list_还有没有空余的内存空间，如果有，用这个空间从disk拉取。
3. 如果free_list_没有空余空间，那么内存已满，从lru淘汰一个页，获取自由空间，从disk拉取。注意，lru淘汰的页如果是被修改过（dirty）的，那么应该把它写入disk而不是直接丢掉。递增pin\_count\_。
4. 如果lru也没东西可淘汰，那就返回nullptr。

#### NewPage

1. 如果free_list_有空间，从内存中直接用空闲空间存放新页。
2. 如果free_list_没空间，那么内存满了，尝试从lru淘汰一个，留出空间。
3. 如果lru也没东西可淘汰，那就返回nullptr。
4. 在disk里分配空间，获取到逻辑页号。设置新页的pin\_count\_为1。
5. 将页号在引用参数中传回，将内存开辟的页return。

#### UnpinPage

注意，这里Unpin的时候会传入是否dirty，也就是调用者是否修改过这个数据页。显然应该使

***这个page的is\_dirty\_ = 这个page的is\_dirty\_ || is\_dirty\_***

因为可能有多重调用，只要有一重调用是dirty的，那么它就是dirty的。

判断完dirty后，使pin\_count\_--，当pin\_count\_为0时，将它放进lru淘汰队列。

#### DeletePage

给的原本的注释存在一些问题，实际情况应该为：

1. 如果这个页不在内存里，直接从硬盘里删除。
2. 如果这个页在内存里而且pin\_count\_>0，无法删除，返回false。
3. 如果这个页在内存里而且pin\_count\_==0，也就是在lru淘汰队列中，那么直接从淘汰队列里删除，把内存里的空间清理出来，最后从硬盘里删除。

#### pin_count_

当且仅当pin\_count\_为0时，一个page会被纳入lru的管理，等待合适的时机将它回收。在每一次FetchPage和NewPage时，我们都使该page的pin\_count\_++，而在UnpinPage时令pin\_count\_--。为了维护好pin\_count\_的值，如果一个方法并不生产出Page*供其他方法使用，那么它应该在内部使用完fetch或new得到的page后，及时地unpin它。如果一个方法用fetch和new获得满足特定条件的page提供给调用者，那么应该由调用者来unpin它。

## Record Manager



## Index Manager

![graphviz](https://beetpic.oss-cn-hangzhou.aliyuncs.com/img/graphviz.svg)

### b_plus_internal_page

中间节点类，需要注意的是这里采用的internal node策略是第一个key为invalid。

### b_plus_leaf_page

叶子节点类，key和value完全成对匹配。

### 泛型的方法参数匹配

原本的框架有这样令人不快的地方：我们常常需要用泛型N来同时操作internal node和leaf node，然而在使用他们的方法时，却会遇到方法签名不一致的情况：

+ void BPlusTreeLeafPage::MoveAllTo(BPlusTreeLeafPage *recipient);
+ void BPlusTreeInternalPage::MoveAllTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager);

于是我们可以增加无用的参数来让泛型签名吻合 =>

+ void BPlusTreeLeafPage::MoveAllTo(BPlusTreeLeafPage *recipient, const KeyType &, BufferPoolManager *);

## Catalog Manager



## SQL Executor

