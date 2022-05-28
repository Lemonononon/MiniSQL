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

看起来是一个deque能够解决的问题，然而实际上复用队列中的页时，我们需要去里面遍历找它，然后取出来，连接其前后节点。为了降低查找的复杂度，加上哈希表实现。map<key, LinkedNode>将key直接映射到deque中的链表节点，这样查找就变成了$O(1)$。

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

### MemHeap

首先，我们为什么在这里需要有一个MemHeap？它和普通的`malloc`、`free`管理空间有什么不同？

这里其实只是一个统一化的内存管理模式。比如一个Row对象，我们发现在它的构造函数中，每次构造都直接new SimpleMemHeap作为自己的heap\_，也就是说每一个Row都独立享有一个MemHeap，而它的内存都交给它来管理。当需要delete一个Row时，我们看\~Row() => \~SimpleMemHeap() => 将MemHeap中所有空间都安全地释放掉。这样统一的内存释放防止了程序员编码不严谨带来的内存泄漏。



另外，第二章比较主要的内容就是序列化与反序列化了，所以先进行一波简单的介绍。

### Serialize (序列化)

简单来说，就是将我们minisql中的`row`、`column`、`schema`、`field`等对象以字节流(char*)的方式储存在硬盘中，使他被持续化地储存，并可以在需要时通过反序列化操作读出我们想要的数据。

例子：

```cpp
void SerializeA(char *buf, A &a) {
    // 将id写入到buf中, 占用4个字节, 并将buf向后推4个字节
    WriteIntToBuffer(&buf, a.id, 4);
    WriteIntToBuffer(&buf, strlen(a.name), 4);
    WriteStrToBuffer(&buf, a.name, strlen(a.name));
}
```

### Deserialize (反序列化)

反序列化就是从disk中读出所存的内容，将以char方式存在disk中的数据读出并存到`row`、`column`、`schema`、`field`等对象中。总得来说，序列化与反序列化操作是对称的，具体实现可以看代码。

例子：

```cpp
void DeserializeA(char *buf, A *&a) {
    a = new A();
    // 从buf中读4字节, 写入到id中, 并将buf向后推4个字节
    a->id = ReadIntFromBuffer(&buf, 4);
    // 获取name的长度len
    auto len = ReadIntFromBuffer(&buf, 4);
    a->name = new char[len];
    // 从buf中读取len个字节拷贝到A.name中, 并将buf向后推len个字节
    ReadStrFromBuffer(&buf, a->name, len);
}
```

### Rowid

对于数据表中的每一行记录，都有一个唯一标识符`RowId`（`src/include/common/rowid.h`）与之对应。`RowId`同时具有逻辑和物理意义，在物理意义上，它是一个64位整数，是每行记录的唯一标识；而在逻辑意义上，它的高32位存储的是该`RowId`对应记录所在数据页的`page_id`，低32位存储的是该`RowId`在`page_id`对应的数据页中对应的是第几条记录，即`slot_num`。

### Table Heap

简单来说，一个table heap里面以双向链表的方式储存了许多的table page，而table page里又存了一个又一个的row。此时要想定位一个row，就需要用到我们说的rowid了，通过高32位获取page_id，低32位获取在该page中的位置。	

### TableHeap:InsertTuple(&row, *txn)

传入要插入的row的指针，我们遍历整个tableheap中的所有page，找到容纳该row的位置并插入，同时用该插入位置生成rowid赋值给row指针，从而实现输出的效果。若成功插入就返回true，否则输出false。

### TableHeap:UpdateTuple(&new_row, &rid, *txn)

将`RowId`为`rid`的记录`old_row`替换成新的记录`new_row`，并将`new_row`的`RowId`通过`new_row.rid_`返回。

*这里在实现的时候由于助教改了需求，所以目前有一些小的瑕疵，但我决定最后debug需要修改时再说（*

### TableHeap:ApplyDelete(&rid, *txn)

```
// Step1: Find the page which contains the tuple.
// Step2: Delete the tuple from the page.
// Find the page which contains the tuple.
```

很简单，没啥好说的

###  TableHeap:GetTuple(*row, *txn)

获取`RowId`为`row->rid_`的记录，同样没啥好说的

### TableIterator

堆表记录迭代器是可以遍历整个heap中所有page的所有row的迭代器，通过iterator++来移动迭代器达到遍历heap的效果。

```cpp
class TableHeap;

class TableIterator {

public:
  // you may define your own constructor based on your member variables
  explicit TableIterator(TableHeap * t,RowId rid);

  explicit TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  // add your own private member variables here
  TableHeap* table_heap_;
  Row* row_;
};
```

### TableIterator &TableIterator::operator++() 

有点复杂，有以下几步

1. 若当前已经是tableheap的end，则直接返回end()
2. 先找与当前iter**同page且未被删除**的row，找不到跳到3
3. 切换到下一个page，若当前已经是最后一页跳到4，否则跳回2
4. 返回end()

### TableIterator TableHeap::Begin(Transaction *txn)

获取堆表的首条记录，作为初始迭代器

### TableIterator TableHeap::End()

用INVALID_ROWID标注end,此时rowid=(page_id,slot_id)=(-1,0)

```c++
TableIterator TableHeap::End() {
  return TableIterator(this,INVALID_ROWID);
}
```

### 本模块的注意事项

+ 用到了bufferpool中的许多函数
+ 许多基本功能其实都已经由tablepage写好，我的工作主要是加了许多逻辑的判断
+ 在读写page前都需要上锁,完成后解锁并unpinpage。 RLatch()、RULatch()、WLatch()、WULatch()、buffer_pool_manager_->UnpinPage（）
+ txn为bonus中需要自己实现的，可以用来优化row读写的辅助结构，目前没啥思路
+ 另外一个bonus为更改float的实现方式来降低精度损失，我觉得看起来好像可以做做（整个完成后）


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

### select

![graphviz (2)](https://beetpic.oss-cn-hangzhou.aliyuncs.com/img/graphviz%20(2).svg)

```sql
select * from tb1 where a="A" and b="B" and c="C" or d="D";
```

<img src="https://beetpic.oss-cn-hangzhou.aliyuncs.com/img/image-20220528161514292.png" alt="image-20220528161514292" style="zoom: 25%;" />

限制条件的语法树如图。

注意，sql中not and or的优先级依然是not最高，and其次，or最低，也就是说每遇到一个or，这就是一个新的独立条件，需要将其保存下来（考虑放在context中）。简单起见，我采用一个while循环，从根到叶，每次读右儿子，并往左儿子移一个。如果遇到or，把or的右儿子和当前的条件（也存在context里）组合成一个完整条件，放进context，重置当前条件。遇到非逻辑词（非not and or）时，结束。

然后关于索引，如果是一连串的and，我们应该先看看现有的索引中列相匹配的，然后进行代价估算，择其一。

如果包含or，我们知道，即使是商业DBMS，在遇到or时索引也往往是失效的，直接不用索引。全表扫描。
