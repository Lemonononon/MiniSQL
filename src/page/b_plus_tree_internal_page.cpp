#include "page/b_plus_tree_internal_page.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "utils/exception.h"

//****************************HELPER METHODS AND UTILITIES***************************

/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(1);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key{array_[index].first};
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 * 如果没找到，这里返回一个GetSize()，下面的insertNodeAfter就有用了
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return GetSize();
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  // replace with your own code
  ValueType val{array_[index].second};
  return val;
}

//**************************LOOKUP********************************

/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // replace with your own code
  // TODO: 跑通后来优化为二分查找
  for (int i = 1; i < GetSize(); ++i) {
    // array_[i].first > key
    if (comparator(array_[i].first, key) > 0) {
      return array_[i - 1].second;
    }
  }
  return array_[GetSize() - 1].second;
}

// *******************************INSERTION*************************************

/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 *
 * degree = 4;
 * 2 3 5 -> insert 7 -> 2 3 5 7 ->       5
 *                                 2 3      5 7
 * old_value指向原节点(2 3 5 7 -> 2 3)，new_key这里就是5，new_value指向被原节点砍出来的新节点5 7
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array_[0].second = old_value;
  array_[1].first = new_key;
  array_[1].second = new_value;
  IncreaseSize(1);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int insertIndex = ValueIndex(old_value) + 1;
  int oriSize = GetSize();
  for (int i = oriSize; i > insertIndex; --i) {
    array_[i] = array_[i - 1];
  }
  IncreaseSize(1);
  array_[insertIndex] = {new_key, new_value};
  return GetSize();
}

// *************************************SPLIT****************************************

/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  int halfSize = (GetSize() + 1) / 2;
  recipient->CopyNFrom(array_ + GetSize() - halfSize, halfSize, buffer_pool_manager);
  IncreaseSize(-halfSize);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  // 如果是叶子节点，完整复制，如果是内部节点，第一个key被无视即可
  for (int i = 0; i < size; ++i) {
    array_[i] = items[i];
    auto page = buffer_pool_manager->FetchPage(items[i].second);
    auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(node->GetPageId(), true);
  }
  // 初始化时大小为1，只增加size-1，因为这上下两个方法是出于分裂而使用的，比如兄弟拆了两个key&value
  // pair来新节点，显然新节点只有一个有效key
  IncreaseSize(size - 1);
}

// **********************************REMOVE*********************************

/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  // TODO: 作为类成员变量的柔性数组的内存是怎么申请，又怎么释放的？
  for (int i = index; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  // replace with your own code
  IncreaseSize(-1);
  return ValueAt(0);
}

//******************************MERGE********************************

/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 * 在节点需要和其他节点（不一定是兄弟）结合时会用到这个操作，因为internal的key是比value少一个的，结合的时候要去父亲那里把key搞回来
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, middle_key);
  recipient->CopyAllFrom(array_, GetSize(), buffer_pool_manager);
  // 更新disk中的pages的parent
  // 即把我（this）的儿子的parent全都换成recipient
  for (int i = 0; i < GetSize(); ++i) {
    auto page = buffer_pool_manager->FetchPage(ValueAt(i));
    auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    node->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(ValueAt(i), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  int ori_size = GetSize();
  for (int i = 0; i < size; ++i) {
    array_[ori_size + i] = items[i];
  }
  IncreaseSize(size);
}

//********************************REDISTRIBUTE*************************************

/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, middle_key);
  recipient->CopyLastFrom(array_[0], buffer_pool_manager);
  auto child_page = buffer_pool_manager->FetchPage(array_[0].second);
  if (child_page == nullptr) {
    throw Exception("out of memory");
  }
  auto child = reinterpret_cast<BPlusTreePage*>(child_page);
  child->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(array_[0].second, true);
  for (int i = 0; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  array_[GetSize()] = pair;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  recipient->SetKeyAt(0, middle_key);
  recipient->CopyFirstFrom(array_[GetSize() - 1], buffer_pool_manager);
  auto child_page = buffer_pool_manager->FetchPage(array_[GetSize() - 1].second);
  if (child_page == nullptr) {
    throw Exception("out of memory");
  }
  auto child = reinterpret_cast<BPlusTreePage*>(child_page);
  child->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(array_[GetSize() - 1].second, true);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  for (int i = GetSize(); i > 0; --i) {
    array_[i] = array_[i - 1];
  }
  array_[0] = pair;
  IncreaseSize(1);
}

template class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;