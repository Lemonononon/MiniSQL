#include "index/b_plus_tree.h"
#include <string>
#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"
#include "utils/exception.h"

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  // 初始化为空
  root_page_id_ = INVALID_PAGE_ID;
  auto index_roots_page = buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID);
  auto index_roots = reinterpret_cast<IndexRootsPage*>(index_roots_page->GetData());
  page_id_t* root_page_id_receiver = new page_id_t();
  if (index_roots->GetRootId(index_id, root_page_id_receiver)) {
    root_page_id_ = *root_page_id_receiver;
  }
  delete root_page_id_receiver;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

// ****************************************SEARCH***********************************************

/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(FindLeafPage(key)->GetData());
  bool res = false;
  if (leaf != nullptr) {
    ValueType value;
    if (leaf->Lookup(key, value, comparator_)) {
      result.push_back(value);
      res = true;
    }
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return res;
}

// ***************************************INSERTION********************************************

/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  bool res = false;
  if (IsEmpty()) {
    StartNewTree(key, value);
    res = true;
  } else {
    res = InsertIntoLeaf(key, value, transaction);
  }
  return res;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto page = buffer_pool_manager_->NewPage(root_page_id_);
  if (page == nullptr) {
    throw Exception("out of memory");
  }
  // true表示是插入而不是更新
  UpdateRootPageId(true);
  auto root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  root->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  root->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(FindLeafPage(key)->GetData());
  if (leaf == nullptr) {
    throw Exception("insert failed");
  }
  ValueType tmp = value;
  if (leaf->Lookup(key, tmp, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return false;
  }
  if (leaf->GetSize() < leaf->GetMaxSize()) {
    leaf->Insert(key, value, comparator_);
  } else {
    auto sibling = Split<B_PLUS_TREE_LEAF_PAGE_TYPE>(leaf);
    if (comparator_(key, sibling->KeyAt(0)) < 0) {
      leaf->Insert(key, value, comparator_);
    } else {
      sibling->Insert(key, value, comparator_);
    }
    sibling->SetNextPageId(leaf->GetNextPageId());
    leaf->SetNextPageId(sibling->GetPageId());
    InsertIntoParent(leaf, sibling->KeyAt(0), sibling, transaction);
    buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr) {
    throw Exception("out of memory");
  }
  auto sibling = reinterpret_cast<N *>(page->GetData());
  sibling->Init(page_id, INVALID_PAGE_ID, sibling->IsLeafPage() ? leaf_max_size_ : internal_max_size_);
  node->MoveHalfTo(sibling, buffer_pool_manager_);
  return sibling;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    auto page = buffer_pool_manager_->NewPage(root_page_id_);
    if (page == nullptr) {
      throw Exception("out of memory");
    }
    auto root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
    root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId(false);
    //    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  } else {
    page_id_t parent_page_id = old_node->GetParentPageId();
    auto page = buffer_pool_manager_->FetchPage(parent_page_id);
    if (page == nullptr) {
      throw Exception("out of memory");
    }
    auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
    if (parent_page->GetSize() < parent_page->GetMaxSize()) {
      parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      new_node->SetParentPageId(parent_page->GetPageId());
      //      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    } else {
      // 递归分裂
      auto sibling_page = Split<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>(parent_page);
      if (comparator_(key, sibling_page->KeyAt(0)) < 0) {
        new_node->SetParentPageId(parent_page->GetPageId());
        parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      } else if (comparator_(key, sibling_page->KeyAt(0)) == 0) {
        new_node->SetParentPageId(sibling_page->GetPageId());
        sibling_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      } else {
        new_node->SetParentPageId(sibling_page->GetPageId());
        sibling_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        old_node->SetParentPageId(sibling_page->GetPageId());
      }

      //      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
      InsertIntoParent(parent_page, sibling_page->KeyAt(0), sibling_page);
      buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) return;
  bool if_delete = false;
  auto page = FindLeafPage(key);
  auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page);
  if (leaf != nullptr) {
    int ori_size = leaf->GetSize();
    // 如果ori_key_index不为ori_size，则有此记录
    int ori_key_index = leaf->KeyIndex(key, comparator_);
    if (ori_key_index != ori_size) {
      leaf->RemoveAndDeleteRecord(key, comparator_);
      // 更新父亲节点的索引
      // 这里需要一个循环一路更新上去
      if (!leaf->IsRootPage() && ori_key_index == 0) {
        //        auto parent_page = buffer_pool_manager_->FetchPage(leaf->GetParentPageId());
        //        auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
        //        KeyComparator>*>(parent_page->GetData()); parent->SetKeyAt(parent->ValueIndex(leaf->GetPageId()),
        //        leaf->KeyAt(0)); buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
        UpdateParentKey(leaf);
      }
      if (leaf->GetSize() < leaf->GetMinSize()) {
        if_delete = CoalesceOrRedistribute(leaf, transaction);
      }
    }
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  if (if_delete) {
    buffer_pool_manager_->DeletePage(page->GetPageId());
  }
}

/*
 * 递归地更新父亲节点的key值
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::UpdateParentKey(N *node) {
  if (node->IsRootPage()) return;
  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
  int update_index = parent->ValueIndex(node->GetPageId());
  parent->SetKeyAt(update_index, node->KeyAt(0));
  if (update_index == 0) {
    UpdateParentKey(parent);
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }
  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (parent_page == nullptr) {
    throw Exception("out of memory");
  }
  auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
  // 用parent去寻找前面的兄弟，如果node是parent的第一个孩子，那可以找后面的兄弟
  int node_index = parent->ValueIndex(node->GetPageId());
  int sibling_page_id;
  if (node_index == 0) {
    sibling_page_id = parent->ValueAt(node_index + 1);
  } else {
    sibling_page_id = parent->ValueAt(node_index - 1);
  }
  auto sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
  if (sibling_page == nullptr) {
    throw Exception("out of memory");
  }
  auto sibling = reinterpret_cast<N *>(sibling_page);
  bool if_delete = false;
  bool if_delete_parent = false;
  if (sibling->GetSize() + node->GetSize() > node->GetMaxSize()) {
    // redistribute
    //    if (node_index == 0) {
    //      // 把sibling的第一个移到node的最后一个
    //      Redistribute<N>(sibling, node, 0);
    //    } else {
    //      // 把sibling的最后一个移到node的第一个
    //      Redistribute<N>(sibling, node, 1);
    //    }
    Redistribute<N>(sibling, node, node_index);
    buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
  } else {
    // merge
    if (node_index == 0) {
      // 后面的兄弟合并进node，node不用删除
      if_delete = false;
      if_delete_parent = Coalesce<N>(node, sibling, parent, 1, transaction);
      buffer_pool_manager_->UnpinPage(sibling_page_id, true);
      buffer_pool_manager_->DeletePage(sibling_page_id);
    } else {
      // node合并进前面的兄弟
      if_delete = true;
      if_delete_parent = Coalesce<N>(sibling, node, parent, node_index, transaction);
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
    }
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  if (if_delete_parent) {
    buffer_pool_manager_->DeletePage(parent->GetPageId());
  }
  return if_delete;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                              Transaction *transaction) {
  // 从parent中获取node的第一个key（如果node是internal的话）
  node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
  // 因为合并了，所以parent需要砍掉一个key
  parent->Remove(index);
  bool if_delete = false;
  if (parent->GetSize() < parent->GetMinSize()) {
    if_delete = CoalesceOrRedistribute(parent, transaction);
  }
  return if_delete;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  auto parent_page_id = node->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
  if (index == 0) {
    auto key = parent->KeyAt(parent->ValueIndex(neighbor_node->GetPageId()));
    neighbor_node->MoveFirstToEndOf(node, key, buffer_pool_manager_);
    parent->SetKeyAt(parent->ValueIndex(neighbor_node->GetPageId()), neighbor_node->KeyAt(0));
  } else {
    auto key = parent->KeyAt(parent->ValueIndex(node->GetPageId()));
    neighbor_node->MoveLastToFrontOf(node, key, buffer_pool_manager_);
    parent->SetKeyAt(parent->ValueIndex(node->GetPageId()), node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(false);
      return true;
    }
    return false;
  }
  // 不是叶子则是internal
  if (old_root_node->GetSize() == 1) {
    // 如果根节点只有1了，说明不需要这个根了，让孩子成为新根
    auto root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(old_root_node);
    root_page_id_ = root->ValueAt(0);
    UpdateRootPageId(false);
    // 注意，我们也需要修正一下新根的parent为invalid
    auto new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    auto new_root = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  KeyType key{};
  Page * page = FindLeafPage(key, true);
  auto leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  return INDEXITERATOR_TYPE(true, &comparator_, leaf_page, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *page = FindLeafPage(key, false);
  auto leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  return INDEXITERATOR_TYPE(false, &comparator_, leaf_page, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  //end传的page跟begin一样，都是left most，后续工作在index_iterator中进行
  KeyType key{};
  Page *page = FindLeafPage(key, true);
  auto leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  return INDEXITERATOR_TYPE(false, &comparator_, leaf_page, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 * 意思是如果leftMost，那么只返回最最左边的那一片叶子page，否则就是正常的全局搜索
 * 注意，这里并不需要查找key是否确切存在，只需要返回可能的那片叶子
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  auto node = buffer_pool_manager_->FetchPage(root_page_id_);
  while (!reinterpret_cast<BPlusTreePage *>(node->GetData())->IsLeafPage()) {
    auto internal_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node->GetData());
    page_id_t child_page_id;
    if (leftMost) {
      child_page_id = internal_node->ValueAt(0);
    } else {
      child_page_id = internal_node->Lookup(key, comparator_);
    }
    buffer_pool_manager_->UnpinPage(internal_node->GetPageId(), false);
    node = buffer_pool_manager_->FetchPage(child_page_id);
  }
  // 退出循环后，这里已经是叶子节点
  return node;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto index_roots_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  if (index_roots_page == nullptr) {
    throw Exception("out of memory");
  }
  auto index_roots = reinterpret_cast<IndexRootsPage *>(index_roots_page->GetData());
  if (insert_record) {
    index_roots->Insert(index_id_, root_page_id_);
  } else {
    index_roots->Update(index_id_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template class BPlusTree<int, int, BasicComparator<int>>;

template class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;

template class BPlusTree<GenericKey<128>, RowId, GenericComparator<128>>;
