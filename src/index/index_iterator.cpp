#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator( const KeyType & key, KeyComparator *comparator, Page *page, BufferPoolManager *buffer_pool_manager){
  comparator_ = comparator;
  buffer_pool_manager_ = buffer_pool_manager;
  page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page);
  size_ = page_->GetSize();
  index_ = page_->KeyIndex(key, *comparator);
}

//type:true-begin, false-end
INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator( bool type, KeyComparator *comparator, Page *page, BufferPoolManager *buffer_pool_manager){
  comparator_ = comparator;
  buffer_pool_manager_ = buffer_pool_manager;
  auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page);
  size_ = leaf_node->GetSize(); //当前节点的size
  //begin
  if (type){
    index_ = 0; //从第一次结点中的第0号元素开始
  }else{
    //end
    index =
    page_id_t next_page_id;
    next_page_id = leaf_node->GetNextPageId();
    while(next_page_id != INVALID_PAGE_ID){
      Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
      leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page);
      next_page_id = leaf_node->GetNextPageId();
    }
    //leaf_node已经是最后一个叶节点了
  }
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {

}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {
//  ASSERT(false, "Not implemented yet.");
  return page_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
//  ASSERT(false, "Not implemented yet.");
    index_++;
    if (index_ >= size_){
      //需要切换到下一个叶结点
      if (page_->GetNextPageId() != INVALID_PAGE_ID){
        Page *next_page = buffer_pool_manager_->FetchPage(page_->GetNextPageId());
        page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page);
        size_ = page_->GetSize();
        index_ = 0;
      }else{
        return this->;
      }
    }
    return *this;
}


INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  KeyComparator kc(*comparator_);
  return kc( page_->GetItem(index_).first, itr.page_->GetItem(itr.index_).first) == 0;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
