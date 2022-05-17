#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"


INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator( const KeyType & key, KeyComparator *comparator, B_PLUS_TREE_LEAF_PAGE_TYPE *page, BufferPoolManager *buffer_pool_manager){
  comparator_ = comparator;
  buffer_pool_manager_ = buffer_pool_manager;
  page_ = page;
  index_ = page_->KeyIndex(key, *comparator);
  data_ = page_->GetItem(index_);
}

//type:true-begin, false-end
INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator( bool type, KeyComparator *comparator, B_PLUS_TREE_LEAF_PAGE_TYPE *page, BufferPoolManager *buffer_pool_manager){
  comparator_ = comparator;
  buffer_pool_manager_ = buffer_pool_manager;

  //begin
  if (type){
    index_ = 0; //从第一次结点中的第0号元素开始
    page_ = page;
    data_ = page_->GetItem(index_);
  }else{
    //end
    index_ = 0;
    page = nullptr;
    KeyType key{0};
    ValueType value;
    data_ = MappingType(key, value);
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
    //到达结尾之后再++，就返回一个end_iter
    static IndexIterator end_iter (false, comparator_, page_, buffer_pool_manager_);
    index_++;
    if (index_ >= page_->GetSize()){
      //需要切换到下一个叶结点
      if (page_->GetNextPageId() != INVALID_PAGE_ID){
        Page *next_page = buffer_pool_manager_->FetchPage(page_->GetNextPageId());
        page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page->GetData());
        index_ = 0;
      }else{
        //返回end
        *this =  end_iter;
      }
    }
    return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  KeyComparator kc(*comparator_);
  return kc( data_.first, itr.data_.first) == 0;
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
