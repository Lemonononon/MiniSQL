#ifndef MINISQL_INDEX_ITERATOR_H
#define MINISQL_INDEX_ITERATOR_H

#include "page/b_plus_tree_leaf_page.h"

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  explicit IndexIterator( const KeyType & key, KeyComparator *comparator, Page *, BufferPoolManager *buffer_pool_manager);

  //type:true-begin, false-end
  explicit IndexIterator( bool type, KeyComparator *comparator, Page *, BufferPoolManager *buffer_pool_manager);

  ~IndexIterator();

  /** Return the key/value pair this iterator is currently pointing at. */
  const MappingType &operator*();

  /** Move to the next key/value pair.*/
  IndexIterator &operator++();

  /** Return whether two iterators are equal */
  bool operator==(const IndexIterator &itr) const;

  /** Return whether two iterators are not equal. */
  bool operator!=(const IndexIterator &itr) const;

private:
  // add your own private member variables here
 int index_; //用来取值的index，0 ~ size-1
 int size_; //当前结点的size
 B_PLUS_TREE_LEAF_PAGE_TYPE *page_; //目前在哪个结点
 KeyComparator *comparator_;
 BufferPoolManager *buffer_pool_manager_;
};


#endif //MINISQL_INDEX_ITERATOR_H
