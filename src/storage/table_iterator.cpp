#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(TableHeap * t,RowId rid): table_heap_(t){
    Row r(rid);
    row_  = &r;
}

TableIterator::TableIterator(const TableIterator &other) {
    table_heap_ = other.table_heap_;
    row_ = other.row_;
}

TableIterator::~TableIterator() {
    delete row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return  row_->GetRowId() == itr.row_->GetRowId() ? true: false;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return  row_->GetRowId() == itr.row_->GetRowId() ? false: true;
}

const Row &TableIterator::operator*() {
    return *(row_);
}

Row *TableIterator::operator->() {
  return row_;
}

TableIterator &TableIterator::operator++() {
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(row_->GetRowId().GetPageId()));
  //如果是本页最后一个row，则跳到下一页
  RowId new_rid;
  RowId old_rid = row_->GetRowId();
  page->RLatch();
  while(!page->GetNextTupleRid(old_rid,&new_rid)){
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(old_rid.GetPageId(), false);
    page_id_t new_page_id = page->GetNextPageId();
    page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(new_page_id));
    page->RLatch();
    page->GetFirstTupleRid(&old_rid);
    //待续
  }

}

TableIterator TableIterator::operator++(int) {
  return TableIterator();
}
