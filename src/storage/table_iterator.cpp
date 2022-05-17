#include "storage/table_iterator.h"
#include "common/macros.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(TableHeap *t, RowId rid) : table_heap_(t) {
  Row r(rid);
  row_ = &r;
}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_ = other.table_heap_;
  row_ = other.row_;
}

TableIterator::~TableIterator() {delete row_;}

inline bool TableIterator::operator==(const TableIterator &itr) const {
  return row_->GetRowId() == itr.row_->GetRowId() ? true : false;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return row_->GetRowId() == itr.row_->GetRowId() ? false : true;
}

const Row &TableIterator::operator*() { return *(row_); }

Row *TableIterator::operator->() { return row_; }

TableIterator &TableIterator::operator++() {
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(row_->GetRowId().GetPageId()));
  ASSERT(page != nullptr, "no available pages");
  RowId new_rid;
  RowId old_rid = row_->GetRowId();
  page->RLatch();
  // 如果是本页最后一个row，则跳到下一页
  while (!page->GetNextTupleRid(old_rid, &new_rid)) {
    page_id_t new_page_id = page->GetNextPageId();
    if (new_page_id == INVALID_PAGE_ID) {
      // 如果找遍了所有page都无空位 则返回一个指向堆尾的迭代器
      page->RUnlatch();
      table_heap_->buffer_pool_manager_->UnpinPage(old_rid.GetPageId(), false);
      row_->SetRowId(RowId(INVALID_PAGE_ID, 0));
      break;
    }
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(old_rid.GetPageId(), false);
    page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(new_page_id));
    page->RLatch();
    if (page->GetFirstTupleRid(&new_rid)) {
      // 找到了合适的row返回当前迭代器
      page->RUnlatch();
      table_heap_->buffer_pool_manager_->UnpinPage(new_page_id, false);
      row_->SetRowId(new_rid);
      table_heap_->GetTuple(row_, nullptr);
      break;
    }
  }
  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator p(table_heap_, row_->GetRowId());
  ++(*this);
  return TableIterator{p};
}
