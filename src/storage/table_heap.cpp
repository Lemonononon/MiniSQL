#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  // if tuple is too big, return false
  // PAGE_SIZE-32 means the remaining space after init page
  if (row.GetSerializedSize(schema_) > PAGE_SIZE - 32) {
    return false;
  }
  // Find a page that can contain this tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(GetFirstPageId()));
  while (true) {
    // If the page could not be found, then abort the transaction.
    if (page == nullptr) {
      return false;
    }
    // Otherwise, insert tuple.
    page->WLatch();
    // 若insert完成,则返回true
    if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
      return true;
    }
    // 否则继续找下一个page是否可以insert
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    page_id_t next = page->GetNextPageId();
    // 若当前page不是最后一个page则继续循环,是最后一个就new一个page
    if (next != INVALID_PAGE_ID) {
      page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next));
    } else {
      // new一个page并完成插入
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next));
      page->SetNextPageId(next);
      new_page->Init(next, page->GetPageId(), log_manager_, txn);
      new_page->WLatch();
      new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
      new_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(next, true);  // 存疑
      return true;
    }
  }
}
bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }

  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  // rid is old row, get its page and update
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(page== nullptr) return false;
  Row old(rid);
  // get old row by get_tuple
  if (!GetTuple(&old, txn)) {
    return false;
  }
  // update old row
  page->WLatch();
  int type = page->UpdateTuple(row, &old, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  // if type==0, success, return true
  // if type==1 or type==2, fail, return false
  // if type==3, space is not enough, we need delete and insert
  if (type == 0) {
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
  } else {
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;
  }
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // delete
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if (page == nullptr) return false;
  page->RLatch();
  if (!page->GetTuple(row, schema_, txn, lock_manager_)) {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return true;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  RowId rid;
  page->RLatch();
  page->GetFirstTupleRid(&rid);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(first_page_id_, false);
  return TableIterator(this,rid);
}

TableIterator TableHeap::End() {
  // 默认page_id = INVALID_PAGE_ID slot_num为0
  RowId rid;
  return  TableIterator(this,rid);
}
