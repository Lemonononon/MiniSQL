#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {

  // Find a page that can contain this tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row.GetRowId().GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, insert tuple.
  page->WLatch();
  // 若insert过程出错,则false
  if(!page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
    page->WUnlatch();
    return false;
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
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
  Row old(rid);
  // get old row by get_tuple
  if(!GetTuple(&old,txn)){
    return false;
  }
  // update old row
  page->WLatch();
  int type = page->UpdateTuple(row,&old,schema_,txn,lock_manager_,log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  // if type==0, success, return true
  // if type==1 or type==2, fail, return false
  // if type==3, space is not enough, we need delete and insert
  if(type==0){
    return true;
  }
  return false;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // delete
  page->WLatch();
  page->ApplyDelete(rid,txn,log_manager_);
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
  page->RLatch();
  if(!page->GetTuple(row,schema_,txn,lock_manager_)) {
    page->RLatch();
    return false;
  }
  page->RLatch();
  return true;
}

TableIterator TableHeap::Begin(Transaction *txn) { return TableIterator(); }

TableIterator TableHeap::End() { return TableIterator(); }
