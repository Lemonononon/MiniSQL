#include "catalog/catalog.h"

/*
 * 序列化catalog的meta_data,看似不需要像record里面那样返回偏移值
 * 序列化顺序:
 * 1.magic_number
 * 2.table_meta_pages_.size()
 * 3.index_meta_pages_.size()
 * 4.table_meta_pages_,每次写入两个byte,分别为table_id_t, page_id_t
 * 5.index_meta_pages_,每次写入两个byte,分别为index_id_t, page_id_t
 */
void CatalogMeta::SerializeTo(char *buf) const {
  // write magic_number
  MACH_WRITE_INT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  // write table_meta_pages_.size()
  MACH_WRITE_INT32(buf, table_meta_pages_.size());
  buf += 4;
  // write index_meta_pages_.size()
  MACH_WRITE_INT32(buf, index_meta_pages_.size());
  buf += 4;
  // write table_meta_pages_
  auto table_meta_pages_it = table_meta_pages_.begin();
  while (table_meta_pages_it != table_meta_pages_.end()) {
    MACH_WRITE_UINT32(buf, table_meta_pages_it->first);
    buf += 4;
    MACH_WRITE_INT32(buf, table_meta_pages_it->second);
    buf += 4;
    table_meta_pages_it++;
  }
  // write index_meta_pages_
  auto index_meta_pages_it = index_meta_pages_.begin();
  while (index_meta_pages_it != index_meta_pages_.end()) {
    MACH_WRITE_UINT32(buf, index_meta_pages_it->first);
    buf += 4;
    MACH_WRITE_INT32(buf, index_meta_pages_it->second);
    buf += 4;
    index_meta_pages_it++;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  // ASSERT(false, "Not Implemented yet");
  return nullptr;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  // ASSERT(false, "Not Implemented yet");
  return 0;
}

CatalogMeta::CatalogMeta() {}

CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager),
      lock_manager_(lock_manager),
      log_manager_(log_manager),
      heap_(new SimpleMemHeap()) {
  // ASSERT(false, "Not Implemented yet");
}

CatalogManager::~CatalogManager() { delete heap_; }

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Transaction *txn,
                                    TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}