#include "catalog/catalog.h"
using namespace std;
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
  // 在给定的内存池里new 一个 catalogMeta
  auto *De = new (heap->Allocate(sizeof(CatalogMeta))) CatalogMeta();
  // DeserializeForm
  // check magic number
  if (MACH_READ_UINT32(buf) != CATALOG_METADATA_MAGIC_NUM) {
    cout << "ERROR: NOT CATALOGMETA MAGIC_NUMBER!" << endl;
    return nullptr;
  }
  buf += 4;
  // read size
  int32_t table_meta_pages_size = MACH_READ_INT32(buf);
  buf += 4;
  int32_t index_meta_pages_size = MACH_READ_INT32(buf);
  buf += 4;
  // read into table_meta_pages_
  uint32_t table_id_t;
  int32_t page_id_t;
  for (int i = 0; i < table_meta_pages_size; i++) {
    table_id_t = MACH_READ_UINT32(buf);
    buf += 4;
    page_id_t = MACH_READ_INT32(buf);
    buf += 4;
    De->table_meta_pages_[table_id_t] = page_id_t;
  }
  // read into index_meta_pages
  uint32_t index_id_t;
  for (int i = 0; i < index_meta_pages_size; i++) {
    index_id_t = MACH_READ_UINT32(buf);
    buf += 4;
    page_id_t = MACH_READ_INT32(buf);
    buf += 4;
    De->index_meta_pages_[index_id_t] = page_id_t;
  }

  return De;
}

/* 返回序列化的大小
 * 有可能有问题，比如说在序列化之后，index_meta_pages_和table_meta_pages_又发生了变化
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  // magic_number(4)+table_meta_size(4)+index_meta_size(4)+8*each_element_in_map
  return 12 + 8 * (table_meta_pages_.size() + index_meta_pages_.size());
}

CatalogMeta::CatalogMeta() {}

CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager),
      lock_manager_(lock_manager),
      log_manager_(log_manager),
      heap_(new SimpleMemHeap()) {}

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