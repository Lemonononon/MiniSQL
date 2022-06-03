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
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
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
      heap_(new SimpleMemHeap()) {

  if (init) {
    // step1: 实例化一个新的CatalogMeta
    catalog_meta_ = CatalogMeta::NewInstance(heap_);
    // 刷新CatalogManager的几个nextid
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
  } else {
    // step1: 反序列化CatalogMetadata
    Page *meta_data_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
//    meta_data_page->RLatch();
    catalog_meta_ = CatalogMeta::DeserializeFrom(meta_data_page->GetData(), heap_);
//    meta_data_page->RUnlatch();
    // step2: 刷新CatalogManager的几个nextid
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
    // step3: 更新CatalogManager的数据
    // 根据catalogMeta中的数据更新Table和Index
    for (auto table_meta_page_it : catalog_meta_->table_meta_pages_) {
      ASSERT(LoadTable(table_meta_page_it.first, table_meta_page_it.second)==DB_SUCCESS,"LoadTable Failed!");
    }
    for (auto index_meta_page_it : catalog_meta_->index_meta_pages_) {
      ASSERT(LoadIndex(index_meta_page_it.first, index_meta_page_it.second)==DB_SUCCESS,"LoadIndex Failed");
    }
  }
}

CatalogManager::~CatalogManager() {
  //我不是很清楚什么时候需要Flush,以防万一，现在这里Flush一下
  FlushCatalogMetaPage();
  delete heap_;
}

// 新建Table,将做好的table_info返回到参数里
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Transaction *txn,
                                    TableInfo *&table_info) {
  // step1: 检查table是否已经存在
  if (table_names_.find(table_name) != table_names_.end()) return DB_TABLE_ALREADY_EXIST;
  // step2: 新建TableInfo,TableMetaData,TableHeap
  table_info = TableInfo::Create(heap_);
  table_id_t table_id = next_table_id_++;
  page_id_t table_page;
  ASSERT(buffer_pool_manager_->NewPage(table_page) != nullptr, "Get a nullptr in New a table Page from CreateTable");
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_page, schema, log_manager_, lock_manager_,
                                            table_info->GetMemHeap());
  // 这里直接拿了table_heap的FirstPageId,但是这个table_heap
  TableMetadata *meta_data = TableMetadata::Create(table_id, table_name, table_page, schema, table_info->GetMemHeap());
  table_info->Init(meta_data, table_heap);
  // step3: 更新CatalogManager和CatalogMetaData
  table_names_[table_name] = table_id;
  tables_[table_id] = table_info;
  // 新建一个数据页来序列化table_meta_data
  page_id_t meta_data_page_id;
  Page *meta_data_page = buffer_pool_manager_->NewPage(meta_data_page_id);
  ASSERT(meta_data_page != nullptr, "Get a nullptr in New a table meta data Page from CreateTable");
//  meta_data_page->WLatch();
  meta_data->SerializeTo(meta_data_page->GetData());
//  meta_data_page->WUnlatch();
  catalog_meta_->table_meta_pages_[table_id] = meta_data_page_id;
  return DB_SUCCESS;
}

// 通过分析应该是查找table_name，然后将其对应的table_info存到给的参数里面
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto table_id_it = table_names_.find(table_name);
  if (table_id_it == table_names_.end()) return DB_TABLE_NOT_EXIST;
  // 返回private方法GetTable的数据库状态，并把查到的内容填到table_info里
  return GetTable(table_id_it->second, table_info);
}

// 我的理解是把所有的table的TableInfo都存到参数里面
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for (auto table : tables_) {
    tables.push_back(table.second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  // step1: 检查Table是否已经存在，Index是否已经存在
  if (table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  if (index_names_[table_name].find(index_name) != index_names_[table_name].end()) return DB_INDEX_ALREADY_EXIST;
  // step2: 新建IndexMetaData,IndexInfo
  index_info = IndexInfo::Create(heap_);
  index_id_t index_id = next_index_id_++;
  table_id_t table_id = table_names_.find(table_name)->second;
  TableInfo *table_info = tables_[table_id];
  // 从对应的table里面检索index_keys对应的下标
  std::vector<uint32_t> key_map;
  for (const auto &index_key_name : index_keys) {
    uint32_t key_index;
    if (table_info->GetSchema()->GetColumnIndex(index_key_name, key_index) == DB_COLUMN_NAME_NOT_EXIST)
      return DB_COLUMN_NAME_NOT_EXIST;
    key_map.push_back(key_index);
  }
  // 新建IndexMetaData并init index_info
  IndexMetadata *meta_data = IndexMetadata::Create(index_id, index_name, table_id, key_map, index_info->GetMemHeap());
  index_info->Init(meta_data, table_info, buffer_pool_manager_);
  // step3: 更新CatalogMetaData和CatalogManager
  // 更新index_names_
  if (index_names_.find(table_name) == index_names_.end()) {
    // index_names_里面没有该table
    std::unordered_map<std::string, index_id_t> map;
    map[index_name] = index_id;
    index_names_[table_name] = map;
  } else {
    // index_names_里面已有对应的table
    index_names_.find(table_name)->second[index_name] = index_id;
  }
  // 更新indexes_
  indexes_[index_id] = index_info;
  // 新建一个数据页来序列化IndexMetaData
  page_id_t meta_data_page_id;
  Page *meta_data_page = buffer_pool_manager_->NewPage(meta_data_page_id);
  ASSERT(meta_data_page != nullptr, "Get a nullptr in New a index meta data Page from CreateIndex");
//  meta_data_page->WLatch();
  meta_data->SerializeTo(meta_data_page->GetData());
//  meta_data_page->WUnlatch();
  catalog_meta_->index_meta_pages_[index_id] = meta_data_page_id;

  return DB_SUCCESS;
}

// 将table_name对应的table中的index_name对应的Index的IndexInfo放入参数中
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // 获取对应的table存放的Index情况
  auto index_name_it = table_names_.find(table_name);
  if (index_name_it == table_names_.end()) return DB_TABLE_NOT_EXIST;
  // 获取对应的Index存放的情况
  auto table_index_it = index_names_.find(table_name);
  if (table_index_it == index_names_.end())
    // 查看index_names里有没有这个table_name，如果没有的话，说明这个table没有index
    return DB_INDEX_NOT_FOUND;
  else {
    auto index_id_it = table_index_it->second.find(index_name);
    if (index_id_it == table_index_it->second.end()) return DB_INDEX_NOT_FOUND;
    // 从indexes_中拿到对应的Info并赋值
    auto index_info_it = indexes_.find(index_id_it->second);
    if (index_info_it == indexes_.end()) return DB_FAILED;
    index_info = index_info_it->second;
  }

  return DB_SUCCESS;
}

// 根据table_name，将对应所有的index的IndexInfo放到参数里面
// WARNING: 如果该table没有index,将返回原indexes以及DB_SUCCESS
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // 获取对应的table存放的Index情况
  auto table_map_it = table_names_.find(table_name);
  if (table_map_it == table_names_.end()) return DB_TABLE_NOT_EXIST;
  // 获取对应的Index存放的情况并Push到indexes中
  if (index_names_.find(table_name) == index_names_.end()) {
    // 该table没有对应的index, do nothing
  } else {
    // 该table有对应的Index
    for (const auto &index_map : index_names_.find(table_name)->second) {
      auto indexes_it = indexes_.find(index_map.second);
      if (indexes_it == indexes_.end()) return DB_FAILED;
      indexes.push_back(indexes_it->second);
    }
  }

  return DB_SUCCESS;
}

// 删除对应名字的table
dberr_t CatalogManager::DropTable(const string &table_name) {
  // 我认为删除数据库的时候不需要回收table的table_id,因为没有重新利用的价值
  // 目前没有回收table对应的各个info,heap的内存的打算(metadata序列化的页将会回收)，仅单独从map中删除

  // step1:查找是否存在table
  auto table_id_it = table_names_.find(table_name);
  if (table_id_it == table_names_.end()) return DB_TABLE_NOT_EXIST;
  table_id_t table_id = table_id_it->second;
  // step2:删除储存table的页和储存matadata的页
  if (!buffer_pool_manager_->DeletePage(tables_[table_id]->GetRootPageId())) return DB_FAILED;
  if (!buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_[table_id])) return DB_FAILED;
  // step3:删除各个map中对应的table
  tables_.erase(tables_.find(table_id));
  table_names_.erase(table_names_.find(table_name));
  catalog_meta_->table_meta_pages_.erase(catalog_meta_->table_meta_pages_.find(table_id));
  // 回收各个map中该table的index
  if (index_names_.find(table_name) != index_names_.end()) {
    for (const auto &index_pair : index_names_[table_name]) {
      indexes_.erase(index_pair.second);
    }
    index_names_.erase(table_name);
  }

  return DB_SUCCESS;
}

//根据参数删除对应的index
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // 同DropTable,目前并没有回收index_id以及info占用的内存的打算
  // step1: 查找是否存在table 以及 index
  // 查找table是否存在
  if (table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto table_index_it = index_names_.find(table_name);
  // 查找是否这个table没有index
  if (table_index_it == index_names_.end()) {
    return DB_INDEX_NOT_FOUND;
  } else {
    // 查找是否这个table的Index没有所求的Index
    auto index_name_it = table_index_it->second.find(index_name);
    if (index_name_it == table_index_it->second.end())
      return DB_INDEX_NOT_FOUND;
    else {
      // 存在该index
      // step2: 删除该索引以及存放metadata的数据页
      index_id_t index_id = index_name_it->second;
      IndexInfo *index_info = indexes_[index_id];
      // 删除索引
      index_info->GetIndex()->Destroy();
      // 删除存放metadata的页
      if (!buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_[index_id])) return DB_FAILED;
      // step3: 删除各个map中对应的index
      // 从index_names_删除该index
      if (table_index_it->second.size() == 1) {
        // 如果这个table只有这个Index
        index_names_.erase(table_index_it);
      } else {
        table_index_it->second.erase(index_name);
      }
      // 从indexes_中删除该index
      indexes_.erase(index_id);
    }
  }
  return DB_SUCCESS;
}

// 将CatalogMeta里面的数据写到page中
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // 直接序列化CatalogMetaData到数据页中
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  ASSERT(catalog_meta_page != nullptr, "read catalog_meta_page failed!");
//  catalog_meta_page->WLatch();
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
//  catalog_meta_page->WUnlatch();
  // 立即将数据转存到磁盘
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);

  return DB_SUCCESS;
}

// 读取page_id存的table_meta_data,并更新CatalogManager
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // step1: 拿到存meta_data的页
  auto meta_data_page = buffer_pool_manager_->FetchPage(page_id);
  ASSERT(meta_data_page != nullptr, "Fetch Tabel_meta_data_page failed!");
  // step2: 新建TableMetaData并反序列化
  TableInfo *table_info = TableInfo::Create(heap_);
//  meta_data_page->RLatch();
  TableMetadata *meta_data;
  TableMetadata::DeserializeFrom(meta_data_page->GetData(), meta_data, table_info->GetMemHeap());
//  meta_data_page->RUnlatch();
  ASSERT(table_id == meta_data->GetTableId(), "False Table ID in LoadTable!");
  // step3: 插入table_names_
  table_names_[meta_data->GetTableName()] = table_id;
  // step4: init table_info插入tables_
  // 新建table_heap
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, meta_data->GetFirstPageId(), meta_data->GetSchema(),
                                            log_manager_, lock_manager_, table_info->GetMemHeap());
  table_info->Init(meta_data, table_heap);
  tables_[table_id] = table_info;

  return DB_SUCCESS;
}

// 读取page_id存的table_meta_data,并更新CatalogManager
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // step1: 拿到存meta_data的页
  auto meta_data_page = buffer_pool_manager_->FetchPage(page_id);
  ASSERT(meta_data_page != nullptr, "Fetch Tabel_meta_data_page failed!");
  // step2: 新建IndexMetaData并反序列化
  IndexInfo *index_info = IndexInfo::Create(heap_);
//  meta_data_page->RLatch();
  IndexMetadata *meta_data;
  IndexMetadata::DeserializeFrom(meta_data_page->GetData(), meta_data, index_info->GetMemHeap());
//  meta_data_page->RUnlatch();
  ASSERT(index_id == meta_data->GetIndexId(), "False Index ID in LoadIndex!");
  // step3: 插入index_names_
  table_id_t table_id = meta_data->GetTableId();
  std::string index_name = meta_data->GetIndexName();
  std::string table_name = tables_[table_id]->GetTableName();
  // 更新CatalogManage的map
  if (table_names_.find(table_name) == table_names_.end()) {
    // 没有对应的table
    return DB_TABLE_NOT_EXIST;
  } else {
    // 有对应的table
    if (index_names_.find(table_name) == index_names_.end()) {
      // index_names_里面没有该table
      std::unordered_map<std::string, index_id_t> map;
      map[index_name] = index_id;
      index_names_[table_name] = map;
    } else {
      // index_names_里面已有对应的table
      index_names_.find(table_name)->second[index_name] = index_id;
    }
  }
  // step4: init index_info并插入indexes_
  index_info->Init(meta_data, tables_[table_id], buffer_pool_manager_);
  indexes_[index_id] = index_info;

  return DB_SUCCESS;
}

// private方法，将table_id的TableInfo填到参数table_info里
// 不会存在没有table_id的情况(应该)
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto table_it = tables_.find(table_id);
  if (table_it == tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info = table_it->second;
  return DB_SUCCESS;
}