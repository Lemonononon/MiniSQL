#ifndef MINISQL_INDEXES_H
#define MINISQL_INDEXES_H

#include <memory>

#include "catalog/table.h"
#include "index/generic_key.h"
#include "index/b_plus_tree_index.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"
#include "record/schema.h"
#include "utils/exception.h"

class IndexMetadata {
  friend class IndexInfo;

public:
  static IndexMetadata *Create(const index_id_t index_id, const std::string &index_name,
                               const table_id_t table_id, const std::vector<uint32_t> &key_map,
                               MemHeap *heap);

  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap);

  inline std::string GetIndexName() const { return index_name_; }

  inline table_id_t GetTableId() const { return table_id_; }

  uint32_t GetIndexColumnCount() const { return key_map_.size(); }

  inline const std::vector<uint32_t> &GetKeyMapping() const { return key_map_; }

  inline index_id_t GetIndexId() const { return index_id_; }

private:
  IndexMetadata() = delete;

  explicit IndexMetadata(const index_id_t index_id, const std::string &index_name,
                         const table_id_t table_id, const std::vector<uint32_t> &key_map) {
    index_id_ = index_id;
    index_name_ = index_name;
    table_id_ = table_id;
    key_map_ = key_map;
  }

private:
  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
  index_id_t index_id_;
  std::string index_name_;
  table_id_t table_id_;
  std::vector<uint32_t> key_map_;  /** The mapping of index key to tuple key */
};

/**
 * The IndexInfo class maintains metadata about a index.
 */
class IndexInfo {
public:
  static IndexInfo *Create(MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(IndexInfo));
    return new(buf)IndexInfo();
  }

  ~IndexInfo() {
    delete heap_;
  }

  void Init(IndexMetadata *meta_data, TableInfo *table_info, BufferPoolManager *buffer_pool_manager) {
    // Step1: init index metadata and table info
    meta_data_ = meta_data;
    table_info_ = table_info;
    // Step2: mapping index key to key schema
    vector<uint32_t> column_index;
    // 将metadata中的key_schema(即需要作为索引的Key)push到到column_index中
    for(auto & key_index : meta_data->GetKeyMapping()){
      column_index.push_back(key_index);
    }
    key_schema_ = Schema::ShallowCopySchema(table_info_->GetSchema(),column_index,heap_);
    // Step3: call CreateIndex to create the index
    index_ = CreateIndex(buffer_pool_manager);

    //我想了想，这块暂时不应该存在。如果是重新读取Index的话，那就会导致在原有的Index上重新InsertEntry
    //或许有关这一块的处理得加载CatalogManager::CreateIndex那里
    //目前的状态就是：索引必须在table刚创建的时候创建，不然table中原有的数据不会纳入索引
    //把整个table InsertEntry
//    TableHeap* table_heap = table_info->GetTableHeap();
//    //如果table不为空的话，我们需要InsertEntry
//    if(table_heap->Begin(nullptr) != table_heap->End()){
//      for(auto table_it = table_heap->Begin(nullptr); table_it != table_heap->End(); table_it++){
//        //制作一个只包括key的row
//        std::vector<Field> fields;
//        for(auto i : meta_data->GetKeyMapping()){
//          fields.push_back(*(table_it->GetField(i)));
//        }
//        Row row(fields);
//        index_->InsertEntry(row,table_it->GetRowId(),nullptr);
//      }
//
//    }
  }

  inline Index *GetIndex() { return index_; }

  inline std::string GetIndexName() { return meta_data_->GetIndexName(); }

  inline IndexSchema *GetIndexKeySchema() { return key_schema_; }

  inline MemHeap *GetMemHeap() const { return heap_; }

  inline TableInfo *GetTableInfo() const { return table_info_; }

private:
  explicit IndexInfo() : meta_data_{nullptr}, index_{nullptr}, table_info_{nullptr},
                         key_schema_{nullptr}, heap_(new SimpleMemHeap()) {}

  //我严重怀疑这个函数我写的有问题，到时候可能需要重点关注一下
  //感觉对于KeyLength的大小有点问题
  Index *CreateIndex(BufferPoolManager *buffer_pool_manager) {
      // 要先判断Index_id是否已经存在了，如果已经存在了的话就直接从读出来
      index_id_t index_id = meta_data_->GetIndexId();

      auto index_roots_page = buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID);
      if (index_roots_page == nullptr) {
        throw Exception("out of memory");
      }
      auto index_roots = reinterpret_cast<IndexRootsPage *>(index_roots_page->GetData());
      [[maybe_unused]]auto *page_id = new page_id_t();
      if(index_roots->GetRootId(index_id,page_id)){
        //如果找到了index_id对应的root,说明之前已经存过了
        auto index_page = buffer_pool_manager->FetchPage(*page_id);
        return reinterpret_cast<Index *>(index_page->GetData());
      }

      uint32_t keyLength = 0;
      for(auto column_it : key_schema_->GetColumns()){
        keyLength += column_it->GetLength();
      }
      ASSERT(keyLength <= 128, "WARNING: GenericKey is not big enough. From indexes.h:CreateIndex");
      if(keyLength <= 4)
        return new BPlusTreeIndex<GenericKey<4>, RowId, GenericComparator<4>>
            (meta_data_->GetIndexId(),key_schema_,buffer_pool_manager);
      else if(keyLength <= 8)
        return new BPlusTreeIndex<GenericKey<8>, RowId, GenericComparator<8>>
            (meta_data_->GetIndexId(),key_schema_,buffer_pool_manager);
      else if(keyLength <= 16)
          return new BPlusTreeIndex<GenericKey<16>, RowId, GenericComparator<16>>
            (meta_data_->GetIndexId(),key_schema_,buffer_pool_manager);
      else if(keyLength <= 32)
          return new BPlusTreeIndex<GenericKey<32>, RowId, GenericComparator<32>>
            (meta_data_->GetIndexId(),key_schema_,buffer_pool_manager);
      else if(keyLength <= 64)
        return new BPlusTreeIndex<GenericKey<64>, RowId, GenericComparator<64>>
            (meta_data_->GetIndexId(),key_schema_,buffer_pool_manager);
      else
        return new BPlusTreeIndex<GenericKey<64>, RowId, GenericComparator<64>>
            (meta_data_->GetIndexId(),key_schema_,buffer_pool_manager);

  }

private:
  IndexMetadata *meta_data_;
  Index *index_;
  TableInfo *table_info_;
  IndexSchema *key_schema_;
  MemHeap *heap_;
};

#endif //MINISQL_INDEXES_H
