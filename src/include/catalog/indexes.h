#ifndef MINISQL_INDEXES_H
#define MINISQL_INDEXES_H

#include <memory>

#include "catalog/table.h"
#include "index/generic_key.h"
#include "index/b_plus_tree_index.h"
#include "record/schema.h"

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
                         const table_id_t table_id, const std::vector<uint32_t> &key_map) {}

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
    // 从table_info_中获取columns_
    vector<Column *> my_columns_ = table_info_->GetSchema()->GetColumns();
    // 将每一个column在table中的序号push到column_index中
    for(auto & my_column : my_columns_){
      column_index.push_back(my_column->GetTableInd());
    }
    key_schema_ = Schema::ShallowCopySchema(table_info_->GetSchema(),column_index,heap_);
    // Step3: call CreateIndex to create the index
    index_ = CreateIndex(buffer_pool_manager);
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
  Index *CreateIndex(BufferPoolManager *buffer_pool_manager) {
      uint32_t keyLength = (*(key_schema_->GetColumns().begin()))->GetLength();
      switch (keyLength) {
        case 32:
          return new BPlusTreeIndex<GenericKey<32>,IndexSchema *,BufferPoolManager*>
              (meta_data_->GetIndexId(),key_schema_,buffer_pool_manager);
          break;
        case 64:
          return new BPlusTreeIndex<GenericKey<64>,IndexSchema *,BufferPoolManager*>
              (meta_data_->GetIndexId(),key_schema_,buffer_pool_manager);
          break;
        default:
          //表示使用字符串作为索引
          std::cout << "WARNING: Not prepared yet for string. From indexes.h:CreateIndex" << std::endl;
          break;
      }



  }

private:
  IndexMetadata *meta_data_;
  Index *index_;
  TableInfo *table_info_;
  IndexSchema *key_schema_;
  MemHeap *heap_;
};

#endif //MINISQL_INDEXES_H
