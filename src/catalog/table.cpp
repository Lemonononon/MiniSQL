#include "catalog/table.h"

/*
 * 序列化TableMetadata,虽然文档和头文件都没有给出提示，但是根据part2猜测返回值应该是偏移量
 * 在此处没有储存string的长度，而是试图直接用MACH_READ_FROM的方法来读取string,可能会有问题
 * 序列化顺序:
 * 1.magic_num
 * 2.table_id_
 * 3.table_name_
 * 4.root_page_id_
 * 6.schema_
 */
uint32_t TableMetadata::SerializeTo(char *buf) const {
  // 记录偏移量
  uint32_t ofs = 0;
  // 写入magic_number
  MACH_WRITE_UINT32(buf, TABLE_METADATA_MAGIC_NUM);
  buf += 4;
  ofs += 4;
  // 写入table_id_t
  MACH_WRITE_UINT32(buf, table_id_);
  buf += 4;
  ofs += 4;
  // 写入table_name_
  MACH_WRITE_INT32(buf,table_name_.size());
  buf+=4;
  ofs+=4;
  MACH_WRITE_STRING(buf, table_name_);
  buf += table_name_.length();
  ofs += table_name_.length();
  // 写入root_page_id_
  MACH_WRITE_INT32(buf, root_page_id_);
  buf += 4;
  ofs += 4;
  // 写入schema
  schema_->SerializeTo(buf);
  buf += schema_->GetSerializedSize();
  ofs += schema_->GetSerializedSize();
  return ofs;
}

uint32_t TableMetadata::GetSerializedSize() const {
  // magic_number(4)+table_id_t(4)+table_name_(MACH_STR_SERIALIZED_SIZE(table_name_))+root_page_id_(4)
  return 12 + MACH_STR_SERIALIZED_SIZE(table_name_) + schema_->GetSerializedSize();
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  // 记录偏移量
  uint32_t ofs = 0;
  // check magic_number
  if (MACH_READ_UINT32(buf) != TABLE_METADATA_MAGIC_NUM) {
    cout << "ERROR: NOT INDEX_METADATA MAGIC_NUMBER!" << endl;
    return 0;
  }
  buf += 4;
  ofs += 4;
  // 读取table_id_t
  uint32_t table_id_ = MACH_READ_UINT32(buf);
  buf += 4;
  ofs += 4;
  // 读取table_name_
  //先读取table_name_的长度
  uint32_t len = MACH_READ_INT32(buf);
  buf += 4;
  ofs += 4;
  // 一个字符一个字符地读取
  char *name = new char[len];
  ofs += len;
  for (uint32_t i = 0; i < len; i++) {
    name[i] = MACH_READ_FROM(char, buf);
    buf++;
  }
  name[len] = 0;
  std::string table_name_(name);
  // 读取root_page_id_
  int32_t root_page_id_ = MACH_READ_INT32(buf);
  buf += 4;
  ofs += 4;
  // 读取schema
  Schema *schema_;
  Schema::DeserializeFrom(buf,schema_,heap);
  buf += schema_->GetSerializedSize();
  ofs += schema_->GetSerializedSize();
  // 调用create函数,将反序列化得到的数据填到heap
  table_meta = TableMetadata::Create(table_id_, table_name_, root_page_id_, (TableSchema *)schema_, heap);
  return ofs;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name, page_id_t root_page_id,
                                     TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new (buf) TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
    : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
