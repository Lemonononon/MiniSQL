#include "record/column.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  // bool由于大小为1位，被我当作char存了
  // 写入的顺序以及字节大小：
  // 1.magic_num 4 2.name长度 4 3.name实际数据 name.size()*1 4.type_id(enum类型实际都是int) 4
  // 5.len_ 4      6.table_ind_ 4 7.nullable_ 1 8.unique_ 1

  uint32_t ofs = 0;
  // write magic number
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
  buf += 4;
  ofs += 4;
  // write column name
  MACH_WRITE_UINT32(buf, name_.size());
  buf += 4;
  MACH_WRITE_STRING(buf, name_);
  buf += name_.length();
  ofs += MACH_STR_SERIALIZED_SIZE(name_);
  // write type_id
  MACH_WRITE_UINT32(buf, type_);
  buf += 4;
  ofs += 4;
  // write len_
  MACH_WRITE_UINT32(buf, len_);
  buf += 4;
  ofs += 4;
  // write table_ind_
  MACH_WRITE_UINT32(buf, table_ind_);
  ofs += 4;
  buf += 4;
  // write nullable_
  MACH_WRITE_TO(char, buf, nullable_);
  buf++;
  ofs++;
  // write unique_
  MACH_WRITE_TO(char, buf, unique_);
  buf++;
  ofs++;
  return ofs;
}

uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  return 0;
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  // replace with your code here
  return 0;
}
