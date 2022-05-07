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
  // bool大小也是1 byte
  // 写入的顺序以及字节大小：
  // 1.magic_num 4 2.name长度 4 3.name实际数据 name.size()*1 4.type_id(enum类型实际都是int) 4
  // 5.len_ 4      6.table_ind_ 4 7.nullable_ 1 8.unique_ 1

  uint32_t ofs = 0;
  // write column name
  MACH_WRITE_UINT32(buf, name_.size());
  buf += 4;
  ofs += 4;
  MACH_WRITE_STRING(buf, name_);
  buf += MACH_STR_SERIALIZED_SIZE(name_);
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
  MACH_WRITE_TO(bool, buf, nullable_);
  buf++;
  ofs++;
  // write unique_
  MACH_WRITE_TO(bool, buf, unique_);
  buf++;
  ofs++;
  return ofs;
}

uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  // 具体见SerializeTo
  return MACH_STR_SERIALIZED_SIZE(name_) + 18;
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  // replace with your code here

  // test if empty
  ASSERT(column == nullptr, "Pointer to column is not null in column deserialize.");
  uint32_t ofs = 0;
  /* deserialize field from buf */
  // read name length
  uint32_t len = MACH_READ_UINT32(buf);
  buf += 4;
  ofs += 4;
  // read name one char by one char
  char *name = new char[len];
  ofs += len;
  for (uint32_t i = 0; i < len; i++) {
    name[i] = MACH_READ_FROM(char, buf);
    buf++;
  }
  std::string column_name (name);
  // read type
  TypeId type = MACH_READ_FROM(TypeId, buf);
  buf += 4;
  ofs += 4;
  // read len_
  uint32_t l = MACH_READ_UINT32(buf);
  buf += 4;
  ofs += 4;
  // read table_ind
  uint32_t col_ind = MACH_READ_UINT32(buf);
  buf += 4;
  ofs += 4;
  // read nullable. bool and char are both 1 byte, so we can just read one char
  bool Nullable = MACH_READ_FROM(bool, buf);
  buf++;
  ofs++;
  // read unique. bool and char are both 1 byte, so we can just read one char
  bool uni = MACH_READ_FROM(bool, buf);
  buf++;
  ofs++;
  // 将新生成的对象放到heap中
  column = ALLOC_P(heap, Column)(column_name, type, l, col_ind, Nullable,
                                 uni);
  // return ofs
  return ofs;
}
