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
  // 1.magic_num 4 2.SerializeSize 4 3.name长度 4 4.name实际数据 name.size()*1 5.type_id(enum类型实际都是int) 4
  // 6.len_ 4      7.table_ind_ 4 8.nullable_ 1 9.unique_ 1

  uint32_t ofs = 0;
  // write magic number
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
  buf += 4;
  ofs += 4;
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
  // 具体见SerializeTo
  return MACH_STR_SERIALIZED_SIZE(name_)+22;
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  // replace with your code here
  //read magic number
  uint32_t ofs = 0;
  uint32_t mag = MACH_READ_UINT32(buf);
  ASSERT(mag==210928,"Not Column!");
  buf+=4;
  ofs+=4;
  // test if empty
  ASSERT(column == nullptr, "Pointer to column is not null in column deserialize.");
  //在heap中返回新生成的对象
  ALLOC_P(heap, Column)(column->name_, column->type_, column->table_ind_, column->nullable_, column->unique_);
  /* deserialize field from buf */
  //read name length
  uint32_t len = MACH_READ_UINT32(buf);
  buf+=4;
  ofs+=4;
  //read name one char by one char
  column->name_ = new char[len];
  for(uint32_t i=0;i<len;i++){
    column->name_[i]=MACH_READ_FROM(char,buf);
    buf++;
    ofs++;
  }
  //read len_
  column->len_ = MACH_READ_UINT32(buf);
  buf+=4;
  ofs+=4;
  //read table_ind
  column->table_ind_ = MACH_READ_UINT32(buf);
  buf+=4;
  ofs+=4;
  //read nullable. bool and char are both 1 byte, so we can just read one char
  column->nullable_ = MACH_READ_FROM(char,buf);
  buf++;
  ofs++;
  //read unique. bool and char are both 1 byte, so we can just read one char
  column->unique_ = MACH_READ_FROM(char,buf);
  buf++;
  ofs++;
  //return ofs
  return ofs;
}
