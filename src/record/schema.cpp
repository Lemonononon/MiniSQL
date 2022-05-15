#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t ofs = 0;
  // write magic num
  MACH_WRITE_UINT32(buf,SCHEMA_MAGIC_NUM);
  ofs += 4;
  buf += 4;
  // write size of columns_
  MACH_WRITE_UINT32(buf, GetColumnCount());
  ofs += 4;
  buf += 4;
  // write column pointer(8 byte) one by one
  for (uint32_t i = 0; i < GetColumnCount(); i++) {
    MACH_WRITE_TO(Column *, buf, columns_[i]);
    buf += 8;
    ofs += 8;
  }
  return ofs;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  // magic number(4 byte) + column size(4 byte) + pointers in columns_(8 byte one pointer)
  if (columns_.size() != 0)
    return 4 + 8 * columns_.size();
  else
    return 0;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  // replace with your code here
  uint32_t ofs = 0;
  // read magic num
  ASSERT(MACH_READ_UINT32(buf)==200715,"Not schema!");
  ofs += 4;
  buf += 4;
  // read size of cols
  uint32_t size = MACH_READ_UINT32(buf);
  ofs += 4;
  buf += 4;
  // 利用buf内数据赋值给schema,再将schema放入heap
  std::vector<Column *> c;
  for (uint32_t i = 0; i < size; i++) {
    c.emplace_back((Column *)MACH_READ_FROM(Column *, buf));  // emplace_back is push_back with judge
    buf += 8;
    ofs += 8;
  }
  schema = ALLOC_P(heap, Schema)(c);
  return ofs;
}