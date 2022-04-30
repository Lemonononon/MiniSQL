#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t ofs = 0;
  // write magic number
  MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM);
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
  //magic number(4 byte) + pointers in columns_(8 byte one pointer)
  return 4+8*columns_.size();
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  // replace with your code here
  uint32_t ofs=0;
  //read magic number
  ASSERT(MACH_READ_UINT32(buf)==200715,"Not A Schema");
  ofs+=4;
  buf+=4;
  //在heap中返回新生成的对象
  schema = ALLOC_P(heap, Schema)(schema->columns_);
  //read column size
  uint32_t len = MACH_READ_UINT32(buf);
  buf+=4;
  ofs+=4;
  //read pointer one by one
  for(uint32_t i=0;i<len;i++){
    const Column * p = MACH_READ_FROM(Column *,buf);
    schema->columns_.size();
    schema->columns_.push_back();
  }


  return ofs;
}