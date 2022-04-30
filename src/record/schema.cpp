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
  //read size of cols
  uint32_t size = MACH_READ_UINT32(buf);
  ofs+=4;
  buf+=4;
  //利用buf内数据先创造一个schema,再将此schema返回 模仿了schema.h 中的deep copy schema
  std::vector<Column *> cols;
  for(uint32_t i=0;i<size;i++){
    void * buffer  = heap->Allocate(sizeof(Column));
    cols.push_back(new(buffer)Column(MACH_READ_FROM(Column *,buf)));
    buf+=8;
    ofs+=8;
  }
  void * buffer = heap->Allocate(sizeof(schema));
  schema = new(buffer)Schema(cols);
  return ofs;
}