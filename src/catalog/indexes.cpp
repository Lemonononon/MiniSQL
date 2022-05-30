#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}
/*
 * 序列化IndexMetadata,虽然文档和头文件都没有给出提示，但是根据part2猜测返回值应该是偏移量
 * 在此处没有储存string的长度，而是试图直接用MACH_READ_FROM的方法来读取string,可能会有问题
 * 序列化顺序:
 * 1.magic_num
 * 2.index_id_
 * 3.index_name_
 * 4.table_id_
 * 5.key_map_.size()
 * 6.key_map_
 */
uint32_t IndexMetadata::SerializeTo(char *buf) const {
  //记录偏移量
  uint32_t ofs = 0;
  //写入magic_number
  MACH_WRITE_UINT32(buf,INDEX_METADATA_MAGIC_NUM);
  buf+=4;ofs+=4;
  //写入index_id_t
  MACH_WRITE_UINT32(buf,index_id_);
  buf+=4;ofs+=4;
  //写入index_name_
  MACH_WRITE_INT32(buf, index_name_.size());
  buf+=4;
  ofs+=4;
  MACH_WRITE_STRING(buf, index_name_);
  buf += index_name_.length();
  ofs += index_name_.length();
  //写入table_id_t
  MACH_WRITE_UINT32(buf,table_id_);
  buf+=4;ofs+=4;
  //写入key_map_size()
  MACH_WRITE_INT32(buf,key_map_.size());
  buf+=4;ofs+=4;
  //写入key_map_
  auto key_map_it = key_map_.begin();
  while(key_map_it!=key_map_.end()){
    MACH_WRITE_UINT32(buf,*key_map_it);
    buf+=4;ofs+=4;
    key_map_it++;
  }
  return ofs;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  //magic_num(4)+index_id_t(4)+index_name_(MACH_STR_SERIALIZED_SIZE(index_name_))+
  // table_id_t(4)+key_map_size(4)+4*key_map_size
  return 16+MACH_STR_SERIALIZED_SIZE(index_name_)+4*key_map_.size();
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  //记录偏移量
  uint32_t ofs = 0;
  //check magic_number
  if (MACH_READ_UINT32(buf) != INDEX_METADATA_MAGIC_NUM) {
    cout << "ERROR: NOT INDEX_METADATA MAGIC_NUMBER!" << endl;
    return 0;
  }
  //read index_id_t
  uint32_t index_id_t = MACH_READ_UINT32(buf);
  buf+=4;ofs+=4;
  //read index_name_
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
  std::string index_name_(name);
  //read table_id_t
  uint32_t table_id_t = MACH_READ_UINT32(buf);
  buf+=4;ofs+=4;
  //read key_map_size()
  int32_t key_map_size = MACH_READ_INT32(buf);
  buf+=4;ofs+=4;
  //read key_map_
  std::vector<uint32_t> key_map_;
  uint32_t key_map_element;
  for(int i = 0; i < key_map_size; i++){
    key_map_element = MACH_READ_UINT32(buf);
    buf+=4;ofs+=4;
    key_map_.push_back(key_map_element);
  }
  //调用create函数,将反序列化得到的数据填到heap
  index_meta = IndexMetadata::Create(index_id_t,index_name_,table_id_t,key_map_,heap);
  return ofs;
}