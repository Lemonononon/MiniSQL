#include "record/row.h"
#include <iostream>
using namespace std;

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  // replace with your code here
  // 若整个row空 则啥也不写进去

  if (fields_.size() == 0) {
    return 0;
  }
  uint32_t ofs = 0;
  // write fields num
  uint32_t fields_num = fields_.size();
  MACH_WRITE_UINT32(buf, fields_num);
  ofs += 4;
  buf += 4;
  // write null bitmap
  //注意刚好fields_.size()是8的整数倍的情况
  uint32_t size = (uint32_t)ceil((double)fields_.size() / 8) * 8;
  uint32_t map_num = 0;
  uint32_t map[size];
  while (map_num < size / 8) {
    char bitmap = 0;
    for (uint32_t i = 0; i < 8; i++) {
      // 若该field不为空 则将bitmap中对应位置为1
      if ((map_num * 8 + i < fields_.size()) && (fields_.at(map_num * 8 + i)->IsNull() == false)) {
        bitmap = bitmap | (0x01 << (7 - i));
        map[map_num * 8 + i] = 1;
      } else
        map[map_num * 8 + i] = 0;
    }
    map_num++;
    MACH_WRITE_TO(char, buf, bitmap);
    ofs++;
    buf++;
  }


  // write fields_
  for (uint32_t i = 0; i < fields_.size(); i++) {
    // 不为空的field才被写入
    if (map[i]) {
      uint32_t t = fields_.at(i)->SerializeTo(buf);
      buf += t;
      ofs += t;
    }
  }
  return ofs;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  // replace with your code here
  // read fields num
  uint32_t ofs = 0;
  uint32_t field_num = MACH_READ_UINT32(buf);
  buf += 4;
  ofs += 4;
  // read null bitmap
  //注意刚好fields_.size()是8的整数倍的情况
  uint32_t size = (uint32_t)ceil((double)field_num / 8) * 8;
  uint32_t map_num = 0;
  uint32_t map[size];
  while (map_num < size / 8) {
    char bitmap = MACH_READ_FROM(char, buf);
    buf++;
    ofs++;
    for (uint32_t i = 0; i < 8; i++) {
      if (((bitmap >> (7 - i)) & 0x01) != 0) {  // if not null
        map[i + map_num * 8] = 1;
      } else {
        map[i + map_num * 8] = 0;
      }
    }
    map_num++;
  }
  // deserialize
  for (uint32_t i = 0; i < field_num; i++) {
    TypeId type = schema->GetColumn(i)->GetType();
    uint32_t t;
    Field *f;
    if (type == TypeId::kTypeInt) {
      f = new Field(TypeId::kTypeInt, 0);
    } else if (type == TypeId::kTypeChar) {
      f = new Field(TypeId::kTypeChar, const_cast<char *>(""), strlen(const_cast<char *>("")), false);
    } else if (type == TypeId::kTypeFloat) {
      f = new Field(TypeId::kTypeFloat, 0.0f);
    }
    if (map[i] == 0) {
      t = f->DeserializeFrom(buf, type, &f, true, heap_);
      ofs += t;
      buf += t;
    } else {
      t = f->DeserializeFrom(buf, type, &f, false, heap_);
      ofs += t;
      buf += t;
    }
    fields_.push_back(f);
  }
  return ofs;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  // replace with your code here

  // empty row return 0
  if (fields_.size() == 0) {
    return 0;
  }
  uint32_t sum = 0;
  // header size
  sum += 4 + (uint32_t)ceil((double)fields_.size() / 8);
  // fields
  for (uint32_t i = 0; i < fields_.size(); i++) {
    if (!fields_.at(i)->IsNull()) {
      sum += fields_.at(i)->GetSerializedSize();
    }
  }
  return sum;
}
