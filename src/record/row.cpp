#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  // replace with your code here
  // 若整个row空 则啥也不写进去
  if (fields_.size() == 0) {
    return 0;
  }
  uint32_t ofs = 0;
  // write rowid
  MACH_WRITE_TO(RowId, buf, rid_);
  ofs += sizeof(RowId);
  buf += sizeof(RowId);
  // write fields num
  MACH_WRITE_UINT32(buf, fields_.size());
  ofs += 4;
  buf += 4;
  // write null bitmap
  uint32_t size = (fields_.size() / 8 + 1) * 8;
  uint32_t map_num = 0;
  uint32_t map[size];
  while (map_num < size / 8) {
    char bitmap = 0;
    for (uint32_t i = 0; i < 8; i++) {
      // 若该field不为空 则将bitmap中对应位置为1
      if (!fields_[map_num * 8 + i]->IsNull() && (map_num * 8 + i < fields_.size())) {
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
      uint32_t t = fields_[i]->SerializeTo(buf);
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
  uint32_t size = (field_num / 8 + 1) * 8;
  uint32_t map_num = 0;
  uint32_t map[size];
  while (map_num < size / 8) {
    char bitmap = MACH_READ_FROM(char, buf);
    buf++;
    ofs++;
    for (uint32_t i = 0; i < 8; i++) {
      if (((bitmap >> (7 - i)) & 0x01) != 0) {  // if not null
        map[i + map_num * 8] = 1;
      } else
        map[i + map_num * 8] = 0;
    }
    map_num++;
  }
  // deserialize
  for (uint32_t i = 0; i < field_num; i++) {
    if (map[i] == 0) {
      uint32_t t = fields_[i]->DeserializeFrom(buf, schema->GetColumn(i)->GetType(), &fields_[i], true, heap_);
      ofs += t;
      buf += t;
    } else {
      uint32_t t = fields_[i]->DeserializeFrom(buf, schema->GetColumn(i)->GetType(), &fields_[i], false, heap_);
      ofs += t;
      buf += t;
    }
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
  sum += 4 + (fields_.size() / 8 + 1);
  // fields
  for (uint32_t i = 0; i < fields_.size(); i++) {
    if (fields_[i]->IsNull() != 0) {
      sum += fields_[i]->GetSerializedSize();
    }
  }
  return sum;
}
