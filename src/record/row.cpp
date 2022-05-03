#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  // replace with your code here
  // write fields num
  uint32_t ofs = 0;
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
      if (!fields_[map_num * 8 + i]->IsNull() && (map_num * 8 + i < fields_.size())) {
        bitmap = bitmap | (0x01 << (7 - i));
        map[map_num * 8 + i] = 1;
      }
      map[map_num * 8 + i] = 0;
    }
    map_num++;
    MACH_WRITE_TO(char, buf, bitmap);
    ofs++;
    buf++;
  }
  // write fields
  for (uint32_t i = 0; i < fields_.size(); i++) {
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
  uint32_t size = MACH_READ_UINT32(buf);
  uint32_t map_num = 0;
  buf += 4;
  ofs += 4;
  // read null bitmap
  uint32_t map[size];
  while (map_num< size/8 +1) {
    char bitmap = MACH_READ_FROM(char, buf);
    buf++;
    ofs++;
    map_num++;
    for (uint32_t i = 0; i < 8; i++) {
      if (((bitmap >> (7 - i)) & 0x01) != 0) {  // if not null
        map[i+map_num*8]=1;
      }
      else map[i+map_num*8]=0;
    }
  }
  // deserialize
  for(uint32_t i=0;i<size;i++){
    if(map[i]) {
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

  return 0;
}
