#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {

}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {

}

void LRUReplacer::Unpin(frame_id_t frame_id) {

}

size_t LRUReplacer::Size() {
  return 0;
}