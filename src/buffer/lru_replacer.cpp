#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {

}

LRUReplacer::~LRUReplacer() = default;

// 从等待被替换的队列中删去最近最少使用的数据页，并返回给buffer pool这个页的id（frame_id）
// lru的核心思想就是，如果最近用得比较频繁，那没必要那么快从buffer pool中把它删掉。把很久以前用过的给优先删掉。
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (Size() > 0) {
    *frame_id = deque.back();
    deque.pop_back();
    map.erase(*frame_id);
    return true;
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (map.find(frame_id) != map.end()) {
    deque.erase(map[frame_id]);
    map.erase(frame_id);
  }
}

// Buffer pool Unpin了一个页后，表示目前已经没有对这个页的引用了，这个页进入lrureplacer，处于等待被替换的状态。
// Unpin一个frame_id两次，表示最近已经两次使用这个页并用完要求回收。
void LRUReplacer::Unpin(frame_id_t frame_id) {
  // 如果lrureplacer里还有这个页，则他还在缓冲区中。只需要将其更新到deque的开头。否则直接插入到deque开头
  if (map.find(frame_id) != map.end()) {
    deque.erase(map[frame_id]);
  }
  deque.emplace_front(frame_id);
  map[frame_id] = deque.begin();
}

size_t LRUReplacer::Size() {
  return deque.size();
}