#include "page/bitmap_page.h"

template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  // 在这里allocate还是说只修改bitmap？
  //
  if (page_allocated_ < MAX_CHARS * 8) {
    page_allocated_++;
    page_offset = next_free_page_;
    uint32_t index = 0;
    // find next free page
    // 获取这里需要从0开始查询？因为可能会有前面的page被释放掉了
    // 先用0，后续如果防止碎片化的需求再改
    while (!IsPageFreeLow(index / 8, index % 8) && index < MAX_CHARS * 8) {
      index++;
    }
    next_free_page_ = index;
    // 如果index达到了MAX_CHARS，并且在下次分配之前没有释放，那么下次分配就没有空闲page，allocate失败
    return true;
  }
  return false;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (!IsPageFreeLow(page_offset / 8, page_offset % 8)) {
    if (page_allocated_ == MAX_CHARS * 8) {
      next_free_page_ = page_offset;
    }
    bytes[page_offset / 8] = bytes[page_offset / 8] | (0x80 >> (page_offset % 8));
    page_allocated_--;
    return true;
  }
  return false;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return (bytes[byte_index] & (0x80 >> bit_index)) ? false : true;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;