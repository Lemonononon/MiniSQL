#include <sys/stat.h>
#include <stdexcept>

#include <iostream>
#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"

//#define ENABLE_BPM_DEBUG

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

page_id_t DiskManager::AllocatePage() {
  //  ASSERT(false, "Not implemented yet.");
  //  return INVALID_PAGE_ID;
  uint32_t meta_data_uint[PAGE_SIZE/4];
  memcpy(meta_data_uint, meta_data_, 4096);

  size_t page_id;
  // 寻找第一个没有满额的extent
  uint32_t extent_id = 0;
  while (  *(meta_data_uint+2+extent_id) == BITMAP_SIZE) {
    extent_id++;
  };
  // 读取对应extent的bitmap_page，寻找第一个free的page
  char bitmap[PAGE_SIZE];
  page_id_t bitmap_physical_id = extent_id * (BITMAP_SIZE + 1) + 1;
  ReadPhysicalPage(bitmap_physical_id, bitmap);

  BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap);
  uint32_t next_free_page = bitmap_page->GetNextFreePage();
  page_id = extent_id * BITMAP_SIZE + next_free_page;

  bitmap_page->AllocatePage(next_free_page);
  // 修改meta_data
  if (extent_id >= *(meta_data_uint+1)) ++ *(meta_data_uint+1);
  ++ *(meta_data_uint+2+extent_id);
  ++ *(meta_data_uint);

  memcpy(meta_data_,meta_data_uint, 4096);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  WritePhysicalPage(bitmap_physical_id, bitmap);
  return page_id;
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  //  ASSERT(false, "Not implemented yet.");
  char bitmap[PAGE_SIZE];
  size_t pages_per_extent = 1 + BITMAP_SIZE;
  page_id_t bitmap_physical_id = 1 + MapPageId(logical_page_id) / pages_per_extent;
  ReadPhysicalPage(bitmap_physical_id, bitmap);
  BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap);

  bitmap_page->DeAllocatePage(logical_page_id % BITMAP_SIZE);

  uint32_t meta_data_uint[PAGE_SIZE/4];
//   修改meta_data
  uint32_t extent_id = logical_page_id / BITMAP_SIZE;
  if (-- *(meta_data_uint + 2 + extent_id) == 0) -- *( meta_data_uint + 1 );
  -- *(meta_data_uint);

  memcpy(meta_data_,meta_data_uint, 4096);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  WritePhysicalPage(bitmap_physical_id, bitmap);
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  // 判断对应的bitmap中那一bit是0还是1
  // 读取对应的bitmap
  char bitmap[PAGE_SIZE];
  size_t pages_per_extent = 1 + BITMAP_SIZE;
  page_id_t bitmap_physical_id = 1 + MapPageId(logical_page_id) / pages_per_extent;
  ReadPhysicalPage(bitmap_physical_id, bitmap);

  BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap);
  // 判断
  if (bitmap_page->IsPageFree(logical_page_id % BITMAP_SIZE)) return true;
  return false;
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return logical_page_id + 1 + 1 + logical_page_id / BITMAP_SIZE;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}