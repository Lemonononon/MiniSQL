#include <stdexcept>
#include <sys/stat.h>

#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"

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
 //allocate and deallocate都需要修改meta_data
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  page_id_t pageId;

  //寻找第一个没有满额的extent
  uint32_t extent_id = 0;
  size_t max_size_of_bitmap = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  while( true ){
    if (meta_page->extent_used_page_[extent_id] < max_size_of_bitmap ) break;
    extent_id++;
  };
  //pageId = 已满的extent个数*bitmap size + 现在的extent已经占用的page+1
  pageId = extent_id*max_size_of_bitmap + meta_page->extent_used_page_[extent_id]+1;
  //修改meta_data
  if (extent_id >=meta_page->num_extents_) meta_page->num_extents_+=1;//开辟一个新的extent
  meta_page->num_allocated_pages_++;
  WritePage(META_PAGE_ID, meta_data_);
  return pageId;
  //TODO：暂时未考虑溢出问题
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  ASSERT(false, "Not implemented yet.");
//  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
//  page_id_t physical_page_id = MapPageId(logical_page_id);
//  if ( meta_data_[physical_page_id/8] & (0x80>>(physical_page_id%8)) )  return false;
//  if (logical_page_id < 0)
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  return !(meta_page->GetAllocatedPages() >= uint32_t(logical_page_id )+1);
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  //logical_page_id = physical_page_id - 1 - number_of_bitmap
  page_id_t physical_page_id = logical_page_id + 1 + meta_page->GetExtentNums();

  return physical_page_id;
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