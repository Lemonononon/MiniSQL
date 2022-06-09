#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  //启用LRU替换策略
  replacer_ = new LRUReplacer(pool_size_);
  //启用Clock替换策略
//  replacer_ = new ClockReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  //        先从free_list_里找buffer pool还有没有空位，有的话直接从磁盘拉进空位，
  //        没有的话去看看replacer里有没有东西，有的话把那里对应的页清一个，buffer pool就又有空位力，用此空位
  // 2.     If R is dirty, write it back to the disk.
  //        其他类可以调用并修改buffer
  //        pool中的page，所以会产生缓存中和disk中不一致的情况，也就是dirty，我们需要将dirty的数据写回disk
  //        实际上其他类应该保证修改数据后即调用FlushPage将脏数据存进disk，但在并发的情况下，这个操作可能随时会被打断，如果FetchPage时不把dirty数据写回disk，可能发生这样的情况：
  //        其他类修改完page，刚把它移进free_list_或lru
  //        replacer，此时FetchPage把这个Page直接拿来用了，那么这个修改过的脏数据就丢失了
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  if (page_table_.find(page_id) != page_table_.end()) {
    replacer_->Pin(page_table_[page_id]);
    pages_[page_table_[page_id]].pin_count_++;
//    cout << "+";
    return pages_ + page_table_[page_id];
  } else {
    frame_id_t free_page_index;
    if (free_list_.size() > 0) {
      // 简单起见，直接从末尾拿一个吧
      free_page_index = free_list_.back();
      free_list_.pop_back();
    } else if (replacer_->Size() > 0) {
      frame_id_t *free_frame_receiver = new frame_id_t;
      replacer_->Victim(free_frame_receiver);
      free_page_index = *free_frame_receiver;
      delete free_frame_receiver;
    } else {
      return nullptr;
    }
    // TODO: 1.dirty的判定，什么时候dirty? => 在UnpinPage时，由调用者传入是否dirty，因为可能有多个调用者，所以应该用一个或关系
    //       2.既然有dirty，那么这个page在buffer中的写又在哪里实现? =>
    //       返回的是Page*，而Page类中GetData可以获取data的指针，从指针修改写入即可
    if (pages_[free_page_index].is_dirty_) {
      disk_manager_->WritePage(pages_[free_page_index].page_id_, pages_[free_page_index].data_);
    }
    // 更新page_table_，更新pages_中的那一页page
    page_table_.erase(pages_[free_page_index].page_id_);
    // Page对象里设置了不允许复制，也即重载了运算符=
    //    pages_[free_page_index] = Page();
    pages_[free_page_index].ResetMemory();
    pages_[free_page_index].is_dirty_ = false;
    pages_[free_page_index].page_id_ = page_id;
    page_table_[page_id] = free_page_index;
    disk_manager_->ReadPage(page_id, pages_[free_page_index].data_);
    replacer_->Pin(free_page_index);
    pages_[free_page_index].pin_count_++;
//    cout << "+";
    return pages_ + free_page_index;
  }
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t free_page_index;
  if (free_list_.size() > 0) {
    free_page_index = free_list_.back();
    free_list_.pop_back();
  } else if (replacer_->Size() > 0) {
    frame_id_t *free_frame_receiver = new frame_id_t;
    replacer_->Victim(free_frame_receiver);
    free_page_index = *free_frame_receiver;
    delete free_frame_receiver;
  } else {
    return nullptr;
  }
  if (pages_[free_page_index].is_dirty_) {
    disk_manager_->WritePage(pages_[free_page_index].page_id_, pages_[free_page_index].data_);
  }
  // 更新page_table_，更新pages_中的那一页page
  page_table_.erase(pages_[free_page_index].page_id_);
  // 从disk中分配一个新的page_id，传进引用
  page_id = AllocatePage();
  // Page对象里设置了不允许复制，也即重载了运算符=
  //    pages_[free_page_index] = Page();
  pages_[free_page_index].ResetMemory();
  pages_[free_page_index].is_dirty_ = false;
  pages_[free_page_index].page_id_ = page_id;
  page_table_[page_id] = free_page_index;
  replacer_->Pin(free_page_index);
  pages_[free_page_index].pin_count_ = 1;
//  cout << "+";
  return pages_ + free_page_index;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if (page_table_.find(page_id) == page_table_.end()) {
    // 如果不在内存里，直接从硬盘中删除
    DeallocatePage(page_id);
    return true;
  } else if (pages_[page_table_[page_id]].pin_count_ > 0) {
    // 如果在内存中而被pin，则不能删除
    return false;
  } else {
    // 如果在内存中而没有被pin，则删除，并从replacer移除
    replacer_->Pin(page_table_[page_id]);
    DeallocatePage(page_id);
    pages_[page_table_[page_id]].ResetMemory();
    pages_[page_table_[page_id]].is_dirty_ = false;
    free_list_.emplace_back(page_table_[page_id]);
    page_table_.erase(page_id);
    return true;
  }
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if (page_table_.find(page_id) == page_table_.end()) return false;
  pages_[page_table_[page_id]].pin_count_--;
//  cout << "-";
  pages_[page_table_[page_id]].is_dirty_ = pages_[page_table_[page_id]].is_dirty_ || is_dirty;
  if (pages_[page_table_[page_id]].pin_count_ == 0) {
    replacer_->Unpin(page_table_[page_id]);
  }
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if (page_table_.find(page_id) == page_table_.end()) return false;
  disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].data_);
  pages_[page_table_[page_id]].is_dirty_ = false;
  return true;
}

bool BufferPoolManager::FlushAllPages() {
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID) {
      if (FlushPage(pages_[i].page_id_)) {
        // cout << "success" << endl;
      } else {
        // cout << "failed" << endl;
      }
    }
  }
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) { disk_manager_->DeAllocatePage(page_id); }

bool BufferPoolManager::IsPageFree(page_id_t page_id) { return disk_manager_->IsPageFree(page_id); }

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}