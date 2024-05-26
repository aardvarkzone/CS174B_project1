//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
  diskManager = disk_manager;
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * { 
  Page *p = new Page();
  if(replacer_->Size() > 0 || free_list_.size() > 0) {
    frame_id_t frameId;
    if(free_list_.size() > 0) {
     frameId = free_list_.front();
     free_list_.pop_front();

    } else {
      replacer_->Evict(&frameId);
    }

    *page_id = AllocatePage();

    p->page_id_ = *page_id;

    if(pages_[frameId].is_dirty_) {
      pages_[frameId].ResetMemory();
      pages_[frameId].pin_count_ = 0;
      pages_[frameId].is_dirty_ = false;
    }


    //? maybe this idk
    pages_[frameId].pin_count_ = 1;
    replacer_->SetEvictable(frameId, false);
    replacer_->RecordAccess(frameId);
    return p;
  }

  return nullptr; 
  
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  auto page = page_table_.find(page_id);
  frame_id_t frameId;

  if(page == page_table_.end()) {
    if(free_list_.size() > 0) {
     frameId = free_list_.front();
     free_list_.pop_front();

    } else {
      replacer_->Evict(&frameId);
    }
  } else {
    frameId = page->second;
  }

  if(pages_[frameId].is_dirty_) {
    pages_[frameId].ResetMemory();
    pages_[frameId].pin_count_ = 0;
    pages_[frameId].is_dirty_ = false;
  }

  char * page_data = nullptr;
  diskManager->ReadPage(page_id, page_data);
  pages_[frameId].page_id_ = page_id;
  pages_[frameId].data_ = page_data;
  pages_[frameId].is_dirty_ = false;
  replacer_->SetEvictable(frameId, false);
  replacer_->RecordAccess(frameId);
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {

  auto page = page_table_.find(page_id);
  if(page == page_table_.end() || pages_[page->second].pin_count_ == 0) {
    return false;
  }
  pages_[page->second].pin_count_--;
  if(pages_[page->second].pin_count_ == 0) {
    replacer_->SetEvictable(page->second, true);
  }
  pages_[page->second].is_dirty_ = is_dirty;
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
  auto page = page_table_.find(page_id);
  if (page == page_table_.end()) {
      return false; // Page not found in the page table
  }
  diskManager->WritePage(page_id, pages_[page->second].data_);
  pages_[page->second].is_dirty_ = false;
  return false; 
}

void BufferPoolManager::FlushAllPages() {
 for (auto& entry : page_table_) {
        size_t pageId = entry.second;
        FlushPage(pageId);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { return false; }

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
