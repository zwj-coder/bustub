//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

using iter = std::unordered_map<page_id_t, frame_id_t>::iterator;
BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);
  iter it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    return &pages_[it->second];
  }
  if (!free_list_.empty()) {
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();
    return &pages_[frame_id];
  }
  frame_id_t frame_id_;
  if (!replacer_->Victim(&frame_id_)) {
    return nullptr;
  }
  if (pages_[frame_id_].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id_].GetPageId(), pages_[frame_id_].GetData());
  }
  page_table_.erase(page_id);
  page_table_.insert({page_id, frame_id_});
  pages_[frame_id_].page_id_ = page_id;
  pages_[frame_id_].is_dirty_ = false;
  pages_[frame_id_].pin_count_ = 1;
  pages_[frame_id_].ResetMemory();
  disk_manager_->ReadPage(page_id, pages_[frame_id_].GetData());
  return &pages_[frame_id_];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(latch_);
  iter it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  pages_[it->second].pin_count_--;
  if (pages_[it->second].pin_count_ <= 0) {
    replacer_->Unpin(it->second);
  }
  pages_[it->second].is_dirty_ |= is_dirty;
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  assert(page_id != INVALID_PAGE_ID);
  std::lock_guard<std::mutex> lock(latch_);
  iter it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[it->second].GetData());
  pages_[it->second].is_dirty_ = false;
  return false;
}


Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);
  size_t i;
  for (i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() == 0) {
      break;
    }
  }
  if (i == pool_size_) {
    return nullptr;
  }
  frame_id_t frame_id_;
  if (!free_list_.empty()) {
    frame_id_ = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Victim(&frame_id_)) {
      return nullptr;
    }
    if (pages_[frame_id_].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id_].GetPageId(), pages_[frame_id_].GetData());
      pages_[frame_id_].is_dirty_ = false;
    }
    page_table_.erase(pages_[frame_id_].GetPageId());
  }
  page_id_t page_id_ = disk_manager_->AllocatePage();
  *page_id = page_id_;
  pages_[frame_id_].is_dirty_ = false;
  pages_[frame_id_].pin_count_ = 1;
  pages_[frame_id_].page_id_ = page_id_;
  pages_[frame_id_].ResetMemory();
  page_table_.insert({page_id_, frame_id_});
  return &pages_[frame_id_];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lock(latch_);
  iter it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  if (pages_[it->second].GetPinCount() > 0) {
    return false;
  }
  disk_manager_->DeallocatePage(page_id);
  page_table_.erase(it);
  free_list_.push_back(it->second);
  pages_[it->second].page_id_ = INVALID_PAGE_ID;
  pages_[it->second].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (auto it = page_table_.begin(); it != page_table_.end(); ++it) {
    FlushPageImpl(it->first);
  }
}

}  // namespace bustub
