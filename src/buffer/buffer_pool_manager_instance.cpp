//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  latch_.lock();
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    // LOG_INFO("Flush frame id is %d", frame_id);
    Page *page = &pages_[frame_id];
    disk_manager_->WritePage(page_id, page->GetData());
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  latch_.lock();
  // 0.
  // LOG_INFO("get a new page id %d from disk manger",*page_id);
  // 1.
  bool pin_all(true);
  for (int i = 0; i < static_cast<int>(pool_size_); i++) {
    if (pages_[i].pin_count_ == 0) {
      pin_all = false;
      break;
    }
  }
  if (pin_all) {
    latch_.unlock();
    // LOG_INFO("buffer pool is busy.............");
    return nullptr;
  }
  // 2.
  // fetch from free list
  if (!free_list_.empty()) {
    *page_id = AllocatePage();
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();
    // Update page's metadata
    Page *page = &pages_[frame_id];
    page->page_id_ = *page_id;
    page->pin_count_++;
    page->is_dirty_ = false;
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    // add to page_table
    page_table_[*page_id] = frame_id;
    latch_.unlock();
    // LOG_INFO("the frame according to new page %d is %d from free list ", *page_id, frame_id);
    return page;
  }
  // pick a victim page from replacer
  frame_id_t frame_id;
  if (replacer_->Victim(&frame_id)) {
    *page_id = AllocatePage();
    Page *page = &pages_[frame_id];
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->GetData());
      page->is_dirty_ = false;
    }
    // update metadata
    page_table_.erase(page->page_id_);
    page_table_[*page_id] = frame_id;
    page->page_id_ = *page_id;
    page->pin_count_++;
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    latch_.unlock();
    // LOG_INFO("the frame according to new page %d is %d from replacer", *page_id, frame_id);
    return page;
  }
  latch_.unlock();
  return nullptr;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  latch_.lock();
  // 1.1
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    replacer_->Pin(frame_id);
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    latch_.unlock();
    return page;
  }
  // 1.2
  if (!free_list_.empty()) {
    // if pool is not full, find a free frame in free_list;
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();
    // Update page's metadata
    Page *page = &pages_[frame_id];
    page->page_id_ = page_id;
    page->pin_count_++;
    page->is_dirty_ = false;
    disk_manager_->ReadPage(page_id, page->data_);
    // add to page_table
    page_table_[page_id] = frame_id;
    latch_.unlock();
    return page;
  }

  // if pool is full, replace a page which found in replacer
  frame_id_t frame_id = -1;
  if (replacer_->Victim(&frame_id)) {
    Page *page = &pages_[frame_id];
    // if P is firty write it back to the disk
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
    }
    // update page's metadata
    page_table_.erase(page->page_id_);
    page->page_id_ = page_id;
    page->pin_count_++;

    disk_manager_->ReadPage(page_id, page->data_);
    // add to page_table
    page_table_[page_id] = frame_id;
    latch_.unlock();
    return page;
  }
  // LOG_INFO("victim frame id is %d", frame_id);
  latch_.unlock();
  return nullptr;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  latch_.lock();
  // 1.
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    if (pages_[frame_id].pin_count_ != 0) {
      // 2.
      latch_.unlock();
      return false;
    }
    // 3.
    Page *page = &pages_[frame_id];
    memset(page->data_, 0, PAGE_SIZE);
    page->pin_count_ = 0;
    if (page->IsDirty()) {
      disk_manager_->WritePage(page_id, page->data_);
      page->is_dirty_ = false;
    }
    // replacer_->Unpin(frame_id);
    free_list_.push_back(frame_id);
    page_table_.erase(page_id);
  }
  latch_.unlock();
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  // LOG_INFO("Unpin page id is %d", page_id);
  // false if the page pin count is <= 0 before this call, true otherwise
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    // LOG_INFO("unpin frame id is %d", frame_id);
    Page *page = &pages_[frame_id];
    if (is_dirty) {
      page->is_dirty_ = true;
    }
    if (page->GetPinCount() <= 0) {
      latch_.unlock();
      return false;
    }
    page->pin_count_--;
    if (page->pin_count_ == 0) {
      replacer_->Unpin(frame_id);
    }
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
