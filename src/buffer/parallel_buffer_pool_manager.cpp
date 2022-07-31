//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : num_instances_(num_instances), pool_size_(pool_size) {
  // Allocate and create individual BufferPoolManagerInstances
  bpis_ = new BufferPoolManager *[num_instances_];
  for (uint32_t i = 0; i < num_instances; i++) {
    bpis_[i] = new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (uint32_t i = 0; i < num_instances_; i++) {
    delete bpis_[i];
  }
  delete[] bpis_;
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return num_instances_ * pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return bpis_[page_id % num_instances_];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  Page *page = GetBufferPoolManager(page_id)->FetchPage(page_id);
  // if (page == nullptr) {
  //   LOG_INFO("Fetch page failed");
  // } else {
  //   LOG_INFO("Fetch page sucessed");
  // }
  return page;
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  bool if_new = false;
  Page *page;
  uint32_t i;
  for (i = 0; i < num_instances_; i++) {
    page = bpis_[((i + start_index_) % num_instances_)]->NewPage(page_id);
    if (page != nullptr) {
      if_new = true;
      break;
    }
  }
  if (if_new) {
    start_index_ = (i + 1) % num_instances_;
    // LOG_INFO("success create a new page start_index_ %d ",start_index_);
    return page;
  } else {
    start_index_ = (start_index_ + 1) % num_instances_;
  }
  // LOG_INFO("cannot create a new page ");
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
}

}  // namespace bustub
