//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  max_pages_ = num_pages;
  num_pages_ = 0;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (lists_.empty()) {
    return false;
  }
  frame_id_t victim_frame_id = lists_.back();
  lists_.pop_back();
  frames_.erase(victim_frame_id);
  *frame_id = victim_frame_id;
  num_pages_--;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (frames_.find(frame_id) == frames_.end()) {
    return;
  }
  auto itr = frames_[frame_id];
  lists_.erase(itr);
  frames_.erase(frame_id);
  num_pages_--;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (frames_.find(frame_id) == frames_.end()) {
    lists_.push_front(frame_id);
    frames_[frame_id] = lists_.begin();
    num_pages_++;
  }
}

size_t LRUReplacer::Size() { return num_pages_; }

}  // namespace bustub
