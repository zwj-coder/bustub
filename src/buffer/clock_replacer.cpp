//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages):in_replacer_(std::vector<bool>(num_pages, false)), refs_(std::vector<bool>(num_pages, true)), size_(0), clock_hand_(0){}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (size_ == 0) {
    frame_id = nullptr;
    return false;
  }
  for(;;) {
    if (clock_hand_ == in_replacer_.size()) clock_hand_ = 0;
    if (in_replacer_[clock_hand_]) {
      if (refs_[clock_hand_]) refs_[clock_hand_++] = false;
      else {
        *frame_id = clock_hand_;
        in_replacer_[clock_hand_++] = false;
        size_--;
        return true;
      }
    } else {
      clock_hand_++;
    }
  }
  return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (!in_replacer_[frame_id]) {
    return;
  }
  in_replacer_[frame_id] = false;
  size_--;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (in_replacer_[frame_id]) {
    refs_[frame_id] = true;
    return;
  }
  refs_[frame_id] = true;
  in_replacer_[frame_id] = true;
  size_++;
}

size_t ClockReplacer::Size() {
  std::lock_guard<std::mutex> lock(latch_);
  return size_;
}

}  // namespace bustub
