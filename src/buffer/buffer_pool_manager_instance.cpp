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

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (!GetAvailableFrame(&fid)) {
    // std::cout << "In Newpg, no more availble frame\n";
    return nullptr;
  }
  *page_id = AllocatePage();
  page_table_->Insert(*page_id, fid);
  pages_[fid].pin_count_ = 1;
  pages_[fid].page_id_ = *page_id;
  // be careful, order matters here.
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  return &pages_[fid];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (!page_table_->Find(page_id, fid)) {
    if (!GetAvailableFrame(&fid)) {
      return nullptr;
    }
    page_table_->Insert(page_id, fid);
    pages_[fid].page_id_ = page_id;
    disk_manager_->ReadPage(page_id, pages_[fid].data_);
  }
  pages_[fid].pin_count_++;
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  return &pages_[fid];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (!page_table_->Find(page_id, fid)) {
    return false;
  }
  if (pages_[fid].GetPinCount() <= 0) {
    return false;
  }
  pages_[fid].is_dirty_ |= is_dirty;
  if (--pages_[fid].pin_count_ <= 0) {
    replacer_->SetEvictable(fid, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (!page_table_->Find(page_id, fid)) {
    return false;
  }
  disk_manager_->WritePage(pages_[fid].page_id_, pages_[fid].data_);
  pages_[fid].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (int idx = 0; idx < static_cast<int>(pool_size_); idx++) {
    FlushPgImp(pages_[idx].page_id_);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (!page_table_->Find(page_id, fid)) {
    return true;
  }
  if (pages_[fid].pin_count_ >= 1) {
    return false;
  }
  pages_[fid].ResetMemory();
  pages_[fid].page_id_ = INVALID_PAGE_ID;
  pages_[fid].pin_count_ = 0;
  pages_[fid].is_dirty_ = false;
  page_table_->Remove(page_id);
  replacer_->Remove(fid);
  free_list_.push_back(fid);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::GetAvailableFrame(frame_id_t *out_frame_id) -> bool {
  frame_id_t fid;
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
    *out_frame_id = fid;
    return true;
  }

  if (replacer_->Evict(&fid)) {
    if (pages_[fid].is_dirty_) {
      disk_manager_->WritePage(pages_[fid].page_id_, pages_[fid].data_);
      pages_[fid].is_dirty_ = false;
      pages_[fid].ResetMemory();
    }
    page_table_->Remove(pages_[fid].page_id_);
    *out_frame_id = fid;
    return true;
  }
  return false;
}

}  // namespace bustub
