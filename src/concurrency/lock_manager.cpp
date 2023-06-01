//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include <set>

namespace bustub {
auto DeleteTxnLockSetForTable(Transaction *txn, LockManager::LockMode &lock_mode, const table_oid_t &oid) -> void {
  if (lock_mode == LockManager::LockMode::SHARED) {
    txn->GetSharedTableLockSet()->erase(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->erase(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  return;
  }
}

auto DeleteTxnLockSetForRow(Transaction *txn, LockManager::LockMode &lock_mode, const table_oid_t &oid, const RID &rid) -> void {
  if (lock_mode == LockManager::LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].erase(rid);
    if((*txn->GetSharedRowLockSet())[oid].empty()) {
      (*txn->GetSharedRowLockSet()).erase(oid);
    }
    return;
  }
  if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
    if((*txn->GetExclusiveRowLockSet())[oid].empty()) {
      (*txn->GetSharedRowLockSet()).erase(oid);
    }
    return;
  }
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    exit(-1);
  }

  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && (lock_mode != LockMode::INTENTION_SHARED || lock_mode != LockMode::SHARED)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else {
    if (lock_mode != LockMode::INTENTION_EXCLUSIVE || lock_mode != LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(),
AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  for (auto iter : lock_request_queue->request_queue_) {
    if (iter->txn_id_ == txn->GetTransactionId() && iter->granted_) {
      //upgrading to the same lock_mode, return directily
      if(iter->lock_mode_ == lock_mode) {
	return true;
      }
      // disallow concurrent upgrading for simplicity
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
	txn->SetState(TransactionState::ABORTED);
	throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      if (iter->lock_mode_ == LockMode::SHARED && (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) {
	txn->SetState(TransactionState::ABORTED);
	throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      if (iter->lock_mode_ == LockMode::INTENTION_EXCLUSIVE && ( lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) {
	txn->SetState(TransactionState::ABORTED);
	throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      if (iter->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && ( lock_mode != LockMode::EXCLUSIVE)) {
	txn->SetState(TransactionState::ABORTED);
	throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      if (iter->lock_mode_ == LockMode::EXCLUSIVE) {
	txn->SetState(TransactionState::ABORTED);
	throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      lock_request_queue->request_queue_.remove(iter);
      DeleteTxnLockSetForTable(txn, iter->lock_mode_, oid);
      delete iter;
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      break;
    }
  }
  auto *lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.push_back(lock_request);
  while(!lock_request_queue->GrantLockForTableOrRow(txn, lock_mode)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      if(lock_request_queue->upgrading_ == txn->GetTransactionId()) {
	lock_request_queue->upgrading_ = INVALID_TXN_ID;
      }
      delete lock_request;
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  if (!(*txn->GetSharedRowLockSet())[oid].empty() || !(*txn->GetExclusiveRowLockSet())[oid].empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  bool found = false;
  for(auto iter : lock_request_queue->request_queue_) {
    if (txn->GetTransactionId() == iter->txn_id_ && iter->granted_) {
      found = true;
      if ((txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && (iter->lock_mode_ == LockMode::EXCLUSIVE || iter->lock_mode_ == LockMode::SHARED)) || (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && (iter->lock_mode_ == LockMode::EXCLUSIVE)) || (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && (iter->lock_mode_ == LockMode::EXCLUSIVE))) {
	txn->SetState(TransactionState::SHRINKING);
      }
      DeleteTxnLockSetForTable(txn, iter->lock_mode_, oid);
      lock_request_queue->request_queue_.remove(iter);
      delete iter;
      break;
    }
  }
  if (found) {
    lock_request_queue->cv_.notify_all();
    lock.unlock();
  } else {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    exit(-1);
  }

  if(lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && (lock_mode != LockMode::SHARED)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else {
    if (lock_mode != LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(),
AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  //iterate through list to find presented or not
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  if (lock_mode == LockMode::SHARED) {
    lock_request_queue->latch_.lock();
    bool hold_lock_on_table = false;
    for(auto iter: lock_request_queue->request_queue_) {
      if (iter->txn_id_ == txn->GetTransactionId() && iter->granted_) {
	hold_lock_on_table = true;
	break;
      }
    }
    lock_request_queue->latch_.unlock();
    if(!hold_lock_on_table) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  } else {
    lock_request_queue->latch_.lock();
    bool hold_lock_on_table = false;
    for(auto iter : lock_request_queue->request_queue_) {
      if (iter->txn_id_ == txn->GetTransactionId() && iter->granted_ && (iter->lock_mode_ == LockMode::EXCLUSIVE || iter->lock_mode_ == LockMode::INTENTION_EXCLUSIVE || iter->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
	  hold_lock_on_table = true;
	  break;
      }
    }
    lock_request_queue->latch_.unlock();
    if (!hold_lock_on_table) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto row_lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lock(row_lock_request_queue->latch_);
  for (auto iter : row_lock_request_queue->request_queue_) {
    if (iter->txn_id_ == txn->GetTransactionId() && iter->granted_) {
      if (iter->lock_mode_ == lock_mode) {
	return true;
      }
      if (row_lock_request_queue->upgrading_ != INVALID_TXN_ID) {
	txn->SetState(TransactionState::ABORTED);
	throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if (iter->lock_mode_ == LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
	txn->SetState(TransactionState::ABORTED);
	throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      row_lock_request_queue->request_queue_.remove(iter);
      DeleteTxnLockSetForRow(txn, iter->lock_mode_, iter->oid_, iter->rid_);
      delete iter;
      row_lock_request_queue->upgrading_ = txn->GetTransactionId();
      break;
    }
  }

  auto *lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  row_lock_request_queue->request_queue_.push_back(lock_request);
  while(!row_lock_request_queue->GrantLockForTableOrRow(txn, lock_mode, true)) {
    row_lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      row_lock_request_queue->request_queue_.remove(lock_request);
      if (row_lock_request_queue->upgrading_ == txn->GetTransactionId()) {
	row_lock_request_queue->upgrading_ = INVALID_TXN_ID;
      }
      delete lock_request;
      row_lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { 
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  bool found = false;
  for(auto iter : lock_request_queue->request_queue_) {
    if (txn->GetTransactionId() == iter->txn_id_ && iter->granted_) {
      found = true;
      if ((txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) || (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && (iter->lock_mode_ == LockMode::EXCLUSIVE)) || (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && (iter->lock_mode_ == LockMode::EXCLUSIVE))) {
	txn->SetState(TransactionState::SHRINKING);
      }
      lock_request_queue->request_queue_.remove(iter);
      DeleteTxnLockSetForRow(txn, iter->lock_mode_, oid, rid);
      delete iter;
      break;
    }
  }
  lock.unlock();
  lock_request_queue->cv_.notify_all();
  if (!found) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  return true;
}


auto Compatible(const std::set<LockManager::LockMode> set, const LockManager::LockMode lock_mode) -> bool {
  if (lock_mode == LockManager::LockMode::SHARED) {
    return set.find(LockManager::LockMode::EXCLUSIVE) == set.end() && set.find(LockManager::LockMode::INTENTION_EXCLUSIVE) == set.end() && set.find(LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) == set.end();
  }
  if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    return set.empty();
  }
  if (lock_mode == LockManager::LockMode::INTENTION_SHARED) {
    return set.find(LockManager::LockMode::EXCLUSIVE) == set.end();
  }
  if (lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE) {
    return set.find(LockManager::LockMode::SHARED) == set.end() && set.find(LockManager::LockMode::EXCLUSIVE) == set.end() && set.find(LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) == set.end();
  }
  if (lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return set.find(LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) == set.end() && set.find(LockManager::LockMode::SHARED) == set.end() && set.find(LockManager::LockMode::EXCLUSIVE) == set.end() && set.find(LockManager::LockMode::INTENTION_EXCLUSIVE) == set.end();
  }
  return false;
}



void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  bool found = false;
  for (const auto &txn_id : waits_for_[t1]) {
    if (txn_id == t2 ) {
      found = true;
      break;
    }
  }

  if (!found) {
    waits_for_[t1].push_back(t2);
  }
  std::sort(waits_for_[t1].begin(), waits_for_[t1].end());
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  for (auto iter = waits_for_[t1].begin() ; iter != waits_for_[t1].end(); iter++ ) {
    if ((*iter) == t2) {
      waits_for_[t1].erase(iter);
      break;
    }
  }
}

auto LockManager::DFS(std::vector<txn_id_t> cycle_vector, bool &is_cycle, txn_id_t *txn_id) -> void {
  if (waits_for_.find(cycle_vector[cycle_vector.size() - 1]) == waits_for_.end()) {
    return;
  }
  for (auto txn : waits_for_[cycle_vector[cycle_vector.size() -1]]) {
    if (is_cycle) {
      return;
    }
    auto iter = std::find(cycle_vector.begin(), cycle_vector.end(), txn);
    if (iter != cycle_vector.end()) {
      is_cycle = true;
      *txn_id = *iter;
      while (iter != cycle_vector.end()) {
	if (*txn_id < *iter) {
	  *txn_id = *iter;
	}
	iter++;
      }
      auto transaction = TransactionManager::GetTransaction(*txn_id);
      transaction->SetState(TransactionState::ABORTED);
    }
    if (!is_cycle) {
      cycle_vector.push_back(txn);
      DFS(cycle_vector, is_cycle, txn_id);
      cycle_vector.pop_back();
    }
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::vector<txn_id_t> txn_vector;
  txn_vector.reserve(waits_for_.size());
  for (const auto &wait : waits_for_) {
    txn_vector.push_back(wait.first);
  }
  std::sort(txn_vector.begin(), txn_vector.end(), std::greater<>());
  for (auto txn : txn_vector) {
    std::vector<txn_id_t> cycle_vector;
    bool is_cycle = false;
    cycle_vector.push_back(txn);
    DFS(cycle_vector, is_cycle, txn_id);
    if (is_cycle) {
      return true;
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::unique_lock<std::mutex> lock(waits_for_latch_);
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto& pair : waits_for_) {
    for (const auto &to_txn_id : pair.second) {
      edges.emplace_back(pair.first, to_txn_id);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      waits_for_latch_.lock();
      table_lock_map_latch_.lock();
      for (const auto &table_pairs : table_lock_map_) {
	table_pairs.second->latch_.lock();
	for (auto i_request : table_pairs.second->request_queue_) {
	  for (auto j_request : table_pairs.second->request_queue_) {
	    if (!i_request->granted_ && j_request->granted_ && !Compatible({j_request->lock_mode_}, i_request->lock_mode_)) {
	      AddEdge(i_request->txn_id_, j_request->txn_id_);
	    }
	  }
	}
	table_pairs.second->latch_.unlock();
      }
      table_lock_map_latch_.unlock();

      row_lock_map_latch_.lock();
      for (const auto &row_pairs : row_lock_map_) {
	row_pairs.second->latch_.lock();
	for (auto &i_request : row_pairs.second->request_queue_) {
	  for (auto &j_request : row_pairs.second->request_queue_) {
	    if (!i_request->granted_ && j_request->granted_ && !Compatible({j_request->lock_mode_}, i_request->lock_mode_)) {
	      AddEdge(i_request->txn_id_, j_request->txn_id_);
	    }
	  }
	}
	row_pairs.second->latch_.unlock();
      }
      row_lock_map_latch_.unlock();

      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
	for (const auto &wait : waits_for_) {
	  RemoveEdge(wait.first, txn_id);
	}
	waits_for_.erase(txn_id);
	table_lock_map_latch_.lock();
	for (const auto &table_pairs : table_lock_map_) {
	  table_pairs.second->cv_.notify_all();
	}
	table_lock_map_latch_.unlock();

	row_lock_map_latch_.lock();
	for (const auto &row_pairs : row_lock_map_) {
	  row_pairs.second->cv_.notify_all();
	}
	row_lock_map_latch_.unlock();
      }
      waits_for_latch_.unlock();
    }
  }
}

auto AddTxnLockSetForTable(Transaction *txn, LockManager::LockMode &lock_mode, const table_oid_t &oid) -> void {
  switch(lock_mode) {
    case LockManager::LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockManager::LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockManager::LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
  }
}

auto AddTxnLockSetForRow(Transaction *txn, LockManager::LockMode &lock_mode, const table_oid_t &oid, const RID &rid) -> void {
  if (lock_mode == LockManager::LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].insert(rid);
    return ;
  }
  if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
    return;
  }
}

auto LockManager::LockRequestQueue::GrantLockForTableOrRow(Transaction *txn, LockManager::LockMode lock_mode, bool isRow) -> bool {
  std::set<LockMode> granted_set;
  std::set<LockMode> wait_set;
  LockRequest *lock_request = nullptr;
  for(auto &iter : request_queue_) {
    if (iter->granted_) {
      granted_set.insert(iter->lock_mode_);
    }
    if (iter->txn_id_ == txn->GetTransactionId()) {
      lock_request = iter;
    }
  }

  if(Compatible(granted_set, lock_mode)) {
    if (upgrading_ != INVALID_TXN_ID) {
      if(upgrading_ == txn->GetTransactionId()) {
	upgrading_ = INVALID_TXN_ID;
	if (lock_request != nullptr) {
	  lock_request->granted_ = true;
	  if(!isRow) {
	    AddTxnLockSetForTable(txn, lock_mode, lock_request->oid_);
	  } else {
	    AddTxnLockSetForRow(txn, lock_mode, lock_request->oid_, lock_request->rid_);
	  }
	  return true;
	}
      }
      return false;
    }
    /** find all wait txn that has higher priority in FIFO manner */
    for (auto &iter : request_queue_) {
      if( iter->txn_id_ != txn->GetTransactionId()) {
	if( !iter->granted_) {
	  wait_set.insert(iter->lock_mode_);
	} 
      } else {
	break;
      }
    }
    if (Compatible(wait_set, lock_mode)) {
      if (lock_request != nullptr) {
	lock_request->granted_ = true;
	if (!isRow) {
	  AddTxnLockSetForTable(txn, lock_mode, lock_request->oid_);
	} else {
	  AddTxnLockSetForRow(txn, lock_mode, lock_request->oid_, lock_request->rid_);
	}
	return true;
      }
    }
  }
  return false;
}
}  // namespace bustub
