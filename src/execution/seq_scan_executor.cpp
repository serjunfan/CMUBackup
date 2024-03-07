//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto exec_ctx = GetExecutorContext();
  auto catalog = exec_ctx->GetCatalog();
  auto table_oid = plan_->GetTableOid();
  auto table_info = catalog->GetTable(table_oid);
  table_heap_ = table_info->table_.get();
  iter_ = std::make_unique<TableIterator>(table_heap_->Begin(exec_ctx_->GetTransaction()));

  try {
    if (exec_ctx->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
        !exec_ctx->GetLockManager()->LockTable(exec_ctx->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
                                               table_info->oid_)) {
      throw ExecutionException("lock table share failed");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("seq scan TransactionAbort");
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  try {
    if (!exec_ctx_->GetTransaction()->GetSharedRowLockSet()->empty()) {
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
          !exec_ctx_->GetLockManager()->UnlockRow(
              exec_ctx_->GetTransaction(), exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->oid_, *rid)) {
        throw ExecutionException("unlock row share failed");
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("seq scan TransactionAbort");
  }

  if (*iter_ != table_heap_->End()) {
    try {
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
          !exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->oid_,
                                                (*(*iter_)).GetRid())) {
        throw ExecutionException("lock row intention shared failed");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("seq scan TransactionAbort");
    }
    *tuple = *((*iter_)++);
    *rid = tuple->GetRid();
    return true;
  }
  return false;
}

/*
  while ((*iter_) != table_heap_->End() && !((*iter_)->IsAllocated())) {
    (*iter_)++;
  }
  if ((*iter_) == table_heap_->End()) {
    return false;
  }
  *tuple = *((*iter_)++);
  *rid = tuple->GetRid();
  return true;
}
*/
}  // namespace bustub
