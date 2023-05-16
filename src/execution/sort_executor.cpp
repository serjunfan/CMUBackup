#include "execution/executors/sort_executor.h"
namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while(child_executor_->Next(&tuple, &rid)) {
    sorted_.push_back(tuple);
  }
  std::sort(sorted_.begin(), sorted_.end(), [this](const Tuple &a, const Tuple &b) {
      for(auto [type, expr] : plan_->GetOrderBy()) {
      bool default_order_by = (type == OrderByType::DEFAULT || type == OrderByType::ASC);
      if(expr->Evaluate(&a, child_executor_->GetOutputSchema()).CompareLessThan(expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
	  return default_order_by;
	  }
	  if(expr->Evaluate(&a, child_executor_->GetOutputSchema()).CompareGreaterThan(expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
	  return !default_order_by;
	  }
	  }
	  return true;
	  });
  iter_ = sorted_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(iter_ != sorted_.end()) {
    *tuple = (*iter_);
    iter_++;
    return true;
  }
  return false;
}

}  // namespace bustub
