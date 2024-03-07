#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // LOG_DEBUG("lock root\n");
  root_latch_.RLock();
  if (IsEmpty()) {
    // LOG_DEBUG("unlock root\n");
    root_latch_.RUnlock();
    return false;
  }
  bool found = false;
  auto page = GetLeafPage(key, Operation::Read);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(key, leaf_page->KeyAt(i)) == 0) {
      result->emplace_back(leaf_page->ValueAt(i));
      found = true;
    }
  }
  // LOG_DEBUG("unlock pageid %d\n", page->GetPageId());
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return found;
}

// return leaf page
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(const KeyType &key, Operation op, Transaction *transaction, bool first_pass,
                                 Page *prev_page) -> Page * {
  if (!first_pass) {
    root_latch_.WLock();
    transaction->AddIntoPageSet(nullptr);
  }
  page_id_t next_page_id = root_page_id_;
  while (true) {
    Page *page = buffer_pool_manager_->FetchPage(next_page_id);
    auto tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

    if (first_pass) {
      if (tree_page->IsLeafPage() && op != Operation::Read) {
        page->WLatch();
        transaction->AddIntoPageSet(page);
      } else {
        page->RLatch();
      }
      FreePrevPage(prev_page);
    } else {
      assert(op != Operation::Read);
      page->WLatch();
      if (IsPageSafe(tree_page, op)) {
        ReleaseWLatches(transaction);
      }
      transaction->AddIntoPageSet(page);
    }
    if (tree_page->IsLeafPage()) {
      if (first_pass && !IsPageSafe(tree_page, op)) {
        ReleaseWLatches(transaction);
        // return nullptr;
        return GetLeafPage(key, op, transaction, false);
      }
      return page;
    }
    prev_page = page;
    auto internal_page = static_cast<InternalPage *>(tree_page);
    next_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    for (int i = 1; i < internal_page->GetSize(); i++) {
      if (comparator_(internal_page->KeyAt(i), key) > 0) {
        next_page_id = internal_page->ValueAt(i - 1);
        break;
      }
    }
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // LOG_DEBUG("lock root\n");
  root_latch_.RLock();
  if (IsEmpty()) {
    root_latch_.RUnlock();
    root_latch_.WLock();
    // transaction->AddIntoPageSet(nullptr);
    if (IsEmpty()) {
      Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
      // LOG_DEBUG("first page = %d\n", root_page_id_);
      UpdateRootPageId(1);
      auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
      leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
      leaf_page->SetKeyValueAt(0, key, value);
      leaf_page->IncreaseSize(1);
      leaf_page->SetNextPageId(INVALID_PAGE_ID);
      root_latch_.WUnlock();
      // ReleaseWLatches(transaction);
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      return true;
    }
    root_latch_.WUnlock();
    root_latch_.RLock();
    // ReleaseWLatches(transaction);
  }

  Page *page = GetLeafPage(key, Operation::Insert, transaction);
  /*
  if(page == nullptr) {
    page = GetLeafPage(key, Operation::Insert, transaction, false);
  }
  */
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  // check for dup
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(key, leaf_page->KeyAt(i)) == 0) {
      ReleaseWLatches(transaction);
      // buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      return false;
    }
  }

  leaf_page->Insert(key, value, comparator_);

  if (leaf_page->GetSize() < leaf_max_size_) {
    ReleaseWLatches(transaction);
    // buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  }
  // leaf page is full, split.
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  auto new_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  new_leaf_page->Init(new_page_id, leaf_page->GetParentPageId(), leaf_max_size_);
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_page_id);
  leaf_page->MoveDataTo(new_leaf_page, (leaf_max_size_ + 1) / 2);

  BPlusTreePage *old_tree_page = leaf_page;
  BPlusTreePage *new_tree_page = new_leaf_page;
  KeyType split_key = new_leaf_page->KeyAt(0);
  // split_key = oldtreelast or newtreefirst ?//
  while (true) {
    if (old_tree_page->IsRootPage()) {
      Page *new_page = buffer_pool_manager_->NewPage(&root_page_id_);
      auto new_root_page = reinterpret_cast<InternalPage *>(new_page->GetData());
      new_root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
      // split_key here is not used, just a placeholder.
      new_root_page->SetKeyValueAt(0, split_key, old_tree_page->GetPageId());
      new_root_page->SetKeyValueAt(1, split_key, new_tree_page->GetPageId());
      new_root_page->IncreaseSize(2);
      old_tree_page->SetParentPageId(root_page_id_);
      new_tree_page->SetParentPageId(root_page_id_);
      UpdateRootPageId();
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      break;
    }

    page_id_t parent_page_id = old_tree_page->GetParentPageId();
    Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    // unpin it right away to make sure pin_count = 1;
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    auto parent_internal_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
    parent_internal_page->Insert(split_key, new_tree_page->GetPageId(), comparator_);
    new_tree_page->SetParentPageId(parent_internal_page->GetPageId());
    if (parent_internal_page->GetSize() <= internal_max_size_) {
      // buffer_pool_manager_->UnpinPage(parent_page_id, true);
      break;
    }
    // not sure which way is the right way to go.
    /*
    if (parent_internal_page->GetSize() < internal_max_size_) {
      parent_internal_page->Insert(split_key, new_tree_page->GetPageId(), comparator_);
      new_tree_page->SetParentPageId(parent_page_id);
      //buffer_pool_manager_->UnpinPage(parent_page_id, true);
      break;
    }
    */
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    auto new_internal_page = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_internal_page->Init(new_page_id, parent_internal_page->GetParentPageId(), internal_max_size_);
    // updated version started from here

    int new_page_size = (internal_max_size_ + 1) / 2;
    // move half of parent page data to new internal page
    size_t start_index = parent_internal_page->GetSize() - new_page_size;
    for (int i = start_index, j = 0; i < parent_internal_page->GetSize(); i++, j++) {
      new_internal_page->SetKeyValueAt(j, parent_internal_page->KeyAt(i), parent_internal_page->ValueAt(i));
      Page *page = buffer_pool_manager_->FetchPage(parent_internal_page->ValueAt(i));
      auto tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
      if (tree_page == nullptr) {
        throw std::logic_error("internal tree page point to null page");
      }
      tree_page->SetParentPageId(new_page_id);
      buffer_pool_manager_->UnpinPage(tree_page->GetPageId(), true);
    }
    parent_internal_page->SetSize(internal_max_size_ - new_page_size + 1);
    new_internal_page->SetSize(new_page_size);
    /*
        int ins_at = 1;
        while (ins_at < parent_internal_page->GetSize() &&
               comparator_(parent_internal_page->KeyAt(ins_at), split_key) < 0) {
          ins_at++;
        }
        int last = (internal_max_size_ - 1) / 2;
        if (ins_at <= last) {
          auto key = parent_internal_page->KeyAt(last);
          auto value = parent_internal_page->ValueAt(last);
          for (int i = ins_at; i < last; i++) {
            parent_internal_page->SetKeyValueAt(i + 1, parent_internal_page->KeyAt(i),
       parent_internal_page->ValueAt(i));
          }
          parent_internal_page->SetKeyValueAt(ins_at, split_key, new_tree_page->GetPageId());
          new_internal_page->SetKeyValueAt(0, key, value);
          for (int i = last + 1, j = 1; i < internal_max_size_; i++, j++) {
            new_internal_page->SetKeyValueAt(j, parent_internal_page->KeyAt(i), parent_internal_page->ValueAt(i));
          }
        } else {
          //parent_index refer to parent internal page(old)
          //cur index refer to new parent internal page(new)
          int parent_index = last + 1;
          int cur = 0;
          while (parent_index < internal_max_size_) {
            if (parent_index == ins_at) {
              new_internal_page->SetKeyValueAt(cur, split_key, new_tree_page->GetPageId());
              cur++;
            }
            new_internal_page->SetKeyValueAt(cur, parent_internal_page->KeyAt(parent_index),
                                             parent_internal_page->ValueAt(parent_index));
            parent_index++;
            cur++;
          }
          if (cur < internal_max_size_ / 2 + 1) {
            new_internal_page->SetKeyValueAt(cur, split_key, new_tree_page->GetPageId());
          }
        }
        parent_internal_page->SetSize((internal_max_size_ + 1) / 2);
        for (int i = 0; i < parent_internal_page->GetSize(); i++) {
          Page *page = buffer_pool_manager_->FetchPage(parent_internal_page->ValueAt(i));
          auto tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
          tree_page->SetParentPageId(parent_internal_page->GetPageId());
          buffer_pool_manager_->UnpinPage(tree_page->GetPageId(), true);
        }
        new_internal_page->SetSize(internal_max_size_ / 2 + 1);
        for (int i = 0; i < new_internal_page->GetSize(); i++) {
          Page *page = buffer_pool_manager_->FetchPage(new_internal_page->ValueAt(i));
          auto tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
          tree_page->SetParentPageId(new_internal_page->GetPageId());
          buffer_pool_manager_->UnpinPage(tree_page->GetPageId(), true);
        }
    */
    // buffer_pool_manager_->UnpinPage(old_tree_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_tree_page->GetPageId(), true);
    old_tree_page = parent_internal_page;
    new_tree_page = new_internal_page;
    split_key = new_internal_page->KeyAt(0);
  }

  // buffer_pool_manager_->UnpinPage(old_tree_page->GetPageId(), true);
  ReleaseWLatches(transaction);
  buffer_pool_manager_->UnpinPage(new_tree_page->GetPageId(), true);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // LOG_DEBUG("lock root\n");
  root_latch_.RLock();
  if (IsEmpty()) {
    root_latch_.RUnlock();
    return;
  }

  Page *page = GetLeafPage(key, Operation::Remove, transaction);
  /*
  if(page == nullptr) {
    page = GetLeafPage(key, Operation::Remove, transaction, false);
  }
  */
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  leaf_page->Remove(key, comparator_);

  if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
    HandleUnderflow(leaf_page, transaction);
  }
  ReleaseWLatches(transaction);
  DeletePages(transaction);
  // buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::HandleUnderflow(BPlusTreePage *page, Transaction *transaction) {
  // LOG_DEBUG("######pageid = %d,IsRootpage = %d  ########\n", page->GetPageId(), page->IsRootPage());
  if (page->IsRootPage()) {
    if (page->GetSize() > 1 || (page->IsLeafPage() && page->GetSize() == 1)) {
      // buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      return;
    }
    if (page->IsLeafPage()) {
      transaction->AddIntoDeletedPageSet(page->GetPageId());
      root_page_id_ = INVALID_PAGE_ID;
    } else {
      // At here, the delete is not safe, so both root_latch and root page will be hold, no other thread could sneak in,
      // so no need to lock new_root_page
      // BUSTUB_ASSERT(page->GetSize() > 0, "Internal page root size shouldn't be decreased to 0");
      // BUSTUB_ASSERT(!page->IsLeafPage(), "Internal_page is not LeafPage");
      auto old_root_page = static_cast<InternalPage *>(page);
      auto root_page = buffer_pool_manager_->FetchPage(old_root_page->ValueAt(0));
      auto new_root_page = reinterpret_cast<LeafPage *>(root_page->GetData());
      root_page_id_ = new_root_page->GetPageId();
      // buffer_pool_manager_->DeletePage(root_page_id_);
      transaction->AddIntoDeletedPageSet(old_root_page->GetPageId());
      new_root_page->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
    }
    UpdateRootPageId();
    return;
  }
  page_id_t left_sibling_id;
  page_id_t right_sibling_id;
  GetSiblings(page, left_sibling_id, right_sibling_id);
  if (left_sibling_id == INVALID_PAGE_ID && right_sibling_id == INVALID_PAGE_ID) {
    throw std::logic_error("Non root page suppose to have at least sibling");
  }
  BPlusTreePage *left_sibling_page = nullptr;
  BPlusTreePage *right_sibling_page = nullptr;
  if (left_sibling_id != INVALID_PAGE_ID) {
    auto left_page = buffer_pool_manager_->FetchPage(left_sibling_id);
    // LOG_DEBUG("lock sibling leaf pageid %d\n", left_page->GetPageId());
    left_page->WLatch();
    left_sibling_page = reinterpret_cast<BPlusTreePage *>(left_page->GetData());
  }
  if (right_sibling_id != INVALID_PAGE_ID) {
    auto right_page = buffer_pool_manager_->FetchPage(right_sibling_id);
    // LOG_DEBUG("lock sibling right pageid %d\n", right_page->GetPageId());
    right_page->WLatch();
    right_sibling_page = reinterpret_cast<BPlusTreePage *>(right_page->GetData());
  }
  auto parent_page =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(page->GetParentPageId())->GetData());
  // In handleundleflow means already holding nessasary locks
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  // root_page->WUnlatch();
  if (TryBorrow(page, left_sibling_page, parent_page, true) ||
      TryBorrow(page, right_sibling_page, parent_page, false)) {
    UnpinSiblings(left_sibling_id, right_sibling_id);
    // current page will be unpined at the previous call to handlerUnderflow
    // buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return;
  }
  BPlusTreePage *left_page;
  BPlusTreePage *right_page;
  if (left_sibling_page != nullptr) {
    left_page = left_sibling_page;
    right_page = page;
  } else {
    left_page = page;
    right_page = right_sibling_page;
  }
  MergePage(left_page, right_page, parent_page, transaction);
  UnpinSiblings(left_sibling_id, right_sibling_id);
  if (parent_page->GetSize() < parent_page->GetMinSize()) {
    HandleUnderflow(parent_page, transaction);
  }
  // buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetSiblings(BPlusTreePage *page, page_id_t &left, page_id_t &right) {
  if (page->IsRootPage()) {
    throw std::invalid_argument("Can't get root's siblings");
  }
  auto parent_page =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(page->GetParentPageId())->GetData());
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
  int index = parent_page->FindValue(page->GetPageId());
  if (index == -1) {
    throw std::logic_error("Child page is not present");
  }
  left = right = INVALID_PAGE_ID;
  if (index != 0) {
    left = parent_page->ValueAt(index - 1);
  }
  if (index != parent_page->GetSize() - 1) {
    right = parent_page->ValueAt(index + 1);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::TryBorrow(BPlusTreePage *page, BPlusTreePage *sibling_page, InternalPage *parent_page,
                               bool sibling_at_left) -> bool {
  if (sibling_page == nullptr) {
    return false;
  }
  int sibling_page_size = sibling_page->GetSize();
  auto leaf_sibling_page_min_size = static_cast<LeafPage *>(sibling_page)->GetMinSize();
  auto internal_sibling_page_min_size = static_cast<InternalPage *>(sibling_page)->GetMinSize();
  /*
   LOG_DEBUG("Isleafpage = %d, sibling_page_size = %d, leaf_sibling_page_min_size = %d, internal_page_min_size = %d\n",
   sibling_page->IsLeafPage(), sibling_page_size, leaf_sibling_page_min_size, internal_sibling_page_min_size);
   */
  if ((sibling_page->IsLeafPage() && sibling_page_size <= leaf_sibling_page_min_size) ||
      (!sibling_page->IsLeafPage() && sibling_page_size <= internal_sibling_page_min_size)) {
    return false;
  }

  int sibling_borrow_at = sibling_at_left ? sibling_page->GetSize() - 1 : (page->IsLeafPage() ? 0 : 1);
  //parent_update_at refers to the key index in parent page.
  int parent_update_at = parent_page->FindValue(page->GetPageId()) + (sibling_at_left ? 0 : 1);

  // LOG_DEBUG("sibling_page_id = %d\n", sibling_page->GetPageId());
  // LOG_DEBUG("sibling_page Size = %d\n", sibling_page->GetSize());
  // LOG_DEBUG("sibling_borrow_at = %d, parent_update_at = %d\n", sibling_borrow_at, parent_update_at);

  KeyType update_key;
  if (page->IsLeafPage()) {
    auto leaf_page = static_cast<LeafPage *>(page);
    auto leaf_sibling_page = static_cast<LeafPage *>(sibling_page);
    leaf_page->Insert(leaf_sibling_page->KeyAt(sibling_borrow_at), leaf_sibling_page->ValueAt(sibling_borrow_at),
                      comparator_);
    leaf_sibling_page->Remove(leaf_sibling_page->KeyAt(sibling_borrow_at), comparator_);
    update_key = sibling_at_left ? leaf_page->KeyAt(0) : leaf_sibling_page->KeyAt(0);
  } else {
    auto internal_page = static_cast<InternalPage *>(page);
    auto internal_sibling_page = static_cast<InternalPage *>(sibling_page);
    update_key = internal_sibling_page->KeyAt(sibling_borrow_at);
    page_id_t child_id;
    if (sibling_at_left) {
      internal_page->Insert(parent_page->KeyAt(parent_update_at), internal_page->ValueAt(0), comparator_);
      internal_page->SetValueAt(0, internal_sibling_page->ValueAt(sibling_borrow_at));
      child_id = internal_page->ValueAt(0);
    } else {
      internal_page->SetKeyValueAt(internal_page->GetSize(), parent_page->KeyAt(parent_update_at),
                                   internal_sibling_page->ValueAt(0));
      internal_page->IncreaseSize(1);
      internal_sibling_page->SetValueAt(0, internal_sibling_page->ValueAt(1));
      child_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    }
    internal_sibling_page->RemoveAt(sibling_borrow_at);
    Page *page = buffer_pool_manager_->FetchPage(child_id);
    auto child_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    child_page->SetParentPageId(internal_page->GetPageId());
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
  parent_page->SetKeyAt(parent_update_at, update_key);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnpinSiblings(page_id_t left, page_id_t right) {
  if (left != INVALID_PAGE_ID) {
    auto left_page = buffer_pool_manager_->FetchPage(left);
    buffer_pool_manager_->UnpinPage(left, true);
    // LOG_DEBUG("unlock left sibling pageid %d\n", left);
    left_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(left, true);
  }
  if (right != INVALID_PAGE_ID) {
    auto right_page = buffer_pool_manager_->FetchPage(right);
    buffer_pool_manager_->UnpinPage(right, true);
    // LOG_DEBUG("unlock right sibling pageid %d\n", right);
    right_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(right, true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergePage(BPlusTreePage *left_page, BPlusTreePage *right_page, InternalPage *parent_page,
                               Transaction *transaction) {
  if (left_page->IsLeafPage()) {
    auto left_leaf_page = static_cast<LeafPage *>(left_page);
    auto right_leaf_page = static_cast<LeafPage *>(right_page);
    for (int i = 0; i < right_leaf_page->GetSize(); i++) {
      left_leaf_page->Insert(right_leaf_page->KeyAt(i), right_leaf_page->ValueAt(i), comparator_);
    }
    left_leaf_page->SetNextPageId(right_leaf_page->GetNextPageId());
    parent_page->RemoveAt(parent_page->FindValue(right_page->GetPageId()));
  } else {
    auto left_internal_page = static_cast<InternalPage *>(left_page);
    auto right_internal_page = static_cast<InternalPage *>(right_page);
    left_internal_page->Insert(parent_page->KeyAt(parent_page->FindValue(right_page->GetPageId())),
                               right_internal_page->ValueAt(0), comparator_);
    SetPageParentId(right_internal_page->ValueAt(0), left_internal_page->GetPageId());
    parent_page->RemoveAt(parent_page->FindValue(right_page->GetPageId()));
    // LOG_DEBUG("right_page_id = %d, right_page_size = %d, parent_page_id = %d, parent_page_size = %d\n",
    // right_page->GetPageId(), right_page->GetSize(), parent_page->GetPageId(), parent_page->GetSize());
    for (int i = 1; i < right_internal_page->GetSize(); i++) {
      left_internal_page->Insert(right_internal_page->KeyAt(i), right_internal_page->ValueAt(i), comparator_);
      SetPageParentId(right_internal_page->ValueAt(i), left_internal_page->GetPageId());
    }
  }
  transaction->AddIntoDeletedPageSet(right_page->GetPageId());
  // buffer_pool_manager_->DeletePage(right_page->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetPageParentId(page_id_t child_page_id, page_id_t parent_page_id) {
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_page_id)->GetData());
  page->SetParentPageId(parent_page_id);
  buffer_pool_manager_->UnpinPage(child_page_id, true);
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  root_latch_.RLock();
  if (IsEmpty()) {
    root_latch_.RUnlock();
    return End();
  }
  page_id_t next_page_id = root_page_id_;
  Page *prev_page = nullptr;
  while (true) {
    Page *page = buffer_pool_manager_->FetchPage(next_page_id);
    page->RLatch();
    if (prev_page == nullptr) {
      root_latch_.RUnlock();
    } else {
      prev_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), false);
    }
    prev_page = page;

    auto tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (tree_page->IsLeafPage()) {
      return INDEXITERATOR_TYPE(tree_page->GetPageId(), 0, buffer_pool_manager_);
    }
    auto internal_page = static_cast<InternalPage *>(tree_page);
    if (internal_page == nullptr) {
      throw std::bad_cast();
    }
    next_page_id = internal_page->ValueAt(0);
  }
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  root_latch_.RLock();
  Page *page = GetLeafPage(key, Operation::Read);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  return INDEXITERATOR_TYPE(page->GetPageId(), leaf_page->LowerBound(key, comparator_), buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsPageSafe(BPlusTreePage *tree_page, Operation op) -> bool {
  if (op == Operation::Read) {
    return true;
  }

  if (op == Operation::Insert) {
    if (tree_page->IsLeafPage()) {
      return tree_page->GetSize() < tree_page->GetMaxSize() - 1;
    }
    return tree_page->GetSize() < tree_page->GetMaxSize();
  }

  if (op == Operation::Remove) {
    if (tree_page->IsRootPage()) {
      if (tree_page->IsLeafPage()) {
        return tree_page->GetSize() > 1;
      }
      return tree_page->GetSize() > 2;
    }
    if (tree_page->IsLeafPage()) {
      return tree_page->GetSize() > tree_page->GetMinSize();
    }
    auto internal_page = static_cast<InternalPage *>(tree_page);
    return internal_page->GetSize() > internal_page->GetMinSize();
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseWLatches(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }
  auto page_set = transaction->GetPageSet();
  while (!page_set->empty()) {
    Page *page = page_set->front();
    page_set->pop_front();
    if (page == nullptr) {
      // LOG_DEBUG("unlock root page\n");
      root_latch_.WUnlock();
    } else {
      // LOG_DEBUG("unlock pageid %d\n", page->GetPageId());
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeletePages(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }
  auto deleted_page_set = transaction->GetDeletedPageSet();
  for (auto deleted_page_id : *(deleted_page_set)) {
    buffer_pool_manager_->DeletePage(deleted_page_id);
  }
  deleted_page_set->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FreePrevPage(Page *prev_page) {
  if (prev_page == nullptr) {
    root_latch_.RUnlock();
  } else {
    prev_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), false);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
