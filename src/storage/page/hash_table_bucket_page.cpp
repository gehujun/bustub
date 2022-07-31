//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  
  for (uint32_t bucket_idx = 0; bucket_idx < next_occupied_index_; bucket_idx++) {
    if((cmp(key, array_[bucket_idx].first) == 0)){
      result->emplace_back(array_[bucket_idx].second);
    }
  }
  return result->size() == 0 ? false : true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::(KeyType key, ValueType value, KeyComparator cmp) {
  // 桶条目溢出
  
  if(next_occupied_index_ >= BUCKET_ARRAY_SIZE){
    LOG_INFO("bucket entry overflowing!");
    return false;
  }
  
  // 重复键值对查询
  // for (uint32_t bucket_idx = 0; bucket_idx < next_occupied_index_; bucket_idx++) {
  //   if ((cmp(key, array_[bucket_idx].first) == 0)) {
  //     //查找重复的KV对,怎么比对value的大小？
  //     // MappingType kv_entry = 
  //     LOG_INFO("there are some duplicate key");
  //     return false;
  //   }
  // }
  // LOG_INFO("bucket page insert kv entry at %d bucket_idx",next_occupied_index_);
  SetOccupiedBit(next_occupied_index_, 1);
  SetReadableBit(next_occupied_index_, 1);
  array_[next_occupied_index_].first = key;
  array_[next_occupied_index_].second = value;
  // array_[next_occupied_index_] = MappingType(key, value);
  if(cmp(key,array_[next_occupied_index_].first) != 0){
    LOG_INFO("error insert");
    return false;
  }
  next_occupied_index_++;
  
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  uint32_t bucket_idx = 0;
  for (; bucket_idx < static_cast<uint32_t>(BUCKET_ARRAY_SIZE); bucket_idx++) {
    if (GetOccupiedBit(bucket_idx) && GetReadableBit(bucket_idx)) {
      if ((cmp(key, array_[bucket_idx].first) == 0)) {
        SetReadableBit(bucket_idx, 0);
        return true;
      }
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  // LOG_INFO(" bucket_idx is %d",bucket_idx);
  if (GetOccupiedBit(bucket_idx) && GetReadableBit(bucket_idx)) {
    return array_[bucket_idx].first;
  }
  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  if (GetOccupiedBit(bucket_idx) && GetReadableBit(bucket_idx)) {
    return array_[bucket_idx].second;
  }
  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  if (GetOccupiedBit(bucket_idx) && GetReadableBit(bucket_idx)) {
    SetReadableBit(bucket_idx, 0);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  // PrintBucket();
  return GetOccupiedBit(bucket_idx) == 1 ? true : false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  SetOccupiedBit(bucket_idx, 1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return GetReadableBit(bucket_idx) == 1 ? true : false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  SetReadableBit(bucket_idx, 1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  return 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
