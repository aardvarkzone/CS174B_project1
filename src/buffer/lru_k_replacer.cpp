//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <climits>

namespace bustub {
    // at top

    LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

    /*evict the frame with largest backward k-distance compared to all other
    evictable frames being tracked by the Replacer. Store the frame id in the output parameter and return
    True. If there are no evictable frames return False*/
    auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
        latch_.lock();
        if(curr_size_ == 0) {
            latch_.unlock();
            return false;
        }
        size_t leastRecentAccess = SIZE_MAX;
        LRUKNode theNode;
        for(size_t i = 0; i < replacer_size_; i++) {
            auto node = node_store_.find(i);
            if(node != node_store_.end() && node->second.is_evictable_) {
                if(node->second.history_.size() >= k_) {
                    size_t access =  node->second.history_.at(((node->second.history_.size() / k_) * k_) - 1);
                    if(access < leastRecentAccess) {
                        theNode = node->second;         
                        leastRecentAccess = access;            
                    }           
                } else {
                    theNode = node->second;
                    break;
                }
                       
            } 
        }
        *frame_id = theNode.fid_;
        this->Remove(*frame_id);
        
        
        latch_.unlock();
        return true; 
    }
    /*Record that given frame id is accessed at current timestamp.
    ◦ Called after a page is pinned in the BufferPoolManager*/
    void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
        latch_.lock();
        auto node = node_store_.find(frame_id);
        if(node != node_store_.end()) {
            node->second.history_.push_back(current_timestamp_);
            current_timestamp_++;
        } else {
            LRUKNode l;
            l.history_.push_back(current_timestamp_);
            l.is_evictable_ = false;
            l.fid_ = frame_id;
            l.k_ = k_;
            node_store_.insert({frame_id, l});
        }
        latch_.unlock();
    }


    /*This method controls whether a frame
    is evictable or not. It also controls LRUKReplacer’s size. You’ll know when to call this function when
    you implement the BufferPoolManager. To be specific, when the pin count of a page reaches 0, its
    corresponding frame is marked evictable and replacer’s size is incremented.*/
    void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
        latch_.lock();

        auto evictedNode = node_store_.find(frame_id);
        if(evictedNode != node_store_.end()) {
            if(evictedNode->second.is_evictable_ && !set_evictable) {
                curr_size_ -= 1;

            } else if(!evictedNode->second.is_evictable_ && set_evictable){
                curr_size_ += 1;

            }
            //otherwise don't fuck w size aka in the case that you stay the same
            evictedNode->second.is_evictable_ = set_evictable;
        }
    

        latch_.unlock(); 

    }

    void LRUKReplacer::Remove(frame_id_t frame_id) {
        //TODO NEED TO FIX THIS TO BE A COND VAR
        // latch_.lock();
        node_store_.erase(frame_id);
        --curr_size_;
        //latch_.unlock();
    }

    /*This method returns the number of evictable frames that are currently in the LRUKReplacer.*/
    auto LRUKReplacer::Size() -> size_t { 
        latch_.lock();
        size_t size = curr_size_;
        latch_.unlock();
        return size; 
    }
}  // namespace bustub
