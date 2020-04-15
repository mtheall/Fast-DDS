// Copyright 2020 Proyectos y Sistemas de Mantenimiento SL (eProsima).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef _FASTDDS_SHAREDMEM_ROBUST_INTERPROCESS_CONDITION_
#define _FASTDDS_SHAREDMEM_ROBUST_INTERPROCESS_CONDITION_

#include <boost/interprocess/sync/detail/locks.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>

namespace eprosima {
namespace fastdds {
namespace rtps {

namespace bi = boost::interprocess;

class RobustInterprocessCondition
{
public:

    RobustInterprocessCondition()
        : free_(MAX_LISTENERS)
    {

    }

    ~RobustInterprocessCondition()
    {
    }

    //!If there is a thread waiting on *this, change that
    //!thread's state to ready. Otherwise there is no effect.
    void notify_one()
    {
        bi::scoped_lock<bi::interprocess_mutex> lock(semaphore_indexes_mutex_);

        auto num_listeners = listening_.size();

        if (num_listeners)
        {
            auto sem_index = dequeue_listener(--num_listeners);
            semaphores_pool_[sem_index].post();
        }
    }

    //!Change the state of all threads waiting on *this to ready.
    //!If there are no waiting threads, notify_all() has no effect.
    void notify_all()
    {
        bi::scoped_lock<bi::interprocess_mutex> lock(semaphore_indexes_mutex_);

        auto num_listeners = listening_.size();

        while (num_listeners)
        {
            auto sem_index = dequeue_listener(--num_listeners);
            semaphores_pool_[sem_index].post();
        }
    }

    //!Releases the lock on the interprocess_mutex object associated with lock, blocks
    //!the current thread of execution until readied by a call to
    //!this->notify_one() or this->notify_all(), and then reacquires the lock.
    template <typename L>
    void wait(
            L& lock)
    {        
        do_wait(*lock.mutex());
    }

    //!The same as:
    //!while (!pred()) wait(lock)
    template <typename L, typename Pr>
    void wait(
            L& lock,
            Pr pred)
    {        
        while (!pred())
            do_wait(*lock.mutex());
    }

    //!Releases the lock on the interprocess_mutex object associated with lock, blocks
    //!the current thread of execution until readied by a call to
    //!this->notify_one() or this->notify_all(), or until time abs_time is reached,
    //!and then reacquires the lock.
    //!Returns: false if time abs_time is reached, otherwise true.
    template <typename L>
    bool timed_wait(
            L& lock,
            const boost::posix_time::ptime& abs_time)
    {
        //Handle infinity absolute time here to avoid complications in do_timed_wait
        if (abs_time == boost::posix_time::pos_infin)
        {
            this->wait(lock);
            return true;
        }
        return this->do_timed_wait(abs_time, *lock.mutex());
    }

    //!The same as:   while (!pred()) {
    //!                  if (!timed_wait(lock, abs_time)) return pred();
    //!               } return true;
    template <typename L, typename Pr>
    bool timed_wait(
            L& lock,
            const boost::posix_time::ptime& abs_time,
            Pr pred)
    {
        //Posix does not support infinity absolute time so handle it here
        if (abs_time == boost::posix_time::pos_infin)
        {
            wait(lock, pred);
            return true;
        }
        while (!pred()){
            if (!do_timed_wait(abs_time, *lock.mutex()))
            {
                return pred();
            }
        }
        return true;
    }

private:

    static constexpr uint32_t MAX_LISTENERS = 128;

    class SemaphoreIndexes
    {
private:

        uint32_t indexes_[MAX_LISTENERS];
        uint32_t size_;

public:

        SemaphoreIndexes()
            : size_(0)
        {
        }

        SemaphoreIndexes(
                uint32_t size)
            : size_(size)
        {
            assert(size <=  MAX_LISTENERS);

            for (uint32_t i = 0; i < size; i++)
            {
                indexes_[i] = i;
            }
        }

        inline uint32_t push(
                uint32_t index)
        {
            indexes_[size_++] = index;
            return size_-1;
        }

        inline uint32_t pop()
        {            
            assert(size_ > 0);

            return indexes_[--size_];
        }

        inline uint32_t remove(
                uint32_t pos)
        {
            assert(pos < size_);

            auto index = indexes_[pos];

            memmove(&indexes_[pos], &indexes_[pos+1], (size_ - pos) * sizeof(uint32_t));

            return index;
        }

        inline uint32_t size()
        {
            return size_;
        }
    };

    bi::interprocess_semaphore semaphores_pool_[MAX_LISTENERS];
    bi::interprocess_mutex semaphore_indexes_mutex_;
    SemaphoreIndexes listening_;
    SemaphoreIndexes free_;

    inline uint32_t enqueue_listener(
            uint32_t* sem_index_ptr)
    {
        if(free_.size() == 0)   
        {
            throw(bi::interprocess_exception("RobustInterprocessCondition: too much listeners!"));
        }

        auto sem_index = free_.pop();
        *sem_index_ptr = sem_index;
        return listening_.push(sem_index);
    }

    inline uint32_t dequeue_listener(
            uint32_t listening_pos)
    {
        auto sem_index = listening_.remove(listening_pos);
        free_.push(sem_index);
        return sem_index;
    }

    inline void do_wait(
            bi::interprocess_mutex& mut)
    {        
        uint32_t sem_index;

        {
            bi::scoped_lock<bi::interprocess_mutex> lock_enqueue(semaphore_indexes_mutex_);
            enqueue_listener(&sem_index);
        }

        {
            // Release caller's lock
            bi::ipcdetail::lock_inverter<bi::interprocess_mutex> inverted_lock(mut);
            bi::scoped_lock<bi::ipcdetail::lock_inverter<bi::interprocess_mutex>> unlock(inverted_lock);

            semaphores_pool_[sem_index].timed_wait(boost::posix_time::pos_infin);
        }
    }

    inline bool do_timed_wait(
            const boost::posix_time::ptime& abs_time,
            bi::interprocess_mutex& mut)
    {
        bool ret;
        uint32_t pos;
        uint32_t sem_index;

        {
            bi::scoped_lock<bi::interprocess_mutex> lock_enqueue(semaphore_indexes_mutex_);
            pos = enqueue_listener(&sem_index);
        }

        {
            // Release caller's lock
            bi::ipcdetail::lock_inverter<bi::interprocess_mutex> inverted_lock(mut);
            bi::scoped_lock<bi::ipcdetail::lock_inverter<bi::interprocess_mutex>> unlock(inverted_lock);

            ret = semaphores_pool_[sem_index].timed_wait(abs_time);
        }

        if(!ret)
        {
            bi::scoped_lock<bi::interprocess_mutex> lock_dequeue(semaphore_indexes_mutex_);
            dequeue_listener(pos);
        }

        return ret;
    }
};

} // namespace rtps
} // namespace fastdds
} // namespace eprosima

#endif // _FASTDDS_SHAREDMEM_ROBUST_INTERPROCESS_CONDITION_
