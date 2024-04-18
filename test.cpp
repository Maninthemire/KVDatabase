#include "skiplist/tskiplist.h"
#include <iostream>
#include <vector>
#include <thread>
#include <random>
#include <atomic>

constexpr int NUM_THREADS = 10;
constexpr int INITIAL_INSERTS_PER_THREAD = 10;
constexpr int NUM_OPERATIONS = 1000;

std::atomic<bool> start_flag(false);

enum class Operation {
    Insert,
    Delete,
    Search
};

// Function to perform initial insertions to populate the skip list
template<typename K, typename V>
void initial_insertions(tSkipList<K, V>& skip_list, int thread_id) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis(0, 128);

    for (int i = 0; i < INITIAL_INSERTS_PER_THREAD; ++i) {
        int key = dis(gen);
        V value = key * 10;
        skip_list.insert_element(key, value);
        std::cout << "Thread " << thread_id << " initially inserted key " << key << std::endl;
    }
}

// Perform random operations on the skip list
template<typename K, typename V>
void perform_operations(tSkipList<K, V>& skip_list, int thread_id) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis(0, 128);
    std::uniform_int_distribution<int> op_dis(0, 2); // Only insert, delete, search

    while (!start_flag) {} // Wait until all threads are ready to start

    for (int i = 0; i < NUM_OPERATIONS; ++i) {
        int key = dis(gen);
        V value = key * 10;
        Operation op = static_cast<Operation>(op_dis(gen));

        switch(op) {
            case Operation::Insert:
                skip_list.insert_element(key, value);
                std::cout << "Thread " << thread_id << " inserted key " << key << std::endl;
                break;
            case Operation::Delete:
                skip_list.delete_element(key);
                std::cout << "Thread " << thread_id << " deleted key " << key << std::endl;
                break;
            case Operation::Search:
                if (skip_list.search_element(key, value)) {
                    std::cout << "Thread " << thread_id << " found key " << key << " with value " << value << std::endl;
                } else {
                    std::cout << "Thread " << thread_id << " did not find key " << key << std::endl;
                }
                break;
        }
    }
}

int main() {
    tSkipList<int, int> skip_list(7); // Initialize with a maximum level of 7

    std::vector<std::thread> threads;

    // Initial insertions
    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back(initial_insertions<int, int>, std::ref(skip_list), i);
    }

    // Wait for initial insertion threads to complete
    for (auto& t : threads) {
        t.join();
    }
    threads.clear();

    start_flag = false; // Reset flag for concurrent operations

    // Start threads for concurrent operations
    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back(perform_operations<int, int>, std::ref(skip_list), i);
    }

    start_flag = true; // Allow all threads to start operations

    // Wait for all threads to complete
    for (auto& t : threads) {
        t.join();
    }

    return 0;
}
