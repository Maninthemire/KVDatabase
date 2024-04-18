/*
 * @file        : threadsafeskiplist.h
 * @Author      : zhenxi
 * @Date        : 2024-04-18
 * @copyleft    : Apache 2.0
 * Description  : This file contains the declaration of the tSkipList class, which is designed 
 *                for manage the skip list in a thread safe and efficient manner. It encapsulates 
 *                operations such as search, insert and delete nodes.
 */

#ifndef THREAD_SAFE_SKIP_LIST_H
#define THREAD_SAFE_SKIP_LIST_H


#include <iostream> 
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <mutex>
#include <fstream>
#include <atomic>

#define STORE_FILE "store/dumpFile"

std::string delimiter = ":";


/**
 * @struct MutexNode
 * @brief The MutexNode struct is used to manage thread safe nodes in the skip list.
 */
template<typename K, typename V> 
struct MutexNode {
    std::mutex mtx;
    int node_level;
    K key;
    V value;
    MutexNode<K, V> **forward;
    
    /**
     * @brief Constructor for MutexNode.
     * To initialze the data structures.
     */
    MutexNode(int level, K k, V v) : node_level(level), key(k), value(v) {
        forward = new MutexNode<K, V>*[level + 1];
        for (int i = 0; i <= level; ++i)
            forward[i] = NULL;
    }

    /**
     * @brief Deconstructor for MutexNode.
     * To free the array of pointers.
     */
    ~MutexNode() {
        delete[] forward;  
    }
};

/**
 * @class tSkipList
 * @brief The tSkipList class is used to manage the thread safe skip list.
 */
template <typename K, typename V> 
class tSkipList {
    using SkipNode = MutexNode<K, V>;
public: 
    /**
     * @brief Constructor for SkipList.
     * To initialze the data structures.
     */
    tSkipList(int);

    /**
     * @brief Deconstructor for MutexNode.
     * To free the array of pointers.
     */
    ~tSkipList();
    
    /**
     * @return The number of elements in the skip node.
     */
    int size();

    /**
     * @brief To insert an element.
     * @return 0 if succeeds, 1 if key exists.
     */
    int insert_element(K, V);

    /**
     * @brief To search element by its key.
     * @param value_out To retrive the value.
     * @return True if found, False if not found.
     */
    bool search_element(K, V&);
    
    /**
     * @brief To delete element by its key.
     * If not found, do nothing.
     */
    void delete_element(K);
    
    /**
     * @brief To dump the skip list to the file.
     */
    void dump_file();
    
    /**
     * @brief To load the skip list from the file.
     */
    void load_file();

private:    
    /**
     * @brief To clear a node and the next nodes recursively.
     */
    void clear(SkipNode *);

    /**
     * @brief Deconstructor for MutexNode.
     * To free the array of pointers.
     */
    int get_random_level();

    /**
     * @brief To parse the str into key and value.
     */
    void string_to_kv(const std::string& str, std::string* key, std::string* value);
    
    /**
     * @brief To check if the string is a k-v pair.
     */
    bool is_valid_string(const std::string& str);
  
    // Maximum level of the skip list 
    const int _max_level;

    // current level of skip list 
    std::atomic<int> _skip_list_level;

    // pointer to header node 
    SkipNode * _header;

    // file operator
    std::mutex _file_mutex;
    std::ofstream _file_writer;
    std::ifstream _file_reader;

    // skiplist current element count
    std::atomic<int> _element_count;
};


template<typename K, typename V> 
tSkipList<K, V>::tSkipList(int max_level) : _max_level(max_level), _skip_list_level(0), _element_count(0) {
    // create header node and initialize key and value to null
    K k;
    V v;
    this->_header = new SkipNode(k, v, _max_level);
};

template<typename K, typename V> 
tSkipList<K, V>::~tSkipList() {

    this->_header->mtx.lock();
    if (_file_writer.is_open()) {
        _file_writer.close();
    }
    if (_file_reader.is_open()) {
        _file_reader.close();
    }

    //递归删除跳表链条
    if(_header->forward[0]!=nullptr){
        clear(_header->forward[0]);
    }
    delete(_header);
}

template<typename K, typename V> 
int tSkipList<K, V>::size() { 
    return _element_count;
}

template<typename K, typename V>
int tSkipList<K, V>::insert_element(const K key, const V value) {        
    // Generate a random level for node
    int random_level = get_random_level();

    this->_header->mtx.lock();
    std::mutex * cur_mutex = &(this->_header->mtx);
    
    SkipNode *current = this->_header;

    // create update array and initialize it 
    // update is array which put node that the node->forward[i] should be operated later
    SkipNode *update[_max_level+1];
    memset(update, 0, sizeof(SkipNode*)*(_max_level+1));  

    bool hold_lock = random_level > _skip_list_level;
    // if random_level is bigger, the lock of _header should be hold
    for(int i = _skip_list_level; i >= 0; i--) {
        while(current->forward[i] != NULL && current->forward[i]->key < key) {
            current->forward[i]->mtx.lock();

            current = current->forward[i]; 

            std::mutex * tmp_mutex = cur_mutex;
            cur_mutex = &(current->mtx);
            if(!hold_lock)
                tmp_mutex->unlock();
            else
                hold_lock = false;
        }
        update[i] = current;
        hold_lock = true;
    }

    // reached level 0 and forward pointer to right node, which is desired to insert key.
    current = current->forward[0];

    // if current node have key equal to searched key, we get it
    if (current != NULL && current->key == key) {
        std::cout << "key: " << key << ", exists" << std::endl;
        
        // unlock ever prev node
        update[random_level]->mtx.unlock();
        for (int i = random_level; i > 0; i--)
            if(update[i - 1] != update[i])
                update[i - 1]->mtx.unlock();
        return 1;
    }

    // if current is NULL that means we have reached to end of the level 
    // if current's key is not equal to key that means we have to insert node between update[0] and current node 
    if (current == NULL || current->key != key ) {

        // If random level is greater thar skip list's current level, initialize update value with pointer to header
        if (random_level > _skip_list_level) {
            for (int i = _skip_list_level+1; i < random_level+1; i++) {
                update[i] = _header;
            }
            _skip_list_level = random_level;
        }

        // create new node with random level generated 
        SkipNode* inserted_node = new SkipNode(key, value, random_level);
        
        // insert node 
        for (int i = 0; i <= random_level; i++) {
            inserted_node->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = inserted_node;
        }
        _element_count ++;

        // unlock ever prev node
        update[random_level]->mtx.unlock();
        for (int i = random_level; i > 0; i--)
            if(update[i - 1] != update[i])
                update[i - 1]->mtx.unlock();
    }
    return 0;
}

template<typename K, typename V> 
bool tSkipList<K, V>::search_element(K key, V& value_out) {
    this->_header->mtx.lock();
    std::mutex * cur_mutex = &(this->_header->mtx);

    SkipNode *current = _header;

    // start from highest level of skip list
    for (int i = _skip_list_level; i >= 0; i--) {
        while (current->forward[i] && current->forward[i]->key < key) {
            current->forward[i]->mtx.lock();

            current = current->forward[i]; 

            std::mutex * tmp_mutex = cur_mutex;
            cur_mutex = &(current->mtx);
            tmp_mutex->unlock();
        }
    }
    
    current->forward[0]->mtx.lock();

    current = current->forward[0]; 
    
    std::mutex * tmp_mutex = cur_mutex;
    cur_mutex = &(current->mtx);
    tmp_mutex->unlock();

    // if current node have key equal to searched key, we get it
    if (current and current->key == key) {
        value_out = current->value;
        cur_mutex->unlock();
        return true;
    }

    cur_mutex->unlock();
    return false;
}

template<typename K, typename V> 
void tSkipList<K, V>::delete_element(K key) {
    this->_header->mtx.lock();
    std::mutex * cur_mutex = &(this->_header->mtx);

    SkipNode *current = this->_header; 

    SkipNode *update[_max_level+1];
    memset(update, 0, sizeof(SkipNode*)*(_max_level+1));

    bool hold_lock = false;

    // start from highest level of skip list
    for (int i = _skip_list_level; i >= 0; i--) {
        while (current->forward[i] !=NULL && current->forward[i]->key < key) {
            current->forward[i]->mtx.lock();

            current = current->forward[i]; 

            std::mutex * tmp_mutex = cur_mutex;
            cur_mutex = &(current->mtx);
            if(!hold_lock)
                tmp_mutex->unlock();
            else
                hold_lock = false;
        }
        update[i] = current;
        hold_lock = true;
    }
    current->forward[0]->mtx.lock();
    current = current->forward[0];
    int update_level = _skip_list_level;
    if (current != NULL && current->key == key) {       
        // start for lowest level and delete the current node of each level
        for (int i = 0; i <= _skip_list_level; i++) {

            // if at level i, next node is not target node, break the loop.
            if (update[i]->forward[i] != current) 
                break;

            update[i]->forward[i] = current->forward[i];
        }

        // Remove levels which have no elements
        while (_skip_list_level > 0 && _header->forward[_skip_list_level] == 0) {
            _skip_list_level --; 
        }

        delete current;
        _element_count --;
    }
    // unlock ever prev node
    update[update_level]->mtx.unlock();
    for (int i = update_level; i > 0; i--)
        if(update[i - 1] != update[i])
            update[i - 1]->mtx.unlock();
    return;
}

template<typename K, typename V> 
void tSkipList<K, V>::dump_file() {
    _file_mutex.lock();
    _file_writer.open(STORE_FILE);
    
    this->_header->forward[0]->mtx.lock();
    std::mutex * cur_mutex = &(this->_header->forward[0]->mtx);

    SkipNode *node = this->_header->forward[0]; 

    while (node != NULL) {
        _file_writer << node->key << ":" << node->value << "\n";

        node->forward[0]->mtx.lock();

        node = node->forward[0]; 

        std::mutex * tmp_mutex = cur_mutex;
        cur_mutex = &(node->mtx);
        tmp_mutex->unlock();
    }
    cur_mutex->unlock();

    _file_writer.flush();
    _file_writer.close();
    _file_mutex.unlock();
    return ;
}

template<typename K, typename V> 
void tSkipList<K, V>::load_file() {
    _file_mutex.lock();
    _file_reader.open(STORE_FILE);
    std::string line;
    std::string* key = new std::string();
    std::string* value = new std::string();
    while (getline(_file_reader, line)) {
        string_to_kv(line, key, value);
        if (key->empty() || value->empty()) {
            continue;
        }
        // Define key as int type
        insert_element(stoi(*key), *value);
    }
    delete key;
    delete value;
    _file_reader.close();
    _file_mutex.unlock();
    return;
}

template <typename K, typename V>
void tSkipList<K, V>::clear(SkipNode * cur)
{
    if(cur->forward[0]!=nullptr){
        clear(cur->forward[0]);
    }
    delete(cur);
}

template<typename K, typename V>
int tSkipList<K, V>::get_random_level(){

    int k = 1;
    while (rand() % 2) {
        k++;
    }
    k = (k < _max_level) ? k : _max_level;
    return k;
};

template<typename K, typename V>
void tSkipList<K, V>::string_to_kv(const std::string& str, std::string* key, std::string* value) {

    if(!is_valid_string(str)) {
        return;
    }
    *key = str.substr(0, str.find(delimiter));
    *value = str.substr(str.find(delimiter)+1, str.length());
}

template<typename K, typename V>
bool tSkipList<K, V>::is_valid_string(const std::string& str) {

    if (str.empty()) {
        return false;
    }
    if (str.find(delimiter) == std::string::npos) {
        return false;
    }
    return true;
}


#endif //THREAD_SAFE_SKIP_LIST_H