/*
 * @file        : skiplist.h
 * @Author      : zhenxi
 * @Date        : 2024-04-18
 * @copyleft    : Apache 2.0
 * Description  : This file contains the declaration of the SkipList class, which is designed 
 *                for manage the skip list in a safe and efficient manner. It encapsulates 
 *                operations such as search, insert and delete nodes.
 */

#ifndef SKIP_LIST_H
#define SKIP_LIST_H


#define STORE_FILE "store/dumpFile"

std::string delimiter = ":";

#include <iostream> 
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <fstream>


/**
 * @struct Node
 * @brief The Node struct is used to manage nodes in the skip list.
 */
template<typename K, typename V> 
struct Node {
    K key;
    V value;
    int node_level;
    Node<K, V> **forward;
    
    /**
     * @brief Constructor for Node.
     * To initialze the data structures.
     */
    Node(K k, V v, int level) : key(k), value(v), node_level(level) {
        forward = new Node<K, V>*[level + 1];
        for (int i = 0; i <= level; ++i)
            forward[i] = NULL;
    }

    /**
     * @brief Deconstructor for Node.
     * To free the array of pointers.
     */
    ~Node() {
        delete[] forward;  
    }
};

/**
 * @class SkipList
 * @brief The SkipList class is used to manage the skip list.
 */
template <typename K, typename V> 
class SkipList {
    using SkipNode = Node<K, V>;
public: 
    /**
     * @brief Constructor for SkipList.
     * To initialze the data structures.
     */
    SkipList(int);

    /**
     * @brief Deconstructor for MutexNode.
     * To free the array of pointers.
     */
    ~SkipList();
    
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
     * @brief To display the skip list element by element.
     */
    void display_list();

    /**
     * @brief To search element by its key.
     * @return True if found, False if not found.
     */
    bool search_element(K);
    
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
    int _skip_list_level;

    // pointer to header node 
    SkipNode *_header;

    // file operator
    std::ofstream _file_writer;
    std::ifstream _file_reader;

    // skiplist current element count
    int _element_count;
};


template<typename K, typename V> 
SkipList<K, V>::SkipList(int max_level) : _max_level(max_level), _skip_list_level(0), _element_count(0){
    // create header node and initialize key and value to null
    K k;
    V v;
    this->_header = new SkipNode(k, v, _max_level);
};

template<typename K, typename V> 
SkipList<K, V>::~SkipList() {

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
int SkipList<K, V>::size() { 
    return _element_count;
}

template<typename K, typename V>
int SkipList<K, V>::insert_element(const K key, const V value) {
    SkipNode *current = this->_header;

    // create update array and initialize it 
    // update is array which put node that the node->forward[i] should be operated later
    SkipNode *update[_max_level+1];
    memset(update, 0, sizeof(SkipNode*)*(_max_level+1));  

    // start form highest level of skip list 
    for(int i = _skip_list_level; i >= 0; i--) {
        while(current->forward[i] != NULL && current->forward[i]->key < key) {
            current = current->forward[i]; 
        }
        update[i] = current;
    }

    // reached level 0 and forward pointer to right node, which is desired to insert key.
    current = current->forward[0];

    // if current node have key equal to searched key, we get it
    if (current != NULL && current->key == key) {
        std::cout << "key: " << key << ", exists" << std::endl;
        return 1;
    }

    // if current is NULL that means we have reached to end of the level 
    // if current's key is not equal to key that means we have to insert node between update[0] and current node 
    if (current == NULL || current->key != key ) {
        
        // Generate a random level for node
        int random_level = get_random_level();

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
        std::cout << "Successfully inserted key:" << key << ", value:" << value << std::endl;
        _element_count ++;
    }
    return 0;
}

template<typename K, typename V> 
void SkipList<K, V>::display_list() {

    std::cout << "\n*****Skip List*****"<<"\n"; 
    for (int i = 0; i <= _skip_list_level; i++) {
        SkipNode *node = this->_header->forward[i]; 
        std::cout << "Level " << i << ": ";
        while (node != NULL) {
            std::cout << node->key << ":" << node->value << ";";
            node = node->forward[i];
        }
        std::cout << std::endl;
    }
}

template<typename K, typename V> 
bool SkipList<K, V>::search_element(K key) {

    std::cout << "search_element-----------------" << std::endl;
    SkipNode *current = _header;

    // start from highest level of skip list
    for (int i = _skip_list_level; i >= 0; i--) {
        while (current->forward[i] && current->forward[i]->key < key) {
            current = current->forward[i];
        }
    }

    //reached level 0 and advance pointer to right node, which we search
    current = current->forward[0];

    // if current node have key equal to searched key, we get it
    if (current and current->key == key) {
        std::cout << "Found key: " << key << ", value: " << current->value << std::endl;
        return true;
    }

    std::cout << "Not Found Key:" << key << std::endl;
    return false;
}

template<typename K, typename V> 
void SkipList<K, V>::delete_element(K key) {
    SkipNode *current = this->_header; 
    SkipNode *update[_max_level+1];
    memset(update, 0, sizeof(SkipNode*)*(_max_level+1));

    // start from highest level of skip list
    for (int i = _skip_list_level; i >= 0; i--) {
        while (current->forward[i] !=NULL && current->forward[i]->key < key) {
            current = current->forward[i];
        }
        update[i] = current;
    }

    current = current->forward[0];
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

        std::cout << "Successfully deleted key "<< key << std::endl;
        delete current;
        _element_count --;
    }
    return;
}

template<typename K, typename V> 
void SkipList<K, V>::dump_file() {

    std::cout << "dump_file-----------------" << std::endl;
    _file_writer.open(STORE_FILE);
    SkipNode *node = this->_header->forward[0]; 

    while (node != NULL) {
        _file_writer << node->key << ":" << node->value << "\n";
        std::cout << node->key << ":" << node->value << ";\n";
        node = node->forward[0];
    }

    _file_writer.flush();
    _file_writer.close();
    return ;
}

template<typename K, typename V> 
void SkipList<K, V>::load_file() {

    _file_reader.open(STORE_FILE);
    std::cout << "load_file-----------------" << std::endl;
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
        std::cout << "key:" << *key << "value:" << *value << std::endl;
    }
    delete key;
    delete value;
    _file_reader.close();
}

template <typename K, typename V>
void SkipList<K, V>::clear(SkipNode * cur)
{
    if(cur->forward[0]!=nullptr){
        clear(cur->forward[0]);
    }
    delete(cur);
}

template<typename K, typename V>
int SkipList<K, V>::get_random_level(){

    int k = 1;
    while (rand() % 2) {
        k++;
    }
    k = (k < _max_level) ? k : _max_level;
    return k;
};

template<typename K, typename V>
void SkipList<K, V>::string_to_kv(const std::string& str, std::string* key, std::string* value) {

    if(!is_valid_string(str)) {
        return;
    }
    *key = str.substr(0, str.find(delimiter));
    *value = str.substr(str.find(delimiter)+1, str.length());
}

template<typename K, typename V>
bool SkipList<K, V>::is_valid_string(const std::string& str) {

    if (str.empty()) {
        return false;
    }
    if (str.find(delimiter) == std::string::npos) {
        return false;
    }
    return true;
}


#endif //SKIP_LIST_H