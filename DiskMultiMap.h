#ifndef DISKMULTIMAP_H_
#define DISKMULTIMAP_H_

#include <string>
#include <vector>
#include "MultiMapTuple.h"
#include "BinaryFile.h"

class DiskMultiMap
{
public: //DO NOT MAKE ANY CHANGES TO THIS 
    
    class Iterator
    {
    public:
        Iterator();
        Iterator(vector<BinaryFile::Offset> locations, DiskMultiMap* target);
        
        // You may add additional constructors
        
        //add copy constructor and assignment operator?        
        bool isValid() const;
        Iterator& operator++();
        MultiMapTuple operator*();
        
    private:
        // Your private member declarations will go here
        string m_key; 
        std::vector<BinaryFile::Offset> m_locations;
        int index;
        DiskMultiMap* map;
    };
    
    DiskMultiMap();
    ~DiskMultiMap();
    bool createNew(const std::string& filename, unsigned int numBuckets);
    bool openExisting(const std::string& filename);
    void close();
    bool insert(const std::string& key, const std::string& value, const std::string& context);
    Iterator search(const std::string& key);
    int erase(const std::string& key, const std::string& value, const std::string& context);
    
private:
    // Your private member declarations will go here
    
    const int max_string_size = 120;
    
    BinaryFile m_map;
    
    struct TableNode{
        char key[121];
        char value[121];
        char context[121];
        BinaryFile::Offset next; 
    };
    
    
    struct TableHeader{
        BinaryFile::Offset map_start;
        BinaryFile::Offset empty_slots;
        BinaryFile::Offset end_of_file;
        
    };
    
    struct OffsetNode{
        BinaryFile::Offset empty_space;
        BinaryFile::Offset next; 
        
    };
    
    
    int numBuckets;
    
    //returns the location of the offset that points to the first node where the key is
    BinaryFile::Offset bucketPointer(const std::string key);
    
    //returns the location of the first node with the key
    BinaryFile::Offset firstNode(const std::string key);
};

#endif // DISKMULTIMAP_H_