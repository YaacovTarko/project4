//
//  DiskMultiMap.cpp
//  project 4
//
//  Created by Yaacov Tarko on 3/5/16.
//  Copyright (c) 2016 Yaacov Tarko. All rights reserved.
//

#include <vector>
#include <functional>
#include "DiskMultiMap.h"
#include "MultiMapTuple.h" 


//ITERATOR IMPLEMENTATION
DiskMultiMap::Iterator::Iterator() {
    index = 0;
    map = nullptr; 
}

DiskMultiMap::Iterator::Iterator(vector<BinaryFile::Offset> locations, DiskMultiMap* target){
    m_locations = locations;
    index = 0;
    map = target; 
}


bool DiskMultiMap::Iterator::isValid() const{
    return(m_locations.empty() && index >= 0 && index < m_locations.size() && map != nullptr);
}

DiskMultiMap::Iterator& DiskMultiMap::Iterator::operator++(){
    index++;
    return *this;
}

MultiMapTuple DiskMultiMap::Iterator::operator*(){
    TableNode gets_data;
    map->m_map.read(gets_data, index);
    MultiMapTuple to_return;
    to_return.key = gets_data.key;
    to_return.value = gets_data.value;
    to_return.context = gets_data.context;
    
    return to_return; 
    
}







//DISKMULTIMAP IMPLEMENTATION
DiskMultiMap::DiskMultiMap(){
    numBuckets = 0;
}

DiskMultiMap::~DiskMultiMap(){
    if(m_map.isOpen()) m_map.close();
}

bool DiskMultiMap::createNew(const std::string& filename, unsigned int numBuckets){
    if(m_map.isOpen()) m_map.close();
    if(!m_map.createNew(filename)) return false; //returns false if it can't create a new file
    //SET UP THE FILE
    
    //creates the header that will identify different locations in the map
    DiskMultiMap::TableHeader header;
    header.map_start = sizeof(header);
    
    //adds blank nodes representing the first map (file to site). -1 BUCKET REPRESENTS UNFILLED BUCKET. WILL BE CHANGED TO VALID NUMBER WHEN SOMETHING IS ADDED
    BinaryFile::Offset associated_data = -1;
    
    BinaryFile::Offset current = sizeof(header);
    for(int i = 0; i < numBuckets; i++){
        
        //add the link to the bucket to the map
        m_map.write(associated_data, current);
        current += sizeof(associated_data);
    }
    
    
    
    /*
     header.site_to_file = current;
     
    //I DON'T THINK I EVEN NEED 2 TABLES (BECAUSE BOTH ARE STRINGS AND WE CAN JUST LOOK THRU BOTH OF THEM)
    
    //add blank nodes representing the second map (site to file)
    for(int i = 0; i < numBuckets; i++){
        
        //add the link to the bucket to the map
        m_map.write(associated_data, current);
        current += sizeof(associated_data);

    }
    */
    header.empty_slots = current; 
    header.end_of_file = current;
    return true;
}


bool DiskMultiMap::openExisting(const std::string& filename){
    if(m_map.isOpen()) m_map.close();
    return  (m_map.openExisting(filename));
    
}


void DiskMultiMap::close(){
    m_map.close();
    
}
bool DiskMultiMap::insert(const std::string& key, const std::string& value, const std::string& context){
   //makes sure strings aren't too long
    if(key.length() > 120 || value.length() > 120 || context.length() > 120) return false;

    
    //hashes the key to find the proper 'pointer' and checks the hash table to where the new node should go
    std::hash<std::string> stringhash;
    BinaryFile::Offset target = stringhash(key) %numBuckets;
    BinaryFile::Offset location;
    m_map.read(location, target);
    
    //figures out where the new node should be inserted (finds the bottom of the appropriate bucket
    if(location == -1){
        TableHeader gets_end;
        m_map.read(gets_end, 0);
        location = gets_end.end_of_file;
        //updates hash table to know where to point
        m_map.write(location, target);
    }
    else{
        TableNode gets_end;
        BinaryFile::Offset previous = -1;  //-1 to crash program if previous doesn't get initialized
        while(location != -1){
            previous = location;
            m_map.read(gets_end, location);
        }
        
        location = previous;
    }
    
    //creates the node to insert
    TableNode to_insert;
    to_insert.next = -1;
    for(int i = 0; i < key.size(); i++){
        to_insert.key[i] = key[i];
    }
    for(int i = 0; i< value.size(); i++){
        to_insert.value[i] = value[i];
    }
    for(int i = 0; i < context.size(); i++){
        to_insert.context[i] = context[i];
    }
    
    
    //inserts the node (finally) 
    m_map.write(to_insert, location);
    return true;
}


DiskMultiMap::Iterator DiskMultiMap::search(const std::string& key){
    vector<BinaryFile::Offset> iterator_contents;
    
    //finds the proper bucket to check for associated values
    std::hash<std::string> stringhash;
    BinaryFile::Offset target = stringhash(key)%numBuckets;
    BinaryFile::Offset location;
    m_map.read(location, target);
    //location is now pointing to the first node in the bucket
    
    //goes through the bucket adding the locations of each node to the iterator's contents
    TableNode current;
    while(location != -1){
        m_map.read(current, location);
        if(current.key == key)   iterator_contents.push_back(location);
        location = current.next;
    }
    
    //return an iterator containing those nodes
    return Iterator(iterator_contents, this);
}


int DiskMultiMap::erase(const std::string& key, const std::string& value, const std::string& context){
    
    
    return 0;
}
