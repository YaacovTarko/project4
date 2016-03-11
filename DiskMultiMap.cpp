//
//  DiskMultiMap.cpp
//  project 4
//
//  Created by Yaacov Tarko on 3/5/16.
//  Copyright (c) 2016 Yaacov Tarko. All rights reserved.
//


#include <vector>
#include <functional>
//#define _CRT_SECURE_NO_WARNING removed for xcode testing, may be needed on VS to avoid warnings
#include <cstring>
#include "DiskMultiMap.h"
#include "MultiMapTuple.h"


//ITERATOR IMPLEMENTATION
DiskMultiMap::Iterator::Iterator() {
	index = 0;
	map = nullptr;
}

DiskMultiMap::Iterator::Iterator(queue<BinaryFile::Offset> locations, DiskMultiMap* target){
	m_locations = locations;
	index = 0;
	map = target;
}



bool DiskMultiMap::Iterator::isValid() const{
	return(!m_locations.empty() && map != nullptr);
}

DiskMultiMap::Iterator& DiskMultiMap::Iterator::operator++(){
	if(!isValid()) return *this;
	m_locations.pop();
	return *this;
}

MultiMapTuple DiskMultiMap::Iterator::operator*(){
	MultiMapTuple to_return;
	if(isValid()){
		TableNode gets_data;
		map->m_map.read(gets_data, m_locations.front());
		
		to_return.key = gets_data.key;
		to_return.value = gets_data.value;
		to_return.context = gets_data.context;
	}
	
	else{
		to_return.key = to_return.value = to_return.context = "";
		
	}
	
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
	header.key_map = sizeof(header);
	
	//adds blank nodes representing the first map (file to site). -1 BUCKET REPRESENTS UNFILLED BUCKET. WILL BE CHANGED TO VALID NUMBER WHEN SOMETHING IS ADDED
	BinaryFile::Offset associated_data = -1;
	BinaryFile::Offset current = sizeof(header);
	
	//adds the nodes representing the map from keys to values
	for(int i = 0; i < numBuckets; i++){
		
		//add the link to the bucket to the map
		m_map.write(associated_data, current);
		current += sizeof(associated_data);
	}
	
	
	header.empty_slots = -1;  //the section that shows where empty slots are will be added later
	header.end_of_file = current;
	
	//adds the header to the file
	m_map.write(header, 0);
	
	this->numBuckets = numBuckets;
	
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
	if(key.length() > max_string_size || value.length() > max_string_size || context.length() > max_string_size) return false;
	
	bool replaced_deleted_node = false;
	
	//the final location that the new node will be inserted into shall be stored in this offset
	BinaryFile::Offset location_to_insert = -1;
	
	//determines if there are empty spaces where a node has been deleted, where this node should be inserted
	TableHeader header;
	m_map.read(header, 0);
	
	cerr <<header.empty_slots << endl;

	if(header.empty_slots != -1){
		
		replaced_deleted_node = true;
		
		//finds an OffsetNode that stores the location of an empty space, where a TableNode can be inserted
		OffsetNode emptyfinder;
		OffsetNode onebefore;
		emptyfinder.next = header.empty_slots;
		onebefore.next = emptyfinder.next;
		
		while (emptyfinder.next != -1){
			m_map.read(emptyfinder, emptyfinder.next);
			
			if(emptyfinder.empty_space != -1){  //if the offset node points to a valid memory location
				location_to_insert = emptyfinder.empty_space;
				
				emptyfinder.empty_space = -1;
				
				m_map.write(emptyfinder, onebefore.next);
			}
			onebefore.next = emptyfinder.next;
		}
		
	}
	
	
	//if there are no empty spaces
	else{
		//hashes the key to find the proper 'pointer' and checks the hash table to where the new node should go
		BinaryFile::Offset target =  keyBucketOffset(key);
		m_map.read(location_to_insert, target);
		
		//figures out where the new node should be inserted (finds the bottom of the appropriate bucket)
		if(location_to_insert == -1){      //if this is the first node in the bucket
			TableHeader gets_end;
			m_map.read(gets_end, 0);
			
			location_to_insert = gets_end.end_of_file;
			
			//updates hash table to know where to point
			m_map.write(location_to_insert, target);
		}
		
		else{ //if there's already at least one other node in the bucket
			TableNode gets_last;
			gets_last.next_key = location_to_insert;
			BinaryFile::Offset previous = -1;  //-1 to crash program if previous doesn't get initialized
			while(gets_last.next_key != -1){
				previous = gets_last.next_key;
				m_map.read(gets_last, gets_last.next_key);
			}
			
			location_to_insert = previous; //'location' points to the location of the last node in the bucket
			
			//updates the 'next' pointer of the last item in the bucket to point at the new node
			//figures out where the new node will be placed
			DiskMultiMap::TableHeader gets_header;
			m_map.read(gets_header, 0);
			BinaryFile::Offset end_of_file = gets_header.end_of_file;
			
			gets_last.next_key = end_of_file;
			m_map.write(gets_last, location_to_insert);
			
			//location-- where the node will eventually be written
			location_to_insert = end_of_file;
			
		}
		
	}
	
	
	
	//creates the node to insert
	TableNode to_insert;
	to_insert.next_key = -1;
	
	
	std::strcpy(to_insert.key, key.c_str());
	std::strcpy(to_insert.value, value.c_str());
	std::strcpy(to_insert.context, context.c_str());
	
	
	//inserts the node (finally)
	m_map.write(to_insert, location_to_insert);
	
	//updates 'end of file' to point to the location after the node you just inserted (don't do this if you're replacing a deleted node)
	if(!replaced_deleted_node){
		m_map.read(header, 0);
		header.end_of_file += sizeof(to_insert);
		m_map.write(header, 0);
	}
	
	
	return true;
}


DiskMultiMap::Iterator DiskMultiMap::search(const std::string& key){
	queue<BinaryFile::Offset> iterator_contents;
		
	//finds the proper bucket to check for associated values
	BinaryFile::Offset location = firstKeyNode(key);
	//location is now pointing to the first node in the bucket (or to -1)
	
	//iterates through the bucket adding the locations of each node to the iterator's contents
	TableNode current;
	while(location != -1){
		m_map.read(current, location);
		if(current.key == key)   iterator_contents.push(location);
		location = current.next_key;
	}
	
	
	//return an iterator containing those nodes
	return Iterator(iterator_contents, this);
}


//DOES NOT PROPERLY UPDATE KEY-TO-VALUE HASH TABLE
int DiskMultiMap::erase(const std::string& key, const std::string& value, const std::string& context){
	//finds the bucket to check for associated values
	int numerased = 0;
	BinaryFile::Offset target = keyBucketOffset(key);
	
	//if there isn't anything in the bucket, return 0
	if(target == -1) return 0;
	
	BinaryFile::Offset location = firstKeyNode(key);
	TableNode current;
	TableNode previous;
	
	previous.next_key = -1;
	BinaryFile::Offset prev_loc = -1;
	
	TableHeader header;
	m_map.read(header, 0);
	
	
	while(location != -1){
		m_map.read(current, location);
		if( current.key == key && current.value == value && current.context == context){
			
			if (previous.next_key == -1){  //if this is the first node in the bucket
				//makes current.next the first node in the bucket
				m_map.write(current.next_key, target);
				
			}
			else{
				//makes the one after current come immediately after previous in the list
				previous.next_key = current.next_key;
				m_map.write(previous, prev_loc);
			}
			//add the location of this node to the list of empty spaces
			
			//creates the node that will be added
			OffsetNode deleted_loc;
			deleted_loc.next = -1;
			deleted_loc.empty_space = location;
			
			
			//if this is the only empty slot in the list
			if (header.empty_slots == -1){
				
				m_map.write(deleted_loc, header.end_of_file);
				
				//updates the header to take into account that we just added the first empty space (will write later)
				header.empty_slots = header.end_of_file;
				
			}
			
			else{ //if there are other empty slots in the list
				
				//ADD: CHECK IF THERE ARE OFFSETNODES POINTING TO -1, IF SO CHANGE THEM TO POINT TO THE NOW-EMPTY NODE ☑️
				
				
				OffsetNode current;
				BinaryFile::Offset current_loc;
				m_map.read(current, header.empty_slots);
				current_loc = header.empty_slots;
				while(current.next != -1){
					//if there's an invalid offsetnode, point it to the deleted node instead of creating a new offsetnode
					if(current.empty_space == -1){
						current.empty_space = location;
						m_map.write(current, current_loc);
						goto dont_create_offsetnode;
					}
					m_map.read(current, current.next);
					current_loc = current.next;
				}
				
				m_map.write(deleted_loc, header.end_of_file);
			}
			
			//updates header's filesize to take into account that we added a new OffsetNode
			header.end_of_file += sizeof(deleted_loc);
			m_map.write(header, 0);
			
		dont_create_offsetnode:
			numerased++;
		}
		
		previous = current;
		location = current.next_key;
		prev_loc = previous.next_key;
	}
	
	
	return numerased;
}

//returns the location of the first node in the bucket that the key maps to
BinaryFile::Offset DiskMultiMap::firstKeyNode(const std::string key){
	BinaryFile::Offset target = keyBucketOffset(key);
	BinaryFile::Offset location;
	m_map.read(location, target);
	
	return location;
}



//gets the offset of the slot in the hash table associated with the key
BinaryFile::Offset DiskMultiMap::keyBucketOffset(const std::string key){
	std::hash<std::string> stringhash;
	BinaryFile::Offset target = stringhash(key) % numBuckets;
	target*= sizeof(BinaryFile::Offset);
	
	DiskMultiMap::TableHeader header;
	m_map.read(header, 0);
	target += header.key_map;
	
	return target;
}

