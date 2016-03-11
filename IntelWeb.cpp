//
//  IntelWeb.cpp
//  project 4
//
//  Created by Yaacov Tarko on 3/5/16.
//  Copyright (c) 2016 Yaacov Tarko. All rights reserved.
//

#include "IntelWeb.h"
#include "DiskMultiMap.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_set>
#include <queue> 

using namespace std; 

IntelWeb::IntelWeb(){
    
}


IntelWeb::~IntelWeb(){
    key_to_val.close();
    val_to_key.close();
}

bool IntelWeb::createNew(const std::string& filePrefix, unsigned int maxDataItems){
    bool succeeded = true;
    close();
    
    if(!key_to_val.createNew(filePrefix + "keyfirst", 2*maxDataItems) ) succeeded = false;
    if(!val_to_key.createNew(filePrefix + "valfirst", 2*maxDataItems) ) succeeded = false;

    
    if(!succeeded) close(); 
    
    return succeeded;
}

bool IntelWeb::openExisting(const std::string& filePrefix){
    bool succeeded = true;
    
    if(!key_to_val.openExisting(filePrefix + "keyfirst") ) succeeded = false;
    if(!val_to_key.openExisting(filePrefix + "valfirst") ) succeeded = false;

    
    if(!succeeded) close();
    
    return succeeded;
}


void IntelWeb::close(){
    key_to_val.close();
    val_to_key.close();

}


bool IntelWeb::ingest(const std::string& telemetryFile){
    bool succeeded = true;
    ifstream inputFile(telemetryFile);
    //tests that the file opened properly
    if(!inputFile) return false;
    
    //iterates through the file and stores telemetry data on disk
    string line;
    string key;
    string value;
    string context;

    while(getline(inputFile, line)){
        istringstream sstream(line);
        if(! (sstream >> context >> key >> value) ){
            cerr << "Error. Telemetry data cannot be read.";
            return false;
        }
        
        key_to_val.insert(key, value, context);
        val_to_key.insert(value, key, context); 
        
    }
    
    
    
    return succeeded;
}

//currently implementing without the 'interactions' vector
unsigned int IntelWeb::crawl(const std::vector<std::string>& indicators,  //initially known bad guys
                   unsigned int minPrevalenceToBeGood,           
                   std::vector<std::string>& badEntitiesFound,    //stores in lex. order
                   std::vector<InteractionTuple>& interactions    //stores all events involving at least 1 bad guy
){
	
    known_interactions.createNew("Interactions", 100000000);
    //empties passed parameters
    while(!badEntitiesFound.empty())  badEntitiesFound.pop_back();
    while(!interactions.empty())  interactions.pop_back();

	
	queue<std::string> suspected_not_searched;        //keeps track of actors that might be bad but whose prevalence hasn't been counted yet
	unordered_set<std::string> known_suspicious;  //keeps track of how many times each bad actor's been discovered
	
	DiskMultiMap::Iterator associated_values;
	DiskMultiMap::Iterator associated_keys;
	
	MultiMapTuple tuple;
	for(int i = 0; i < indicators.size(); i++){
		associated_values = key_to_val.search(indicators[i]);
		associated_keys   = val_to_key.search(indicators[i]);
		
		if(associated_keys.isValid() || associated_values.isValid()){  //if the indicator exists in the map
			
			badEntitiesFound.push_back(indicators[i]);
			known_suspicious.insert(indicators[i]);
			
			while(associated_keys.isValid()){
				tuple = *associated_keys;
				
				if(known_suspicious.count(tuple.value) == 0) //if it hasn't been found yet, search for it
					suspected_not_searched.push(tuple.value);
					known_suspicious.insert(tuple.value);
				
				/*  current implementation doesn't handle the interactions vector
				InteractionTuple interaction;
				interaction.from = tuple.value;
				interaction.to = tuple.key;
				interaction.context = tuple.context;
				
				interactions.push_back(interaction);
				 */
				++associated_keys;
			}
			
			while(associated_values.isValid()){
				tuple = *associated_values;
				
				if(known_suspicious.count(tuple.value) ==0){
					suspected_not_searched.push(tuple.value);
					known_suspicious.insert(tuple.value);

				}
				++associated_values;
			}
		}
	}

	int prevalence;
	
	while(!suspected_not_searched.empty()){
		prevalence = 0;
		associated_keys  = val_to_key.search(suspected_not_searched.front());
		associated_values= key_to_val.search(suspected_not_searched.front());
		
		while(associated_keys.isValid()){
			prevalence++;
			++associated_keys;
		}
		while(associated_values.isValid()){
			prevalence++;
			++associated_values;
		}
		
		if (prevalence < minPrevalenceToBeGood){
			badEntitiesFound.push_back(suspected_not_searched.front());
			
			associated_keys  = val_to_key.search(suspected_not_searched.front());
			associated_values= key_to_val.search(suspected_not_searched.front());
			
			while(associated_keys.isValid()){
				tuple = *associated_keys;
				
				if(known_suspicious.count(tuple.value) ==0){
					suspected_not_searched.push(tuple.value);
					known_suspicious.insert(tuple.value);
					
				}
				
				++associated_keys;

				
			}
			
			while(associated_values.isValid()){
				tuple = *associated_values;
				
				if(known_suspicious.count(tuple.value) ==0){
					suspected_not_searched.push(tuple.value);
					known_suspicious.insert(tuple.value);
					
				}
				++associated_values;
			}
		}
		
	}
	
	
	
    //Plan:  SHOULD BE O(N)
    //detect suspected threats by:
        //for each event you find:
        //if the threat isn't in the 'known_interactions' BinaryFile (if the iterator !isValid()), add it to that file
        //check if the threat is in the 'suspicious' vector by:
                //doing a search on that file, getting an iterator, and seeing if it's valid
                //if it's not in the 'suspicious' vector , add it.
            //check if the threat's prevalence is greater than minPrevalence by:
                //Creating a variable 'count'
                //getting iterators from both maps, and ++ing them while they're valid and adding one to count each time
                //comparing count to minPrevalence
            //if count is less than minPrevalence, add the threat to badentities found
    
    //iterate through 'known_interactions'
                                 
      //code below this line should be deleted
/*
    int prevalence;
    DiskMultiMap::Iterator suspicious_keys, suspicious_vals;
    for(int i = 0; i < suspicious.size(); i++){   //suspicious.size() will keep growing as items are added
        prevalence = 0;
        
        suspicious_keys = key_to_val.search(suspicious[i]);
        suspicious_vals = val_to_key.search(suspicious[i]);
        while(suspicious_keys.isValid() ){
            MultiMapTuple mult = *suspicious_keys;
            InteractionTuple bad_interaction(mult.key, mult.value, mult.context);
            
        }
        
    }

 */
                                 
/*
    for(int i = 0;  i < indicators.size(); i++){
        DiskMultiMap::Iterator malicious_keys = key_to_val.search(indicators[i]);
        DiskMultiMap::Iterator malicious_vals = val_to_key.search(indicators[i]);
        
        while (malicious_keys.isValid() ){
            MultiMapTuple mult = *malicious_keys;
            InteractionTuple bad_interaction(mult.key, mult.value, mult.context);
            
            
            for(int i = 0; i < interactions.size(); i++){
            interactions.push_back(bad_interaction);
            ++malicious_keys;
            }
            
        }
        
        while (malicious_vals.isValid()){
            MultiMapTuple mult = *malicious_vals;
            InteractionTuple bad_interaction(mult.value, mult.key, mult.context);
            interactions.push_back(bad_interaction);
            ++malicious_vals;
            
        }
        
    }
 */
                                 
                                 
    
                                 
    
    
    
    return static_cast<int>(badEntitiesFound.size());
}






bool IntelWeb::purge(const std::string& entity){
    DiskMultiMap::Iterator keytuples = key_to_val.search(entity);
    DiskMultiMap::Iterator valtuples = val_to_key.search(entity);
    
    bool purged = (keytuples.isValid() || valtuples.isValid());
    
    while(keytuples.isValid()){
        MultiMapTuple mult = *keytuples;
        key_to_val.erase(mult.key, mult.value, mult.context);
        val_to_key.erase(mult.value, mult.key, mult.context);
        
        ++keytuples;
    }
    
    while(valtuples.isValid()){
        MultiMapTuple mult = *valtuples;
        key_to_val.erase(mult.value, mult.key, mult.context);
        val_to_key.erase(mult.key, mult.value, mult.context);
        
        ++valtuples;
    }

    
    return purged;
}






