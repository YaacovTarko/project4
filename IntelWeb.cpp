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

IntelWeb::IntelWeb(){
    
}


IntelWeb::~IntelWeb(){
    file_to_site.close();
    site_to_file.close();
    file_to_file.close();
}

bool IntelWeb::createNew(const std::string& filePrefix, unsigned int maxDataItems){
    bool succeeded = true;
    close();
    
    if(!site_to_file.createNew(filePrefix + "stf", 2*maxDataItems) ) succeeded = false;
    if(!file_to_site.createNew(filePrefix + "fts", 2*maxDataItems) ) succeeded = false;
    if(!file_to_file.createNew(filePrefix + "ftf", 2*maxDataItems) ) succeeded = false;

    
    if(!succeeded) close(); 
    
    return succeeded;
}

bool IntelWeb::openExisting(const std::string& filePrefix){
    bool succeeded = true;
    
    if(!site_to_file.openExisting(filePrefix + "stf") ) succeeded = false;
    if(!file_to_site.openExisting(filePrefix + "fts") ) succeeded = false;
    if(!file_to_file.openExisting(filePrefix + "ftf") ) succeeded = false;

    
    if(!succeeded) close();
    
    return succeeded;
}


void IntelWeb::close(){
    site_to_file.close();
    file_to_site.close();
    file_to_file.close();

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
        
        //based on features of file and site names (http: ), determine which map the event belongs in
        string id = key.substr(4, 1);
        if (id == ":"){
            site_to_file.insert(key, value, context);
        }
        else{
            id = value.substr(4, 1);
            if(id == ":"){
                file_to_site.insert(key, value, context);
            }
            else file_to_file.insert(key, value, context);
        }
        
    }
    
    
    
    return succeeded;
}

unsigned int IntelWeb::crawl(const std::vector<std::string>& indicators,  //initially known bad guys
                   unsigned int minPrevalenceToBeGood,           
                   std::vector<std::string>& badEntitiesFound,    //stores in lex. order
                   std::vector<InteractionTuple>& interactions    //stores all events involving at least 1 bad guy

                             ){
    
    while(!badEntitiesFound.empty() ){
        
        badEntitiesFound.pop_back();
    }
    
    unsigned int num_entities_discovered = 0;    //includes those stored in indicators
                                 
    
                                 
    
    
    
    return num_entities_discovered;
}






bool IntelWeb::purge(const std::string& entity){
    bool purged = false;
    
    
    
    return purged;
}






