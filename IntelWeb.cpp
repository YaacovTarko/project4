//
//  IntelWeb.cpp
//  project 4
//
//  Created by Yaacov Tarko on 3/5/16.
//  Copyright (c) 2016 Yaacov Tarko. All rights reserved.
//

#include "IntelWeb.h"
#include "DiskMultiMap.h"

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
    
    
    
    return succeeded;
}

unsigned int IntelWeb::crawl(const std::vector<std::string>& indicators,
                   unsigned int minPrevalenceToBeGood,
                   std::vector<std::string>& badEntitiesFound,
                   std::vector<InteractionTuple>& interactions
                             ){
    unsigned int num_entities_discovered = 0;
    
    
    
    return num_entities_discovered;
}
bool IntelWeb::purge(const std::string& entity){
    bool purged = false;
    
    
    
    return purged;
}
