/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   sharder.hpp
 * Author: Pooja Nilangekar 
 *
 * Created on June 12, 2016, 11:56 AM
 */

#ifndef SHARDER_HPP
#define SHARDER_HPP

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <utility>
#include <set>
#include <thread>
#include <mutex>
#include <cstdlib>
#include <tbb/concurrent_hash_map.h>
#include "ThreadPool.hpp"
#include "logger.hpp"

#define REMOTE 0
#define CLOSED 1
#define OUT 2
#define IN 3

extern tbb::concurrent_hash_map < size_t, unsigned short > vertex_type;
extern tbb::concurrent_hash_map < size_t, std::vector < size_t > > edges;
extern tbb::concurrent_hash_map < size_t, std::set < size_t > > reach;
extern tbb::concurrent_hash_map < size_t, std::map < size_t, size_t > > reach_dist;
extern tbb::concurrent_hash_map < size_t, std::vector < size_t > > updates;
extern tbb::concurrent_hash_map < size_t, std::vector <size_t> > next_updates;

class shard {
    int num_parts;
    size_t num_v, num_e,avg_odegree,current_iter;
    std::string typefile, edgefile;
private:
    
    /*
    * load_type loads the type of each vertex from the typefile.
    */
    void load_type() {

        std::ifstream f(typefile);

        if(!f.is_open()) {
            print_msg("Could not open typefile"+typefile,ERROR);
            exit(EXIT_FAILURE);
        }

        std::string line;
        size_t vid;
        unsigned short tid;
    
        tbb::concurrent_hash_map < size_t, unsigned short >::accessor ac;

        while(std::getline(f,line)) {

            std::stringstream(line) >> vid >> tid;
            vertex_type.insert(ac,vid);
            ac->second = tid;
            ac.release();
            num_v++;
        }
        
        print_msg("Parsed "+std::to_string(num_v)+" vertices from "+typefile,DEBUG);
        f.close();
        return;
    }
    
    /*
    * load_edges loads the structure of the graph from the edgefile.
    * The edges map contains a mapping from each vertexid to a list of all its out neighbours. 
    */
    void load_edges() {

       std::ifstream f(edgefile);

       if(!f.is_open()) {
           print_msg("Could not open edgefile "+edgefile,ERROR);
           exit(EXIT_FAILURE);
       }

       std::string line;
       size_t src,dest;
       
       tbb::concurrent_hash_map < size_t, std::vector < size_t > >::accessor ac;

       while(std::getline(f,line)) {

           std::stringstream(line) >> src >> dest;
           edges.insert(ac,src);
           (ac->second).push_back(dest);
           ac.release();
           num_e++;
       }

       print_msg("Parsed "+std::to_string(num_e)+" edges from "+edgefile,DEBUG);
       f.close();

       return;
   }
    /*
     * split_structure splits the given graph into a predefined number of intervals.
     * It evenly distributes edges across all the intervals. 
     */
    void split_structure() {
        
        size_t edges_per_file = num_e/num_parts; 
        int current_index = 0;
        
        tbb::concurrent_hash_map <size_t, unsigned short >::iterator it = vertex_type.begin();
        tbb::concurrent_hash_map <size_t, unsigned short >::const_iterator vertex_end = vertex_type.end();
        tbb::concurrent_hash_map <size_t, std::vector < size_t > >::accessor ac;
        
        print_msg("Attempting to create "+std::to_string(num_parts)+" with "+std::to_string(edges_per_file)+" edges per file ",DEBUG);
        
        while(current_index < num_parts) {
            
            std::string partfile = edgefile+"_part_"+std::to_string(current_index)+".txt";

            size_t current_edges = 0, current_vertices = 0;
            size_t vid, num_vid_edges = 0;
            unsigned short vid_type;
            std::vector < size_t > vid_edges;
            size_t i;
            
            std::ofstream pf(partfile);
            if(!pf.is_open()) {
                print_msg("Could not open "+partfile, ERROR);
                exit(EXIT_FAILURE);
            }
            
            while((it != vertex_end) && ( (current_edges) <= edges_per_file )) {
                vid = it -> first;
                vid_type = it -> second;
                if( edges.find(ac,vid) ) {
                    vid_edges = ac -> second;
                    num_vid_edges = vid_edges.size();
                }
                else {
                    vid_edges.clear();
                    num_vid_edges = 0;
                }
                ac.release();
                if((current_edges+num_vid_edges) > edges_per_file)
                    break;
                
                pf<<vid<<"\t"<<vid_type<<"\t";
                for(i = 0 ; i < num_vid_edges; i++) {
                    pf<<vid_edges[i]<<"\t";
                }
                pf<<"\n";
            
                current_edges = current_edges + num_vid_edges;
                current_vertices++;
                it++;
            }
            
            if(current_index == (num_parts-1)) {
                while((it != vertex_end)) {
                    vid = it -> first;
                    vid_type = it -> second;
                    if( edges.find(ac,vid) ) {
                        vid_edges = ac -> second;
                        num_vid_edges = vid_edges.size();
                    }
                    else {
                        vid_edges.clear();
                        num_vid_edges = 0;
                    }
                    ac.release();
                    pf<<vid<<"\t"<<vid_type<<"\t";
                    for(i = 0 ; i < num_vid_edges; i++) {
                        pf<<vid_edges[i]<<"\t";
                    }
                    pf<<"\n";
                    current_edges = current_edges + num_vid_edges;
                    current_vertices++;
                    it++;
                }
            }
            
            pf.close();
            print_msg("Saved "+std::to_string(current_vertices)+" vertices with "+std::to_string(current_edges)+"edges into interval "+std::to_string(current_index),DEBUG);
            
            current_index++;
            current_edges = 0;
            current_vertices = 0;
            
        }
       
    }
    /*
     * load_structure loads the structure (vertex information & corresponding edges) of an interval into memory
     */
    void load_structure(int interval) {
        
        std::string partfile = edgefile+"_part_"+std::to_string(interval)+".txt";
        std::ifstream pf(partfile);
        if(!pf.is_open()) {
            print_msg("Could not open "+partfile, ERROR);
            exit(EXIT_FAILURE);
        }
        
        tbb::concurrent_hash_map < size_t, std::vector < size_t > >::accessor ac;
        tbb::concurrent_hash_map < size_t, unsigned short >::accessor t_ac;
        std::string line; 
        size_t vid, dest;
        unsigned short vid_type;
        
        while(std::getline(pf,line)) {

            std::stringstream ss(line);
            ss >> vid >> vid_type;
            
            vertex_type.insert(t_ac,vid);
            t_ac -> second = vid_type;
            t_ac.release();
            
            std::vector < size_t > vid_edges;
            while(ss>>dest) {
                vid_edges.push_back(dest);
            }
            if(! vid_edges.empty()) {
                edges.insert(ac,vid);
                ac -> second = vid_edges; 
                ac.release();
            }
            
        }
        
        pf.close();
        print_msg("Loaded edges into interval "+std::to_string(interval),DEBUG);
    }
    
    /*
     * load_reach_dist loads the distance reachability map of all the open vertices in a particular interval.
     */
    void load_reach_dist(int interval) {
        
        std::string reachfile = edgefile+"_reachdist_"+std::to_string(interval)+".txt";
        std::ifstream rf(reachfile);
        
        if(!rf.is_open()) {
            print_msg("Could not open reachability distance file for interval "+std::to_string(interval),WARNING);
            return;
        }
        
        tbb::concurrent_hash_map < size_t, std::map < size_t, size_t > >::accessor ac;
        std::string line;
        size_t vid, vid_num_reach;
        
        while(std::getline(rf,line)) {
            std::stringstream(line)>>vid>>vid_num_reach;
            size_t dest_id,dist;
            
            std::map < size_t, size_t > vid_dist_reach; 
            for(size_t i = 0; i < vid_num_reach; i++) {
                if(!std::getline(rf,line)) {
                    print_msg("The reachability distance file of Interval "+std::to_string(interval)+"may be corrupt",WARNING);
                    break;
                }
                
                std::stringstream(line)>>dest_id>>dist;
                vid_dist_reach[dest_id] = dist;
            }
            reach_dist.insert(ac,vid);
            ac->second = vid_dist_reach;
            ac.release();
        }
        rf.close();
        print_msg("Loaded the reachability distance of "+std::to_string(reach_dist.size())+" vertices from interval "+std::to_string(interval),DEBUG);
    }
    
    /*
     * load_reach loads the reachability sets of all the closed vertices in the given interval.
     */
    void load_reach(int interval) {
        
        std::string reachfile = edgefile+"_reach_"+std::to_string(interval)+".txt";
        std::ifstream rf(reachfile);
        
        if(!rf.is_open()) {
            print_msg("Could not open reachability file for interval "+std::to_string(interval),WARNING);
            return;
        }
        
        tbb::concurrent_hash_map < size_t, std::set < size_t > >::accessor ac;
        std::string line;
        size_t vid, vid_num_reach;
        
        while(std::getline(rf,line)) {
            std::stringstream(line)>>vid>>vid_num_reach;

            
            if(!std::getline(rf,line)) {
                print_msg("The reachability file of Interval "+std::to_string(interval)+"may be corrupt",WARNING);
                break;
                
            }
            
            reach.insert(ac,vid);
            std::istringstream is(line);
            std::set < size_t > vid_reach(( std::istream_iterator<size_t>( is ) ), std::istream_iterator<size_t>() );
            
            ac->second = vid_reach;
            ac.release();
            
        }
        
        rf.close();
        print_msg("Loaded the reachability of "+std::to_string(reach.size())+" vertices from interval "+std::to_string(interval),DEBUG);
        
    }
    /*
     * save_reach_dist saves the distance reachability map of all the open vertices in a particular interval to the corresponding file. 
     */
    void save_reach_dist(int interval) {
        
        std::string reachfile = edgefile+"_reachdist_"+std::to_string(interval)+".txt";
        std::ofstream rf(reachfile);
        
        if(!rf.is_open()) {
            print_msg("Could not open reachability distance file for interval "+std::to_string(interval),ERROR);
            exit(EXIT_FAILURE);
        }
        
        tbb::concurrent_hash_map < size_t, std::map < size_t, size_t > >::const_iterator reach_dist_end =reach_dist.end();
        for(tbb::concurrent_hash_map < size_t, std::map < size_t, size_t > >::iterator it = reach_dist.begin(); it != reach_dist_end; it++ ){
            size_t vid, vid_num_reach;
            std::map < size_t, size_t > vid_dist_reach = it -> second;
            
            vid = it -> first;
            vid_num_reach = vid_dist_reach.size();
            rf<<vid<<"\t"<<vid_num_reach<<"\n";
            
            std::map < size_t, size_t >::const_iterator vid_dist_reach_end = vid_dist_reach.end();
            for(std::map < size_t, size_t >::iterator vid_it = vid_dist_reach.begin(); vid_it != vid_dist_reach_end; ++vid_it) {
                rf<<vid_it -> first<<"\t"<<vid_it -> second<<"\n";
            }
        }
        rf.close();
        
        print_msg("Saved the reachability distance of "+std::to_string(reach_dist.size())+" vertices from interval "+std::to_string(interval),DEBUG);
        return;
    }
    /*
     * save_reach saves the distance sets of all the closed vertices in a particular interval to the corresponding file. 
     */
    void save_reach(int interval) {
        
        std::string reachfile = edgefile+"_reach_"+std::to_string(interval)+".txt";
        std::ofstream rf(reachfile);
        
        if(!rf.is_open()) {
            print_msg("Could not open reachability file for interval "+std::to_string(interval),ERROR);
            exit(EXIT_FAILURE);
        }
        
        tbb::concurrent_hash_map < size_t, std::set < size_t > >::const_iterator reach_end = reach.end();
        for(tbb::concurrent_hash_map < size_t, std::set < size_t > >::iterator it  = reach.begin(); it != reach_end; ++it) {
            size_t vid, vid_num_reach;
            std::set < size_t > vid_reach = it -> second;
            
            vid = it -> first;
            vid_num_reach = vid_reach.size();
            rf<<vid<<"\t"<<vid_num_reach<<"\n";
            
            std::set < size_t >::const_iterator vid_reach_end = vid_reach.end();
            for(std::set < size_t >::iterator vid_it = vid_reach.begin(); vid_it != vid_reach_end; ++vid_it) {
                rf<<*vid_it<<"\t";
            }
            
            rf<<"\n";
        }
        
        rf.close();
        print_msg("Saved reachability of "+std::to_string(reach.size())+" vertices from interval "+std::to_string(interval),DEBUG);
        return;
    }
    /*
     * save_next_updates appends the updates generated by the current interval to a updates file.
     * The updates are consumed in the subsequent iteration.
     */
    void save_next_updates(int interval) {
        
        std::string updatefile = edgefile+"_updates.txt";
        std::fstream uf;
        if(interval == 0) {
            uf.open(updatefile, std::fstream::out | std::fstream::trunc);
        } else {
            uf.open(updatefile, std::fstream::out | std::fstream::app);
        }
        if(!uf.is_open()) {
            print_msg("Could not open updates file ",ERROR);
            exit(EXIT_FAILURE);
        }
        
        tbb::concurrent_hash_map < size_t, std::vector <size_t> >::iterator it;
        tbb::concurrent_hash_map < size_t, std::vector <size_t> >::const_iterator next_updates_end = next_updates.end();
        
        for(it = next_updates.begin(); it != next_updates_end; it++) {
            size_t vid = it -> first;
            std::vector < size_t > vid_updates = it -> second;
            std::vector < size_t >::iterator vid_it;
            std::vector < size_t >::const_iterator vid_updates_end = vid_updates.end();
            
            uf<<vid<<"\t";
            for(vid_it = vid_updates.begin(); vid_it != vid_updates_end; vid_it++) {
                uf<<*vid_it<<"\t";
            }
            uf<<"\n";
        }
        uf.close();
        print_msg("Saved updates of "+std::to_string(next_updates.size())+" vertices from interval "+std::to_string(interval),DEBUG);
        return;
    }
    /*
     * load_updates loads the updates from the previous iterations into memeory. 
     */
    void load_updates() {
        
        std::string updatefile = edgefile+"_updates.txt";
        std::ifstream uf(updatefile);
        
        if(!uf.is_open()) {
            print_msg("Could not open updates file ",ERROR);
            exit(EXIT_FAILURE);
        }
        
        tbb::concurrent_hash_map < size_t, std::vector <size_t> >::accessor ac;        
        std::string line;
        size_t vid,dest;
        
        while(std::getline(uf,line)) {
            std::stringstream ss(line);
            ss >> vid;
            
            std::vector < size_t > vid_updates;                        
            while(ss>>dest) {
                vid_updates.push_back(dest);
            }
            
            updates.insert(ac,vid);
            ac -> second = vid_updates;
            ac.release();
        }
        
        uf.close();
        print_msg("Loaded "+std::to_string(updates.size())+" vertex updates",DEBUG);
        return;
    }
    
public:
    /*
     * Constructor
     */
    shard(int n, std::string tfile, std::string efile) :
        num_parts(n),
        typefile(tfile),
        edgefile(efile){ 
            num_v = num_e = 0; 
    }
    /*
     * init_shards loads the entire graph into memory and then calls the split_structure function. 
     */  
    void init_shards() {
        
        std::thread et(&shard::load_edges,this);
        std::thread tt(&shard::load_type,this);
        et.join();
        tt.join();
        
        avg_odegree = num_e/num_v;
        
        if(num_parts> 1) {
            split_structure();
            print_msg("Completed splitting graph into "+std::to_string(num_parts)+" intervals",INFO);
        }
        edges.clear();
        vertex_type.clear();
        current_iter = 0;
    }
    
    /*
     * next_iter clears the previous updates and loads the new set of updates into memory.
     */
    void next_iter()
    {
        updates.clear();
        load_updates();
        current_iter++;
    }
    /*
     * load_interval loads the structure and reachability information of the interval into memory. 
     */
    void load_interval(int interval) {
        
        std::thread st (&shard::load_structure,this,interval);
        if( current_iter != 0) {
            std::thread rt (&shard::load_reach,this,interval);
            std::thread rdt (&shard::load_reach_dist, this, interval);
            
            rt.join();
            rdt.join();
            
        }
        
        st.join();
        
        print_msg("Completed loading interval "+std::to_string(interval),INFO);
    }
    /*
     * save_interval stores the reachability information and the updates in the corresponding files.
     */
    void save_interval(int interval) {
        
        std::thread rt (&shard::save_reach,this,interval);
        std::thread rdt (&shard::save_reach_dist,this,interval);
        std::thread nut (&shard::save_next_updates,this,interval);
        
        
        edges.clear();
        vertex_type.clear();
        
        nut.join();
        next_updates.clear();
        
        rt.join();
        reach.clear();
        
        rdt.join();
        reach_dist.clear();
        
        print_msg("Saved interval "+std::to_string(interval),INFO);
    }
    /*
     * Saves the reachability of the entire graph into the _index.txt file.
     */
    void save_reachability() {
        
        std::string indexfile = edgefile+"_index.txt";
        std::ofstream f(indexfile);
        
        if(!f.is_open()) {
            print_msg("Could not open index file ",ERROR);
            exit(EXIT_FAILURE);
        }
        for(int i = 0; i < num_parts; i++) {
            
            std::string partfile = edgefile+"_reachdist_"+std::to_string(i)+".txt"; 
            std::ifstream pf(partfile);
            std::string line;
            if(!pf.is_open()) {
                print_msg("Could not open reachability distance file for interval "+std::to_string(i),ERROR);
                exit(EXIT_FAILURE);
            }
            while(std::getline(pf,line)) {
                f<<line<<"\n";
            }
            pf.close();
        }
        f.close();
        print_msg("Saved the constructed index to file "+indexfile,INFO);
        f.close();
    }
};


#endif /* SHARDER_HPP */

