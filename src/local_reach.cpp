/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   local_reach.cpp
 * Author: Pooja Nilangekar
 *
 * Created on 2 June, 2016, 9:07 AM
 */

#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <set>
#include <vector>
#include <utility>
#include <thread>
#include <mutex>
#include <ctime>
#include <chrono>
#include <cstdlib>
#include <tbb/concurrent_hash_map.h>
#include "ThreadPool.hpp"
#include "logger.hpp"
#include "sharder.hpp"


//std::map < size_t, unsigned short > vertex_type;
tbb::concurrent_hash_map < size_t, unsigned short > vertex_type;
tbb::concurrent_hash_map < size_t, std::vector < size_t > > edges;
tbb::concurrent_hash_map < size_t, std::vector < size_t > > updates;
tbb::concurrent_hash_map < size_t, std::vector <size_t> > next_updates;
tbb::concurrent_hash_map < size_t, std::set < size_t > > reach;
tbb::concurrent_hash_map < size_t, std::map < size_t, size_t > > reach_dist;

size_t total_updates = 0;
std::mutex updates_lock;
std::clock_t start, current;


/*
 * gather_updates function gathers all the updates of its out neighbours from its previous iteration.
 * It consolidates the updates into a set and returns it to the calling function.
 */
std::set < size_t > gather_updates(size_t vid) {
    
    tbb::concurrent_hash_map < size_t, std::vector <size_t> >::const_accessor e_ac;  
    std::set <size_t> neighbour_updates;
    
    if(edges.find(e_ac,vid)) {
        std::vector < size_t > out_vertices = e_ac->second;
        e_ac.release();
    
        tbb::concurrent_hash_map < size_t, std::vector <size_t> >::const_accessor u_ac;
        std::vector < size_t >::iterator out_vertices_end = out_vertices.end();
        for(std::vector < size_t >::iterator it = out_vertices.begin(); it != out_vertices_end; ++it) { 
            
            if(updates.find(u_ac,*it) == true) { 
                std::vector < size_t > out_edge_updates = u_ac->second;
                neighbour_updates.insert(out_edge_updates.begin(), out_edge_updates.end());
            }
            u_ac.release(); 
        }
    }
    return neighbour_updates;
    
}

/*
 * apply_closed_updates function is called to modify the reachability of the closed vertices based on the updates from its neighbours.
 */
std::vector < size_t > apply_closed_updates(size_t vid, std::set < size_t > neighbour_updates) {
    
    
    tbb::concurrent_hash_map< size_t, std::set < size_t> >::accessor r_ac; 
    std::vector < size_t > iter_updates;
    reach.insert(r_ac,vid);
    std::set < size_t > current_reach = r_ac->second;
    
    std::set < size_t >::iterator updates_end = neighbour_updates.end();
    for(std::set < size_t >::iterator it = neighbour_updates.begin(); it != updates_end; ++it ) {
    
        if((current_reach.insert(*it)).second) {
            iter_updates.push_back(*it);
        }
    
    }
    
    r_ac->second = current_reach;
    r_ac.release();
    
    return iter_updates; 
    
}

/*
 * apply_open_updates is similar to apply_closed_updates in functionality with 2 major differences.
 *      1. Along with updating the reachability, the distance is also updated.
 *      2. The updates are not returned as open vertices do not propagate updates.
 */
void apply_open_updates(size_t vid, size_t iter, std::set < size_t > neighbour_updates) {
    
    
    tbb::concurrent_hash_map < size_t, std::map < size_t, size_t > >::accessor r_ac; 
    reach_dist.insert(r_ac,vid);
    std::map < size_t,size_t > current_reach = r_ac->second;
    
    std::set < size_t >::iterator updates_end = neighbour_updates.end();
    for(std::set < size_t >::iterator it = neighbour_updates.begin(); it != updates_end; ++it ) { 
        if(*it == vid) 
            continue;
        if(current_reach.find(*it) == current_reach.end()) { 
            current_reach[*it] = iter;
        }
    }
    
    r_ac->second = current_reach; 

    r_ac.release();
    return;
    
}

/*
 * scatter_updates function scatters the updates to be consumed in the next iteration by appending it to the next_updates map.
 */
void scatter_updates(size_t vid, std::vector < size_t > iter_updates) {
    
        tbb::concurrent_hash_map < size_t, std::vector <size_t> >::accessor nextu_ac;
        next_updates.insert(nextu_ac,vid);
        nextu_ac->second = iter_updates;
        nextu_ac.release();
        
        updates_lock.lock();
        total_updates = total_updates + iter_updates.size();
        updates_lock.unlock();
        
        return;
}

/*
 * The update_vertex function is called for each vertex during each iteration. 
 * Depending on the type_id of the vertex and the iteration count, it calls necessary functions to update the reachability.  
 */
void update_vertex(size_t vid, unsigned short type_id, size_t iter) {

    std::vector < size_t > iter_updates;
    std::set < size_t > neighbour_updates;
    
    if(iter == 0) { 
        
        if((type_id == REMOTE) || (type_id == OUT)) {
            
            iter_updates.push_back(vid);
            scatter_updates(vid, iter_updates);
            return;
        }
    
    } else { 

        if(type_id == CLOSED) {  

            neighbour_updates = gather_updates(vid);
            iter_updates = apply_closed_updates(vid, neighbour_updates);

            if(iter_updates.size()) { 
                scatter_updates(vid, iter_updates);  
            }

            return;
        }
        else if( (type_id == IN) || (type_id == OUT)) { 

            neighbour_updates = gather_updates(vid);
            apply_open_updates(vid, iter, neighbour_updates);
            return;
        }
        
    }
}

/*
 * main initializes the sharder to split the graph into the specified set of intervals. 
 * Processes the graph one interval at a time until no further updates are available. 
 * Stores the final index in the index file.
 */
int main(int argc, char **argv) {
    
    start = std::clock();
    
    if(argc != 4) {
        print_msg("Usage: "+std::string(argv[0])+" <typefile> <edgefile> <number_of_intervals>",ERROR);
        exit(1);
    }
    
    std::string typefile(argv[1]), edgefile(argv[2]);
    int num_intervals = atoi(argv[3]);
    
    shard index_shard(num_intervals, typefile,edgefile);
    index_shard.init_shards();
    
    print_msg("Completed pre-processing. Starting Index construction.", INFO);
    
    bool reach_updates = true;
    int std_threads = std::thread::hardware_concurrency()*2;
    size_t iter = 0;
    
    print_msg("Starting index construction with "+std::to_string(std_threads)+" threads",INFO);
    while( reach_updates ) {
        
        for(int i = 0; i < num_intervals; i++) {
            index_shard.load_interval(i);
            ThreadPool *pool = new ThreadPool(std_threads);
            tbb::concurrent_hash_map < size_t, unsigned short >::const_iterator vertices_end = vertex_type.end();

            for(tbb::concurrent_hash_map <size_t, unsigned short >::iterator it = vertex_type.begin(); it != vertices_end; ++it) {
                size_t vid = it->first;
                unsigned short type_id = it->second;
                pool->enqueue(update_vertex,vid,type_id,iter);
            }

            delete pool;
            print_msg("Completed processing interval "+std::to_string(i),DEBUG);
            index_shard.save_interval(i);
        }
        print_msg("Completed iteration "+std::to_string(iter),INFO);
        
        iter++;
        index_shard.next_iter();
        print_msg(std::to_string(updates.size())+" vertices produced " + std::to_string(total_updates) + " updates to be propagated in iteration "+std::to_string(iter),DEBUG);
        total_updates = 0;
        if(updates.size() == 0) {
            reach_updates = false;
            print_msg("No more updates. Index construction completed",INFO);
        }
        
    }

    index_shard.save_reachability();
    
    return (EXIT_SUCCESS);
}