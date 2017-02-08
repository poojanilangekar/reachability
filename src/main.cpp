/* 
 * File:   main.cpp
 * Author: Pooja Nilangekar
 *
 * Created on 2 February, 2016, 9:15 AM
 */

#include <mpi.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstdlib>
#include <map>
#include <set>
#include <vector>
#include <queue>
#include <utility>
#include <thread>
#include <mutex>
#include <algorithm>
#include <ThreadPool.hpp>
#include "tbb/concurrent_hash_map.h"

#define REMOTE 0
#define CLOSED 1
#define OUT 2
#define IN 3

using namespace std;

int world_rank,world_size,master;
map <size_t,int> vertex_type;
map <size_t,int> remote_address;
map<size_t,vector<size_t> > adjlist;
tbb::concurrent_hash_map<size_t,vector< pair< size_t, int> > > reach;
mutex adjlist_access;

/*
 * A function to compute the local BFS of the given vertex_id.
 */
void BFS_local(size_t vid) 
{
    vector< pair<size_t, int> > local;
    vector< size_t > exp;
    queue < pair < size_t, int> > unexp;
    pair < size_t, int> distpair;
    distpair = make_pair(vid,0);
    unexp.push(distpair);
    exp.push_back(distpair.first);
    
    while(!unexp.empty())
    {
        size_t current;
        int dist;
        distpair = unexp.front();
        unexp.pop();    
        current = distpair.first;
        dist = distpair.second;
        if((vertex_type[current] == CLOSED) || (current == vid))
        {
            adjlist_access.lock();
                vector<size_t> neighbours (adjlist[current]);
            adjlist_access.unlock();
            for(vector<size_t>::iterator it = neighbours.begin(); it != neighbours.end();++it) {
                if(find(exp.begin(), exp.end(), *it) == exp.end()) {
                    exp.push_back(*it);
                    unexp.push(make_pair(*it, dist+1));
                    
                }
            }
        }
        else {
            local.push_back(distpair);
        }
        
    }
    
    tbb::concurrent_hash_map<size_t, vector< pair <size_t, int> > >::accessor ac;
    reach.insert(ac,vid);
    ac->second = local;
    ac.release();
}

/*
 * 
 */
int main(int argc, char** argv) {
    
    int provided;
    MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided); //Initialize MPI in multi threaded mode. Abort if the mode is not supported.

    if(provided < MPI_THREAD_MULTIPLE)
    {
            cout<<"Error: The MPI library does not have full thread support\n";
            MPI_Abort(MPI_COMM_WORLD,1);
            exit(1);
    }
    
    //Get communicator size and rank.
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    master = world_size - 1;
    
    
    if(world_rank != master)
    {
        
        FILE* f = freopen(std::string("LOG_" + std::to_string(world_rank)).c_str(), "w", stdout);
        string filename = argv[1];
        string localfile = filename+"_local_"+to_string(world_rank)+".txt";
        ifstream lf(localfile);
        if(!lf.is_open())
        {
            cerr<<"Could not open "<<localfile;
            exit(1);
        }
        string line;
        while(getline(lf,line)) {
            size_t vid;
            stringstream(line)>>vid;
            vertex_type[vid] = CLOSED;
        }
        lf.close();
        cout<<"Completed local vertex scan.\n";
        string edgefile = filename+"_"+to_string(world_rank)+".txt";
        ifstream ef(edgefile);
        if(!ef.is_open())
        {
            cerr<<"Could not open "<<edgefile;
            exit(1);
        }
        while(getline(ef,line)) {
            size_t src,dest;
            stringstream(line)>>src>>dest;
            if(vertex_type[dest] == REMOTE) {
                vertex_type[src] = OUT;
            }
            else if((vertex_type[src] == REMOTE) && (vertex_type[dest] != OUT)) {
                    vertex_type[dest] = IN;
            }
            if(vertex_type[src] != REMOTE )
                adjlist[src].push_back(dest);
        }
        ef.close();
        cout<<"Completed edge scan.\n";
        string typefile = filename+"_type_"+to_string(world_rank)+".txt";
        ofstream tf(typefile);
        for(map <size_t,int>::iterator it = vertex_type.begin(); it != vertex_type.end();++it)
        {
            tf<<it->first<<"\t"<<it->second<<"\n";
        }
        tf.close();
        /*int max_threads = (thread::hardware_concurrency())*2;
        ThreadPool *pool = new ThreadPool(max_threads);
        int i =0;
        for(map<size_t,int>::const_iterator it = vertex_type.begin(); it != vertex_type.end(); ++it) {
            if((it->second == IN) || (it->second == OUT)) {
                pool->enqueue(BFS_local,it->first);
                
            }
            if((++i%10000) == 0)
                cout<<"Completed "<<i<<"vertices. \n";
        }
        delete pool;
        cout<<"Completed preprocessing of all open vertices. \n";
        string reachfile = filename+"_reach_"+to_string(world_rank)+".txt";
        ofstream rf(reachfile);
        if(!rf.is_open()) 
        {
            cerr<<"Could not open "<<reachfile;
            exit(1);
        }
        for(tbb::concurrent_hash_map<size_t,vector< pair< size_t, int> > >::const_iterator it = reach.begin(); it != reach.end(); ++it )
        {
            rf<<it->first<<"\t"<<(it->second).size()<<"\n";
            for(vector < pair < size_t, int> >::const_iterator vi = (it->second).begin(); vi != (it->second).end(); ++vi)
            {
                rf<<(*vi).first<<"\t"<<(*vi).second<<"\n";
            }
        }
        rf.close();*/
        fclose(f);
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    if(world_rank == master)
        cout<<"All slave workers completed preprocessing.";
    MPI_Finalize();

    
    return 0;
}

