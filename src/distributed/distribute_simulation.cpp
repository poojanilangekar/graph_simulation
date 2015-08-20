#include <mpi.h>
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <streambuf>
#include <cstdlib>
#include <cstddef>
#include <json.hpp>
#include <map>
#include <utility>
#include <list>
#include <set>
#include <algorithm>

#define ROOT 4

using namespace std;
using namespace nlohmann;

typedef struct x_uv{
    int u;
    uint32_t v;
    
    x_uv(int i, uint32_t j)
    {
        u = i;
        v = j;
    }

    inline bool operator<(const x_uv  &o)  const 
    {   
        return u < o.u || (u == o.u && v < o.v);
    }
    
    inline bool operator==(const x_uv &o) const
    {
        return (u == o.u) && (v == o.v);
    }
}x_uv;

map < pair <int,int>, list <uint32_t> > dependency_graph(json f_obj, int current)
{
    map < pair <int,int>, list <uint32_t> > dgraph;
    for(json::iterator it = f_obj["Fi.I"].begin(); it != f_obj["Fi.I"].end(); ++it)
        dgraph[make_pair(current,(*it)["fragment"])].push_back((*it)["node"]);
    for(json::iterator it = f_obj["Fi.O"].begin(); it != f_obj["Fi.O"].end(); ++it)
        dgraph[make_pair((*it)["fragment"],current)].push_back((*it)["node"]);
    return dgraph;

}

void fill_out_degree(json &query)
{
    for( size_t i = 0; i < query["node"].size(); i++)
        query["node"][i]["out_degree"] = 0;
    for( size_t i = 0; i < query["edge"].size(); i++)
    {
        size_t snode = query["edge"][i]["source"];
        query["node"][snode]["out_degree"] = int(query["node"][snode]["out_degree"]) + 1;
    }
    
}

map< uint32_t, json> get_nodes(json fragment)
{
    map <uint32_t, json> nodes;
    for(json::iterator it = fragment["node"].begin(); it != fragment["node"].end(); ++it)
    {
        json node = *it;
        uint32_t id = node["id"];
        node.erase("id");
        nodes[id] = node;
    }
    return nodes;
}

map < uint32_t, list <uint32_t> > get_edges(json fragment)
{
    map<uint32_t, list<uint32_t> > edges;
    for(json::iterator it = fragment["edge"].begin(); it != fragment["edge"].end(); ++it)
        edges[(*it)["source"]].push_back((*it)["target"]);
    return edges;
}

bool check_equal(json datanode, json querynode) 
{
    for(json::iterator it = querynode.begin(); it != querynode.end(); ++it)
    {
        if((it.key() != "id") && (it.key() != "out_degree"))
        {
            if(datanode.find(it.key()) != datanode.end())
            {
                if(datanode[it.key()] != it.value())
                    return false;
            }
            else
                return false;
        }
    }
    return true;
}



set < x_uv > iEval(json &query, map <uint32_t, json> &fnodes, map <uint32_t, list< uint32_t > >&fedges)
{
    set < x_uv > l; 
    for(map<uint32_t, json>::iterator v = fnodes.begin(); v != fnodes.end(); ++v)
    {
        for(int u=0; u < query["node"].size(); u++)
        {
            if(check_equal(v->second,query["node"][u]))
            {
                    int od = query["node"][u]["out_degree"];
                    if(od  == 0)
                        v->second["rvec"][u] = true;
                    else
                    {
                        map<uint32_t, list<uint32_t> >::iterator it = fedges.find(v->first);
                        if(it == fedges.end())
                        {
                            v->second["rvec"][u] = false;
                            l.insert(x_uv(u,v->first));
                        }
                        else
                        {
                            for(int e = 0; e < query["edge"].size(); e++)
                            {
                                int source = query["edge"][e]["source"], target = query["edge"][e]["target"];
                                if(u == source)
                                {   set<uint32_t> desc;
                                    for(list<uint32_t>::iterator t = (it->second).begin(); t != (it->second).end(); ++t)
                                    {
                                        if(fnodes.find(*t) != fnodes.end())
                                        {    if(check_equal(fnodes[*t],query["node"][target]))
                                             {  desc.insert(*t);
                                                fnodes[*t]["anc"][target].push_back(v->first);
                                             }   
                                        }
                                        else
                                            desc.insert(*t);
                                    }
                                    if(desc.empty())
                                    {   
                                        v->second["rvec"][u] = false;
                                        l.insert(x_uv(u,v->first));
                                        break;
                                    }
                                    else
                                        v->second["rvec"][u][target] = desc;
                                }
                            }
                        }
                    }
            }
            else
            {
                v->second["rvec"][u] = false;
                l.insert(x_uv(u,v->first));
            }
        }
    }
    return l;
}


map < int, set < x_uv > > compute_li(int rank,set < x_uv >l, map<uint32_t,json> nodes,map < pair <int,int>, list<uint32_t> > dgraph)
{   
    map < int, set < x_uv > >li;
    for(set< x_uv >::iterator it = l.begin(); it != l.end(); ++it)
    {
        for(map < pair <int,int>, list<uint32_t> >::iterator ij = dgraph.begin(); ij != dgraph.end(); ++ij)
        {
            if((ij->first).first == rank)
            {
                if(find((ij->second).begin(),(ij->second).end(),(*it).v) != (ij->second).end())
                    li[(ij->first).second].insert(*it);
            }
        }
        if(nodes[(*it).v].find("anc") != nodes[(*it).v].end())
        {
            if(!(nodes[(*it).v]["anc"][(*it).u].is_null()))
                li[rank].insert(*it);
        }
    }
    return li;

}

set <x_uv> update_false(set <x_uv> l1, map<uint32_t,json> nodes)
{
    set <x_uv> l2;
    for(set<x_uv>::iterator uv = l1.begin(); uv != l1.end(); ++uv)
    {
        int u = (*uv).u;
        uint32_t v = (*uv).v;
        for(map<uint32_t,json>::iterator z = nodes.begin(); z != nodes.end(); z++)
        {
            for(int i=0; i < (z->second)["rvec"].size(); i++)
            {
                
                if((z->second)["rvec"][i].is_array())
                {
                    if((z->second)["rvec"][i][u].is_array())
                    {
                        list<uint32_t> zu((z->second)["rvec"][i][u].begin(), (z->second)["rvec"][i][u].end());
                        zu.remove(v);
                        if(zu.empty())
                        {   
                            (z->second)["rvec"][i] = false;
                            l2.insert(x_uv(i,z->first));
                        }
                        else
                            (z->second)["rvec"][i][u] = zu;  
                    }
                }
            }
        }
    }
    return l2;

}



int main(int argc, char * argv[])
{
    MPI_Init(NULL,NULL);
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    
    json fragment;
    map < pair <int,int>, list<uint32_t> > dgraph;
    map <uint32_t, json> nodes;
    map <uint32_t, list< uint32_t > >edges;
 
    char *buf;
    int filesize;

    const int nitems = 2;
    int block_lengths[2] = {1,1};
    MPI_Datatype types[2] = {MPI_INT, MPI_UINT32_T};
    MPI_Datatype mpi_x_uv;
    
    MPI_Aint offsets[2];
    offsets[0] = offsetof(x_uv,u);
    offsets[1] = offsetof(x_uv,v);
        
    MPI_Type_create_struct(nitems,block_lengths,offsets,types,&mpi_x_uv);
    MPI_Type_commit(&mpi_x_uv);
    if(world_rank != ROOT)
    {
        string filename = argv[world_rank+1];
        ifstream fragmentf(filename);
        stringstream fss;
        fss << fragmentf.rdbuf();
        fragment = json::parse(fss);
        dgraph = dependency_graph(fragment,world_rank);
        nodes = get_nodes(fragment);
        edges = get_edges(fragment);
        fragment.clear();
        
    } 
    else
    {
        ifstream qf(argv[world_size], ifstream::binary);
        if (!qf.is_open())
        {
            cout<<"Could not open file "<<argv[world_size]<<"\n";
            exit(2); 
        }
        qf.seekg(0,qf.end);
        filesize = qf.tellg();
        qf.seekg(0,qf.beg);
        buf = (char *) malloc((filesize+1)*sizeof(char));
        qf.read(buf,filesize);
        buf[filesize] = '\0';
    }
    MPI_Bcast(&filesize,1,MPI_INT,ROOT, MPI_COMM_WORLD);
    if(world_rank != ROOT)
        buf = (char *) malloc((filesize+1)*sizeof(char));
    MPI_Bcast(buf,filesize+1,MPI_CHAR,ROOT,MPI_COMM_WORLD);
    json query = json::parse(buf);
    free(buf);
    fill_out_degree(query);
    bool unchanged = false;
    bool changes[5];
    //while(unchanged != true)
    {
        if(world_rank != ROOT)
        {   set< x_uv > l = iEval(query,nodes,edges);
            ofstream output("partial_"+to_string(world_rank));
            for(set< x_uv >::iterator it = l.begin(); it != l.end(); ++it)
                output<<(*it).u<<"\t"<<(*it).v<<"\n";
            output.close();
            map<int, set < x_uv > > li = compute_li(world_rank,l,nodes,dgraph);
            unchanged = l.empty();
            MPI_Gather(&unchanged,1,MPI_C_BOOL,changes,1,MPI_C_BOOL,4,MPI_COMM_WORLD);
            MPI_Bcast(&unchanged,1,MPI_C_BOOL,ROOT,MPI_COMM_WORLD);
            li[world_rank] = update_false(li[world_rank],nodes);
            //for(set< x_uv >::iterator it = li[world_rank].begin(); it != li[world_rank].end(); ++it)
              //  cout<<(*it).u<<"\t"<<(*it).v<<"\n";
            int send_size[ROOT], receive_size[ROOT];
            MPI_Status status_send[ROOT],status_receive[ROOT];
            MPI_Request request_send[ROOT], request_receive[ROOT];

            for(int i=0; i<ROOT; i++)
            {
                    if(i != world_rank)
                    {
                        send_size[i] = li[i].size();
                        MPI_Isend(send_size+i,1,MPI_INT,i,world_rank,MPI_COMM_WORLD,request_send+i);
                        MPI_Irecv(receive_size+i,1,MPI_INT,i,i,MPI_COMM_WORLD,request_receive+i);
                    }
            }
            for(int i=0; i<ROOT; i++)
            {
                if(i != world_rank)
                {
                        MPI_Wait(request_send+i,status_send+i);
                        MPI_Wait(request_receive+i,status_receive+i);

                }
            }
            

        }
       else
        {
            MPI_Gather(&unchanged,1,MPI_C_BOOL,changes,1,MPI_C_BOOL,4,MPI_COMM_WORLD);
            unchanged = true;
            for(int i=0;i<4;i++)
                unchanged = unchanged & changes[i];
            MPI_Bcast(&unchanged,1,MPI_C_BOOL,ROOT,MPI_COMM_WORLD);
        }
    }
    MPI_Finalize();
}
