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

typedef struct {
    uint32_t nodes,edges,out,in;
}json_info;

json_info get_info(json f_obj)
{
    json_info result;
    result.nodes = f_obj["node"].size();
    result.edges = f_obj["edge"].size();
    result.out = f_obj["Fi.O"].size();
    result.in = f_obj["Fi.I"].size();
    return result;
}

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



set < pair <int,uint32_t> > iEval(json &query, map <uint32_t, json> &fnodes, map <uint32_t, list< uint32_t > >&fedges)
{
    set <pair <int,uint32_t > > l; 
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
                            l.insert(make_pair(u,v->first));
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
                                        l.insert(make_pair(u,v->first));
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
                l.insert(make_pair(u,v->first));
            }
        }
    }
    return l;
}


map < int, set < pair <int,uint32_t> > > compute_li(int rank,set <pair <int, uint32_t> >l, map<uint32_t,json> nodes,map < pair <int,int>, list<uint32_t> > dgraph)
{   
    map < int, set < pair <int,uint32_t> > >li;
    for(set<pair <int,uint32_t> >::iterator it = l.begin(); it != l.end(); ++it)
    {
        for(map < pair <int,int>, list<uint32_t> >::iterator ij = dgraph.begin(); ij != dgraph.end(); ++ij)
        {
            if((ij->first).first == rank)
            {
                if(find((ij->second).begin(),(ij->second).end(),(*it).second) != (ij->second).end())
                    li[(ij->first).second].insert(*it);
            }
        }
        if(nodes[(*it).second].find("anc") != nodes[(*it).second].end())
        {
            if(!(nodes[(*it).second]["anc"][(*it).first].is_null()))
                li[rank].insert(*it);
        }
    }
    return li;

}

int main(int argc, char * argv[])
{
    MPI_Init(NULL,NULL);
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    json fragment;
    json_info fragment_info;
    map < pair <int,int>, list<uint32_t> > dgraph;
    //Create MPI_Datatype
/*    const int nitems = 4;
    int blocklengths[4] = {1,1,1,1};
    MPI_Datatype types[4] = {MPI_UINT32_T,MPI_UINT32_T ,MPI_UINT32_T ,MPI_UINT32_T};
    MPI_Datatype mpi_json_info;

    MPI_Aint offsets[4];
    offsets[0] = offsetof(json_info,nodes);
    offsets[1] = offsetof(json_info,edges);
    offsets[2] = offsetof(json_info,out);
    offsets[3] = offsetof(json_info,in);
       
    MPI_Type_create_struct(nitems, blocklengths, offsets,types, &mpi_json_info);
    MPI_Type_commit(&mpi_json_info);*/
    //Create MPI_Datatype
    if(world_rank != ROOT)
    {
        string filename = argv[world_rank+1];
        ifstream fragmentf(filename);
        stringstream fss;
        fss << fragmentf.rdbuf();
        fragment = json::parse(fss);
        fragment_info = get_info(fragment);
        
    } 
    //json_info fragments_info[4];
    map <uint32_t, json> nodes;
    map <uint32_t, list< uint32_t > >edges;
    //MPI_Gather(&fragment_info,1,mpi_json_info,fragments_info,1,mpi_json_info,4,MPI_COMM_WORLD);
    char *buf;
    int filesize;
    if(world_rank == ROOT)
    {
       /* for(int i=0; i<ROOT; i++)
        {
            cout<<i<<"\n";
            cout<<fragments_info[i].nodes<<"\t"<<fragments_info[i].edges<<"\t"<<fragments_info[i].out<<"\t"<<fragments_info[i].in<<"\n";
        }*/
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
    else
    {
        dgraph = dependency_graph(fragment,world_rank);
        nodes = get_nodes(fragment);
        edges = get_edges(fragment);
        fragment.clear();
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
        {   set< pair<int, uint32_t> > l = iEval(query,nodes,edges);
            ofstream output("partial_"+to_string(world_rank));
            for(set<pair<int, uint32_t> >::iterator it = l.begin(); it != l.end(); ++it)
                output<<(*it).first<<"\t"<<(*it).second<<"\n";
            output.close();
            map<int, set < pair < int, uint32_t > > > li = compute_li(world_rank,l,nodes,dgraph);
            unchanged = l.empty();
            MPI_Gather(&unchanged,1,MPI_C_BOOL,changes,1,MPI_C_BOOL,4,MPI_COMM_WORLD);
            MPI_Bcast(&unchanged,1,MPI_C_BOOL,ROOT,MPI_COMM_WORLD);
            cout<<world_rank<<"\t"<<unchanged<<"\n";

        }
       else
        {
            MPI_Gather(&unchanged,1,MPI_C_BOOL,changes,1,MPI_C_BOOL,4,MPI_COMM_WORLD);
            for(int i=0;i<4;i++)
                unchanged = unchanged & changes[i];
            MPI_Bcast(&unchanged,1,MPI_C_BOOL,ROOT,MPI_COMM_WORLD);
        }
    }
    MPI_Finalize();
}
