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
    for( size_t i = 0; i < query["edge"].size(); i++) //For each edge in the Query Graph, increment the out_degree of the source vertex.
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
    const int nitems = 4;
    int blocklengths[4] = {1,1,1,1};
    MPI_Datatype types[4] = {MPI_UINT32_T,MPI_UINT32_T ,MPI_UINT32_T ,MPI_UINT32_T};
    MPI_Datatype mpi_json_info;

    MPI_Aint offsets[4];
    offsets[0] = offsetof(json_info,nodes);
    offsets[1] = offsetof(json_info,edges);
    offsets[2] = offsetof(json_info,out);
    offsets[3] = offsetof(json_info,in);
       
    MPI_Type_create_struct(nitems, blocklengths, offsets,types, &mpi_json_info);
    MPI_Type_commit(&mpi_json_info);
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
    json_info fragments_info[4];
    map <uint32_t, json> nodes;
    map <uint32_t, list< uint32_t > >edges;
    MPI_Gather(&fragment_info,1,mpi_json_info,fragments_info,1,mpi_json_info,4,MPI_COMM_WORLD);
    char *buf;
    int filesize;
    if(world_rank == ROOT)
    {
        for(int i=0; i<ROOT; i++)
        {
            cout<<i<<"\n";
            cout<<fragments_info[i].nodes<<"\t"<<fragments_info[i].edges<<"\t"<<fragments_info[i].out<<"\t"<<fragments_info[i].in<<"\n";
        }
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
    
    if(world_rank == ROOT)
    {
        cout<<query["node"].size()<<"\t"<<query["edge"].size()<<"\n";
    }
    else
    {
                //iEval()
    }
    MPI_Finalize();
}
