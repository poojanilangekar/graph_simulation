#include <mpi.h>
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <streambuf>
#include <cstdlib>
#include <cstddef>
#include <json.hpp>

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

int main(int argc, char * argv[])
{
    MPI_Init(NULL,NULL);
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    json fragment;
    json_info fragment_info;
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
    MPI_Gather(&fragment_info,1,mpi_json_info,fragments_info,1,mpi_json_info,4,MPI_COMM_WORLD);
    if(world_rank == ROOT)
    {
        for(int i=0; i<ROOT; i++)
        {
            cout<<i<<"\n";
            cout<<fragments_info[i].nodes<<"\t"<<fragments_info[i].edges<<"\t"<<fragments_info[i].out<<"\t"<<fragments_info[i].in<<"\n";
        }
    }
    MPI_Finalize();
}
