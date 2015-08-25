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
#include <thread>
#include <mutex>
#include <unistd.h>

#define ROOT 4

using namespace std;
using namespace nlohmann;

//struct x_uv to store a pair (u,v) where u in Vq and v in Vi. The pair is used to denote false/true matches.
typedef struct x_uv{
    int u;
    uint32_t v;
    
    x_uv(int i, uint32_t j)
    {
        u = i;
        v = j;
    }
    x_uv()
    {};
    x_uv(const x_uv& o)
    {
        u = o.u;
        v = o.v;
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


//Mutex for the set Lin, containing the set of x_uv having set to false
mutex Lout_mutex, Lin_mutex;
set <x_uv> Lout, Lin;

/*bool unchanged = false;
mutex unchanged_mutex;


bool ifunchanged()
{
    unchanged_mutex.lock();
    bool val = unchanged;
    unchanged_mutex.unlock();
    return val;
    
}*/


//Returns annotated dependency graph for a site.
//The map is of the form (fi,fj) -> list(v). Where fi is the source fragment and fj is the destination fragment. 
//list(v) is the list of nodes which fj requires from fi. 
map < pair <int,int>, list <uint32_t> > dependency_graph(json f_obj, int current)
{
    map < pair <int,int>, list <uint32_t> > dgraph;
    for(json::iterator it = f_obj["Fi.I"].begin(); it != f_obj["Fi.I"].end(); ++it)
        dgraph[make_pair(current,(*it)["fragment"])].push_back((*it)["node"]);
    for(json::iterator it = f_obj["Fi.O"].begin(); it != f_obj["Fi.O"].end(); ++it)
        dgraph[make_pair((*it)["fragment"],current)].push_back((*it)["node"]);
    return dgraph;

}

//Function to fill the out_degree of all the query nodes. 
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
//Constructs a map of nodes for the fragment.
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

//Constructs a map of edges for the fragment.
map < uint32_t, list <uint32_t> > get_edges(json fragment)
{
    map<uint32_t, list<uint32_t> > edges;
    for(json::iterator it = fragment["edge"].begin(); it != fragment["edge"].end(); ++it)
        edges[(*it)["source"]].push_back((*it)["target"]);
    return edges;
}

//Checks the equality of the datanode and the query node.
//Returns true only if all the labels of the querynode match the labels of the datanode. 
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

//Contructs and ships the partial result from the current fragment to the node. 
void ship_partial_result(map<uint32_t, json> fnodes,json query, MPI_Datatype mpi_x_uv)
{
    list <x_uv> p_result;
    for(map<uint32_t,json>::iterator v = fnodes.begin(); v != fnodes.end(); ++v)
    {
        for(int u=0; u < query["node"].size(); u++)
        {
            if(v->second["rvec"][u].is_boolean())
            {
                bool uv = v->second["rvec"][u];
                if(uv == false)
                    continue;
            }
            p_result.push_back(x_uv(u,v->first));
        }
    }
    x_uv *p_array=new x_uv[p_result.size()];
    copy(p_result.begin(),p_result.end(),p_array);
    MPI_Send(p_array,p_result.size(),mpi_x_uv,ROOT,ROOT+1,MPI_COMM_WORLD);
    delete [] p_array;
}

void union_results(json query, MPI_Datatype mpi_x_uv)
{
    set <x_uv> result;
    for(int i = 0; i < ROOT; i++)
    {
        MPI_Status status;
        MPI_Probe(MPI_ANY_SOURCE, ROOT+1,MPI_COMM_WORLD,&status);
        int count;
        MPI_Get_count(&status,mpi_x_uv,&count);
        if(status.MPI_SOURCE < ROOT)
        {         
            x_uv *l = new x_uv[count];
            MPI_Recv(l,count,mpi_x_uv,status.MPI_SOURCE,ROOT+1,MPI_COMM_WORLD,&status);
            result.insert(l,l+count);
            delete[] l;
        }      
    }
    ofstream output("result.txt");
    for(set<x_uv>::iterator it = result.begin(); it != result.end(); ++it)
        output<<(*it).u<<"\t"<<(*it).v<<"\n";
    output.close();
}

//Initial Evaluation of the fragment.
void iEval(json &query, map <uint32_t, json> &fnodes, map <uint32_t, list< uint32_t > >&fedges)
{
    set < x_uv > l; 

    //For each node in the fragment, check if the labels of the query nodes. 
    for(map<uint32_t, json>::iterator v = fnodes.begin(); v != fnodes.end(); ++v)
    {
        for(int u=0; u < query["node"].size(); u++)
        {
            if(check_equal(v->second,query["node"][u]))
            {
                //If the labels are equal and the out degree of the query node is zero, then it is a match. 
                int od = query["node"][u]["out_degree"];
                if(od  == 0)
                    v->second["rvec"][u] = true;
                else
                {
                    //If the out_degree is not 0, add dependencies.
                    map<uint32_t, list<uint32_t> >::iterator it = fedges.find(v->first);
                    
                    //If the fragment node has a out_degree of zero, then the nodes are not matching. Add it to the set of false nodes.
                    if(it == fedges.end())
                    {
                        v->second["rvec"][u] = false;
                        l.insert(x_uv(u,v->first));
                    }
                    else
                    {
                        //Else iterate through all the edges in the query if the source node is the current query node.
                        for(int e = 0; e < query["edge"].size(); e++)
                        {
                            int source = query["edge"][e]["source"], target = query["edge"][e]["target"];
                            if(u == source)
                            //For each edge, create a set of descendants for the target node.
                            {   set<uint32_t> desc;

                                for(list<uint32_t>::iterator t = (it->second).begin(); t != (it->second).end(); ++t)
                                {
                                    //If the target node is in the current fragment, check for equality with the query target and add to the set of descendants.
                                    if(fnodes.find(*t) != fnodes.end())
                                    {    if(check_equal(fnodes[*t],query["node"][target]))
                                         {  desc.insert(*t);
                                            //Add the source node as an ancestor of the target node with respect to the target node of the query.
                                            fnodes[*t]["anc"][target].push_back(v->first);
                                         }   
                                    }
                                    //If the target node is not present in the current fragment, add it directly to the set of descendants.
                                    else
                                        desc.insert(*t);
                                }
                                //If the source node in the fragment does not have any descendants, the nodes are not matching. Add it to the set of false nodes.
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
            //If labels are not equal, the nodes don't match. Add it to the set of false nodes.
            else
            {
                v->second["rvec"][u] = false;
                l.insert(x_uv(u,v->first));
            }
        }
    }
    Lout_mutex.lock();
        Lout.insert(l.begin(),l.end());
    Lout_mutex.unlock();
}

//Function to receive all the false nodes from other fragments. 

void recv_l_uv(int world_rank,MPI_Datatype mpi_x_uv)
{

    try
    {
        while(true)
        {
            MPI_Status status;
            MPI_Probe(MPI_ANY_SOURCE, world_rank,MPI_COMM_WORLD,&status);
            int count;
            MPI_Get_count(&status,mpi_x_uv,&count);
            if(status.MPI_SOURCE < ROOT)
            {     
                
                x_uv *l = new x_uv[count];
                MPI_Recv(l,count,mpi_x_uv,status.MPI_SOURCE,world_rank,MPI_COMM_WORLD,&status);
                Lin_mutex.lock();
                    Lin.insert(l,l+count);
                Lin_mutex.unlock();
                delete[] l;
            }      
        }
    }
    catch (exception& e)
    {
        terminate();
    }
}

//Function to send the false updated to other fragments (and update current fragment).
void send_l_uv(int rank, map<uint32_t,json> nodes,map < pair <int,int>, list<uint32_t> > dgraph, MPI_Datatype mpi_x_uv)
{   
    set <x_uv> l;
    Lout_mutex.lock();
        l.insert(Lout.begin(),Lout.end());
        Lout.clear();
    Lout_mutex.unlock();

    map < int, set < x_uv > >l_ij;
    for(set< x_uv >::iterator it = l.begin(); it != l.end(); ++it)
    {
        for(map < pair <int,int>, list<uint32_t> >::iterator ij = dgraph.begin(); ij != dgraph.end(); ++ij)
        {
            if((ij->first).first == rank)
            {
                if(find((ij->second).begin(),(ij->second).end(),(*it).v) != (ij->second).end())
                    l_ij[(ij->first).second].insert(*it);
            }
        }
        if(nodes[(*it).v].find("anc") != nodes[(*it).v].end())
        {
            if(!(nodes[(*it).v]["anc"][(*it).u].is_null()))
                l_ij[rank].insert(*it);
        }
    }
    x_uv *send_uv[ROOT];
    int send_size[ROOT];

    for(int i=0; i<ROOT; i++)
    {
            if(i != rank)
            {
                send_size[i] = l_ij[i].size();
                send_uv[i] = new x_uv[send_size[i]];
                if(send_uv[i] != 0)
                {
                    copy(l_ij[i].begin(),l_ij[i].end(),send_uv[i]);
                    MPI_Send(send_uv[i],send_size[i],mpi_x_uv,i,i,MPI_COMM_WORLD);
                    delete[] send_uv[i];
                }
            }
            else
            {
                Lin_mutex.lock();
                    Lin.insert(l_ij[i].begin(),l_ij[i].end());
                Lin_mutex.unlock();
            }
    }

}

//Function to update the false nodes.
void update_false(map<uint32_t,json>& nodes)
{
    set<x_uv> l1;
    Lin_mutex.lock();
        l1.insert(Lin.begin(),Lin.end());
        Lin.clear();
    Lin_mutex.unlock();

    set <x_uv> l2;
    
    for(set<x_uv>::iterator uv = l1.begin(); uv != l1.end(); ++uv) //Iterate through all the  nodes set as false in the input set.
    {
        int u = (*uv).u;
        uint32_t v = (*uv).v;
        for(map<uint32_t,json>::iterator z = nodes.begin(); z != nodes.end(); z++)
        {
            for(int i=0; i < (z->second)["rvec"].size(); i++)
            {
                //Iterate through all the 'rvec's of the all the nodes in the current fragment.
                if((z->second)["rvec"][i].is_array())
                {
                    if((z->second)["rvec"][i][u].is_array())
                    {
                        //Remove v from z['rvec'][i][u], if v was the only remaining dependency, mark z['rvec'][i] as false. Add it to the list of false nodes.
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
    Lout_mutex.lock();
        Lout.insert(l2.begin(),l2.end());
    Lout_mutex.unlock();
}



int main(int argc, char * argv[])
{
    int provided;
    MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided); //Initialize MPI in multi threaded mode. Abort if the mode is not supported.
    thread *recv_th;
    if(provided < MPI_THREAD_MULTIPLE)
    {
            cout<<"Error: The MPI library does not have full thread support\n";
            MPI_Abort(MPI_COMM_WORLD,1);
            exit(1);
    }

    //Get world_rank and world_size
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    
    //Data Structures for each fragment
    json fragment;
    map < pair <int,int>, list<uint32_t> > dgraph;
    map <uint32_t, json> nodes;
    map <uint32_t, list< uint32_t > >edges;
 
    //Buffer 'buf' to store the query distributed by the co-ordinator ROOT.
    char *buf;
    int filesize;

    //Define MPI structure for struct x_uv
    const int nitems = 2;
    int block_lengths[2] = {1,1};
    MPI_Datatype types[2] = {MPI_INT, MPI_UINT32_T};
    MPI_Datatype mpi_x_uv;
    
    MPI_Aint offsets[2];
    offsets[0] = offsetof(x_uv,u);
    offsets[1] = offsetof(x_uv,v);
        
    MPI_Type_create_struct(nitems,block_lengths,offsets,types,&mpi_x_uv);
    MPI_Type_commit(&mpi_x_uv);


    //If world_rank in not ROOT, read the corresponding fragment into the json.
    //Construct the edges, nodes and the dependency graph.
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
    //If world_rank is ROOT, read the file containing the query and store it in buf.
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

    //Broadcast the filesize and query buf to all the processes. 
    MPI_Bcast(&filesize,1,MPI_INT,ROOT, MPI_COMM_WORLD);
    if(world_rank != ROOT)
        buf = (char *) malloc((filesize+1)*sizeof(char));
    MPI_Bcast(buf,filesize+1,MPI_CHAR,ROOT,MPI_COMM_WORLD);

    //Parse the contents of buf into a json and compute the out degree of all the query nodes.
    json query = json::parse(buf);
    free(buf);
    fill_out_degree(query);


    if(world_rank != ROOT)
    {   
        iEval(query,nodes,edges);
        recv_th = new thread(recv_l_uv,world_rank,mpi_x_uv);
        send_l_uv(world_rank,nodes,dgraph,mpi_x_uv);
        sleep(10);
        update_false(nodes);
        ship_partial_result(nodes,query, mpi_x_uv);
        
    }
    else
    {
        union_results(query,mpi_x_uv);
    }
    MPI_Finalize();

    if(world_rank != ROOT)
    {
    try
    {
        recv_th->detach();
        delete recv_th;
    }
     catch (exception& e)
    {
        cout << "Standard exception: " << e.what();
        terminate();
    }

    }
}
