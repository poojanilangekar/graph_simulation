//Dynamic edges. 

#define DYNAMICEDATA 1

#include <mpi.h>
#include <assert.h>
#include <iostream>
#include <string>
#include <set>
#include <map>
#include <vector>
#include <algorithm>
#include "tbb/concurrent_hash_map.h" 
#include <fstream>
#include <streambuf>
#include <thread>
#include <utility>


#include <json.hpp>

#include "graphchi_basic_includes.hpp"
#include "api/dynamicdata/chivector.hpp"

using namespace graphchi;

//Vertex Data is of int and Edge data is of type chivector<vid_t>
typedef int VertexDataType;
typedef chivector<vid_t> EdgeDataType;

/* 
 * vertex_json contains label data of the input graph.
 * query_json contains the labels and the structure of the query graph.
 * rvec_map is a mapping between each node and its corresponding result vectors. 
 */
nlohmann::json query_json;
tbb::concurrent_hash_map<unsigned int, nlohmann::json> rvec_map;
std::map<vid_t,int> vertex_map;

struct GraphSimulation : public GraphChiProgram<VertexDataType, EdgeDataType> {
    

/*
 * The check_equal function determines if the labels of the datanode and the querynode are equal.
 * The function iterates through all the labels of the query node and compares its value to that of the datanode.
 * It returns true only if all the labels match; returns false otherwises.
 */
     bool check_equal(int datanode, nlohmann::json querynode) {
         int label = querynode["label"];
         if(label == datanode)
             return true;
         return false;
    }
     
     void load_label(std::string filename)
    {
        std::ifstream inf(filename);
        assert(inf.is_open());
        std::string line;
        vid_t vertex_id;
        int l;
        while(std::getline(inf,line)) {
            std::stringstream(line)>>vertex_id>>l;
            vertex_map[vertex_id] = l;
        }
        inf.close();
    }
    
    
    void update(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, graphchi_context &gcontext) {
            
        /*
         * Concurrent accessor to access the rvec value corresponding to the current vertex.
         */
         
        tbb::concurrent_hash_map<unsigned int, nlohmann::json>::accessor ac;
        rvec_map.insert(ac,vertex.id());
        nlohmann::json rvec = ac->second;
        
        int dependencies; //The number of active dependencies of the current vertex. 
        
        /*
         * vertex_false to keep track of all the query vertices marked false for the vertex in the current iteration
         */

        std::vector<vid_t> vertex_false;

        

        /*
         * If the vertex has a null rvec, it is being computed for the first time. 
         * Compare the vertex with each of the vertices in the query graph.
         * If the current node matches the query node, add the dependencies of the query node to the rvec.
         * If the query node does not have any dependencies, set rvec[i] as true. (This implies a direct match)
         * If the query node and the current node don't match, set rvec[i] to false and add i to vertex_false. 
         */
        
        if(rvec.is_null()){
           
            dependencies = 0; //Vertex is being computed for the first time and hence has zero dependencies. 
            
            for(unsigned int i=0; i < query_json["node"].size(); i++) {
                if(check_equal(vertex_map[vertex.id()],query_json["node"][i])) {    
                    unsigned int out_d = query_json["node"][i]["out_degree"];
                    if(out_d == 0){
                        rvec[i] = true;
                    }
                    else if(vertex.num_outedges() == 0)
                    {
                        rvec[i] = false;
                        vertex_false.push_back(i);
                    }
                    else
                    {   for(unsigned int j=0; j <query_json["edge"].size(); j++){
                            unsigned int source = query_json["edge"][j]["source"], target = query_json["edge"][j]["target"];
                            if(i == source )
                                rvec[i][target] = vertex.num_outedges();
                        }
                        dependencies++;
                    }

                }
                else
                {
                    rvec[i] = false;
                    vertex_false.push_back(i);
                }
            }
            /*
             * If the vertex has dependencies, schedule the children of the current vertex (outedges).
             */
            if(dependencies != 0){
                for(int i = 0; i <vertex.num_outedges();i++)
                    gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id());
            }
            
            /*
             * Vertex data is set to the number of dependencies.
             * If the vertex data is greater than 0, then it is processed whenever it is scheduled in the subsequent iterations.
             * If the vertex data is 0, it is not processed in the subsequent iterations.  
             */
            vertex.set_data(dependencies);
        } 
            
        dependencies = vertex.get_data();
        
        /*
         * If the current vertex has dependencies, it has to be processed.
         * Collect the edge data of all it's outgoing edges and for each outgoing edge which is updated, update the corresponding dependency.
         * Else, clear all the outedges. 
         */
        
        if(dependencies != 0 ) {
            
            nlohmann::json updates;
            
            for(int i = 0; i < vertex.num_outedges(); i++){
                chivector<vid_t> * e_vector = vertex.outedge(i)->get_vector();
                int evector_size = e_vector->size();
                for( int j =0; j < evector_size; j++){
                    vid_t t = e_vector->get(j);
                    if(updates[t].is_null())
                        updates[t] = 1;
                    else {
                        int n = updates[t];
                        updates[t] = n +1;
                    }
                }
                e_vector->clear();
            }
            
            for(vid_t i = 0; i < updates.size(); i++ ) {
                if(updates[i].is_null())
                    continue;
                int cur_updates = updates[i];
                for(size_t j = 0; j < rvec.size(); j++){
                    if(rvec[j].is_boolean())
                        continue;
                    if(rvec[j][i].is_number()){
                        int prev_dep = rvec[j][i];
                        if(prev_dep <= cur_updates) {
                            rvec[j] = false;
                            vertex_false.push_back(j);
                            dependencies --;
                        }
                        else
                            rvec[j][i] = prev_dep - cur_updates;
                    }
                }
            }
            vertex.set_data(dependencies);
        } else {
            for(int i = 0; i < vertex.num_outedges(); i++){
                chivector<vid_t> * e_vector = vertex.outedge(i)->get_vector();
                if(e_vector->size())
                    e_vector ->clear();
            }
        }
   
        /*
         * If a node has been set to false in the current iteration, propagate the update through all the inedges.
         */
        if(vertex_false.size() != 0){
            for(int i=0; i < vertex.num_inedges(); i++){
                chivector<vid_t> * e_vector = vertex.inedge(i) -> get_vector();
                 for(unsigned int j = 0; j < vertex_false.size(); j++)
                     e_vector->add(vertex_false[j]);
                 gcontext.scheduler->add_task(vertex.inedge(i)->vertex_id());
            }
        }
        
        // Update the result vector and release the accsessor. 
        ac->second = rvec;
        ac.release();

    }
    
    /**
     * Called before an iteration starts.
     */
    void before_iteration(int iteration, graphchi_context &gcontext) {
    }
    
    /**
     * Called after an iteration has finished.
     */
    void after_iteration(int iteration, graphchi_context &gcontext) {
      /*
       * If there were changes in the current iteration, then iterate once more.
       * If there were no changes, stop execution by setting current iteration to last iteration. 
       */
        if(gcontext.scheduler->num_tasks() > 0)
            gcontext.set_last_iteration(iteration+1);
        else
            gcontext.set_last_iteration(iteration);
    }
    
    /**
     * Called before an execution interval is started.
     */
    void before_exec_interval(vid_t window_st, vid_t window_en, graphchi_context &gcontext) {
        std::string filename = gcontext.filename+"_label_"+std::to_string(window_st)+"_"+std::to_string(window_en);
        load_label(filename);
    }
    
    /**
     * Called after an execution interval has finished.
     */
    void after_exec_interval(vid_t window_st, vid_t window_en, graphchi_context &gcontext) {
        vertex_map.clear();
    }
    
};

/*
 * Function to fill the outdegree of the query nodes. 
 */
void fill_out_degree(nlohmann::json &query)
{
    for( size_t i = 0; i < query["node"].size(); i++)
        query["node"][i]["out_degree"] = 0;
    for( size_t i = 0; i < query["edge"].size(); i++)
    {
        size_t snode = query["edge"][i]["source"];
        query["node"][snode]["out_degree"] = int(query["node"][snode]["out_degree"]) + 1;
    }
    
}

/*
 * Function to parse the vertexfile.
 * The label data of the input graph is stored in a json object. 
 */
void fill_label(std::string vfilename, std::string fileprefix,std::vector< std::pair <vid_t, vid_t> > intervals )
{
    std::ifstream vf(vfilename);
    assert(vf.is_open());
    std::string line;
    int in = 0, nintervals = intervals.size();
    vid_t start_in = intervals[in].first, end_in = intervals[in].second;
    std::ofstream inf(fileprefix+"_label_"+std::to_string(start_in)+"_"+std::to_string(end_in));
    assert(inf.is_open());
    while(std::getline(vf,line)) {
        vid_t vertex_id;
        int l;
        std::stringstream(line)>>vertex_id>>l;
        if(vertex_id > end_in) {
            inf.close();
            in++; 
            if(in == nintervals)
                break;
            start_in = intervals[in].first;
            end_in = intervals[in].second;
            inf.open(fileprefix+ "_label_"+std::to_string(start_in)+"_"+std::to_string(end_in));
            assert(inf.is_open());        
        }
        inf<<vertex_id<<" "<<l<<"\n";
       
    }
    vf.close();
    if(inf.is_open())
        inf.close();
}


int main(int argc, const char ** argv) {
    
    graphchi_init(argc, argv);
    
    int provided;    
    MPI_Init_thread(NULL,NULL, MPI_THREAD_FUNNELED, &provided);
    if(provided < MPI_THREAD_FUNNELED)
    {
        std::cout<<"Error: The MPI Library does not have support multithreaded processes.\n";
        exit(1);
    }
    
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    int pname_len;
    
    MPI_Get_processor_name(processor_name,&pname_len);
    logstream(LOG_INFO) << "Launched worker with id "<<world_rank<<" at node "<<processor_name<<"\n";

    /*
     * Redirect stdout and stderr to the log file identified by the rank of the worker process. 
     */
    FILE* f = freopen(std::string("LOG_"+std::to_string(world_rank)).c_str(),"w",stdout);
    assert(f != NULL);
    dup2(fileno(stdout), fileno(stderr));

    MPI_Comm master;
    MPI_Comm_get_parent(&master); //Communicator of Master, to ship results back to the master.
    
    std::string queryfile = get_option_string("queryfile"); //Query file
    std::ifstream qfile(queryfile);
    std::stringstream qss;
    qss << qfile.rdbuf();
    query_json = nlohmann::json::parse(qss);
    
    std::string filename = get_option_string("file");  // Base filename

    std::string vertexfile = get_option_string("vertexfile"); //Vertex file 

    metrics m("graph_simulation");


    bool scheduler       = true; // Always enable scheduling.
    
    //Shard creation.
    std::string file_type = get_option_string("filetype", std::string());
    int nshards          = convert_if_notexists<vid_t>(filename, std::to_string(world_size),file_type);
    
    /* 
     * Parse the queryfile.
     * The queryfile contains the query pattern. It includes the label data and the structure of the query.
     */
    fill_out_degree(query_json);
   
    /*
     * For inmemorymode, The number of iterations is determined by the number of edges times number of vertices. (Worst Case)
     */
    int niters = query_json["node"].size()*query_json["edge"].size(); 
    
    
    //Initailize the graph simulation program. 
    GraphSimulation program;
    graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m); 
    
    
    
    std::vector< std::pair<vid_t,vid_t> > intervals = engine.get_intervals();
    fill_label(vertexfile,filename, intervals);
    /*
     * Optimize the graphchi engine and run the graph_simulation program.
     */
    int n_threads = std::thread::hardware_concurrency();

    engine.set_exec_threads(n_threads);
    engine.set_load_threads(n_threads);
    int memory = std::ceil((float)(get_option_int("memory",1024) * get_option_float("alpha",0.75)));
    engine.set_membudget_mb(memory);

    assert(world_size > 1);
    engine.run(program,niters,world_rank); 


    /* After completion of the graphchi program, ship the results to the master. 
     * Construct a map result which contains a mapping between each node in the query pattern and the possible matches from the input graph.
     * If the size of the query pattern is equal to the size of the result match, then query is found. Ship the result arrays with the corresponding tags. 
     * Else the data graph does not match the query. Ship an empty message to the master.
     */
    
    
    std::map<vid_t, std::set<vid_t> > result;
    for (tbb::concurrent_hash_map<vid_t, nlohmann::json>::const_iterator it = rvec_map.begin(); it != rvec_map.end() ; ++it) {
        nlohmann::json rvec = it->second;
        for(unsigned int i = 0; i < rvec.size(); i++){
            if(!rvec[i].is_boolean()){
                result[i].insert(it->first);
            }else if(rvec[i]){
                result[i].insert(it->first);
            }
        }
   }
    if(result.size() != query_json["node"].size())
    {
        vid_t *p_array = NULL;
        for(size_t i = 0; i < query_json["node"].size(); i++)
            MPI_Send(p_array,0,MPI_UINT32_T,0,i,master);
    }
    else {

        for(size_t i = 0; i < query_json["node"].size();i++){
            vid_t *p_array = new vid_t[result[i].size()];
            std::copy(result[i].begin(),result[i].end(),p_array);
            MPI_Send(p_array,result[i].size(),MPI_UINT32_T,0,i,master);
            delete[] p_array;
        }
    }
    metrics_report(m);
    
    MPI_Finalize();
    return 0;
}

