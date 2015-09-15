//Dynamic edge and vertex data.

#define DYNAMICEDATA 1
#define DYNAMICVERTEXDATA 1

#include <mpi.h>
#include <assert.h>
#include <iostream>
#include <string>
#include <set>
#include <map>
#include <vector>
#include <list>
#include <algorithm>
#include "tbb/concurrent_hash_map.h" 
#include <fstream>
#include <streambuf>


#include <json.hpp>

#include "graphchi_basic_includes.hpp"
#include "api/dynamicdata/chivector.hpp"

using namespace graphchi;

//Vertex Data and Edge data are of type chivector<vid_t>
typedef chivector<vid_t> VertexDataType;
typedef chivector<vid_t> EdgeDataType;

/* 
 * vertex_json contains label data of the input graph.
 * query_json contains the labels and the structure of the query graph.
 * rvec_map is a mapping between each node and its corresponding result vectors. 
 */
nlohmann::json vertex_json, query_json;
tbb::concurrent_hash_map<unsigned int, nlohmann::json> rvec_map;

//Structure to send the result pairs to master
typedef struct x_uv{
    vid_t u;
    vid_t v;
    
    x_uv(vid_t i, vid_t j)
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


struct GraphSimulation : public GraphChiProgram<VertexDataType, EdgeDataType> {
    

/*
 * The check_equal function determines if the labels of the datanode and the querynode are equal.
 * The function iterates through all the labels of the query node and compares its value to that of the datanode.
 * It returns true only if all the labels match; returns false otherwises.
 */
     bool check_equal(nlohmann::json datanode, nlohmann::json querynode) {
        for (nlohmann::json::iterator it = querynode.begin(); it != querynode.end(); ++it) {
            if ((it.key() != "id") && (it.key() != "out_degree")) {
                if (datanode.find(it.key()) != datanode.end()) {
                    if(datanode[it.key()].is_string())
                    {
                        std::string d = datanode[it.key()], q= it.value();
                        if(d.find(q)== std::string::npos)
                            return false;
                    }
                    else if (datanode[it.key()] != it.value())
                        return false;
                } else
                    return false;
            }
        }
        return true;
    }
    
    
    void update(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, graphchi_context &gcontext) {
            
        /*
         * Concurrent accessor to access the rvec value corresponding to the current vertex.
         */
        
        
        tbb::concurrent_hash_map<unsigned int, nlohmann::json>::accessor ac;
        rvec_map.insert(ac,vertex.id());
        nlohmann::json rvec = ac->second;
        
        /*
         * chivector v_vector to store the matches of the current vertex.
         * vertex_false to keep track of all the query vertices marked false for the vertex in the current iteration
         * vertex_outedges to keep track of the outedges of the current vertex
         */
        chivector<vid_t> * v_vector = vertex.get_vector();
        v_vector->clear();

        std::vector<vid_t> vertex_outedges;
        std::vector<vid_t> vertex_false;

        for (int i = 0; i < vertex.num_outedges(); i++) {
            vertex_outedges.push_back(vertex.outedge(i)->vertex_id());
        }

        /*
         * If the vertex has a null rvec, it is being computed for the first time. 
         * Compare the vertex with each of the vertices in the query graph.
         * If the current node matches the query node, add the dependencies of the query node to the rvec.
         * If the query node does not have any dependencies, set rvec[i] as true. (This implies a direct match)
         * If the query node and the current node don't match, set rvec[i] to false and add i to vertex_false. 
         */
        
        if(rvec.is_null()){
            bool has_dependencies = false;
            for(unsigned int i=0; i < query_json["node"].size(); i++) {
                if(check_equal(vertex_json[vertex.id()],query_json["node"][i])) {    
                    unsigned int out_d = query_json["node"][i]["out_degree"];
                    if(out_d == 0){
                        rvec[i] = true;
                    }
                    else if(vertex_outedges.size() == 0)
                    {
                        rvec[i] = false;
                        vertex_false.push_back(i);
                    }
                    else
                    {    for(unsigned int j=0; j <query_json["edge"].size(); j++){
                            unsigned int source = query_json["edge"][j]["source"], target = query_json["edge"][j]["target"];
                            if(i == source )
                                rvec[i][target] = vertex_outedges;
                        }
                    has_dependencies = true;
                    }

                }
                else
                {
                    rvec[i] = false;
                    vertex_false.push_back(i);
                }
            }
            /*
             * If the vertex has dependencies, schedule the outedges.
             */
            if(has_dependencies){
                for(int i = 0; i <vertex.num_outedges();i++)
                    gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id());
            }
        } 
        for(int i = 0; i < vertex.num_outedges(); i++){
            chivector<vid_t> * e_vector = vertex.outedge(i)->get_vector();
            if(e_vector->size() != 0 ) {
                for(unsigned int j =0; j < e_vector->size(); j++){
                    vid_t t = e_vector->get(j);
                    for(unsigned int k = 0; k < rvec.size(); k++ ) {

                        if(rvec[k].is_boolean())
                            continue;
                        if(rvec[k][t].is_null())
                            continue;

                        std::vector <vid_t> desc(rvec[k][t].begin(),rvec[k][t].end());

                        std::vector <vid_t>::iterator it = std::find(desc.begin(), desc.end(), vertex.outedge(i)->vertex_id());
                        if(it != desc.end())
                        {
                            desc.erase(it);
                            if(desc.size() == 0){
                                rvec[k] = false;
                                vertex_false.push_back(k);
                            }
                            else
                                rvec[k][t] = desc;
                        }                                
                    }
                }
            }
            e_vector->clear();
        }


        v_vector->clear();
        for(unsigned int i = 0; i < rvec.size(); i++){
            if(!rvec[i].is_boolean()) 
                v_vector->add(i);
            else if(rvec[i])
                v_vector->add(i);
        }    

        if(vertex_false.size() != 0){
            for(int i=0; i < vertex.num_inedges(); i++){
                chivector<vid_t> * e_vector = vertex.inedge(i) -> get_vector();
                 for(unsigned int j = 0; j < vertex_false.size(); j++)
                     e_vector->add(vertex_false[j]);
                 gcontext.scheduler->add_task(vertex.inedge(i)->vertex_id());
            }
        }
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
    }
    
    /**
     * Called after an execution interval has finished.
     */
    void after_exec_interval(vid_t window_st, vid_t window_en, graphchi_context &gcontext) {        
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

int main(int argc, const char ** argv) {
    
    graphchi_init(argc, argv);
    
    double start_time, end_time;
    
    MPI_Init(NULL,NULL);
    
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    
    //Define MPI structure for struct x_uv
    const int nitems = 2;
    int block_lengths[2] = {1,1};
    MPI_Datatype types[2] = {MPI_UINT32_T, MPI_UINT32_T};
    MPI_Datatype mpi_x_uv;
    
    MPI_Aint offsets[2];
    offsets[0] = offsetof(x_uv,u);
    offsets[1] = offsetof(x_uv,v);
        
    MPI_Type_create_struct(nitems,block_lengths,offsets,types,&mpi_x_uv);
    MPI_Type_commit(&mpi_x_uv);

    
    if(world_rank == 0)
        start_time = MPI_Wtime();

    std::string queryfile = get_option_string("queryfile");
    std::ifstream qfile(queryfile);
    std::stringstream qss;
    qss << qfile.rdbuf();
    query_json = nlohmann::json::parse(qss);
    
    std::string filename = get_option_string("file");  // Base filename
        
    if(world_rank != 0)
    {

        FILE* f = freopen(std::string("LOG_"+std::to_string(world_rank)).c_str(),"w",stdout);
        dup2(fileno(stdout), fileno(stderr));


        metrics m("graph_simulation");


        bool scheduler       = true; // Always enable scheduling.

        //Shard creation.
        assert((world_size-1) >= 1);
        int nshards          = convert_if_notexists<vid_t>(filename, std::to_string(world_size-1));
        int start_interval = world_rank;
        /** 
         * Parse the vertexfile and the queryfile.
         * The vertex file contains the label data of the input graph.
         * The queryfile contains the query pattern. It includes the label data and the structure of the query.
        **/
        std::string vertexfile = get_option_string("vertexfile");

        std::ifstream vfile(vertexfile);
        std::stringstream vss;
        vss << vfile.rdbuf();
        vertex_json = nlohmann::json::parse(vss);

        fill_out_degree(query_json);

        int niters = query_json["edge"].size(); //For inmemorymode, The number of iterations is determined by the number of edges. (Worst Case)

        //Initailize the graph simulation program. 
        GraphSimulation program;
        graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m); 
        
        assert((world_rank-1) >= 0);
        
        if(world_size > 2)
            engine.run(program,niters,world_rank-1);
        else
            engine.run(program,niters);
        
        
        /* After completion of the graphchi program, ship the results to the master. 
         * Construct a map result which contains a mapping between each node in the query pattern and the possible matches from the input graph.
         * If the size of the query pattern is equal to the size of the result match, then query is found. Ship the result in an array of structure x_uv
         * Else the data graph does not match the query. Ship an empty message to the master.
         */
        //std::string outputfile = "output_"+std::to_string(world_rank)+".txt";
        std::map<vid_t, std::set<vid_t> > result;
        size_t result_size = 0;
        for (tbb::concurrent_hash_map<vid_t, nlohmann::json>::const_iterator it = rvec_map.begin(); it != rvec_map.end() ; ++it) {
            nlohmann::json rvec = it->second;
            for(unsigned int i = 0; i < rvec.size(); i++){
                if(!rvec[i].is_boolean()){
                    result[i].insert(it->first);
                    result_size++;
                }else if(rvec[i]){
                    result[i].insert(it->first);
                    result_size++;
                }
            }
       }
        if(result.size() != query_json["node"].size())
        {
            x_uv *p_array = NULL;
            MPI_Send(p_array,0,mpi_x_uv,0,0,MPI_COMM_WORLD);
        }
        else {
            x_uv *p_array=new x_uv[result_size];
            result_size  = 0;
            for(std::map<vid_t, std::set<vid_t> >::iterator it = result.begin(); it != result.end(); ++it ) {
                vid_t u = it->first;
                for(std::set<vid_t>::iterator s = (it->second).begin(); s != (it->second).end();++s){
                    p_array[result_size++] = x_uv(u,*s);
                } 
            }
            MPI_Send(p_array,result_size,mpi_x_uv,0,0,MPI_COMM_WORLD);
            delete [] p_array;
        }
        metrics_report(m);
        
    }
   
    if(world_rank == 0){
        /*
         * At the master, receive the messages from all the other processes.
         * If the size of the result is equal to the size of the query patter, print the corresponding matches to the output file.
         * Else print 'No match!' to indicate that the graph did not match the query.
         * Display the total runtime. 
         */
        
        int ship_size = 0;
        std::map<vid_t, std::set <vid_t> > result;
        for(int i=1; i < world_size;i++){
            MPI_Status status;
            MPI_Probe(MPI_ANY_SOURCE, 0,MPI_COMM_WORLD,&status);
            int count;  
            MPI_Get_count(&status,mpi_x_uv,&count);
            if(status.MPI_SOURCE > 0)
            {         
                ship_size = ship_size + count;
                x_uv *l = new x_uv[count];
                MPI_Recv(l,count,mpi_x_uv,status.MPI_SOURCE,0,MPI_COMM_WORLD,&status);
                for(int j = 0; j <count; j++)
                {
                    result[l[j].u].insert(l[j].v);
                }
                delete[] l;
            }
            else
                i--;  
        }
        
        std::string outputfile = get_option_string("outputfile",filename+"_output.txt");
        std::ofstream output(outputfile);
        if(result.size() != query_json["node"].size())
            output<<"No match";
        else
        {
            for(std::map <vid_t, std::set<vid_t> >::iterator it = result.begin(); it != result.end(); ++it)
            {
                output<<it->first<<"\n";
                for(std::set<vid_t>::iterator i = (it->second).begin(); i != (it->second).end(); ++i)
                    output<<*i<<"\t";
                output<<"\n";
            }
        }
        output.close();
        
        end_time = MPI_Wtime();
        std::cout<<"Runtime: "<<(end_time-start_time)<<"sec\n";
        std::cout<<"Data Shipped: "<<sizeof(x_uv)*ship_size<<"B\n";
    }
    MPI_Finalize();
    return 0;
}
