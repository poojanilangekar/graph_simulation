//Dynamic edge and vertex data.

#define DYNAMICEDATA 1
#define DYNAMICVERTEXDATA 1

#include <string>
#include <set>
#include <map>
#include <vector>
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


        //For iteration 0
        if (gcontext.iteration == 0) {
            
           
            chivector<vid_t> * v_vector = vertex.get_vector();
            v_vector->clear();
            
            /* Initialize a json object, rvec to maintain the result vector of the current node.
             * Maintain the list of outedges of the current vector. 
             * These outedges are the children and must match the children of the query node.
             * Maintain a vector for all the nodes set to false in the current iteraion. 
             */
            nlohmann::json rvec;
            std::vector<vid_t> vertex_outedges;
            std::vector<vid_t> vertex_false;
            
            for (int i = 0; i < vertex.num_outedges(); i++) {
                vertex_outedges.push_back(vertex.outedge(i)->vertex_id());
            }
            
            /*
             * Iterate through all the query nodes, and check the equality with the current node. 
             * If the current node matches the query node, add the dependencies of the query node to the rvec.
             * If the query node does not have any dependencies, set rvec[i] as true. (This implies a direct match)
             * If the query node and the current node don't match, set rvec[i] to false and add i to vertex_false.
             */
            for(unsigned int i=0; i < query_json["node"].size(); i++) {
                if(check_equal(vertex_json[vertex.id()],query_json["node"][i])) {    
                    unsigned int out_d = query_json["node"][i]["out_degree"];
                    if(out_d == 0){
                        rvec[i] = true;
                        v_vector->add(i);
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
                        v_vector->add(i);
                    }
                    
                }
                else
                {
                    rvec[i] = false;
                    vertex_false.push_back(i);
                }
            }
            
            // Access the element of rvec_map with key equal to current vertex id. 
            // Assign rvec as the value for current vertex id.
            tbb::concurrent_hash_map<unsigned int, nlohmann::json>::accessor ac;
            rvec_map.insert(ac,vertex.id());
            ac->second = rvec;
            ac.release();
            
            /*
             * If the size of vertex_false is not zero, changes have been made in the current iteration.
             * For all the inedges, add the all the elements of vertex_false to the edge vector.
             * Schedule all the inedges for the next iteration.
             * 
             */
            
            if(vertex_false.size() != 0) {
                for(int i = 0; i < vertex.num_inedges(); i++) {
                    chivector<vid_t> * e_vector = vertex.inedge(i)->get_vector();
                    e_vector->clear();
                    for(unsigned int j = 0; j< vertex_false.size(); j++){
                        e_vector->add(vertex_false[j]);
                    }
                    gcontext.scheduler->add_task(vertex.inedge(i)->vertex_id());
                }
            }    

        } 
        
        //For iteration 1..n
        else{
            
            /*
             * Retrieve the rvec for the current node from the rvec_map. 
             * Intialize vertex_false to maintain the vertices set to false.  
             */
            tbb::concurrent_hash_map<unsigned int, nlohmann::json>::accessor ac;
            rvec_map.find(ac,vertex.id());
            nlohmann::json rvec = ac->second;
            std::vector<vid_t> vertex_false;
            
            chivector<vid_t> * v_vector = vertex.get_vector();
            
                    
            
            /*
             * Iterate through all the outedges.
             * Iterate through each edge vector (e_vector).
             * Get the target value, t from each element in the edge vector.
             * If rvec[k] isn't a bool and rvec[k] has dependencies on t, remove j from rvec[k][t].
             * If rvec[k][t] is now empty, set rvec[k] = false and add it to vertex_false.
             * Else update rvec[k][t].
             */
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
            
            /*
             * If the size of vertex_false is not zero, changes have been made in the current iteration.
             * Update the vertex vector.
             * For all the inedges, add the all the elements of vertex_false to the edge vector.
             * Schedule all the inedges for the next iteration.
             */
           
            
            
            if(vertex_false.size() != 0){
            //Update the new match nodes for the vertex by adding only the current matches to the vertex vector.
                v_vector->clear();
                for(unsigned int i = 0; i < rvec.size(); i++){
                    if(!rvec[i].is_boolean()) 
                        v_vector->add(i);
                    else if(rvec[i])
                        v_vector->add(i);
                }
                for(int i=0; i < vertex.num_inedges(); i++){
                   chivector<vid_t> * e_vector = vertex.inedge(i) -> get_vector();
                    for(unsigned int j = 0; j < vertex_false.size(); j++)
                        e_vector->add(vertex_false[j]);
                    gcontext.scheduler->add_task(vertex.inedge(i)->vertex_id());
                }
            }
            ac -> second = rvec;
            ac.release();
        }

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
    
    
    metrics m("graph_simulation");
    
    
    std::string filename = get_option_string("file");  // Base filename
    bool scheduler       = true; // Always enable scheduling.
    
    //Shard creation.
    int nshards          = convert_if_notexists<vid_t>(filename, get_option_string("nshards", "auto"));
    
    /** 
     * Parse the vertexfile and the queryfile.
     * The vertex file contains the label data of the input graph.
     * The queryfile contains the query pattern. It includes the label data and the structure of the query.
    **/
    std::string vertexfile = get_option_string("vertexfile");
    std::string queryfile = get_option_string("queryfile");
    
    
    std::ifstream vfile(vertexfile);
    std::stringstream vss;
    vss << vfile.rdbuf();
    vertex_json = nlohmann::json::parse(vss);
    
    std::ifstream qfile(queryfile);
    std::stringstream qss;
    qss << qfile.rdbuf();
    query_json = nlohmann::json::parse(qss);
    fill_out_degree(query_json);

    int niters = query_json["edge"].size(); //For inmemorymode, The number of iterations is determined by the number of edges. (Worst Case)
    
    //Initailize the graph simulation program. 
    GraphSimulation program;
    graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m); 
    //std::vector<std::pair<vid_t, vid_t> >  intervals = engine.get_intervals(); 
    engine.run(program, niters);

    /* After completion of the graphchi program, print the results to the output file.
     * Construct a map result which contains a mapping between each node in the query pattern and the possible matches from the input graph.
     * If the size of the query pattern is equal to the size of the result match, then query is found. Print the match.
     * Else the data graph does not match the query ant print 'No Match'. 
     */
    std::string outputfile = get_option_string("outputfile",filename+"_output.txt");
    std::map<vid_t, std::set<vid_t> > result;
    for (tbb::concurrent_hash_map<vid_t, nlohmann::json>::const_iterator it = rvec_map.begin(); it != rvec_map.end() ; ++it) {
        nlohmann::json rvec = it->second;
        for(unsigned int i = 0; i < rvec.size(); i++){
            if(!rvec[i].is_boolean())
                result[i].insert(it->first);
            else if(rvec[i])
                result[i].insert(it->first);
        }
   }
  
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
    metrics_report(m);
    
    return 0;
}