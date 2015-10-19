
#include <iostream>
#include <cstdio>
#include <vector>
#include <string>
#include <map>
#include <thread>
#include <sstream>
#include <graphlab.hpp>
#include <json.hpp>
#include <chrono>
#include <ctime>
#include <iosfwd>
#include <bits/stl_bvector.h>

/*
 * Vertex data type is string. It stores the serialized string of the json object for each vertex.
 * Edge data type is a vector of type size_t (Same data type as vertex id).
 */
typedef std::string vertex_data_type;
typedef std::vector<size_t> edge_data_type;

typedef graphlab::distributed_graph<vertex_data_type, edge_data_type> graph_type;

/*
 * vertex_json stores is used to load the label data from the file into the application and then save the vertex data for each vertex.
 * query_json contains the structure of the query graph along with the label data.
 */
nlohmann::json vertex_json, query_json;


/*
 * Function to fill the json object from the input file. 
 */
void fill_json(std::string filename , nlohmann::json &j) {
    std::ifstream jf(filename);
    std::stringstream jss;
    jss << jf.rdbuf();
    j = nlohmann::json::parse(jss);

}
/*
 * Used to fill the out degree of each vertex in the query graph.
 * The out degree of the query vertices is used by the engine for computation. 
 */
void fill_out_degree(nlohmann::json &query) {
    for (size_t i = 0; i < query["node"].size(); i++)
        query["node"][i]["out_degree"] = 0;
    for (size_t i = 0; i < query["edge"].size(); i++) {
        size_t snode = query["edge"][i]["source"];
        query["node"][snode]["out_degree"] = int(query["node"][snode]["out_degree"]) + 1;
    }

}

/*
 * The check_equal function determines if the labels of the datanode and the querynode are equal.
 * The function iterates through all the labels of the query node and compares its value to that of the datanode.
 * It returns true only if all the labels match; returns false otherwises.
 */
bool check_equal(nlohmann::json datanode, nlohmann::json querynode) {
    for (nlohmann::json::iterator it = querynode.begin(); it != querynode.end(); ++it) {
        if ((it.key() != "id") && (it.key() != "out_degree")) {
            if (datanode.find(it.key()) != datanode.end()) {
                if (datanode[it.key()].is_string()) {
                    std::string d = datanode[it.key()], q = it.value();
                    if (d.find(q) == std::string::npos)
                        return false;
                } else if (datanode[it.key()] != it.value())
                    return false;
            } else
                return false;
        }
    }
    return true;
}

/*
 * init_vertex function serializes the json object of each vertex and stores the resultant string as vertex data. 
 */
void init_vertex(graph_type::vertex_type& vertex) {
    if(vertex.data().empty())
    {    std::stringstream ss;
        ss << vertex_json[vertex.id()];
        vertex.data() = ss.str();
    }
}

/*
 * The gather type of the function is a map of type <size_t,int>
 * It contains a mapping between each query vertex id and the number of vertices set to false. 
 */
struct map_gather {
   
    std::map<size_t,int> false_map;
    
    map_gather& operator+=(const map_gather& other) {
        for(std::map<size_t,int>::const_iterator it = other.false_map.begin(); it != other.false_map.end(); ++it){
            size_t key = it->first;
            false_map[key] = false_map[key] + (it->second);
        }
        return *this;
    }
    
    
    void save(graphlab::oarchive& oarc) const {
        oarc << false_map;
    }
    
    void load(graphlab::iarchive& iarc){
        iarc >> false_map;
    }
};

/*
 * The graph simulation vertex program. It does not contain any data members, hence it derives from IS_POD_TYPE.
 * The direction of the gather function is the out edges of a vertex. 
 * The gather function reads the edge data and creates the corresponding false_map.
 * The apply function has two conditions, 
 * If the result vector (rvec) of the vertex is not initialized, it compares the vertex to each vertex in the query and populates the result vector.
 * Else, it iterates through the map of the gather function and updates all the corresponding vertices to false in the result vector.
 * The direction of the scatter function is the in edges of a vertex.
 * The scatter function copies the "updates" array of the vertex into the edge vector.  
 */

class graph_simulation: public graphlab::ivertex_program<graph_type, map_gather>, public graphlab::IS_POD_TYPE {
public:
    
    edge_dir_type gather_edges(icontext_type& context, const vertex_type &vertex) const {
        return graphlab::OUT_EDGES;
    }
    
    gather_type gather(icontext_type & context, const vertex_type& vertex, edge_type& edge) const {
        map_gather partial;
        for(edge_data_type::iterator it = edge.data().begin(); it != edge.data().end(); ++it)
            partial.false_map[*it] = 1;
        edge.data().clear();
        return partial;
    }
    
    void apply(icontext_type& context, vertex_type &vertex, const gather_type& total) {
       std::stringstream vss;
       vss << vertex.data(); 
        nlohmann::json vertex_data = nlohmann::json::parse(vss);
        vertex.data().clear();
        vertex_data.erase("updates");
        int dependencies;
        if(vertex_data["rvec"].is_null()){
            dependencies = 0;
            for(size_t i = 0; i < query_json["node"].size(); i++){
                if(check_equal(vertex_data,query_json["node"][i])){
                    int out_d = query_json["node"][i]["out_degree"];
                    if(out_d == 0)
                        vertex_data["rvec"][i] = true;
                    else{
                        if(vertex.num_out_edges() == 0){
                            vertex_data["rvec"][i] = false;
                            vertex_data["updates"].push_back(i);
                        } else {
                            for(size_t j = 0; j < query_json["edge"].size(); j++){
                                size_t s = query_json["edge"][j]["source"], t = query_json["edge"][j]["target"];
                                if(i == s){
                                    vertex_data["rvec"][i][t] = vertex.num_out_edges();
                                    dependencies++;
                                }
                            }
                        }
                        
                    }
                } else {
                    vertex_data["rvec"][i] = false;
                    vertex_data["updates"].push_back(i);
                }
            }
            vertex_data["dependencies"] = dependencies;
        }
        dependencies = vertex_data["dependencies"];
        if(dependencies != 0){
            std::map<size_t,int> result = total.false_map;
            for(std::map<size_t,int>::iterator it = result.begin(); it != result.end(); ++it) {
                size_t key = it->first;
                int value = it->second;
                for(size_t j = 0; j < vertex_data["rvec"].size(); j++) {
                    if(vertex_data["rvec"][j].is_boolean())
                        continue;
                    if(vertex_data["rvec"][j][key].is_number()) {
                        int prev_dep = vertex_data["rvec"][j][key];
                        if(prev_dep <= value){
                            vertex_data["updates"].push_back(j);
                            vertex_data["rvec"][j] = false;
                            dependencies--;
                        }
                        else
                            vertex_data["rvec"][j][key] = prev_dep - value;
                    }
                    
                }
            }
            vertex_data["dependencies"] = dependencies;
        }
        std::stringstream ss;
        ss<<vertex_data;
        vertex.data() = ss.str();
    }
    
    edge_dir_type scatter_edges(icontext_type& context, const vertex_type& vertex) const {
        return graphlab::IN_EDGES;
    }
    
    void scatter(icontext_type& context, const vertex_type& vertex, edge_type& edge) const {
        std::stringstream vss;
        vss << vertex.data();
        nlohmann::json vertex_data = nlohmann::json::parse(vss);
        if(vertex_data["updates"].is_array()) 
        {
            std::vector<size_t> edge_data(vertex_data["updates"].begin(),vertex_data["updates"].end());
            edge.data() = edge_data;
            context.signal(edge.source());
        }
        
    }
    
    
};

/*
 * vertex_writer is the class to store the result of the vertices to the files.
 * It iterates through the result vectors of each vertex and if a match is found, prints it to the result file.
 */
class vertex_writer {
    
public:
    std::string save_vertex(graph_type::vertex_type v){
        std::stringstream vss;
        vss<<v.data();
        nlohmann::json vertex_data = nlohmann::json::parse(vss);
        std::stringstream ss;
        for(size_t i = 0; i < vertex_data["rvec"].size(); i++){
            if(vertex_data["rvec"][i].is_boolean()){
                if(vertex_data["rvec"][i])
                    ss<<v.id()<<"\t"<<i<<"\n";
            }
            else
                ss<<v.id()<<"\t"<<i<<"\n";
        }
        return ss.str();
    }
    std::string save_edge(graph_type::edge_type e) {
        return "";
    }
};


int main(int argc, char** argv) {
  
  auto t_start = std::chrono::high_resolution_clock::now(); 
  
  graphlab::mpi_tools::init(argc, argv); //Initialize MPI for graphlab program. 
  
  /*
   * Intialization steps including setting up distributed control and parsing the command line options. 
   */
  graphlab::distributed_control dc;
  global_logger().set_log_level(LOG_INFO);
  
  graphlab::command_line_options clopts("Graph Simulation.");
  
  graphlab::launch_metric_server();
 
  std::string edge_file,vertex_file,query_file;
  std::string format;
  std::string exec_type = "asynchronous";
  std::string saveprefix;
  
  clopts.attach_option("edge_file",edge_file,"The graph edge file. Required.");
  clopts.add_positional("edge_file");
  
  clopts.attach_option("vertex_file",vertex_file,"The graph vertex file in json format. Required.");
  clopts.add_positional("vertex_file");
  
  clopts.attach_option("query_file",query_file,"The query file in the json format. Required.");
  clopts.add_positional("query_file");
  
  clopts.attach_option("format",format, "The edge file format.");
  clopts.attach_option("engine",exec_type,"The engine type. Set to synchronous or asynchronus.");
  clopts.attach_option("saveprefix",saveprefix,"If set, the result of the graph simulation will be saved to a sequence of files with prefix saveprefix.");
  
  if(!clopts.parse(argc,argv)) {
      dc.cout() << "Error in parsing command line arguments. \n";
      clopts.print_description();
      return 1;
  }
  
  if(edge_file == "") {
      dc.cout() << "The edge file is not specified. Cannot continue.\n";
      return 1;
  }
  
  if(vertex_file == "") {
      dc.cout() << "The vertex file is not specified. Cannot continue.\n";
      return 1;
  }
  
  if(query_file == "") {
      dc.cout() << "The query file is not specified. Cannot continue.\n";
      return 1;
  }
  
  graph_type graph(dc, clopts);
  
  
  /*
   * Load the query and vertex data into the application. 
   * Load the edge data of the graph into the engine and finalize the graph.
   * Initialize the vertex data with the label data from vertex_json.
   */
  std::thread thread_fill_query(&fill_json,query_file,std::ref(query_json));
  std::thread thread_fill_vertex(&fill_json,vertex_file,std::ref(vertex_json));  
  
  dc.cout() << "Loading graph in format: " << format << std::endl;
  graph.load_format(edge_file,format);
  
  thread_fill_query.join();
  fill_out_degree(query_json);
 
  graph.finalize();
  
  thread_fill_vertex.join();
  graph.transform_vertices(init_vertex);
 
  vertex_json.clear();
  
  /*
   * Set up the omni engine and signal all the vertices.
   * Start the engine.
   */
  graphlab::omni_engine<graph_simulation> omni_engine(dc,graph,exec_type,clopts);
  omni_engine.signal_all();
  omni_engine.start();
  
  /*
   * If the saveprefix is set, save the result using vertex_writer. 
   */
  if(saveprefix != "") {
    graph.save(saveprefix,vertex_writer(),false,true,false,1);
    /*
    * Send the matches to the co-ordinator node. 
    */
    size_t rank, size;
    rank = graphlab::mpi_tools::rank();
    size = graphlab::mpi_tools::size();
    
    std::string outputfile = saveprefix+"."+std::to_string(rank+1)+"_of_"+std::to_string(size);
    
    std::ifstream of(outputfile);
    assert(of != NULL);
   
    std::map<size_t,std::vector<size_t> > result;
    std::string line;
    while(getline(of,line)){
        size_t data_id, query_id;
        std::stringstream(line)>>data_id>>query_id;
        result[query_id].push_back(data_id);
    }
    of.close();
    if(rank != 0) {
        for(int i = 0; i <query_json["node"].size();i++){
            int send_size = result[i].size();
            graphlab::mpi_tools::send(send_size,0,i);
            std::cout<<"Send size from rank "<<rank<<" for node "<<i<<" is "<<send_size<<"\n";
            if(send_size != 0 ) {
                std::vector<size_t> send_vector(result[i].begin(),result[i].end());
                graphlab::mpi_tools::send(send_vector,0,i);
            }
        }
    }
    else {
        for(int i = 0; i < query_json["node"].size();i++) {
            for(size_t j = 1; j < size; j++) {
                int recv_size;
                graphlab::mpi_tools::recv(recv_size,j,i);
                std::cout<<"Receive size from rank "<<j<<" for node "<<i<<" is "<<recv_size<<"\n";
                if(recv_size != 0) {
                    std::vector<size_t> recv_vector(recv_size);
                    graphlab::mpi_tools::recv(recv_vector,j,i);
                    result[i].insert(result[i].end(),recv_vector.begin(), recv_vector.end());
                }
            }
        }
        std::string finaloutput = saveprefix+".final";
        std::ofstream fo(finaloutput);
        assert(fo != NULL);
        if(result.size() != query_json["node"].size()) {
            fo<<"No Match!\n";
        } else { 
            for(std::map<size_t, std::vector<size_t> >::iterator i = result.begin(); i != result.end(); ++i) {
                fo<<i->first<<"\n";
                for(std::vector<size_t>::iterator j = (i->second).begin(); j != (i->second).end(); ++j)
                    fo<<*j<<"\t";
                fo<<"\n";
            }
        }
        fo.close();
    }
  
  }
  
  /*
   * Stop the metrics server and calculate the runtime.
   * Call mpi_finalize.
   */
  graphlab::stop_metric_server();
  auto t_end = std::chrono::high_resolution_clock::now();
  dc.cout() << "Runtime: "<< std::chrono::duration<double, std::milli>(t_end-t_start).count()<<" ms\n";
  graphlab::mpi_tools::finalize();
  return 0;

}
