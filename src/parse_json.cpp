#include <json.hpp>

#include <iostream>
#include <string>
#include <fstream>
#include <streambuf>
#include <map>


using namespace nlohmann;
using namespace std;

bool DIRECTED;
json nodes;
map<size_t, map<size_t, float> > edges;


void set_directed(json j_obj)
{
	//Set whether directed or undirected
	DIRECTED = (j_obj.begin().value() == 1)?true:false;
}

bool is_directed()
{
	//Return whether directed or not
	return DIRECTED;
}

void create_node(json j_obj)
{
	json jnode;
	for(json::iterator it = j_obj.begin(); it != j_obj.end(); ++it)
	{
		json::iterator ac = it.value().begin(); //Accessor to access theelement
		jnode[ac.key()] = ac.value();
	}
	size_t id  = jnode["id"];
	jnode.erase(jnode.find("id"));
	nodes[id] = jnode;
}

void create_edge(json j_obj)
{
	json jedge;
	for(json::iterator it = j_obj.begin(); it != j_obj.end(); ++it)
	{	
		json::iterator ac = it.value().begin();
		jedge[ac.key()] = ac.value();
	}
	size_t source = jedge["source"], target = jedge["target"];
	jedge.erase(jedge.find("source"));
	jedge.erase(jedge.find("target"));
	if(jedge.size() == 0)
	{	
		edges[source][target] = 1;
		if(!is_directed())
		{
			edges[target][source] = 1;
		}
	}
	else
	{
		edges[source][target] = jedge["value"];
		if(!is_directed())
		{
			edges[target][source] = jedge["value"];
		}
	}	 

}


void json_iterate(json j_obj)
{
	//Print all available information about the input file.
	for(json::iterator it = j_obj.begin(); it != j_obj.end(); ++it)
	{		
		cout<<it.key()<<" => "<<it.value()<<"\n";
	}
}

void json_iterate_graph(json jgraph)
{
	for(size_t i= 0; i <jgraph.size(); i++)
	{
		
		if(jgraph[i].begin().key() == "node")
			create_node(jgraph[i].begin().value());
			//cout<<jgraph.at(i).begin().value()<<"\n";
		else if(jgraph[i].begin().key() == "edge")
			create_edge(jgraph[i].begin().value());
			//cout<<"edge\t"<<jgraph.at(i).begin().value()<<"\n";
		else if(jgraph[i].begin().key()  == "directed")
			set_directed(jgraph.at(i));
	}
}

int main(int argc , char* argv[])
{
    	ifstream j(argv[1]);
    	stringstream jss;
    	jss << j.rdbuf();
    	json j_file = json::parse(jss);
    	j.close();
	for(size_t i=0; i< j_file.size(); i++)
	{	if(j_file[i].begin().key() == "graph")
    			json_iterate_graph(j_file[i]["graph"]);
		else
			json_iterate(j_file[i]);
	}
	if(is_directed())
		cout<<"GRAPH is directed!\n";
	else
		cout<<"GRAPH is undirected!\n";
	cout<<"Number of nodes: "<<nodes.size();
	cout<<"Number of edges: "<<edges.size();
}

