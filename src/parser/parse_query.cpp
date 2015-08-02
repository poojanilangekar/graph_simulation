#include <iostream>
#include <string>
#include <fstream>
#include <streambuf>
#include <thread>
#include <map>
#include <list>
#include <utility>

#include <json.hpp>

using namespace std;
using namespace nlohmann;

json j_query,j_nodes;
map < size_t, map < size_t, int> > distmat;
map < size_t, map < size_t, int> > qfe;

map <size_t, map < pair < size_t, size_t >, list <size_t> > > anc;
map <size_t, map < pair < size_t, size_t >, list <size_t> > > desc; 

void fill_distmat(string filename)
{
	ifstream f(filename);
	string line;
        while(getline(f, line))
        {
		size_t source,dest;
		int dist;
                stringstream(line)>>source>>dest>>dist;
               	distmat[source][dest] = dist;
        }
        f.close();

}

void add_to_anc(size_t candidate, size_t source, size_t target)
{
	int limit = qfe[source][target];
	pair <size_t, size_t> edge (source, target);
	for(map <size_t, int>::iterator it = distmat[candidate].begin(); it !=  distmat[candidate].end();++it)
	{
		if((it->first != candidate) && (it->second <= limit))
		{		
			anc[it->first][edge].push_back(candidate);	
		}
	}

}

void add_to_desc(size_t candidate, size_t source, size_t target)
{
	int limit = qfe[source][target];
	pair <size_t, size_t> edge (source,target);
	for(map< size_t, map < size_t, int> >::iterator it = distmat.begin(); it != distmat.end(); ++it)
	{	
		if(candidate != it->first)
		{
			size_t current = it -> first;
			map <size_t, int>::iterator cit = (it->second).find(candidate);
			if(cit != (it->second).end())
			{
				if(cit->second <= limit)
					desc[current][edge].push_back(candidate);	
			}
		}	
 
	}
}


void compute_anc_desc()
{

	for(size_t i = 0; i < j_query["edge"].size();i++)
	{
		size_t snode = j_query["edge"][i]["source"], tnode = j_query["edge"][i]["target"];
		for(size_t j = 0; j < j_nodes.size(); j++)
		{
			if((!j_nodes[j].is_null()) && (j_nodes[j]["source"] == j_query["node"][snode]["source"]))
				add_to_anc(j,snode,tnode);
			if((!j_nodes[j].is_null()) && (j_nodes[j]["source"] == j_query["node"][tnode]["source"]))
				add_to_desc(j,snode,tnode);
		}
	}

}

void parse_query_graph()
{
	for(size_t i=0; i<j_query["edge"].size();i++)
	{
		json qnode = j_query["edge"][i];
		qfe[qnode["source"]][qnode["target"]] = qnode["fe"];
	}
	compute_anc_desc(); 
	
}

int main(int argc, char* argv[])
{
	string qfile = argv[1], nfile = argv[2], efile = argv[3];
	thread readdistmat(fill_distmat,efile);
	ifstream j(qfile);
	stringstream jss;
	jss << j.rdbuf();
	j_query = json::parse(jss);
	j.close();
	j.open(nfile);
	stringstream nss;
	nss <<j.rdbuf();
	j_nodes = json::parse(nss);
	j.close();
	readdistmat.join();
	parse_query_graph();
}



