#include <iostream>
#include <string>
#include <fstream>
#include <streambuf>
#include <thread>
#include <map>

#include <json.hpp>

using namespace std;
using namespace nlohmann;

json j_query,j_nodes;
map < size_t, map < size_t, int> > distmat;
map < size_t, map < size_t, int> > qfe;
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


void parse_query_graph()
{
	for(size_t i=0; i<j_query["edge"].size();i++)
	{
		json qnode = j_query["edge"][i];
		qfe[qnode["source"]][qnode["target"]] = qnode["fe"];
	} 
	//compute_anc();

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



