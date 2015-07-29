#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <algorithm>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/graphviz.hpp>
#include <boost/graph/johnson_all_pairs_shortest.hpp>
#include<time.h>
using namespace std;
using namespace boost;
int main(int argc, char ** argv)
{
	clock_t t=clock();
	fstream fin (argv[1]);
	string line;
  	typedef adjacency_list<listS, vecS, directedS, no_property, property< edge_weight_t, int> > graph_t;
	graph_t mygraph;
	while(getline(fin,line))
	{
		//line.erase(line.begin(), find_if(line.begin(), line.end(), not1(ptr_fun<int, int>(isspace))));
		if(line[0] == '#')
			continue;
		int source, dest;
		stringstream(line)>>source>>dest;
		get(edge_weight, mygraph)[add_edge(source,dest,mygraph).first] = 1;
	}
	fin.close();
	vector< vector <int> > D(num_vertices(mygraph), vector <int>(num_vertices(mygraph)));
	johnson_all_pairs_shortest_paths(mygraph,D);
	cout<<((float)(clock()-t))/CLOCKS_PER_SEC;
	return 0;
	
}
		
