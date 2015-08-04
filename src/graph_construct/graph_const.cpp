#include <iostream>
#include <time.h>
#include <fstream>
#include <sstream>
#include <string>
#include <map>
#include <vector>
#include <queue>
#include <utility>
#include <algorithm>
#include <thread>
#include <mutex>
#include <ThreadPool.hpp>

using namespace std;

map < int, vector<int> > adjlist; //Adjacency List to represent the graph.
ofstream dfile;
mutex shared; //Mutex to ensure safe access to the distance matrix

void sssp(int source) //Calculate the Shortest Path using BFS. 
{
	map <int, int> distmat;
	queue < pair<int, int> > q; //A queue to contain the nodes to be explored. 
	vector <int> visited; //A list of all visited nodes. 
	pair <int, int> distpair; //pair consisting of a vertex and the distance from the source.
	q.push(make_pair(source,0)); //Enqueue the source node with distance 0.
	visited.push_back(source);
	while(!q.empty()) //Until the queue is empty
	{
		int current, distance;
		distpair = q.front(); //Dequeue the first node and explore its neighbours.
		q.pop();
		current = distpair.first;
		distance = distpair.second;
		distmat[current] = distance; //Update the Distance Matrix.
		for(vector<int>::iterator it = adjlist[current].begin(); it != adjlist[current].end(); ++it)
		{
			if(find(visited.begin(), visited.end(),*it) == visited.end()) //For each neighbour, if the node is not visited, mark as visited and enqueue it with distance+1.
			{	
				q.push(make_pair(*it,distance + 1));
				visited.push_back(*it);
			}	
		}
	}
	shared.lock();
		for(map<int,int>::iterator it = distmat.begin(); it != distmat.end(); ++it)	
			dfile<<source<<" "<<it->first<<" "<<it->second<<"\n";
	shared.unlock();

}

int main(int argc, char* argv[])
{
	time_t start = time(0);
	//Read the input file given an argument.
	if ( argc != 2 )
	{
		cout<<"Usage: "<<argv[0]<<" <filename>";
		return 1;
	}
	string filename = argv[1];
	ifstream data_file(filename);
	if(!data_file.is_open()) 
	{
		cout<<argv[1]<<" could not be opened!";
		return 1;
	}
	//Parse the file line by line. The file is provided in edgelist format.
	string line;
	while(getline(data_file, line))
	{
		//Ignore comments
		if(line[0] == '#')
			continue;
		//Add each edge to the graph.
		int source, dest;
		stringstream(line)>>source>>dest;
		adjlist[source].push_back(dest);
		adjlist[dest];
	}
	data_file.close();
	//Construct the distance Matrix by filling in the single source shortest path (sssp) for each source node.
	string outputfile = filename + "_dmat.txt";
	dfile.open(outputfile, ofstream::out);
	int max_threads = thread::hardware_concurrency();
	ThreadPool *pool = new ThreadPool(max_threads); //ThreadPool to enqueue the tasks to a set of 'max_threads'.
	for(map<int, vector<int> >::const_iterator it = adjlist.begin(); it!= adjlist.end(); ++it)
	{
			pool->enqueue(sssp,it->first); //Create a task to call the sssp function for each vertex	

	}
	//Join all threads. (Ensure all the tasks are complete.
	delete pool;
	dfile.close();
	//Calculate the rutime of the program
	cout<<"Time: "<<difftime(time(0),start);
	exit(0);
}
