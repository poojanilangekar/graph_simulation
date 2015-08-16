#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdlib>
#include <string>
#include <map>
#include <set>

using namespace std;

map < size_t, set <size_t> > graph;
bool izero = true;

int main(int argc, char * argv[])
{
    if(argc != 2)
    {
        cout<<"USAGE: "<<argv[0]<<" <edgelistfile> \n";
        exit(1);
    }
    ifstream ifile(argv[1]);
    if(!ifile.is_open())
    {
        cout<<argv[1]<<"could not be opened!\n";
        exit(1);
    }
    string line;
    while(getline(ifile,line))
    {
        if(line[0] == '#')
            continue;
        size_t source, target;
        stringstream(line)>>source>>target;
        graph[source].insert(target);
        graph[target].insert(source);
    }
    ifile.close();
    size_t graph_size = 0;
    for(map<size_t, set <size_t> >::iterator it = graph.begin(); it != graph.end(); ++it)
        graph_size = graph_size + (it->second).size();
    graph_size = graph_size/2;
    if(graph.find(0) == graph.end())
        izero = false;
    string graphfile(string(argv[1])+string(".graph"));
    ofstream ofile(graphfile);
    if(!ofile.is_open())
    {
        cout<<graphfile<<"could not be openeed!\n"; 
        exit(1);
    }

        ofile<<((graph.rbegin())->first)+(izero?1:0)<<" "<<graph_size<<"\n";
    for(size_t i = (graph.begin())->first; i <= (graph.rbegin())->first; i++)
    {
        for(set<size_t>::iterator it = graph[i].begin(); it != graph[i].end(); ++it)
        {
            if(izero)
                ofile<<(*it)+1<<" ";
            else
                ofile<<*it<<" ";
        }   
        ofile<<"\n";

    }
    /*for(map < size_t, set <size_t> >::iterator it = graph.begin(); it != graph.end(); ++it)
    {
        for(set<size_t>::iterator si = (it->second).begin(); si != (it->second).end(); ++si)
        {
            if(izero)
                ofile<<(*si)+1<<" ";
            else
                ofile<<*si<<" ";
        }   
        ofile<<"\n";
    }*/
    ofile.close();
}
