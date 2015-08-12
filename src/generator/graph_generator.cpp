#include <iostream>
#include <fstream>
#include <ctime>
#include <cstdlib>
#include <set>
#include <vector>
#include <string>
#include <utility>
#include <unistd.h>

#include <json.hpp>

using namespace std;
using namespace nlohmann;

int main()
{
    time_t rawtime;
    struct tm * timeinfo;
    char buffer[80];

    time (&rawtime);
    timeinfo = localtime (&rawtime);

    strftime (buffer,80,"%F",timeinfo);
    string date(buffer);
    int v,e,l;
    cout<<"Enter the number of nodes, edges and labels.\n";
    cin>>v>>e>>l;
    vector<string> labels(l);
    cout<<"Enter the labels.\n";
    for(size_t i=0; i<l; i++)
    {
        string label;
        cin>>label;
        labels.insert(labels.begin(),label);
    }
    srand((unsigned)time(0)); 
    json graph;
    for(size_t i = 0; i < v; i++)   
    {   
        size_t k = rand()%l;
        string label(labels.at(k));
        graph["node"][i]["label"] = label;
    }
    set<json> graphedges;
    while(graphedges.size() < e)
    {   
        size_t source = rand()%v;
        size_t target = rand()%v;
        json edge;
        if(source != target)
        {
            edge["source"] = source;
            edge["target"] = target;
            graphedges.insert(edge);
        }
    }
    graph["edge"] = graphedges;
    string nodefile = date+"_nodes_"+to_string(v)+"_edges_"+to_string(e)+"_node.json";
    string edgefile = date+"_nodes_"+to_string(v)+"_edges_"+to_string(e)+"_edge.txt";
    
    pid_t pid = fork();
    if(pid == 0)
    {
        ofstream efile(edgefile);
        for(size_t i=0; i < graph["edge"].size(); i++)
            efile<<graph["edge"][i]["source"]<<"\t"<<graph["edge"][i]["target"]<<"\n";
        efile.close();
        return 0;
    }
    else
    {
        ofstream nfile(nodefile);
        nfile<<graph["node"].dump(2);
        nfile.close();
    }

}


