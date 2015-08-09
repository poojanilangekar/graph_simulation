#include <iostream>
#include <fstream>
#include <streambuf>
#include <sstream>
#include <ctime>
#include <cstdlib>
#include <set>

#include <json.hpp>

using namespace std;
using namespace nlohmann;

json dgraph;
json qgraph;

const string currentDateTime() {
    time_t     now = time(0);
    struct tm  tstruct;
    char       buf[80];
    tstruct = *localtime(&now);
    strftime(buf, sizeof(buf), "%Y%m%d%X", &tstruct);

    return buf;
}

void populate_nodes(int n)
{
    set<json> querynodes;
     while(querynodes.size() != n)
    {
        querynodes.insert(dgraph[(rand()%(dgraph.size()))+1]);
    }
    qgraph["node"] = querynodes;

}

void populate_edges(int n, int e, int k)
{
    json edge;
    int source,target,fe;
    set<json> queryedges;
    while(queryedges.size() < e)
    {
        source = (rand()%n);
        target = (rand()%n);
        if(source != target)
        {
            edge["source"] = source;
            edge["target"] = target;
            queryedges.insert(edge);
        }
    }
    qgraph["edge"] = queryedges;
    for(int i =0; i < e; i++)
    {    qgraph["edge"][i]["fe"] = (rand()%(k-1))+2;
    }
}


int main(int argc, char * argv[])
{
    if(argc != 5)
    {
        cout<<"USAGE: "<<argv[0]<<" <number-of-nodes> <number-of-edges> <upper-bound-on-path-length> <data-graph>\n";
        exit(1);
    }    
    istringstream nss(argv[1]);
    istringstream ess(argv[2]);
    istringstream kss(argv[3]);
    
    int n,e,k;
    if(!(nss>>n))
    {
        cout<<"ERROR: Invalid number of nodes!\n";
        exit(1);
    }
    if(!(ess>>e))
    {
        cout<<"ERROR: Invalid number of edges!\n";
        exit(1);
    }
    if(!(kss>>k))
    {
        cout<<"ERROR: Invalid upper bound on path length!\n";
        exit(1);
    }

    if( (n<=0) || (e<=0) || (k<=1) )
    {
        cout<<"ERROR: Invalid parameters!\n";
        exit(1);
    }   
    
    string inputfile(argv[4]);
    ifstream d(inputfile);
    if(!d.is_open())
    {
        cout<<"ERROR: Could not open "<<inputfile<<endl;
        exit(1);
    }
    stringstream dss; 
    dss << d.rdbuf();
    dgraph = json::parse(dss);
    d.close();
    string outputfile = inputfile+"_query_"+currentDateTime()+".json";
    srand((unsigned)time(0));
    populate_nodes(n);
    populate_edges(n,e,k);
    ofstream qfile(outputfile);
    qfile<<qgraph.dump(2);
    qfile.close();

}

