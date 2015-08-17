
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <streambuf>
#include <cstdlib>
#include <json.hpp>
#include <set>

using namespace std;
using namespace nlohmann;

json nodes;
map < size_t, int > nodeparts;
json parts[4];


void parse_part(string partfile, bool indexzero)
{
    ifstream pf(partfile);
    if(!pf.is_open())
    {
        cerr<<"Could not open "<<partfile;
        exit(1);
    }
    string line; 
    size_t node_num;
    if(indexzero)
        node_num = 0;
    else
        node_num = 1;
    while(getline(pf,line))
    {
        int processor;
        stringstream(line)>>processor;
        nodeparts[node_num] = processor;
        node_num++;
    }
    pf.close();

}

void compute_parts(string edgefile)
{
    set <json> outnodes[4];
    set <json> innodes[4];
    for(map<size_t, int>::iterator it = nodeparts.begin(); it != nodeparts.end(); ++it )
    {
        nodes[it->first]["id"] = it->first;
        parts[it->second]["node"].push_back(nodes[it->first]);
    }
    ifstream ef(edgefile);
    if(!ef.is_open())
    {
        cerr<<"Could not open "<<edgefile;
        exit(1);
    }
    string line;
    while(getline(ef,line))
    {
        size_t source, target;
        json current_edge;
        stringstream(line)>>source>>target;
        current_edge["source"] = source;
        current_edge["target"] = target;
        parts[nodeparts.find(source)->second]["edge"].push_back(current_edge);
        if(nodeparts.find(source)->second != nodeparts.find(target)->second)
        {
            json outnode;
            outnode["node"] = target;
            outnode["fragment"] = nodeparts.find(target)->second;
            outnodes[nodeparts.find(source)->second].insert(outnode);
            json innode;
            innode["node"] = target;
            innode["fragment"] = nodeparts.find(source)->second;
            innodes[nodeparts.find(target)->second].insert(innode);  
        }
        
    }
    for(int i=0; i<4; i++)
    {   
        parts[i]["Fi.O"] = outnodes[i];
        parts[i]["Fi.I"] = innodes[i];
    }
    
}


int main(int argc, char * argv[])
{
    if(argc != 4)
    { 
        cerr<<"Usage: "<<argv[0]<<" <nodes-file> <edge-file> <part-file> \n";
        exit(0);
    }
    string nodefile(argv[1]);
    string edgefile(argv[2]);
    string partfile(argv[3]);
    ifstream nf(nodefile);
    if(!nf.is_open())
    {
        cerr<<"Could not open "<<nodefile;
        exit(1);
    }
    stringstream nfss;
    nfss << nf.rdbuf();
    nodes = json::parse(nfss);
    nf.close();
    parse_part(partfile, !nodes[0].is_null());
    compute_parts(edgefile);
    for(int i=0; i<4; i++)
    {
        string filename = string(nodefile+"_part_"+to_string(i)+".json");
        ofstream of(filename);
        of<<parts[i].dump(2);
        of.close();
    }  
    
}
