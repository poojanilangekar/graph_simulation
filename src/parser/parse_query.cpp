#include <iostream>
#include <cstdio>
#include <string>
#include <fstream>
#include <streambuf>
#include <thread>
#include <map>
#include <list>
#include <set>
#include <utility>
#include <algorithm>
#include <ctime>

#include <json.hpp>

using namespace std;
using namespace nlohmann;

json  query,nodes; //JSON Objects to contain the Query Graph and the Nodes of the Data Graph.
map < size_t, map < size_t, int> > distmat; //Distance Matrix of the Data Graph.
map < size_t, map < size_t, int> > qfe; //Map containing the fe(u,u') for each node in the Query Graph.

map <size_t, map < pair < size_t, size_t >, set <size_t> > > anc; // A Map of Ancestors for each node in the Data Graph with respect to each edge in the Query Graph.
map <size_t, map < pair < size_t, size_t >, set <size_t> > > desc; //A Map of Descendants for each node in the Data Graph with respect to each edge in the Query Graph.

map <size_t, set < size_t > > mat; // Map of all nodes in the Query Graph to the set of all matching nodes in the Data Graph.
map <size_t, set < size_t > > premv; // Map of all nodes in the Query Graph (u) to the set of nodes in the Data Graph which don't match any parent of u.


bool check_equal(json datanode, json querynode) //Check the equality of the datanode and the querynode.
{
	for(json::iterator it = querynode.begin(); it != querynode.end(); ++it)
	{
    //Iterate through each key of the querynode, if the key is not "id" or "out_degree", compare its value with that of the datanode, return fale if unequal. 
		if((it.key() != "id") && (it.key() != "out_degree"))
		{
			if(datanode.find(it.key()) != datanode.end()) 
			{
				if(datanode[it.key()] != it.value())
					return false;
			}
			else
				return false;
		}			
	}
	return true; //If the values of all the keys in the querynode match the datanode, return true. 
}

const std::string currentDateTime() {
    time_t     now = time(0);
    struct tm  tstruct;
    char       buf[80];
    tstruct = *localtime(&now);
    strftime(buf, sizeof(buf), "%Y-%m-%d.%X", &tstruct);

    return buf;
}

void fill_mat()
{
	for(int u = 0; u < query["node"].size(); u++) //Iterate through all the nodes in the Query Graph, u.
    {
		int od = query["node"][u]["out_degree"]; 
		for(int x = 0; x < nodes.size(); x++) //Iterate through all the nodes in the Data Graph, x.
        {
			if(check_equal(nodes[x],query["node"][u])) //If the labels of u and x are equal, check the out degree of u. 
            {	
				if((od == 0)) //If the out degree of u is 0, insert x in mat[u].
               			mat[u].insert(x);
                else
                {
                //Else if both u and x have non zero out degrees, insert x in mat[u].
                    map< size_t, map < size_t, int> >::iterator it = distmat.find(x);
                    if(it != distmat.end())
                    {
                        mat[u].insert(x); 
                    }
                }
            }
        }
    }
}


void fill_premv()
{
	for( map < size_t, map < size_t, int > >::iterator it = distmat.begin(); it != distmat.end(); ++it) 
    //Iterate through each vertex, x, in the Data Graph with non-zero out-degree.
	{	
		size_t gs = it->first; //Source node in the Data Graph. (gs=x)
		set<size_t> matchp, unmatchp; //matchp contains the set of nodes which match the parent(source node) and unmatchp contains the set of nodes that don't match the parent(source node).
		for(size_t i = 0; i < query["edge"].size(); i++)
        //Iterate through each edge in the Query Graph.
		{
			size_t qs = query["edge"][i]["source"], qt = query["edge"][i]["target"]; //Source and target node in the Query Graph. (qs=u' and qt=u)
			bool flag = false; //Flag to determine if the node (x) should belong to premv or not. Set to false to indicate x should not belong to premv. 
			if(check_equal(nodes[gs],query["node"][qs]))
			//CONDITION 1: fA(x) satisfies fv(u1)
			{
				for(map < size_t, int >::iterator dit = (it->second).begin(); dit != (it->second).end(); ++dit)
				{	
					size_t gt = dit->first; //Target node in the Data Graph. (gt=x')
                    if(mat[qt].find(gt) != mat[qt].end()) //CONDITION 2: x' belongs to mat[u]
                    {
                        if((dit->second) <= qfe[qs][qt]) //CONDITION 3: len(x/../x') <= fe(u',u)
                            flag = true; // x can be a parent of u
                    }

				}
			}
			if(flag)
			    matchp.insert(qt); //x satisfies the conditions for being a parent of u with respect to the edge (qs,qt).
			else
				unmatchp.insert(qt); //x does not satisfy the conditions for being a parent of u with respect to the edge (qs,qt).
			
		}
		set<size_t> result; //The result set consists of all target nodes (u) for which the node (x) does not satisfy the conditions to be any parent of u.
		set_difference(unmatchp.begin(),unmatchp.end(),matchp.begin(),matchp.end(),inserter(result,result.begin()));
		for(set<size_t>::iterator u = result.begin(); u != result.end(); ++u)
			premv[*u].insert(gs); 
	}	
}


map < size_t, set <size_t> >  match()
{
	map < size_t, set <size_t> > mgraph;
	fill_mat();
	fill_premv();
	while( premv.size() != 0) //While there exists u in the Query Graph with premv(u) which is not empty.
	{
		for(map< size_t, set < size_t > >::iterator pi = premv.begin(); pi != premv.end(); ++pi) //Iterate through the non empty premv. 
		{	size_t u = pi->first;
			for(size_t i = 0; i < query["edge"].size(); i++)	
			{	
            //Iterate through each edge in the Query Graph where the target node is u. 
				if( u != query["edge"][i]["target"])
					continue;
				size_t s = query["edge"][i]["source"]; //The source node in the Query Graph. (s = u') 
				set <size_t> intersect; //intersect is the set of all nodes which belong to both premv[u] and mat[u'].
				set_intersection(premv[u].begin(),premv[u].end(),mat[s].begin(),mat[s].end(),inserter(intersect,intersect.begin()));
				for(set<size_t>::iterator z = intersect.begin(); z != intersect.end(); ++z) //Remove all z from mat[u'].
					mat[s].erase(*z);
				if(mat[s].size() == 0) //If mat[u'] is empty, return an empty graph; this indicates no match could be found. 
					return mgraph;
				for(size_t j = 0; j < query["edge"].size(); j++)
				{
					if(query["edge"][j]["target"] == s)
					{
                        //For all nodes u" such that (u",u') is an edge in the Query Graph. 
						pair<size_t,size_t> edge(query["edge"][j]["source"],query["edge"][j]["target"]);
                        for(set<size_t>::iterator z=intersect.begin(); z != intersect.end(); ++z)								
                        {   
                            set<size_t> ancestors = anc[*z][edge];
                            for(set<size_t>::iterator li = ancestors.begin(); li != ancestors.end(); ++li) //z' is *li
                            {
                            //For all nodes z' belonging to anc[z][(u",u')] but not belonging to premv[u'].	
                                if(premv.find(s) != premv.end())
                                {
                                    if(premv[s].find(*li) != premv[s].end())
                                        continue; //z' belongs to premv[u'] and hence is not processed. 
                                }
                                set<size_t> descendants = desc[*li][edge]; //Descendants of z' with respect to edge (u",u').
                                set<size_t> mats = mat[s]; //The set of nodes which belong to mat[u']
                                set<size_t> dmi; //The intersection of the nodes which belong to both desc[z'][(u",u')] and mat[u']
                                set_intersection(descendants.begin(),descendants.end(),mats.begin(),mats.end(),inserter(dmi,dmi.begin()));
                                if(dmi.size()==0) //If the intersection is empty, add z' to premv[u']
                                    premv[s].insert(*li);	
                            }
                        }
					} 
				}
			    	
			}	
		    premv.erase(pi->first); //Empty premv[u].
        }	
	}
    for(map< size_t, set <size_t> >::iterator mi = mat.begin(); mi != mat.end(); ++mi)
    {
    //Fill the result graph with (u,x) for each u in the Query Graph and each x in the set mat[u].
        size_t u = mi -> first;
        set <size_t> x = mi -> second;
        for(set<size_t>::iterator si = x.begin(); si != x.end(); ++si)
            mgraph[u].insert(*si);
    }
}


void fill_distmat(string filename)
{
	ifstream f(filename);
	if(!f.is_open())
	{	
		cout<<"ERROR: Could not open "<<filename;
		exit(1);
	}
	string line;
    while(getline(f, line)) //Read the file line by line and fill in the distance matrix.
    {
        size_t source,target;
        int dist;
        stringstream(line)>>source>>target>>dist;
        if(source != target)
            distmat[source][target] = dist;
    }
    f.close();

}


void add_to_anc(size_t candidate, size_t source, size_t target) //Candidate node matches the source node. 
{
	int limit = qfe[source][target]; 
	pair <size_t, size_t> edge (source, target);
	if(distmat.find(candidate) != distmat.end()) //Check if candidate has out edges. 
	{
		for(map <size_t, int>::iterator it = distmat[candidate].begin(); it !=  distmat[candidate].end();++it) 
        //Iterate through all the nodes reachable from candidate node. 
		{
			if((it->first != candidate) && (it->second <= limit))
            //If the distance from the candidate node to the current node is within the limit specified in qfe, add candidate as an ancestor of the current node for the edge (source, target).
			{		
				anc[it->first][edge].insert(candidate);	
			}
		}
	}
}


void add_to_desc(size_t candidate, size_t source, size_t target) //Candidate node matches the target node. 
{
	int limit = qfe[source][target];
	pair <size_t, size_t> edge (source,target);
	for(map< size_t, map < size_t, int> >::iterator it = distmat.begin(); it != distmat.end(); ++it) //Iterate through the Distance Matrix.
	{	
		if(candidate != it->first) 
		{
        //For every node other than the candidate node, if the candidate node is within the limit specified in the qfe add candidate as a descendant of the current node for the edge (source,target).
			size_t current = it -> first;
			map <size_t, int>::iterator cit = (it->second).find(candidate);
			if(cit != (it->second).end())
			{
				if(cit->second <= limit)
					desc[current][edge].insert(candidate);	
			}
		}	
 
	}
}


void compute_anc_desc() //Computes the ancestors and descendants of each node in the Data Graph with respect to the edges in the query.
{

	for(size_t i = 0; i < query["edge"].size();i++) //Iterate through all the edges in the query graph.
	{
        size_t snode = query["edge"][i]["source"], tnode = query["edge"][i]["target"];
		for(size_t j = 0; j < nodes.size(); j++) //Iterate through all the nodes in the Data Graph.
		{
			if(!(nodes[j].is_null()))
            {
                if( check_equal(nodes[j],query["node"][snode]))
				    add_to_anc(j,snode,tnode); //If nodes[j] matches the source node, add it as an ancestor node.
			    if( check_equal(nodes[j],query["node"][tnode]))
				    add_to_desc(j,snode,tnode); //If nodes[j] matches the target node, add it as a descendant node. 
            }
		}
	}

}


void fill_out_degree()
{
    for( size_t i = 0; i < query["node"].size(); i++)
        query["node"][i]["out_degree"] = 0;
	for( size_t i = 0; i < query["edge"].size(); i++) //For each edge in the Query Graph, increment the out_degree of the source vertex.
	{
		size_t snode = query["edge"][i]["source"];
		query["node"][snode]["out_degree"] = int(query["node"][snode]["out_degree"]) + 1; 
	}
}


void parse_graph()
{
	for(size_t i=0; i<query["edge"].size();i++) //Fill fe(u,u') for every edge in the Query Graph.
	{
        qfe[qnode["source"]][qnode["target"]] = 1; //Modify to fit graph simulation with limit 1.   
	}
	compute_anc_desc(); 
    fill_out_degree();
    map < size_t, set <size_t> > s = match(); //Print the result graph if the query is present in the data graph. 
    if(s.size() == 0)
    {
        cout<<"Pattern can not be matched.\n";
        return;
    }
    for(map <size_t, set <size_t> >::iterator it = s.begin(); it != s.end(); ++it)
    {
        cout <<it->first<<"\t"<< (it->second).size()<<"\n";
        set<size_t> us = it->second;
        for(set <size_t>::iterator is = us.begin(); is != us.end(); ++is)
            cout<<*is<<"\t";
        cout<<"\n";
    }
}


int main(int argc, char* argv[])
{
	
	time_t start = time(0);
    if(argc != 4) 
	{
		cout<<"USAGE: "<<argv[0]<<" <query-graph> <data-node-file> <data-distance_matrix-file>\n";
		exit(1);
	}
	string qfile = argv[1], nfile = argv[2], dfile = argv[3];
	thread readdistmat(fill_distmat,dfile); //A thread to fill the distance matrix from the distance matrix file.
	ifstream f(qfile); 
	if(!f.is_open())
	{
		cout<<"ERROR: Could not open "<<qfile;
		exit(1);
	}
	stringstream qss;
	qss << f.rdbuf();
	query = json::parse(qss); //Parse the contents of the query file into the json object `query`.
	f.close();
	f.open(nfile);
	if(!f.is_open())
	{
		cout<<"ERROR: Could not open "<<nfile;
		exit(1);
	}
	stringstream nss;
	nss <<f.rdbuf();
	nodes = json::parse(nss); //Parse the contents of the node file of the Data Graph into the `nodes`.
	f.close();
	readdistmat.join(); //Wait for the distance matrix to be filled.
    string outputfile = nfile+"_"+currentDateTime()+".txt";
	freopen(outputfile.c_str(),"w",stdout);
    parse_graph(); //Parse the Date Graph with respect to the Query Graph.
    fclose(stdout);
    freopen("/dev/tty", "w", stdout);
    cout<<"TIME: "<<difftime(time(0),start)<<"\n";
    exit(0);
}


