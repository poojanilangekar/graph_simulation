#include <iostream>
#include <string>
#include <fstream>
#include <streambuf>
#include <thread>
#include <map>
#include <list>
#include <set>
#include <utility>
#include <algorithm>

#include <json.hpp>

using namespace std;
using namespace nlohmann;

json j_query,j_nodes;
map < size_t, map < size_t, int> > distmat;
map < size_t, map < size_t, int> > qfe;

map <size_t, map < pair < size_t, size_t >, list <size_t> > > anc;
map <size_t, map < pair < size_t, size_t >, list <size_t> > > desc; 

map <size_t, set < size_t > > mat;
map <size_t, set < size_t > > premv;

bool check_equal(json data, json query)
{
	for(json::iterator it = query.begin(); it != query.end(); ++it)
	{
		if((it.key() != "id") && (it.key() != "out_degree"))
		{
			if(data.find(it.key()) != data.end()) 
			{
				if(data[it.key()] != it.value())
					return false;
			}
			else
				return false;
		}			
	}
	return true;
}


void fill_mat()
{
	for(int i = 0; i < j_query["node"].size(); i++)
        {
                string source = j_query["node"][i]["source"];
		int od = j_query["node"][i]["out_degree"];
		for(int j = 0; j < j_nodes.size(); j++)
                {
			if(check_equal(j_nodes[j],j_query["node"][i]))
                        {	
				if((od == 0))
                                        mat[i].insert(j);
                                else
                                {
                                        map< size_t, map < size_t, int> >::iterator it = distmat.find(j);
                                        if(it != distmat.end())
                                        {
                                                mat[i].insert(j);
                                        }
                                }
                        }
                }
        }


}

void fill_premv()
{
	for( map < size_t, map < size_t, int > >::iterator it = distmat.begin(); it != distmat.end(); ++it)
	{	
		size_t gs = it->first;
		set<size_t> matchp, unmatchp;
		for(size_t i = 0; i < j_query["edge"].size(); i++)
		{
			size_t qs = j_query["edge"][i]["source"], qd = j_query["edge"][i]["target"];
			bool flag=false;
			if( !check_equal(j_nodes[gs],j_query["node"][qs]))
				flag = true;
			else 
			{
				int matchd = 0, unmatchd = 0;
				for(map < size_t, int >::iterator dit = (it->second).begin(); dit != (it->second).end(); ++dit)
				{	
					size_t gd = dit->first;
					if(check_equal(j_nodes[gd],j_query["node"][qd]))
					{
						matchd++;
						if(qfe[qs][qd] < dit->second)
							unmatchd++;
					}

				}
				if(matchd == unmatchd)
					flag=true;
			}
			if(flag)
				unmatchp.insert(qd);
			else
				matchp.insert(qd);
			
		}
		set<size_t> result;
		set_difference(unmatchp.begin(),unmatchp.end(),matchp.begin(),matchp.end(),inserter(result,result.end()));
		for(set<size_t>::iterator ri = result.begin(); ri != result.end(); ++ri)
			premv[*ri].insert(gs);
	}	
}

set < pair <size_t, size_t> >  match()
{
	set < pair <size_t, size_t> > mgraph;
	fill_mat();
	fill_premv();
	//while( premv.size() != 0)
	{
		for(size_t i = 0; i < j_query["edge"].size(); i++)	
		{	
			size_t s = j_query["edge"][i]["source"], d = j_query["edge"][i]["target"];
			set <size_t> intersect;
			set_intersection(premv[d].begin(),premv[d].end(),mat[s].begin(),mat[s].end(),inserter(intersect,intersect.begin()));
			cout<<mat[s].size()<<"\n";
			for(set<size_t>::iterator it = intersect.begin(); it != intersect.end(); ++it)
				mat[s].erase(*it);
			cout<<mat[s].size()<<"\n";
			if(mat[s].size() == 0)
				return mgraph;
			for(size_t j = 0; j < j_query["edge"].size(); j++)
			{
				if(j_query["edge"][i]["target"] == s)
				{
					
				} 
			}
		}
	}
}


void fill_distmat(string filename)
{
	ifstream f(filename);
	string line;
        while(getline(f, line))
        {
		size_t source,dest;
		int dist;
                stringstream(line)>>source>>dest>>dist;
		if(source != dest)
               		distmat[source][dest] = dist;
        }
        f.close();

}

void add_to_anc(size_t candidate, size_t source, size_t target)
{
	int limit = qfe[source][target];
	pair <size_t, size_t> edge (source, target);
	if(distmat.find(candidate) != distmat.end())
	{
		for(map <size_t, int>::iterator it = distmat[candidate].begin(); it !=  distmat[candidate].end();++it)
		{
			if((it->first != candidate) && (it->second <= limit))
			{		
				anc[it->first][edge].push_back(candidate);	
			}
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
			if((!j_nodes[j].is_null()) && check_equal(j_nodes[j],j_query["node"][snode]))
				add_to_anc(j,snode,tnode);
			if((!j_nodes[j].is_null()) && check_equal(j_nodes[j],j_query["node"][tnode]))
				add_to_desc(j,snode,tnode);
		}
	}

}

void fill_out_degree()
{
	for( size_t i = 0; i < j_query["edge"].size(); i++)
	{
		size_t snode = j_query["edge"][i]["source"];
		if(j_query["node"][snode]["out_degree"].is_null())
			j_query["node"][snode]["out_degree"] = 1;
		else
			j_query["node"][snode]["out_degree"] = int(j_query["node"][snode]["out_degree"]) + 1; 
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
	fill_out_degree();
	match();
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


