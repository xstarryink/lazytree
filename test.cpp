#include<iostream>
#include"lazytree.hpp"
#include<vector>
#include<set>
#include<random>
#include<chrono>
#include<numeric>
using namespace std;
vector<unsigned int>testdata;
auto test(auto& cont)
{
	auto begin=chrono::high_resolution_clock::now();
	for(unsigned int i:testdata)
		cont.insert(i);
	for(unsigned int i:testdata)
		cont.count(i);
	for(unsigned int i:testdata)
		cont.erase(i);
	auto end=chrono::high_resolution_clock::now();
	return end-begin;
}
main()
{
	ios_base::sync_with_stdio(false);
	testdata.resize(1000000);
	iota(begin(testdata),end(testdata),0);
	random_shuffle(begin(testdata),end(testdata));
	LazyTree<unsigned int,string>lt;
	set<unsigned int,string>RBT;
	cout<<"RBTree:\n"<<test(RBT)/1.0s<<"s\n"
		<<"LazyTree:\n"<<test(lt)/1.0s<<"s\n";
	return 0;
}
