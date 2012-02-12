#include "BigQ.h"



//This struct acts as a sorter for when I use std::sort. Means I should be very efficient with my sorting. Or something?
struct record_sorter {
	OrderMaker sorter;
  //Need a function that will act as constructor
 	record_sorter(OrderMaker &sortorder)
 	{
		sorter = sortorder;
	}
  //Next, the function that works as the sort function
  	bool operator() (Record rec1, Record rec2)
	{
		ComparisonEngine cmp;
		 if(cmp.Compare(&rec1, &rec2, &sorter) < 0) //Strict, so we only want to know if rec1 is LESS THAN rec2
			return true;
		return false;
	}
	
};
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) 
{
	// read data from in pipe sort them into runlen pages

	//Assorted variable declaration. I'll sort them out into a more logical ordering sometime.
	Page p; //Our holding page
	Record readin; //Variable that works as a holder for when we're reading in from the pipe
	vector<Record> run; //Okay, so, the "run" is a group of (NOT POINTERS TO RECORDS BUT THE ACTUAL OBJECTS) records (not a page, because there can be more than PageLen number of records)
	//vector runs<vector<Record>; //Okay, this is the collection of runs.
	Record temp; //Have to have another temp variable for when I'm moving records from the page to the vector
	int runPages = 0; //Number of pages in our current run. If it ever matches runlen, we stop the reading in to sort and output the sorted records to the pipe
	File f; //This is the nice file I will be using to write stuff out to temp files with.

	while(in.Remove(&readin) == 1) //readin now contains a record. Where to store it? A page!
	{
		if(p.Append(&readin) == 0) //Okay, so now a problem: Append can fail. So we need to add handling.
		{
			//Now, we've got a full page, what do we put it in? Vector it in, captain.
			while(p.GetFirst(&temp))
			{
				//Shove the records from the page into the run vector.
				run.push_back(temp);
			}
			//Now the page is empty, we increment the number of pages that are in our current run
			runPages++;
			//And check to see if we've read in a total run
			if(runPages == runlen)
			{
				cout << "I am beginning to sort a run." << endl;
				std::sort(run.begin(),run.end(), record_sorter(sortorder)); //Sort the run
				cout << "I have sorted a run." << endl;
				//And now we need to write it out to file
			}	
			p.Append(&readin);
		}
		
	}

	//We still might have a page of records (if the number of records % size of page = 0, but number of records % runlen*pageSize != 0, or something)
	//So we need to check that, and sort/stuff our vector
	//Worry about that later. Keep thinking about how this is supposed to be set up

	//ANYWAYS

    // construct priority queue over sorted runs and dump sorted data 

	//PQueue, or do a linear scan over the heads of the pages and see which ones are best suited?
	//Speaking of which, still not sure of a good way to find the first record of a page that doesn't also remove it from the page
 
 	// into the out pipe

    // finally shut down the out pipe
	out.ShutDown ();
}

BigQ::~BigQ () {
}

//The purpose here is to take a vector of records and produce a sorted vector in return
void BigQ::FirstPhaseSort (vector<Record *> &sort, OrderMaker &order){
  


}
