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
	
    // construct priority queue over sorted runs and dump sorted data 
	this->input = &in;
	this->output = &out;
	this->order = sortorder;
	this->runlen = runlen;
	//PQueue, or do a linear scan over the heads of the pages and see which ones are best suited?
	//Speaking of which, still not sure of a good way to find the first record of a page that doesn't also remove it from the page
 
 	// into the out pipe

    // finally shut down the out pipe
}

BigQ::~BigQ () {
}

//This function holds the First Phase code of the project.
//Its job is to read in files from pipe until 
void BigQ::FirstPhase(){

	// read data from in pipe sort them into runlen pages
	
	//Assorted variable declaration. I'll sort them out into a more logical ordering sometime.
	Page p; //Our holding page
	//Variables for sorting and storing
	Record readin; //Variable that works as a holder for when we're reading in from the pipe
	Record temp; //Have to have another temp variable for when I'm moving records from the page to the vector
	vector<Record> run; //Okay, so, the "run" is a group of (NOT POINTERS TO RECORDS BUT THE ACTUAL OBJECTS) records (not a page, because there can be more than PageLen number of records)
	
	int runPages = 0; //Number of pages in our current run. If it ever matches runlen, we stop the reading in to sort and output the sorted records to the pipe
	
	//Variables for writing out to file
	f.Open(0,"temprecs");
	int offset = 0; //Offset starts at 0 because File automaticaly thinks "offset+1" since first page is empty.
	int n = 0; 

	while(input->Remove(&readin) == 1){ //We have read a record in from the pipe
		//We now attempt to append the record to the page
		if(p.Append(&readin) == 0)
		{ //If the append fails, we must remove the records from the page, and put them into the vector.
			
			while(p.GetFirst(&temp))
			{
				run.push_back(temp); //Put the record into our run vector
			}
			runPages++; //Increment the number of run pages we currently have
			
			if(runPages == runlen-1)
			{
				//We've reached the number of pages allowed in a run, so we sort that shit.
				
				cout << "I am beginning to sort a run." << endl;
				std::sort(run.begin(),run.end(), record_sorter(this->order)); //Sort the run
				cout << "I have sorted a run." << endl;
				
				
				//And now we need to write it out to file	
				//Okay, I need a page offset variable because if I'm writing these all to the same file
				//Which I am, apparently, then I need to not accidentally overwrite anything.
				//What I've got: A sorted vector, and a file.
				//What I need: A page to store it in (p is empty! We can use that!)
				
				
				for(n = 0; n < run.size(); n++){
					if(p.Append(&run[n]) == 0){
						//Failure to append, meaning we have to add the page to file, then start over
						f.AddPage(&p, offset);
						offset++;
						
						p.EmptyItOut(); //Empty and restart the process
						p.Append(&run[n]);
					}
				}
				//uhm, now we might have a page with a bunch of records in it that aren't in the file.
				f.AddPage(p, offset);
				offset++;
				p.EmptyItOut();//Empty out the page, since we're about to add a new record to it.
			}
			//No matter if we've written it out to file or anything, we still need to append the new record!
			p.Append(&readin);
		}
		
	}
	
	//So, at this point, we have read all the records from the pipe. We have a page containing at least one record. SO:
	while(p.GetFirst(&temp))
		{
			run.push_back(temp);
		}
	cout << "I am sorting the last run." <<endl;
	std::sort(run.begin(),run.end(), record_sorter(this->order));
	cout << "I have sorted the last run." << endl;
	for(n = 0; n < run.size(); n++){
		if(p.Append(&run[n]) == 0){
			//Failure to append, meaning we have to add the page to file, then start over
			f.AddPage(&p, offset);
			offset++;
			
			p.EmptyItOut(); //Empty and restart the process
			p.Append(&run[n]);
		}
	}
	//uhm, now we might have a page with a bunch of records in it that aren't in the file.
	f.AddPage(p, offset);
	offset++;
	p.EmptyItOut();//Empty out the page, since we're about to add a new record to it.
	
	//So at the end of this, f is our link to an opened file with all our runs in it. Now we need to enter phase 2
}
