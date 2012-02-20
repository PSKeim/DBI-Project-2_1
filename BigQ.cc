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
  	bool operator() (Record* rec1, Record* rec2)
	{
		ComparisonEngine cmp;
		 if(cmp.Compare(rec1, rec2, &sorter) < 0) //Strict, so we only want to know if rec1 is LESS THAN rec2
			return true;
		return false;
	}
	/*bool operator() (Record rec1, Record rec2)
	{
		ComparisonEngine cmp;
		 if(cmp.Compare(&rec1, &rec2, &sorter) < 0) //Strict, so we only want to know if rec1 is LESS THAN rec2
			return true;
		return false;
	}*/
	
};
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) 
{
	
    // construct priority queue over sorted runs and dump sorted data 
	this->input = &in;
	this->output = &out;
	this->order = sortorder;
	this->runlen = runlen;
	
	
	
	//Now that all the sorted runs are set up, we can do the initial sort
	cout << "BigQ is now entering First Phase." << endl;
	FirstPhase();
	cout << "BigQ has exited First Phase." <<endl;
	//This has all the records writen out to a file.
	cout<< "BigQ is now entering Second Phase." <<endl;
	SecondPhase();
	cout << "BigQ has exited Second Phase." <<endl;
 	// into the out pipe

    // finally shut down the out pipe
	output->ShutDown();
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
	vector<Record *> run; //Let's turn this into a vector of pointers to records, because of the "Copy causes record pointers to empty out" error pointed out by Morgan
	
	int runPages = 0; //Number of pages in our current run. If it ever matches runlen, we stop the reading in to sort and output the sorted records to the pipe
	
	//Variables for writing out to file
	f.Open(0,"temprecs");
	int offset = 0; //Offset starts at 0 because File automaticaly thinks "offset+1" since first page is empty.
	offsets.push_back(offset); //The first offset starts at 0, for run 1.
	int n = 0; 

	while(input->Remove(&readin) == 1){ //We have read a record in from the pipe
		//We now attempt to append the record to the page
		if(p.Append(&readin) == 0)
		{ //If the append fails, we must remove the records from the page, and put them into the vector.
			
			while(p.GetFirst(&temp))
			{
				//cout << "Run capacity is: "<<run.capacity();
				Record *vecRec;
				vecRec->Copy(&temp);
				run.push_back(vecRec);
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
				
				for(n = 0; n < run.size(); n++){
					if(p.Append(run[n]) == 0){
						//Failure to append, meaning we have to add the page to file, then start over
						f.AddPage(&p, offset);
						offset++;
						
						p.EmptyItOut(); //Empty and restart the process
						p.Append(run[n]);
					}
				}

				//Now we add the last page of the run to the file, and increment the offset (which now represents 
				f.AddPage(&p, offset);
				offset++;//Then we increment offset
				
				//Tracker and Offsets are used in Phase 2, to locate the different runs. (Run1 starts at 0, Run2 starts at Runlen (+- 1), etc
				int tracker = offset;
				offsets.push_back(tracker); //Offset right now will represent where the run ends (because Get/AddPage both increment by 1 when you go into them)
						
				p.EmptyItOut();//Empty out the page, since we're about to add a new record to it.
			}
			//No matter if we've written it out to file or anything, we still need to append the new record!
			p.Append(&readin);
		}
		
	}
	
	//So, at this point, we have read all the records from the pipe. We have a page containing at least one record. SO:
	while(p.GetFirst(&temp))
	{
			Record *vecRec = new Record();
			vecRec->Copy(&temp);
			run.push_back(vecRec);
	}
	cout << "I am sorting the last run." <<endl;
	std::sort(run.begin(),run.end(), record_sorter(this->order));
	cout << "I have sorted the last run." << endl;
	cout << "Run size is " << run.size() << endl;
	for(n = 0; n < run.size(); n++){
		//cout << "N is: " << n << endl;
		//cout << "Rec "<<n<<" is: " << rec[n] << endl;
		//output->Insert(run[n]);
		if(p.Append(run[n]) == 0){
			//Failure to append, meaning we have to add the page to file, then start over
			f.AddPage(&p, offset);
			offset++;
			
			p.EmptyItOut(); //Empty and restart the process
			p.Append(run[n]);
		}
		cout << "Appended record no. " << n <<endl;
	}
	//uhm, now we might have a page with a bunch of records in it that aren't in the file.
	f.AddPage(&p, offset);
	offset++;
	p.EmptyItOut();//Empty out the page, since we're about to add a new record to it.
	cout << " I wrote "<<offset <<" pages to file. " <<endl;
	f.Close();
	//So at the end of this, f is our link to an opened file with all our runs in it. Now we need to enter phase 2
}

//Okay, for second phase, we know that the file now contains a series of pages. 
/**
Strategy time:

SO, when making my run, should I count the number of pages I write to file, and then push back, and remember that offset? Done.

I have a File ("temprecs", name to be changed later) that contains a bunch of runs (that may be a little more or less than runlen pages). 
I have a Vector that contains the location of the different runs in the file (page offsets)

The number of runs in a file is equal to the number of offsets in that vector. We can create an array of pages (size efficiency?), which we fill from the file.
When a page is exhausted, we know where to fill it from in the vector

Likewise, have an array of n records. Do a linear scan over it, pick the best record, and output it to pipe.

When item x is chosen from the record, we go to page x in the array, and call GetFirst. If it returns 0, we go to the offset vector, and check for the proper offset.

This'll work (I'm 99% sure that this is the same method Nick is using), but requires a "fuck you" amount of book keeping. 

So, ideas on making this modular, or another way of simplifying it? Something like "GetNextRunPage" or something would be nice. Still requires book keeping.


Data structures: 

Page pageArray [offsets.size()];
Record recs [offsets.Size()]; 
int offUpdate[offsets.size()](); //This initialized offUpdate to all 0's.
int skip[offsets.size()](); //Tells me which runs have finished 
Offsets is already declared.

So, first step is to fill the page array from File using GetPage and iterating over Offsets (all offset updates will be 0, because we haven't finished a page)

Once we've done this, we fill the record array using GetFirst from each page in the Page Array. (We also increase the offUpdate of each run to 1 at this point)

Now we sit and keep iterating over the Recs array, finding the Record with the smallest value. We note the index of the minimum record, write it out to file, and then refil the record from the page.  

When a page empties, we have to refill it. So, we grab the page from the file using the Offsets[x] + offUpdate[x] as the offset. If Offsets[x] + offUpdate[x] = Offsets[x+1], we've exhausted a run, and thus, we just mark it in the skip array as finished, and then ignore that from then on.

**/

void BigQ::SecondPhasev2(){
	//Data Structures
	int size = offsets.size();
	Page pageArray [size];
	Record recs [size]; 
	int offUpdate[size];
	int skip[size]; 
	
	//Open the file
	f.Open(1,"temprecs");

	//Initialization 
	for(int i = 0; i < size;i++){
		f.GetPage(&pageArray[i],offsets[i]);
		pageArray[i].GetFirst(&recs[i]);
		offUpdate[i] = 0;
		skip[i] = 0;
	}
	
	//Now we've got a page array full of run beginnings, a record array full of records, and the offset and skip arrays are set up. Kewl.
	
	//Now we need to iterate over the record array until we find the records we want.
	int mindex;
	ComparisonEngine cmp = new ComparisonEngine();
	while(true){
		mindex = 0; //Start with guessing the min as the first bucket
		for(int i = 1; i < size; i++){ //Scan over rec list to find minimum record
			
		}
	}
	
	f..Close();
}


void BigQ::SecondPhase(){
	f.Open(1,"temprecs");
	Page p;
	Record temp;

	int numP = f.GetLength();


	for(int i = 0; i < numP-1; i++){
		f.GetPage(&p,i);
		while(p.GetFirst(&temp)){
			output->Insert(&temp);
		}
	}
	f.Close();
}
