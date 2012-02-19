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
				//uhm, now we might have a page with a bunch of records in it that aren't in the file.
				f.AddPage(&p, offset);
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

void BigQ::SecondPhase(){
	f.Open(1,"temprecs");
	
	Page p;
	Record temp;
	//Get ALL DER PAGES!
	int numP = f.GetLength();
	for(int i = 0; i < numP-1; i++){
		f.GetPage(&p,i);
		while(p.GetFirst(&temp)){
			output->Insert(&temp);
		}
	}
	f.Close();
}
