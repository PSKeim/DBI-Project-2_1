#include "BigQ.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) 
{
	// read data from in pipe sort them into runlen pages

	//Assorted variable declaration. I'll sort them out into a more logical ordering sometime.
	Page p = new Page; //Our holding page
	Record readin = new Record; //Variable that works as a holder for when we're reading in from the pipe
	Vector run = new Vector<Record *> //Okay, so, the "run" is a group of records (not a page, because there can be more than PageLen number of records)
	Vector runs = new Vector<Vector<Record *>>; //Okay, this is the collection of runs.
	Record temp = new Record; //Have to have another temp variable for when I'm moving records from the page to the vector
	int runPages = 0; //Number of pages in our current run. If it ever matches runlen, we stop the reading in to sort and output the sorted records to the pipe
	File f = new File(); //This is the nice file I will be using to write stuff out to temp files with.

	while(in.Remove(&readin) == 1)
	{
		//readin now contains a record. Where to store it? A page!
		if(!p.Append(readin)) //Okay, so now a problem: Append can fail. So we need to add handling.
		{
			//Now, we've got a full page, what do we put it in? Vector it in, captain.
			while(p.GetFirst(&temp))
			{
				//Shove the records from the page into the run vector.
				//However, what if the run vector gets to be run size?
				//In that case, we need to sort it, push the records (in order) into the output pipe
				//Once that's done, we can take the newly empty vector, and start the process over again until the pipe is empty.
			}
			//Now the page is empty, we increment the number of pages that are in our current run
			runPages++;
			//And check to see if we've read in a total run
			if(runPages == runLen)
			{
				//If we have, then we must sort the current run vector
				//After I've sorted the current run vector, I need to write it out to a file
				
			}
		}
		
	}

	//We still might have a page of records (saf, if the number of records % size of page = 0, but number of records % runlen*pageSize != 0, or something)
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
