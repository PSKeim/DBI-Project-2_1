#include "test.h"
#include "BigQ.h"
#include <pthread.h>

void *producer (void *arg) {

	Pipe *myPipe = (Pipe *) arg;

	Record temp;
	int counter = 0;

	DBFile dbfile;
	dbfile.Open (rel->path ());
	cout << " producer: opened DBFile " << rel->path () << endl;
	dbfile.MoveFirst ();

	while (dbfile.GetNext (temp) == 1) {
		counter += 1;
		if (counter%100000 == 0) {
			 cerr << " producer: " << counter << endl;	
		}
		myPipe->Insert (&temp);
	}

	dbfile.Close ();
	myPipe->ShutDown ();

	cout << " producer: inserted " << counter << " recs into the pipe\n";
}

void * consumer (void * arg) {
  cout << "consumer started" << endl;
  testutil * t = (testutil *) arg; // see if reinterpret_cast works here later

  ComparisonEngine ceng;

  DBFile dbfile;
  char outfile[100];

  if (t->write) {
    sprintf (outfile, "%s.bigq", rel->path ());
    dbfile.Create (outfile, heap, NULL);
  }

  int err = 0;
  int i = 0;
  int counter = 0;
  // the idea here, is two different things.
  // last will hold whatever record that will end up being the last record we see
  //
  // double buffering setup
  Record rec[2];
  Record *last = NULL, *prev = NULL; // last and prev index into the buffer.

  while (t->pipe->Remove (&rec[i%2])) { // get next guy.
    prev = last;
    last = &rec[i%2];

    counter += 1;
    if (counter%100000 == 0) {
      cerr << " consumer: " << counter << endl;
    }

    // the conditional works at the beginning.
    // if there is at least one thing in the the pipe
    // prev is null, and last is the record we just pulled out
    // so we don't have two things to compare and thus skip compare
    // at the end, we never get here.
    // so really, we just need to make sure that prev is not NULL.
    if (NULL != prev && NULL != last) { // make sure neither is NULL, so that we can follow the pointers and compare them. works at both beginning and end.
      if (1 == ceng.Compare (prev, last, t->order)) { // wrong order coming out of the "sorted order" pipe
        
        cout << endl << endl << endl << "It was said that " << endl << endl << endl;
        prev->Print (rel->schema ());
        cout << "was smaller than" << endl;
        last->Print (rel->schema ());
        cout << counter << endl;
        err++;
      }
      if (t->write) {
        dbfile.Add (*prev);
      }
    }
    if (t->print) {
      last->Print (rel->schema ());
    }
    i++;
  }

  cout << " consumer: removed " << i << " recs from the pipe\n";

  if (t->write) {
    if (last) {
      dbfile.Add (*last);
    }
    cerr << " consumer: recs removed written out as heap DBFile at " << outfile << endl;
    dbfile.Close ();
  }
  cerr << " consumer: " << (i - err) << " recs out of " << i << " recs in sorted order \n";
  if (err) {
    cerr << " consumer: " << err << " recs failed sorted order test \n" << endl;
  }
}




void test1 (int option, int runlen) {

	// sort order for records
	OrderMaker sortorder;
	rel->get_sort_order (sortorder);

	int buffsz = 100; // pipe cache size
	Pipe input (buffsz);
	Pipe output (buffsz);

	// thread to dump data into the input pipe (for BigQ's consumption)
	pthread_t thread1;
	pthread_create (&thread1, NULL, producer, (void *)&input);

	// thread to read sorted data from output pipe (dumped by BigQ)
	pthread_t thread2;
	testutil tutil = {&output, &sortorder, false, false};
	if (option == 2) {
		tutil.print = true;
	}
	else if (option == 3) {
		tutil.write = true;
	}
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);

	BigQ bq (input, output, sortorder, runlen);

	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
}

int main (int argc, char *argv[]) {

	setup ();

	relation *rel_ptr[] = {n, r, c, p, ps, o, li};

	int tindx = 0;
	while (tindx < 1 || tindx > 3) {
		cout << " select test option: \n";
		cout << " \t 1. sort \n";
		cout << " \t 2. sort + display \n";
		cout << " \t 3. sort + write \n\t ";
		cin >> tindx;
	}

	int findx = 0;
	while (findx < 1 || findx > 7) {
		cout << "\n select dbfile to use: \n";
		cout << "\t 1. nation \n";
		cout << "\t 2. region \n";
		cout << "\t 3. customer \n";
		cout << "\t 4. part \n";
		cout << "\t 5. partsupp \n";
		cout << "\t 6. orders \n";
		cout << "\t 7. lineitem \n \t ";
		cin >> findx;
	}
	rel = rel_ptr [findx - 1];

	int runlen;
	cout << "\t\n specify runlength:\n\t ";
	cin >> runlen;
	
	test1 (tindx, runlen);

	cleanup ();
}
