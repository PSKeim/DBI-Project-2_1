
CC = g++ -O2 -Wno-deprecated -ggdb3 -Wall -Wextra -pedantic

tag = -i

ifdef linux
tag = -n
endif

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o test.o -lfl -lpthread
	
a1test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o
	$(CC) -o a1test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o -lfl
	
test.o: test.cc
	$(CC)  -c test.cc

a1-test.o: a1-test.cc
	$(CC)  -c a1-test.cc

Comparison.o: Comparison.cc
	$(CC)  -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC)  -c ComparisonEngine.cc
	
Pipe.o: Pipe.cc
	$(CC)  -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC)  -c BigQ.cc

DBFile.o: DBFile.cc
	$(CC)  -c DBFile.cc

File.o: File.cc
	$(CC)  -c File.cc

Record.o: Record.cc
	$(CC)  -c Record.cc

Schema.o: Schema.cc
	$(CC)  -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
